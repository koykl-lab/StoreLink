//! One compact permanent scheduling row per enrolled namespace, NEVER GC authority.
//! Only raw native markers are durable; qualified configuration is re-observed on
//! restart. No namespace/store binding is deleted, no lease/receipt is created.
//! Recurring bounded host execution and real-service acceptance remain separate.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use thiserror::Error;
use uuid::Uuid;

use super::journal::prefix;
use super::uploads::{check_size, number, text, uuid};
use super::{CommandJournal, UploadError};
use crate::{
    clock::TimestampMs,
    identity::{DomainId, NamespaceId, StoreId, VersionRef},
};

#[cfg(test)]
mod tests;

type Item = HashMap<String, AttributeValue>;
const RECORD: &str = "inventory-progress-v1";
const FIELDS: [&str; 12] = [
    "pk",
    "sk",
    "record",
    "domain",
    "namespace_id",
    "configuration_revision",
    "incarnation",
    "revision",
    "completed_passes",
    "last_completion_ms",
    "key_marker",
    "version_id_marker",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InventoryProgressError {
    #[error("malformed inventory progress or coherent control/namespace response")]
    Corrupt,
    #[error("unsupported inventory progress schema")]
    Unsupported,
    #[error("inventory progress exceeds the existing compact record bound")]
    RecordSize,
    #[error(
        "inventory progress/page differs from the enrolled namespace or qualified configuration"
    )]
    Binding,
    #[error("inventory page input differs from the observed progress cursor")]
    InputCursorMismatch,
    #[error("historical incarnation or configuration requires explicit scheduling reset")]
    ResetRequired,
    #[error("inventory completed-pass counter overflow")]
    CounterOverflow,
    #[error("native inventory progress observation failed; reobserve before listing")]
    NativeRead,
    #[error("inventory progress conflict; reobserve and relist, do not blindly retry")]
    Conflict,
    #[error("inventory progress write outcome UNKNOWN; reobserve and relist, never infer rollback")]
    OutcomeUnknown,
}

#[cfg_attr(not(feature = "s3"), allow(dead_code))]
struct Progress {
    namespace: NamespaceId,
    configuration_revision: Uuid,
    incarnation: Uuid,
    markers: Option<(String, String)>,
    completed_passes: u64,
    last_completion: Option<TimestampMs>,
}
fn bounded(item: &Item) -> Result<(), InventoryProgressError> {
    check_size(item).map_err(|e| match e {
        UploadError::RecordSize => InventoryProgressError::RecordSize,
        _ => InventoryProgressError::Corrupt,
    })
}
fn key(domain: &DomainId, namespace: &NamespaceId) -> Item {
    let hex: String = namespace
        .as_str()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Item::from([
        (
            "pk".into(),
            AttributeValue::S(format!("{}#INVENTORY_PROGRESS#{hex}", prefix(domain))),
        ),
        ("sk".into(), AttributeValue::S("M".into())),
    ])
}

// Structural decoding remains available in DynamoDB-only builds. Scope/prefix
// binding is checked against the coherently loaded namespace before restoration.
fn decode(journal: &CommandJournal, item: &Item) -> Result<Progress, InventoryProgressError> {
    let bad = || InventoryProgressError::Corrupt;
    if text(item, "record").map_err(|_| bad())? != RECORD {
        return Err(InventoryProgressError::Unsupported);
    }
    bounded(item)?;
    let namespace =
        NamespaceId::new(text(item, "namespace_id").map_err(|_| bad())?).map_err(|_| bad())?;
    if item.keys().any(|f| !FIELDS.contains(&f.as_str()))
        || key(journal.domain_id(), &namespace)
            .iter()
            .any(|(k, v)| item.get(k) != Some(v))
        || text(item, "domain").map_err(|_| bad())? != journal.domain_id().as_str()
    {
        return Err(bad());
    }
    let configuration_revision = uuid(item, "configuration_revision").map_err(|_| bad())?;
    let incarnation = uuid(item, "incarnation").map_err(|_| bad())?;
    uuid(item, "revision").map_err(|_| bad())?;
    let completed_passes = number(item, "completed_passes").map_err(|_| bad())?;
    let last_completion = item
        .get("last_completion_ms")
        .map(|_| {
            TimestampMs::new(number(item, "last_completion_ms").map_err(|_| bad())?)
                .map_err(|_| bad())
        })
        .transpose()?;
    if completed_passes == 0 && last_completion.is_some() {
        return Err(bad());
    }
    let markers = match (item.get("key_marker"), item.get("version_id_marker")) {
        (None, None) => None,
        (Some(AttributeValue::S(k)), Some(AttributeValue::S(v))) => {
            // A placeholder store validates only the shared native identity
            // syntax/bounds; it is not a physical-store binding or persisted ID.
            VersionRef::new(StoreId::new("inventory-marker").map_err(|_| bad())?, k, v)
                .map_err(|_| bad())?;
            Some((k.clone(), v.clone()))
        }
        _ => return Err(bad()),
    };
    Ok(Progress {
        namespace,
        configuration_revision,
        incarnation,
        markers,
        completed_passes,
        last_completion,
    })
}

pub(super) fn validate_discovery_row(
    journal: &CommandJournal,
    item: &Item,
) -> Result<(), InventoryProgressError> {
    decode(journal, item).map(|_| ())
}

#[cfg(feature = "s3")]
pub use api::InventoryProgressObservation;

#[cfg(feature = "s3")]
mod api {
    use super::super::{
        MaintenanceObservation, NamespaceSnapshot, PruningError,
        journal::control_key,
        maintenance::{Control, FIELDS as CONTROL_FIELDS},
        pruning::{full_guard, write_error},
        uploads::{decode_namespace, n, namespace_key},
    };
    use super::*;
    use crate::s3::{NamespaceConfiguration, QualifiedNamespace, VersionPage, VersionPageCursor};
    use aws_sdk_dynamodb::{
        config::{Builder as NativeConfig, retry::RetryConfig},
        types::{Get, Put, TransactGetItem, TransactWriteItem},
    };
    use std::fmt;

    /// Opaque coherent control/namespace/progress evidence. Consumed by writes,
    /// not Clone or deserializable: conflict/unknown requires a fresh load.
    pub struct InventoryProgressObservation {
        control: MaintenanceObservation,
        control_item: Item,
        namespace: NamespaceSnapshot,
        namespace_item: Item,
        configuration: NamespaceConfiguration,
        item: Option<Item>,
        progress: Option<Progress>,
        cursor: Option<VersionPageCursor>,
    }
    impl fmt::Debug for InventoryProgressObservation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("InventoryProgressObservation")
                .finish_non_exhaustive()
        }
    }
    impl InventoryProgressObservation {
        pub fn namespace(&self) -> &NamespaceSnapshot {
            &self.namespace
        }
        pub fn requires_reset(&self) -> bool {
            self.progress.as_ref().is_some_and(|p| {
                p.incarnation != self.control.fence().incarnation()
                    || p.configuration_revision != self.namespace.configuration_revision()
            })
        }
        /// Missing progress starts at None; stale progress requires explicit reset.
        pub fn cursor(&self) -> Result<Option<&VersionPageCursor>, InventoryProgressError> {
            if self.requires_reset() {
                return Err(InventoryProgressError::ResetRequired);
            }
            Ok(self.cursor.as_ref())
        }
        pub fn completed_passes(&self) -> u64 {
            self.progress.as_ref().map_or(0, |p| p.completed_passes)
        }
        /// Diagnostic ONLY; never a trusted clock sample or GC/retention authority.
        pub fn last_completion(&self) -> Option<TimestampMs> {
            self.progress.as_ref().and_then(|p| p.last_completion)
        }
    }

    fn configuration_matches(c: &NamespaceConfiguration, ns: &NamespaceSnapshot) -> bool {
        c.id() == ns.id()
            && c.store() == ns.store()
            && c.bucket() == ns.bucket()
            && c.expected_owner() == ns.expected_owner()
            && c.region() == ns.region()
            && c.key_prefix() == ns.key_prefix()
            && c.provider_profile() == ns.provider_profile()
            && c.mpu_max_age_ms() == ns.mpu_max_age_ms()
            && c.mpu_lifecycle_margin_ms() == ns.mpu_lifecycle_margin_ms()
    }
    fn encode(journal: &CommandJournal, p: &Progress) -> Result<Item, InventoryProgressError> {
        let mut item = key(journal.domain_id(), &p.namespace);
        for (name, value) in [
            ("record", RECORD.into()),
            ("domain", journal.domain_id().as_str().into()),
            ("namespace_id", p.namespace.as_str().into()),
            (
                "configuration_revision",
                p.configuration_revision.to_string(),
            ),
            ("incarnation", p.incarnation.to_string()),
            ("revision", Uuid::new_v4().to_string()),
        ] {
            item.insert(name.into(), AttributeValue::S(value));
        }
        item.insert("completed_passes".into(), n(p.completed_passes));
        if let Some(time) = p.last_completion {
            item.insert("last_completion_ms".into(), n(time.as_millis()));
        }
        if let Some((k, v)) = &p.markers {
            item.insert("key_marker".into(), AttributeValue::S(k.clone()));
            item.insert("version_id_marker".into(), AttributeValue::S(v.clone()));
        }
        // Keep both encoding and discovery on the same closed checked schema.
        decode(journal, &item)?;
        Ok(item)
    }

    impl CommandJournal {
        /// One native TransactGet: full control + namespace + progress, with no
        /// projection. Derives namespace ID from the qualified configuration;
        /// there is no raw bucket selector, separate read race or implicit reset.
        pub async fn load_inventory_progress(
            &self,
            qualified: &QualifiedNamespace,
        ) -> Result<InventoryProgressObservation, InventoryProgressError> {
            let c = qualified.configuration();
            let gets = [
                control_key(self.domain_id()),
                namespace_key(self.domain_id(), c.id()),
                key(self.domain_id(), c.id()),
            ]
            .into_iter()
            .map(|key| {
                Get::builder()
                    .table_name(self.coordination_arn())
                    .set_key(Some(key))
                    .build()
                    .map(|get| TransactGetItem::builder().get(get).build())
                    .map_err(|_| InventoryProgressError::Corrupt)
            })
            .collect::<Result<Vec<_>, _>>()?;
            let output = self
                .native_client()
                .transact_get_items()
                .set_transact_items(Some(gets))
                .customize()
                .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
                .send()
                .await
                .map_err(|_| InventoryProgressError::NativeRead)?;
            let rows = output.responses();
            if rows.len() != 3 {
                return Err(InventoryProgressError::Corrupt);
            }
            let control = Control::parse(
                rows[0].item.clone(),
                self.domain_id(),
                self.coordination_arn(),
            )
            .map_err(|_| InventoryProgressError::Corrupt)?;
            let namespace_item = rows[1]
                .item
                .clone()
                .ok_or(InventoryProgressError::Corrupt)?;
            let namespace = decode_namespace(self.domain_id(), c.id(), &namespace_item)
                .map_err(|_| InventoryProgressError::Corrupt)?;
            if !configuration_matches(c, &namespace) {
                return Err(InventoryProgressError::Binding);
            }
            let progress = rows[2]
                .item
                .as_ref()
                .map(|item| decode(self, item))
                .transpose()?;
            if progress.as_ref().is_some_and(|p| &p.namespace != c.id()) {
                return Err(InventoryProgressError::Binding);
            }
            let mut observation = InventoryProgressObservation {
                control: control.observation,
                control_item: rows[0]
                    .item
                    .clone()
                    .ok_or(InventoryProgressError::Corrupt)?,
                namespace,
                namespace_item,
                configuration: c.clone(),
                item: rows[2].item.clone(),
                progress,
                cursor: None,
            };
            // A historical prefix need not fit the current configuration. Decode
            // it structurally, but never restore/use it until explicit reset.
            if !observation.requires_reset() {
                observation.cursor = observation
                    .progress
                    .as_ref()
                    .and_then(|p| p.markers.as_ref())
                    .map(|(k, v)| {
                        VersionPageCursor::restore(qualified, k, v)
                            .map_err(|_| InventoryProgressError::Corrupt)
                    })
                    .transpose()?;
            }
            Ok(observation)
        }

        /// Checkpoint ONLY AFTER ALL bounded page items (including delete-marker
        /// observations) were considered. Report per-item failures for retry on
        /// later sweeps. A crash/deadline/partial page must leave progress unchanged;
        /// reserve a write budget. This API does not autoack or delete anything.
        /// Consumes the native page including its full configuration/input binding,
        /// even for an empty terminal page. Terminal next-marker fields do not
        /// imply continuation. None completion time clears the diagnostic.
        pub async fn checkpoint_considered_inventory_page(
            &self,
            observation: InventoryProgressObservation,
            page: VersionPage,
            completion_time: Option<TimestampMs>,
        ) -> Result<(), InventoryProgressError> {
            self.inventory_binding(&observation)?;
            let cursor = observation.cursor()?;
            if !page.matches_configuration(&observation.configuration) {
                return Err(InventoryProgressError::Binding);
            }
            if page.input_markers() != cursor.map(|c| (c.key_marker(), c.version_id_marker())) {
                return Err(InventoryProgressError::InputCursorMismatch);
            }
            let terminal = page.next_cursor().is_none();
            let next = Progress {
                namespace: observation.namespace.id().clone(),
                configuration_revision: observation.namespace.configuration_revision(),
                incarnation: observation.control.fence().incarnation(),
                markers: page
                    .next_cursor()
                    .map(|c| (c.key_marker().into(), c.version_id_marker().into())),
                completed_passes: if terminal {
                    observation
                        .completed_passes()
                        .checked_add(1)
                        .ok_or(InventoryProgressError::CounterOverflow)?
                } else {
                    observation.completed_passes()
                },
                last_completion: if terminal {
                    completion_time
                } else {
                    observation.last_completion()
                },
            };
            self.write_inventory_progress(observation, next).await
        }

        /// Explicit scheduling-only reset to the coherently observed current
        /// incarnation/configuration. Clears markers/count/time, not namespace or
        /// store binding. Never adopts versions or authorizes historical collection.
        pub async fn reset_inventory_progress(
            &self,
            observation: InventoryProgressObservation,
        ) -> Result<(), InventoryProgressError> {
            self.inventory_binding(&observation)?;
            let next = Progress {
                namespace: observation.namespace.id().clone(),
                configuration_revision: observation.namespace.configuration_revision(),
                incarnation: observation.control.fence().incarnation(),
                markers: None,
                completed_passes: 0,
                last_completion: None,
            };
            self.write_inventory_progress(observation, next).await
        }

        fn inventory_binding(
            &self,
            observation: &InventoryProgressObservation,
        ) -> Result<(), InventoryProgressError> {
            if observation.control.coordination_arn() != self.coordination_arn()
                || observation.control.fence().domain() != self.domain_id()
            {
                return Err(InventoryProgressError::Binding);
            }
            Ok(())
        }
        async fn write_inventory_progress(
            &self,
            observation: InventoryProgressObservation,
            next: Progress,
        ) -> Result<(), InventoryProgressError> {
            let bad = |_| InventoryProgressError::Corrupt;
            let control = full_guard(
                self.coordination_arn(),
                &observation.control_item,
                &CONTROL_FIELDS,
            )
            .map_err(bad)?;
            // Namespace's checked closed schema has no optional fields. Guard
            // every original native field, including physical tuple/profile/MPU.
            let namespace = full_guard(self.coordination_arn(), &observation.namespace_item, &[])
                .map_err(bad)?;
            let mut put = Put::builder()
                .table_name(self.coordination_arn())
                .set_item(Some(encode(self, &next)?));
            if let Some(old) = &observation.item {
                let guard = full_guard(self.coordination_arn(), old, &FIELDS).map_err(bad)?;
                put = put
                    .condition_expression(guard.condition_expression)
                    .set_expression_attribute_names(guard.expression_attribute_names)
                    .set_expression_attribute_values(guard.expression_attribute_values);
            } else {
                put = put
                    .condition_expression("attribute_not_exists(#pk)")
                    .expression_attribute_names("#pk", "pk");
            }
            let put = put.build().map_err(|_| InventoryProgressError::Corrupt)?;
            self.native_client()
                .transact_write_items()
                .set_transact_items(Some(vec![
                    TransactWriteItem::builder()
                        .condition_check(control)
                        .build(),
                    TransactWriteItem::builder()
                        .condition_check(namespace)
                        .build(),
                    TransactWriteItem::builder().put(put).build(),
                ]))
                .customize()
                .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
                .send()
                .await
                .map_err(|e| match write_error(e.as_service_error()) {
                    PruningError::Conflict => InventoryProgressError::Conflict,
                    _ => InventoryProgressError::OutcomeUnknown,
                })?;
            Ok(())
        }
    }
}
