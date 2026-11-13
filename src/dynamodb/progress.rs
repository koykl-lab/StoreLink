//! Bounded metadata traversal scheduling, NEVER deletion/GC authority.
//!
//! Exactly one compact row per domain, independent of generations and work volume.
//! The host must run recurring passes without traffic and reserve a progress-write
//! budget after bounded consideration of EVERY item (including explicit reporting
//! of per-source failures). A deadline/crash before complete consideration must
//! leave the checkpoint unchanged. Unknown writes can repeat pages: point APIs
//! must always reobserve their own native guards. This is not a scheduler or a
//! proof of a clean/empty namespace, and diagnostic time grants no authority.

use std::{collections::HashMap, fmt};

use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::types::{AttributeValue, Get, Put, TransactGetItem, TransactWriteItem};
use thiserror::Error;
use uuid::Uuid;

use super::journal::{control_key, prefix};
use super::maintenance::{Control, FIELDS as CONTROL_FIELDS};
use super::pruning::{full_guard, write_error};
use super::uploads::{check_size, n, number, text, uuid};
use super::{
    CommandJournal, MaintenanceObservation, MetadataCursor, MetadataDiscoveryPage, PruningError,
    UploadError,
};
use crate::{clock::TimestampMs, identity::DomainId};

#[cfg(test)]
mod tests;

type Item = HashMap<String, AttributeValue>;
const RECORD: &str = "metadata-progress-v1";
const FIELDS: [&str; 10] = [
    "pk",
    "sk",
    "record",
    "domain",
    "incarnation",
    "revision",
    "completed_passes",
    "last_completion_ms",
    "cursor_pk",
    "cursor_sk",
];

/// Opaque coherent control/progress evidence, consumed by checkpoint or reset.
/// Not Clone or deserializable: conflicts/unknown writes require a fresh load and
/// rescan, not blind replay. Historical cursors cannot be extracted for use.
pub struct MetadataProgressObservation {
    control: MaintenanceObservation,
    control_item: Item,
    item: Option<Item>,
    progress: Option<Progress>,
}
impl fmt::Debug for MetadataProgressObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataProgressObservation")
            .finish_non_exhaustive()
    }
}
impl MetadataProgressObservation {
    pub fn requires_reset(&self) -> bool {
        self.progress
            .as_ref()
            .is_some_and(|p| p.incarnation != self.control.fence().incarnation())
    }

    /// Missing progress starts at None. A historical incarnation requires explicit
    /// reset; it is never silently treated as a usable or start-of-pass cursor.
    pub fn cursor(&self) -> Result<Option<&MetadataCursor>, MetadataProgressError> {
        if self.requires_reset() {
            return Err(MetadataProgressError::ResetRequired);
        }
        Ok(self.progress.as_ref().and_then(|p| p.cursor.as_ref()))
    }

    /// Scheduling metric, not proof of clean data. Historical metrics remain
    /// visible while requires_reset() is true; reset clears them.
    pub fn completed_passes(&self) -> u64 {
        self.progress.as_ref().map_or(0, |p| p.completed_passes)
    }
    /// Diagnostic ONLY, not a trusted clock sample or GC/retention authority.
    pub fn last_completion(&self) -> Option<TimestampMs> {
        self.progress.as_ref().and_then(|p| p.last_completion)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetadataProgressError {
    #[error("malformed metadata progress or coherent control response")]
    Corrupt,
    #[error("unsupported metadata progress schema")]
    Unsupported,
    #[error("metadata progress exceeds the existing compact record bound")]
    RecordSize,
    #[error("metadata progress/page belongs to another table or domain")]
    Binding,
    #[error("metadata page input differs from the observed progress cursor")]
    InputCursorMismatch,
    #[error("historical progress incarnation requires explicit scheduling reset")]
    ResetRequired,
    #[error("metadata completed-pass counter overflow")]
    CounterOverflow,
    #[error("native progress observation failed; reobserve before scanning")]
    NativeRead,
    #[error("metadata progress conflict; reobserve and rescan, do not blindly retry")]
    Conflict,
    #[error("metadata progress write outcome UNKNOWN; reobserve and rescan, never infer rollback")]
    OutcomeUnknown,
}

struct Progress {
    incarnation: Uuid,
    cursor: Option<MetadataCursor>,
    completed_passes: u64,
    last_completion: Option<TimestampMs>,
}
fn bounded(item: &Item) -> Result<(), MetadataProgressError> {
    check_size(item).map_err(|e| match e {
        UploadError::RecordSize => MetadataProgressError::RecordSize,
        _ => MetadataProgressError::Corrupt,
    })
}
fn key(domain: &DomainId) -> Item {
    Item::from([
        (
            "pk".into(),
            AttributeValue::S(format!("{}#METADATA_PROGRESS", prefix(domain))),
        ),
        ("sk".into(), AttributeValue::S("M".into())),
    ])
}
fn decode(journal: &CommandJournal, item: &Item) -> Result<Progress, MetadataProgressError> {
    let corrupt = || MetadataProgressError::Corrupt;
    let record = text(item, "record").map_err(|_| corrupt())?;
    if record != RECORD {
        return Err(MetadataProgressError::Unsupported);
    }
    bounded(item)?;
    if item.keys().any(|f| !FIELDS.contains(&f.as_str()))
        || key(journal.domain_id())
            .iter()
            .any(|(k, v)| item.get(k) != Some(v))
        || text(item, "domain").map_err(|_| corrupt())? != journal.domain_id().as_str()
    {
        return Err(corrupt());
    }
    let incarnation = uuid(item, "incarnation").map_err(|_| corrupt())?;
    uuid(item, "revision").map_err(|_| corrupt())?;
    let completed_passes = number(item, "completed_passes").map_err(|_| corrupt())?;
    let last_completion = item
        .get("last_completion_ms")
        .map(|_| {
            TimestampMs::new(number(item, "last_completion_ms").map_err(|_| corrupt())?)
                .map_err(|_| corrupt())
        })
        .transpose()?;
    if completed_passes == 0 && last_completion.is_some() {
        return Err(corrupt());
    }
    let cursor = match (item.get("cursor_pk"), item.get("cursor_sk")) {
        (None, None) => None,
        (Some(pk), Some(sk)) => Some(
            MetadataCursor::from_key(
                journal,
                Item::from([("pk".into(), pk.clone()), ("sk".into(), sk.clone())]),
            )
            .map_err(|_| corrupt())?,
        ),
        _ => return Err(corrupt()),
    };
    Ok(Progress {
        incarnation,
        cursor,
        completed_passes,
        last_completion,
    })
}

/// Discovery shares the same closed decoder; no new permanent-row false alarms.
pub(super) fn validate_discovery_row(
    journal: &CommandJournal,
    item: &Item,
) -> Result<(), MetadataProgressError> {
    decode(journal, item).map(|_| ())
}

fn encode(journal: &CommandJournal, progress: &Progress) -> Result<Item, MetadataProgressError> {
    let mut item = key(journal.domain_id());
    for (name, value) in [
        ("record", RECORD.into()),
        ("domain", journal.domain_id().as_str().into()),
        ("incarnation", progress.incarnation.to_string()),
        ("revision", Uuid::new_v4().to_string()),
    ] {
        item.insert(name.into(), AttributeValue::S(value));
    }
    item.insert("completed_passes".into(), n(progress.completed_passes));
    if let Some(time) = progress.last_completion {
        item.insert("last_completion_ms".into(), n(time.as_millis()));
    }
    if let Some(cursor) = &progress.cursor {
        let native = cursor
            .key(journal)
            .map_err(|_| MetadataProgressError::Binding)?;
        item.insert("cursor_pk".into(), native["pk"].clone());
        item.insert("cursor_sk".into(), native["sk"].clone());
    }
    bounded(&item)?;
    Ok(item)
}

impl CommandJournal {
    /// One native TransactGet of full control plus the single domain progress row.
    /// No projection, command admission, clock read, mutation or implicit adoption.
    pub async fn load_metadata_progress(
        &self,
    ) -> Result<MetadataProgressObservation, MetadataProgressError> {
        let gets = [control_key(self.domain_id()), key(self.domain_id())]
            .into_iter()
            .map(|key| {
                Get::builder()
                    .table_name(self.coordination_arn())
                    .set_key(Some(key))
                    .build()
                    .map(|get| TransactGetItem::builder().get(get).build())
                    .map_err(|_| MetadataProgressError::Corrupt)
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
            .map_err(|_| MetadataProgressError::NativeRead)?;
        let rows = output.responses();
        if rows.len() != 2 {
            return Err(MetadataProgressError::Corrupt);
        }
        let control = Control::parse(
            rows[0].item.clone(),
            self.domain_id(),
            self.coordination_arn(),
        )
        .map_err(|_| MetadataProgressError::Corrupt)?;
        let progress = rows[1]
            .item
            .as_ref()
            .map(|item| decode(self, item))
            .transpose()?;
        Ok(MetadataProgressObservation {
            control: control.observation,
            control_item: rows[0].item.clone().ok_or(MetadataProgressError::Corrupt)?,
            item: rows[1].item.clone(),
            progress,
        })
    }

    /// Checkpoint only AFTER every returned item has been considered boundedly.
    /// Per-source failures count as considered only with reported errors, to be
    /// retried on later sweeps. Reserve write budget; incomplete consideration
    /// must leave progress unchanged. Hints/metrics never authorize deletion.
    ///
    /// Consumes observation AND the opaque native page. Empty filtered pages
    /// with continuations still advance, never increment completed passes. A
    /// terminal page returns to start and increments the bounded counter; None
    /// time clears the diagnostic rather than mislabeling an older completion.
    /// Exactly one native write attempt. Conflict/unknown requires reobserve and
    /// rescan; crashes or ambiguous outcomes may repeat work, not prove rollback.
    pub async fn checkpoint_considered_metadata_page(
        &self,
        observation: MetadataProgressObservation,
        page: MetadataDiscoveryPage,
        completion_time: Option<TimestampMs>,
    ) -> Result<(), MetadataProgressError> {
        self.progress_binding(&observation)?;
        if page.binding() != (self.coordination_arn(), self.domain_id()) {
            return Err(MetadataProgressError::Binding);
        }
        if observation.cursor()? != page.input_cursor() {
            return Err(MetadataProgressError::InputCursorMismatch);
        }
        let terminal = page.next_cursor().is_none();
        let next = Progress {
            incarnation: observation.control.fence().incarnation(),
            cursor: page.next_cursor().cloned(),
            completed_passes: if terminal {
                observation
                    .completed_passes()
                    .checked_add(1)
                    .ok_or(MetadataProgressError::CounterOverflow)?
            } else {
                observation.completed_passes()
            },
            last_completion: if terminal {
                completion_time
            } else {
                observation.last_completion()
            },
        };
        self.write_metadata_progress(observation, next).await
    }

    /// Explicit scheduling-only reset: start cursor, zero passes, no diagnostic
    /// completion, current observed incarnation. Never changes data/control or
    /// authorizes historical collection. Also supports explicit same-incarnation
    /// restart. Full observed control AND old progress CAS guard the reset.
    pub async fn reset_metadata_progress(
        &self,
        observation: MetadataProgressObservation,
    ) -> Result<(), MetadataProgressError> {
        self.progress_binding(&observation)?;
        let next = Progress {
            incarnation: observation.control.fence().incarnation(),
            cursor: None,
            completed_passes: 0,
            last_completion: None,
        };
        self.write_metadata_progress(observation, next).await
    }

    fn progress_binding(
        &self,
        observation: &MetadataProgressObservation,
    ) -> Result<(), MetadataProgressError> {
        if observation.control.coordination_arn() != self.coordination_arn()
            || observation.control.fence().domain() != self.domain_id()
        {
            return Err(MetadataProgressError::Binding);
        }
        Ok(())
    }

    async fn write_metadata_progress(
        &self,
        observation: MetadataProgressObservation,
        next: Progress,
    ) -> Result<(), MetadataProgressError> {
        let item = encode(self, &next)?;
        let control = full_guard(
            self.coordination_arn(),
            &observation.control_item,
            &CONTROL_FIELDS,
        )
        .map_err(|_| MetadataProgressError::Corrupt)?;
        let mut put = Put::builder()
            .table_name(self.coordination_arn())
            .set_item(Some(item));
        if let Some(old) = &observation.item {
            // Equality of the entire supported old schema, including absent
            // optional fields and ABA-safe UUID revision. Every mutation changes
            // revision; future schemas must fence old writers, not append behind it.
            let guard = full_guard(self.coordination_arn(), old, &FIELDS)
                .map_err(|_| MetadataProgressError::Corrupt)?;
            put = put
                .condition_expression(guard.condition_expression)
                .set_expression_attribute_names(guard.expression_attribute_names)
                .set_expression_attribute_values(guard.expression_attribute_values);
        } else {
            put = put
                .condition_expression("attribute_not_exists(#pk)")
                .expression_attribute_names("#pk", "pk");
        }
        let put = put.build().map_err(|_| MetadataProgressError::Corrupt)?;
        self.native_client()
            .transact_write_items()
            .set_transact_items(Some(vec![
                TransactWriteItem::builder()
                    .condition_check(control)
                    .build(),
                TransactWriteItem::builder().put(put).build(),
            ]))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| match write_error(e.as_service_error()) {
                PruningError::Conflict => MetadataProgressError::Conflict,
                _ => MetadataProgressError::OutcomeUnknown,
            })?;
        Ok(())
    }
}
