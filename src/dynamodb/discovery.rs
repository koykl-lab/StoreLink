//! Bounded native metadata traversal. Hints are locators, never eligibility or
//! mutation authority: consumers must use fresh coherent point operations.
//!
//! `MetadataDiscoveryPage::input_cursor` and `next_cursor` expose read-only
//! checkpoint inputs for the progress layer. An empty filtered page with a
//! next cursor is not completion. Scan is not an atomic snapshot or sorted walk;
//! recurring complete passes and capacity for *evaluated* rows remain required.

use std::{collections::HashMap, fmt};

use aws_sdk_dynamodb::{operation::scan::ScanOutput, types::AttributeValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::journal::{control_key, parse_receipt, prefix};
use super::maintenance::Control;
use super::protection::{decode_pin, decode_version};
use super::references::{ReferenceId, decode_reference};
use super::uploads::{
    decode_attempt, decode_intent, decode_namespace, decode_staged, decode_store_binding, number,
    text, uuid,
};
use super::{CommandJournal, CommandToken};
use crate::identity::{CommandId, DomainId, NamespaceId, ResourceId, Scope, StoreId, VersionRef};

#[cfg(test)]
mod tests;

type Item = HashMap<String, AttributeValue>;

/// Opaque native continuation, bound to the exact enrolled table ARN and domain.
/// Deserialization checks structure and byte lengths; use checks the binding.
/// A native continuation may refer to another domain or a non-`M` sort key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CursorWire")]
pub struct MetadataCursor {
    table: String,
    domain: DomainId,
    pk: String,
    sk: String,
}

impl fmt::Debug for MetadataCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataCursor").finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorWire {
    table: String,
    domain: DomainId,
    pk: String,
    sk: String,
}

impl TryFrom<CursorWire> for MetadataCursor {
    type Error = MetadataDiscoveryError;

    fn try_from(wire: CursorWire) -> Result<Self, Self::Error> {
        let parts: Vec<_> = wire.table.splitn(6, ':').collect();
        if wire.table.len() > 2048
            || parts.len() != 6
            || parts[0] != "arn"
            || parts[1].is_empty()
            || parts[2] != "dynamodb"
            || parts[3].is_empty()
            || parts[4].is_empty()
            || !parts[5].starts_with("table/")
            || parts[5].len() == 6
            || parts[5][6..].contains('/')
            || !key_length(&wire.pk, 2048)
            || !key_length(&wire.sk, 1024)
        {
            return Err(MetadataDiscoveryError::InvalidCursor);
        }
        Ok(Self {
            table: wire.table,
            domain: wire.domain,
            pk: wire.pk,
            sk: wire.sk,
        })
    }
}

fn key_length(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max
}

impl MetadataCursor {
    pub(super) fn from_key(
        journal: &CommandJournal,
        key: Item,
    ) -> Result<Self, MetadataDiscoveryError> {
        if key.len() != 2 {
            return Err(MetadataDiscoveryError::InvalidCursor);
        }
        Self::try_from(CursorWire {
            table: journal.coordination_arn().into(),
            domain: journal.domain_id().clone(),
            pk: text(&key, "pk")
                .map_err(|_| MetadataDiscoveryError::InvalidCursor)?
                .into(),
            sk: text(&key, "sk")
                .map_err(|_| MetadataDiscoveryError::InvalidCursor)?
                .into(),
        })
    }

    pub(super) fn key(&self, journal: &CommandJournal) -> Result<Item, MetadataDiscoveryError> {
        if self.table != journal.coordination_arn() || &self.domain != journal.domain_id() {
            return Err(MetadataDiscoveryError::CursorBinding);
        }
        Ok(HashMap::from([
            ("pk".into(), AttributeValue::S(self.pk.clone())),
            ("sk".into(), AttributeValue::S(self.sk.clone())),
        ]))
    }
}

/// Checked locators only. No states, deadlines, receipt results or claims are
/// exposed. In particular `Staged` addresses the original intent, not its claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundedMaintenanceHint {
    Receipt(CommandToken),
    Intent(ResourceId),
    Attempt(ResourceId),
    Staged(ResourceId),
    UsePin(ResourceId),
    Version(VersionRef),
    Namespace(NamespaceId),
}

/// One returned row's observation. Issues carry neither raw rows nor payloads.
/// They do not hide other items or invalidate the page's native continuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataDiscoveryItem {
    Hint(BoundedMaintenanceHint),
    /// Checked control, progress, reference edge or store binding; not a cleanup hint.
    Permanent,
    /// Unknown/unhandled family or future permanent-record discriminator.
    Unsupported,
    /// Known family failed its checked identity/record decoder.
    Corrupt,
}

/// One native Scan page, not proof of emptiness or a complete sweep.
pub struct MetadataDiscoveryPage {
    table: String,
    domain: DomainId,
    input_cursor: Option<MetadataCursor>,
    next_cursor: Option<MetadataCursor>,
    items: Vec<MetadataDiscoveryItem>,
    scanned_count: u16,
}

impl fmt::Debug for MetadataDiscoveryPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataDiscoveryPage")
            .field("item_count", &self.items.len())
            .field("scanned_count", &self.scanned_count)
            .field("has_continuation", &self.next_cursor.is_some())
            .finish_non_exhaustive()
    }
}

impl MetadataDiscoveryPage {
    pub(super) fn binding(&self) -> (&str, &DomainId) {
        (&self.table, &self.domain)
    }

    pub fn input_cursor(&self) -> Option<&MetadataCursor> {
        self.input_cursor.as_ref()
    }
    pub fn next_cursor(&self) -> Option<&MetadataCursor> {
        self.next_cursor.as_ref()
    }
    pub fn items(&self) -> &[MetadataDiscoveryItem] {
        &self.items
    }
    /// Evaluated rows before filtering, including other domains. Budget capacity
    /// against this value, not just the number of hints returned.
    pub fn scanned_count(&self) -> u16 {
        self.scanned_count
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetadataDiscoveryError {
    #[error("metadata evaluated-item limit must be 1..=128")]
    InvalidLimit,
    #[error("metadata cursor is malformed")]
    InvalidCursor,
    #[error("metadata cursor belongs to another table or domain")]
    CursorBinding,
    #[error("native metadata page structure is inconsistent")]
    InvalidPage,
    // Native error messages can include response bodies. Deliberately do not
    // propagate them into this payload-free public diagnostic surface.
    #[error("native metadata Scan failed; no traversal progress was produced")]
    Native,
}

impl CommandJournal {
    /// Issue one strongly consistent native Scan page on the canonical enrolled
    /// coordination ARN. The limit bounds evaluated items, not filtered results.
    /// This performs no writes, command admission, time checks or point reaping.
    pub async fn discover_metadata_page(
        &self,
        cursor: Option<&MetadataCursor>,
        evaluated_limit: u16,
    ) -> Result<MetadataDiscoveryPage, MetadataDiscoveryError> {
        if !(1..=128).contains(&evaluated_limit) {
            return Err(MetadataDiscoveryError::InvalidLimit);
        }
        let start = cursor.map(|cursor| cursor.key(self)).transpose()?;
        let output = self
            .native_client()
            .scan()
            .table_name(self.coordination_arn())
            .consistent_read(true)
            .limit(i32::from(evaluated_limit))
            .filter_expression("begins_with(#pk, :domain)")
            .expression_attribute_names("#pk", "pk")
            .expression_attribute_values(":domain", AttributeValue::S(prefix(self.domain_id())))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|_| MetadataDiscoveryError::Native)?;
        self.metadata_page(cursor, evaluated_limit, output)
    }

    fn metadata_page(
        &self,
        cursor: Option<&MetadataCursor>,
        limit: u16,
        output: ScanOutput,
    ) -> Result<MetadataDiscoveryPage, MetadataDiscoveryError> {
        if output.count < 0
            || output.count as usize != output.items().len()
            || output.scanned_count < output.count
            || output.scanned_count > i32::from(limit)
        {
            return Err(MetadataDiscoveryError::InvalidPage);
        }
        let next_cursor = output
            .last_evaluated_key
            .filter(|key| !key.is_empty())
            .map(|key| MetadataCursor::from_key(self, key))
            .transpose()?;
        Ok(MetadataDiscoveryPage {
            table: self.coordination_arn().into(),
            domain: self.domain_id().clone(),
            input_cursor: cursor.cloned(),
            next_cursor,
            items: output
                .items
                .unwrap_or_default()
                .iter()
                .map(|row| classify(self, row))
                .collect(),
            scanned_count: output.scanned_count as u16,
        })
    }
}

fn classify(journal: &CommandJournal, row: &Item) -> MetadataDiscoveryItem {
    classify_checked(journal, row).unwrap_or(MetadataDiscoveryItem::Corrupt)
}

// Errors from decoders never leave this private path: even malformed fields can
// contain sensitive payloads. Routing by family is not identity validation.
fn classify_checked(journal: &CommandJournal, row: &Item) -> Option<MetadataDiscoveryItem> {
    use BoundedMaintenanceHint as Hint;
    let domain = journal.domain_id();
    let pk = text(row, "pk").ok()?;
    let family = pk.strip_prefix(&format!("{}#", prefix(domain)))?;
    if text(row, "sk").ok()? != "M" || !key_length(pk, 2048) {
        return None;
    }
    if row.get("pk") == control_key(domain).get("pk") {
        Control::parse(Some(row.clone()), domain, journal.coordination_arn()).ok()?;
        return Some(MetadataDiscoveryItem::Permanent);
    }
    let hint = match family.split('#').next()? {
        "INVENTORY_PROGRESS" => {
            return Some(
                match super::inventory_progress::validate_discovery_row(journal, row) {
                    Ok(()) => MetadataDiscoveryItem::Permanent,
                    Err(super::inventory_progress::InventoryProgressError::Unsupported) => {
                        MetadataDiscoveryItem::Unsupported
                    }
                    Err(_) => MetadataDiscoveryItem::Corrupt,
                },
            );
        }
        "METADATA_PROGRESS" => {
            return Some(
                match super::progress::validate_discovery_row(journal, row) {
                    Ok(()) => MetadataDiscoveryItem::Permanent,
                    Err(super::progress::MetadataProgressError::Unsupported) => {
                        MetadataDiscoveryItem::Unsupported
                    }
                    Err(_) => MetadataDiscoveryItem::Corrupt,
                },
            );
        }
        "CMD" => {
            let scope = scope(row, "generation")?;
            if scope.domain() != domain {
                return None;
            }
            let id = CommandId::new(scope, uuid(row, "command_uuid").ok()?).ok()?;
            let AttributeValue::B(digest) = row.get("request_digest")? else {
                return None;
            };
            let digest: [u8; 32] = digest.as_ref().try_into().ok()?;
            let token: CommandToken =
                serde_json::from_value(serde_json::json!({"id": id, "digest": digest})).ok()?;
            if row.keys().any(|name| {
                ![
                    "pk",
                    "sk",
                    "domain",
                    "incarnation",
                    "generation",
                    "command_uuid",
                    "request_digest",
                    "status",
                    "result",
                ]
                .contains(&name.as_str())
            }) {
                return None;
            }
            // Reuse receipt identity, canonical key, digest and result checks;
            // discard the historical outcome immediately, never export/log it.
            parse_receipt(row, &token).ok()?;
            Hint::Receipt(token)
        }
        "INTENT" | "ATTEMPT" | "USE" => {
            let id = ResourceId::new(scope(row, "generation")?, uuid(row, "resource_uuid").ok()?)
                .ok()?;
            if id.scope().domain() != domain {
                return None;
            }
            match family.split('#').next()? {
                "INTENT" => {
                    decode_intent(&id, row).ok()?;
                    Hint::Intent(id)
                }
                "ATTEMPT" => {
                    decode_attempt(&id, row).ok()?;
                    Hint::Attempt(id)
                }
                _ => {
                    decode_pin(&id, row).ok()?;
                    Hint::UsePin(id)
                }
            }
        }
        "STAGED" => {
            if text(row, "record").ok()? != "staged-verified-v1" {
                return Some(MetadataDiscoveryItem::Unsupported);
            }
            let id = ResourceId::new(
                scope(row, "intent_generation")?,
                uuid(row, "intent_uuid").ok()?,
            )
            .ok()?;
            if id.scope().domain() != domain {
                return None;
            }
            decode_staged(&id, row).ok()?;
            Hint::Staged(id)
        }
        "OBJ" => {
            let version = VersionRef::new(
                StoreId::new(text(row, "store").ok()?).ok()?,
                text(row, "object_key").ok()?,
                text(row, "version_id").ok()?,
            )
            .ok()?;
            decode_version(domain, &version, row).ok()?;
            Hint::Version(version)
        }
        "NS" => {
            let id = NamespaceId::new(text(row, "namespace_id").ok()?).ok()?;
            decode_namespace(domain, &id, row).ok()?;
            Hint::Namespace(id)
        }
        "REF" => {
            if text(row, "record").ok()? != "reference" {
                return Some(MetadataDiscoveryItem::Unsupported);
            }
            let id = ReferenceId::new(text(row, "reference_id").ok()?).ok()?;
            decode_reference(domain, &id, row).ok()?;
            return Some(MetadataDiscoveryItem::Permanent);
        }
        "STORE" => {
            if text(row, "record").ok()? != "store-binding-v1" {
                return Some(MetadataDiscoveryItem::Unsupported);
            }
            decode_store_binding(domain, row).ok()?;
            return Some(MetadataDiscoveryItem::Permanent);
        }
        _ => return Some(MetadataDiscoveryItem::Unsupported),
    };
    Some(MetadataDiscoveryItem::Hint(hint))
}

fn scope(row: &Item, generation: &str) -> Option<Scope> {
    Scope::new(
        DomainId::new(text(row, "domain").ok()?).ok()?,
        uuid(row, "incarnation").ok()?,
        number(row, generation).ok()?,
    )
    .ok()
}
