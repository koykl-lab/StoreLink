//! Conditional terminal metadata pruning after published compaction (protocol §10).
//!
//! Point operations only: no scanner, physical deletion, adoption, TTL, counter
//! delta, or new durable identity. Terminal state proves the earlier counted
//! transition under the library-only-write contract. Closed creators and immutable
//! resource identities prevent recreation; a fresh command cannot reuse an old ID.
//! Namespace/store configuration and lifecycle discovery remain intact.
//!
//! Each call obtains its own coherent control/candidate observation and makes at
//! most one guarded transaction attempt. Conflict and unknown outcomes both require
//! a fresh read, never rollback. Fair scheduling and physical reclamation remain
//! separate obligations; these operations alone do not prove full collection.

use std::collections::HashMap;

use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Delete, Get, TransactGetItem, TransactWriteItem,
};
use thiserror::Error;

use super::journal::{control_key, parse_receipt, receipt_key};
use super::maintenance::{Control, FIELDS as CONTROL_FIELDS};
use super::protection::{decode_pin, decode_version};
use super::references::object_key;
use super::uploads::{decode_attempt, decode_intent, decode_staged, guard, resource_key};
use super::{
    AttemptState, CommandJournal, CommandToken, IntentState, JournalError, MaintenanceError,
    Outcome, ProtectionError, UploadError, VersionState,
};
use crate::claim::ClaimState;
use crate::identity::{DomainFence, DomainId, ResourceId, Scope, VersionRef};

#[cfg(test)]
mod tests;

type Item = HashMap<String, AttributeValue>;
const RECEIPT_FIELDS: [&str; 9] = [
    "pk",
    "sk",
    "domain",
    "incarnation",
    "generation",
    "command_uuid",
    "request_digest",
    "status",
    "result",
];

/// Current metadata observation or acknowledged deletion, never a historical
/// command outcome or a capability to recreate the candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruningOutcome {
    /// The native conditional deletion was acknowledged.
    Removed,
    /// Absent in this transaction read only. Does not prove prior cleanup,
    /// historical abort, physical absence, or permission to recreate anything.
    ObservationAbsent,
}

#[derive(Debug, Error)]
pub enum PruningError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Maintenance(#[from] MaintenanceError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Protection(#[from] ProtectionError),
    #[error("candidate does not belong to this domain")]
    WrongDomain,
    #[error(
        "candidate is nonterminal or retains references/claims; take the counted terminal transition first"
    )]
    Ineligible,
    #[error("a candidate creation scope is not covered by the published compaction watermark")]
    BeforeWatermark,
    #[error("malformed or unsupported pruning observation")]
    Corrupt,
    #[error(
        "native pruning conditional conflict; reread current control and candidate, not historical rollback"
    )]
    Conflict,
    #[error(
        "native pruning outcome UNKNOWN; reread current control and candidate, never infer rollback"
    )]
    OutcomeUnknown,
}

enum Candidate<'a> {
    Receipt(&'a CommandToken),
    Pin(&'a ResourceId),
    Staged(&'a ResourceId),
    Intent(&'a ResourceId),
    Attempt(&'a ResourceId),
    Version(&'a VersionRef),
}
impl Candidate<'_> {
    fn key(&self, domain: &DomainId) -> Result<Item, PruningError> {
        let (scope, key) = match self {
            Self::Receipt(token) => (token.id().scope(), receipt_key(token.id())),
            Self::Pin(id) => (id.scope(), resource_key("USE", id)),
            Self::Staged(id) => (id.scope(), resource_key("STAGED", id)),
            Self::Intent(id) => (id.scope(), resource_key("INTENT", id)),
            Self::Attempt(id) => (id.scope(), resource_key("ATTEMPT", id)),
            Self::Version(version) => return Ok(object_key(domain, &version.fingerprint())),
        };
        if scope.domain() != domain {
            return Err(PruningError::WrongDomain);
        }
        Ok(key)
    }

    fn validate(&self, fence: &DomainFence, item: &Item) -> Result<(), PruningError> {
        match self {
            Self::Receipt(token) => {
                // Receipts may hold results larger than compact metadata's 4KiB
                // bound. Keep the native item limit; do not call check_size here.
                if item
                    .keys()
                    .any(|field| !RECEIPT_FIELDS.contains(&field.as_str()))
                {
                    return Err(PruningError::Corrupt);
                }
                match parse_receipt(item, token)? {
                    Outcome::Committed(_) | Outcome::Cancelled => {}
                    _ => return Err(PruningError::Ineligible),
                }
                compacted(fence, token.id().scope())?;
            }
            Self::Pin(id) => {
                let pin = decode_pin(id, item)?;
                terminal_claim(pin.claim().state())?;
                compacted(fence, pin.claim().id().scope())?;
                compacted(fence, pin.version_creation_id().scope())?;
            }
            Self::Staged(id) => {
                let staged = decode_staged(id, item)?;
                terminal_claim(staged.claim().state())?;
                // Address A may be old while selection-created claim B is new.
                // All provenance is checked without requiring parent/version rows.
                compacted(fence, staged.intent_id().scope())?;
                compacted(fence, staged.attempt_id().scope())?;
                compacted(fence, staged.claim().id().scope())?;
            }
            Self::Intent(id) => {
                let intent = decode_intent(id, item)?;
                if !matches!(
                    intent.state(),
                    IntentState::Consumed | IntentState::Cancelled
                ) {
                    return Err(PruningError::Ineligible);
                }
                compacted(fence, intent.id().scope())?;
            }
            Self::Attempt(id) => {
                let attempt = decode_attempt(id, item)?;
                if attempt.state() != AttemptState::Sealed {
                    return Err(PruningError::Ineligible);
                }
                compacted(fence, attempt.id().scope())?;
                compacted(fence, attempt.intent_id().scope())?;
            }
            Self::Version(version) => {
                let version = decode_version(fence.domain(), version, item)?;
                if version.state() != VersionState::Deleted
                    || version.references() != 0
                    || version.claims() != 0
                {
                    return Err(PruningError::Ineligible);
                }
                compacted(fence, version.creation_id().scope())?;
            }
        }
        Ok(())
    }
}

fn compacted(fence: &DomainFence, scope: &Scope) -> Result<(), PruningError> {
    if scope.domain() != fence.domain() {
        return Err(PruningError::WrongDomain);
    }
    // Different valid incarnations are historically fenced, not numerically
    // comparable. Restoration must already have fenced and validated metadata.
    if scope.incarnation() == fence.incarnation() && scope.generation() > fence.compacted_through()
    {
        return Err(PruningError::BeforeWatermark);
    }
    Ok(())
}
fn terminal_claim(state: ClaimState) -> Result<(), PruningError> {
    match state {
        ClaimState::Released | ClaimState::Revoked => Ok(()),
        ClaimState::Active => Err(PruningError::Ineligible),
    }
}

impl CommandJournal {
    /// Prune a matching COMMITTED/CANCELLED receipt, including full token digest
    /// validation. Until removed, the recorded result still wins in resolve().
    pub async fn prune_command_outcome(
        &self,
        token: &CommandToken,
    ) -> Result<PruningOutcome, PruningError> {
        self.prune_terminal(Candidate::Receipt(token)).await
    }
    /// Prune only RELEASED/REVOKED metadata, never an ACTIVE (even expired) pin.
    /// Does not require its source version or old application authority to exist.
    pub async fn prune_use_pin(&self, id: &ResourceId) -> Result<PruningOutcome, PruningError> {
        self.prune_terminal(Candidate::Pin(id)).await
    }
    /// Prune terminal staging by original intent address. Both intent and claim
    /// creation scopes must be compacted; no source version row is required.
    pub async fn prune_staged(&self, intent: &ResourceId) -> Result<PruningOutcome, PruningError> {
        self.prune_terminal(Candidate::Staged(intent)).await
    }
    /// Prune only CONSUMED/CANCELLED intent metadata, never OPEN/SELECTED.
    pub async fn prune_upload_intent(
        &self,
        id: &ResourceId,
    ) -> Result<PruningOutcome, PruningError> {
        self.prune_terminal(Candidate::Intent(id)).await
    }
    /// Prune only SEALED attempts. A source intent row need not remain present;
    /// late object effects remain subject to attribution and retained lifecycle.
    pub async fn prune_upload_attempt(
        &self,
        id: &ResourceId,
    ) -> Result<PruningOutcome, PruningError> {
        self.prune_terminal(Candidate::Attempt(id)).await
    }
    /// Prune only an existing DELETED tombstone with zero references and claims.
    /// This does not delete or observe physical bytes, nor adopt missing versions.
    pub async fn prune_deleted_version(
        &self,
        version: &VersionRef,
    ) -> Result<PruningOutcome, PruningError> {
        self.prune_terminal(Candidate::Version(version)).await
    }

    async fn prune_terminal(
        &self,
        candidate: Candidate<'_>,
    ) -> Result<PruningOutcome, PruningError> {
        let gets = [
            control_key(self.domain_id()),
            candidate.key(self.domain_id())?,
        ]
        .into_iter()
        .map(|key| {
            Get::builder()
                .table_name(self.coordination_arn())
                .set_key(Some(key))
                .build()
                .map(|get| TransactGetItem::builder().get(get).build())
                .map_err(|_| PruningError::Corrupt)
        })
        .collect::<Result<Vec<_>, _>>()?;
        let response = self
            .native_client()
            .transact_get_items()
            .set_transact_items(Some(gets))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| JournalError::Native(e.to_string()))?;
        let records = response.responses();
        if records.len() != 2 {
            return Err(PruningError::Corrupt);
        }
        let control = Control::parse(
            records[0].item.clone(),
            self.domain_id(),
            self.coordination_arn(),
        )?;
        let Some(item) = records[1].item.as_ref() else {
            return Ok(PruningOutcome::ObservationAbsent);
        };
        candidate.validate(control.observation.fence(), item)?;
        let actions = deletion_actions(
            self.coordination_arn(),
            records[0]
                .item
                .as_ref()
                .ok_or(JournalError::MissingControl)?,
            item,
            &candidate,
        )?;
        self.native_client()
            .transact_write_items()
            .set_transact_items(Some(actions))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| write_error(e.as_service_error()))?;
        Ok(PruningOutcome::Removed)
    }
}

// Reuse native full equality generation. Add known-schema absent fields (not
// arbitrary field guessing). Codecs reject unknown fields before constructing a
// write. As with existing guards, future authority schemas must fence old writers,
// not append hidden authority behind unchanged identity/revision/discriminators.
fn full_guard(table: &str, item: &Item, fields: &[&str]) -> Result<ConditionCheck, PruningError> {
    let mut check = guard(table, item.clone())?
        .condition_check
        .ok_or(PruningError::Corrupt)?;
    for (index, field) in fields.iter().enumerate() {
        if !item.contains_key(*field) {
            let alias = format!("#absent{index}");
            check
                .condition_expression
                .push_str(&format!(" AND attribute_not_exists({alias})"));
            check
                .expression_attribute_names
                .get_or_insert_default()
                .insert(alias, (*field).into());
        }
    }
    Ok(check)
}
fn deletion_actions(
    table: &str,
    control: &Item,
    item: &Item,
    candidate: &Candidate<'_>,
) -> Result<Vec<TransactWriteItem>, PruningError> {
    let control = full_guard(table, control, &CONTROL_FIELDS)?;
    let row = full_guard(
        table,
        item,
        if matches!(candidate, Candidate::Receipt(_)) {
            &RECEIPT_FIELDS
        } else {
            &[]
        },
    )?;
    let delete = Delete::builder()
        .table_name(row.table_name)
        .set_key(Some(row.key))
        .condition_expression(row.condition_expression)
        .set_expression_attribute_names(row.expression_attribute_names)
        .set_expression_attribute_values(row.expression_attribute_values)
        .build()
        .map_err(|_| PruningError::Corrupt)?;
    Ok(vec![
        TransactWriteItem::builder()
            .condition_check(control)
            .build(),
        TransactWriteItem::builder().delete(delete).build(),
    ])
}
fn write_error(error: Option<&TransactWriteItemsError>) -> PruningError {
    if let Some(TransactWriteItemsError::TransactionCanceledException(e)) = error
        && e.cancellation_reasons()
            .iter()
            .any(|r| r.code() == Some("ConditionalCheckFailed"))
    {
        return PruningError::Conflict;
    }
    PruningError::OutcomeUnknown
}
