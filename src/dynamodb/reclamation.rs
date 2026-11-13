//! Bounded terminal transitions for EXISTING tracked versions (protocol §7).
//!
//! No orphan adoption, inventory, receipt chain or tombstone compaction. Unknown
//! outcomes require a strong reread of the guarded postcondition, not rollback.

use std::collections::HashMap;

use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, TransactWriteItem};
use thiserror::Error;

use super::journal::{control_key, parse_control};
use super::protection::encode_version;
use super::uploads::{guard, replace};
use super::{
    ApplicationActions, CommandJournal, JournalError, ProtectionError, UploadError,
    VersionSnapshot, VersionState,
};
use crate::identity::DomainId;

type Item = HashMap<String, AttributeValue>;

#[derive(Debug, Error)]
pub enum ReclamationError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Protection(#[from] ProtectionError),
    #[error("version creation provenance differs from this domain or is in its future")]
    Binding,
    #[error("version is not in the required state or still has references/protection")]
    Ineligible,
    #[error("native terminal transition conditional conflict; strongly reread current state")]
    Conflict,
    #[error("native terminal transition outcome UNKNOWN; strongly reread, never infer rollback")]
    OutcomeUnknown,
}

impl CommandJournal {
    /// Irreversibly retire an observed READY version with zero references and
    /// claims. Full native old-row equality serializes with reference/pin writes;
    /// a missing record cannot be recreated. This is not an orphan-adoption API.
    ///
    /// Strongly reads and validates control, then freezes that entire observation
    /// in the same transaction. Historical creation provenance is not active
    /// command authority: closed epochs and different valid incarnations permit
    /// terminal cleanup. Same-incarnation future generations are rejected.
    ///
    /// Exactly one SDK transaction attempt, no new receipt/resource/operation ID.
    /// Ok means acknowledged commit. After any error, strongly read_version to
    /// observe the current postcondition; neither a conflict nor missing receipt
    /// establishes historical rollback. An unknown response may leave RETIRING.
    pub async fn retire_unprotected_version(
        &self,
        version: &VersionSnapshot,
    ) -> Result<(), ReclamationError> {
        self.transition_tracked_version(version, VersionState::Ready, VersionState::Retiring)
            .await
    }

    /// Trusted S3 adapter only, AFTER an acknowledged exact-version delete.
    /// No public arbitrary-version mark-deleted operation or Boolean receipt.
    /// Rows remain for later compaction. A conflict/unknown after S3 success is
    /// safe to reread and retry later against the SAME immutable object identity.
    #[cfg(feature = "s3")]
    pub(crate) async fn finish_deleted(
        &self,
        version: &VersionSnapshot,
    ) -> Result<(), ReclamationError> {
        self.transition_tracked_version(version, VersionState::Retiring, VersionState::Deleted)
            .await
    }

    async fn transition_tracked_version(
        &self,
        version: &VersionSnapshot,
        expected: VersionState,
        next: VersionState,
    ) -> Result<(), ReclamationError> {
        eligible(self.domain_id(), version, expected)?;
        let control = self
            .read_upload_item(control_key(self.domain_id()))
            .await?
            .ok_or(JournalError::MissingControl)?;
        let actions = transition_actions(
            self.coordination_arn(),
            self.domain_id(),
            control,
            version,
            expected,
            next,
        )?;
        self.native_client()
            .transact_write_items()
            .set_transact_items(Some(actions))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| transition_error(e.as_service_error()))?;
        Ok(())
    }
}

fn eligible(
    domain: &DomainId,
    version: &VersionSnapshot,
    expected: VersionState,
) -> Result<(), ReclamationError> {
    if version.creation_id().scope().domain() != domain {
        return Err(ReclamationError::Binding);
    }
    if version.state() != expected || version.references() != 0 || version.claims() != 0 {
        return Err(ReclamationError::Ineligible);
    }
    Ok(())
}

// Not a public prepare/edit/execute boundary. Both rows are native conditional
// actions; the canonical version encoder preserves its existing bounded schema.
fn transition_actions(
    table: &str,
    domain: &DomainId,
    control: Item,
    version: &VersionSnapshot,
    expected: VersionState,
    state: VersionState,
) -> Result<Vec<TransactWriteItem>, ReclamationError> {
    eligible(domain, version, expected)?;
    let fence = parse_control(Some(&control), domain)?;
    let creation = version.creation_id().scope();
    if creation.incarnation() == fence.incarnation() && creation.generation() > fence.current() {
        return Err(ReclamationError::Binding);
    }
    let mut next = version.clone();
    next.state = state;
    Ok(ApplicationActions::new(vec![
        guard(table, control)?,
        replace(table, encode_version(version)?, encode_version(&next)?)?,
    ])
    .map_err(JournalError::from)?
    .into_native())
}

fn transition_error(error: Option<&TransactWriteItemsError>) -> ReclamationError {
    if let Some(TransactWriteItemsError::TransactionCanceledException(e)) = error
        && e.cancellation_reasons()
            .iter()
            .any(|r| r.code() == Some("ConditionalCheckFailed"))
    {
        return ReclamationError::Conflict;
    }
    ReclamationError::OutcomeUnknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::{CancellationReason, error::TransactionCanceledException};

    #[test]
    fn errors_never_manufacture_a_historical_abort() {
        assert!(matches!(
            transition_error(None),
            ReclamationError::OutcomeUnknown
        ));
        for code in [
            "ConditionalCheckFailed",
            "TransactionConflict",
            "ValidationError",
        ] {
            let error = TransactWriteItemsError::TransactionCanceledException(
                TransactionCanceledException::builder()
                    .cancellation_reasons(CancellationReason::builder().code(code).build())
                    .build(),
            );
            assert_eq!(
                matches!(transition_error(Some(&error)), ReclamationError::Conflict),
                code == "ConditionalCheckFailed"
            );
        }
    }
}
