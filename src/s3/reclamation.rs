//! Bounded exact deletion of tracked RETIRING content versions, not full GC.
//!
//! No inventory/adoption, latest fallback, HEAD-as-absence, bucket delete or MPU
//! abort. Host-supplied native client retries/timeouts apply to identical exact
//! deletes. No background worker/timer is created. Maintenance fencing and
//! invariant-validated restoration with a fresh incarnation remain operator
//! obligations; metadata control changes do NOT cancel already issued S3 I/O.

use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use thiserror::Error;

use super::QualifiedNamespace;
use crate::{
    dynamodb::{
        CommandJournal, NamespaceSnapshot, ProtectionError, ReclamationError, UploadError,
        VersionSnapshot, VersionState,
    },
    identity::VersionRef,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeletionOutcome {
    /// This call received an exact-delete acknowledgement and metadata commit.
    Deleted,
    /// Current metadata postcondition only, NOT this call's historical execution.
    AlreadyDeleted,
}

#[derive(Debug, Error)]
pub enum DeletionError {
    #[error(transparent)]
    Protection(#[from] ProtectionError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error("tracked version is missing; no adoption or physical absence inferred")]
    MissingVersion,
    #[error("owning namespace is missing; no deletion authorized")]
    MissingNamespace,
    #[error("qualified client/configuration differs from the version's owning namespace")]
    Binding,
    #[error("version is not terminal or still has references/protection")]
    Ineligible,
    #[error("exact S3 deletion outcome UNKNOWN; retry the same identity, never infer absence")]
    OutcomeUnknown,
    #[error(
        "exact S3 delete acknowledged, but metadata completion failed; strongly reread current state"
    )]
    MetadataAfterDelete(#[source] ReclamationError),
}

/// Strongly observe the tracked version and its OWN namespace/configuration.
/// Only RETIRING permits S3 deletion; READY/missing never do. Version identity
/// never reopens and immutable StoreId enrollment prevents physical rebinding.
/// A stale observation can only delete this same recorded key/version/owner.
/// Historical creator and namespace incarnations need not match after an
/// authorized restore; historical provenance is not active command authority.
///
/// Any service/transport failure (including AccessDenied or NoSuchBucket) is
/// unknown, never confirmed absence. Native acknowledgement permits a full-row
/// conditional DELETED replacement; errors there do not roll back S3. A later
/// call rereads and retries the identical exact deletion harmlessly. Terminal
/// records remain for future compaction, which this API does not implement.
pub async fn delete_retiring_version(
    journal: &CommandJournal,
    qualified: &QualifiedNamespace,
    version: &VersionRef,
) -> Result<DeletionOutcome, DeletionError> {
    let observed = journal
        .read_version(version)
        .await?
        .ok_or(DeletionError::MissingVersion)?;
    if observed.state() == VersionState::Ready
        || observed.references() != 0
        || observed.claims() != 0
    {
        return Err(DeletionError::Ineligible);
    }
    let namespace = journal
        .read_namespace(observed.namespace_id())
        .await?
        .ok_or(DeletionError::MissingNamespace)?;
    check_binding(journal, qualified, &observed, &namespace)?;
    if observed.state() == VersionState::Deleted {
        return Ok(DeletionOutcome::AlreadyDeleted);
    }
    let output = qualified
        .native_client()
        .delete_object()
        .bucket(namespace.bucket())
        .expected_bucket_owner(namespace.expected_owner())
        .key(observed.version().key())
        .version_id(observed.version().version())
        .send()
        .await
        .map_err(|_| DeletionError::OutcomeUnknown)?;
    check_ack(&output, observed.version())?;
    journal
        .finish_deleted(&observed)
        .await
        .map_err(DeletionError::MetadataAfterDelete)?;
    Ok(DeletionOutcome::Deleted)
}

fn check_binding(
    journal: &CommandJournal,
    qualified: &QualifiedNamespace,
    version: &VersionSnapshot,
    ns: &NamespaceSnapshot,
) -> Result<(), DeletionError> {
    let c = qualified.configuration();
    if ns.domain() != journal.domain_id()
        || version.creation_id().scope().domain() != journal.domain_id()
        || version.namespace_id() != ns.id()
        || version.configuration_revision() != ns.configuration_revision()
        || version.version().store() != ns.store()
        || !version.version().key().starts_with(ns.key_prefix())
        || c.id() != ns.id()
        || c.store() != ns.store()
        || c.bucket() != ns.bucket()
        || c.expected_owner() != ns.expected_owner()
        || c.region() != ns.region()
        || c.key_prefix() != ns.key_prefix()
        || c.mpu_max_age_ms() != ns.mpu_max_age_ms()
        || c.mpu_lifecycle_margin_ms() != ns.mpu_lifecycle_margin_ms()
        || c.provider_profile() != ns.provider_profile()
        || qualified
            .native_client()
            .config()
            .region()
            .map(|r| r.as_ref())
            != Some(ns.region())
    {
        return Err(DeletionError::Binding);
    }
    Ok(())
}

fn check_ack(output: &DeleteObjectOutput, version: &VersionRef) -> Result<(), DeletionError> {
    // aws-sdk-s3 1.157.0 DeleteObjectOutput / AWS API_DeleteObject: for an
    // explicit version delete, DeleteMarker=true means the removed version WAS
    // a marker. Selection tracks content, so that is inconsistent. Absent/false
    // is allowed. This request always supplies a non-null VersionId and therefore
    // cannot create a bare-key delete marker. If returned, VersionId must match.
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
    if output.delete_marker() == Some(true)
        || output
            .version_id()
            .is_some_and(|id| id != version.version())
    {
        return Err(DeletionError::OutcomeUnknown);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::StoreId;

    #[test]
    fn content_delete_ack_allows_absent_headers_not_markers_or_other_versions() {
        let version = VersionRef::new(StoreId::new("store").unwrap(), "key", "exact").unwrap();
        for marker in [None, Some(false), Some(true)] {
            for id in [None, Some("exact"), Some("other"), Some("null")] {
                let output = DeleteObjectOutput::builder()
                    .set_delete_marker(marker)
                    .set_version_id(id.map(str::to_owned))
                    .build();
                assert_eq!(
                    check_ack(&output, &version).is_ok(),
                    marker != Some(true) && (id.is_none() || id == Some("exact"))
                );
            }
        }
    }
}
