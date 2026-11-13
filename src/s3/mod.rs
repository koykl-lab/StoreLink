//! Components for the qualified versioned-S3 adapter.
//!
//! Native qualification observes the required profile; a fresh-scope token is not
//! persisted enrollment or upload/publication authority. Exclusive ownership,
//! continued configuration monitoring and provider in-flight MPU reclamation
//! remain explicit obligations. Lifecycle assessment alone is not qualification.

mod lifecycle;
mod orphans;
mod qualification;
#[cfg(feature = "dynamodb")]
pub use orphans::{
    KnownOrphanAttribution, ObservedOrphanObject, OrphanAttribution, UnattributedOrphan,
    observe_orphan_object,
};
pub use orphans::{OrphanObservationError, VersionPage, VersionPageCursor, list_version_page};
#[cfg(feature = "dynamodb")]
mod reading;
#[cfg(feature = "dynamodb")]
mod reclamation;
#[cfg(feature = "dynamodb")]
mod transfer;
#[cfg(feature = "dynamodb")]
mod verification;

#[cfg(feature = "dynamodb")]
pub use reading::{ByteRange, PinnedRead, ReadError, open_pinned_read};
#[cfg(feature = "dynamodb")]
pub use reclamation::{DeletionError, DeletionOutcome, delete_retiring_version};
#[cfg(feature = "dynamodb")]
pub use transfer::{TransferError, UploadCandidate, transfer_upload};

#[cfg(feature = "dynamodb")]
pub use verification::{
    VerificationError, VerifiedUploadReceipt, attribution_metadata,
    attribution_metadata_with_extra, verify_candidate,
};

pub use lifecycle::{LifecycleError, LifecycleReport, MpuAgePolicy, assess_lifecycle};
pub use qualification::{
    NamespaceConfiguration, NamespaceConfigurationSpec, QualificationError,
    QualifiedFreshNamespace, QualifiedNamespace, qualify_existing, qualify_fresh,
};
