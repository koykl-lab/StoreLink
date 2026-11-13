//! Regional DynamoDB integration components.
//!
//! These request-boundary components do not yet constitute a complete coordinator.
//! Application expressions remain native; they are not parsed or evaluated here.

mod actions;
mod application_pruning;
mod discovery;
mod inventory_progress;
mod journal;
mod maintenance;
#[cfg(feature = "s3")]
mod orphans;
mod progress;
mod protection;
mod pruning;
mod reclamation;
mod references;
mod tables;
mod uploads;

pub use actions::{ActionError, ApplicationActions, ApplicationInspection};
pub use application_pruning::{ApplicationPruningError, ApplicationPruningObservation};
pub use discovery::{
    BoundedMaintenanceHint, MetadataCursor, MetadataDiscoveryError, MetadataDiscoveryItem,
    MetadataDiscoveryPage,
};
pub use inventory_progress::InventoryProgressError;
#[cfg(feature = "s3")]
pub use inventory_progress::InventoryProgressObservation;
pub use journal::{
    ActionOrigin, ApplicationItemObservation, ApplicationItemsObservation, AttemptFailure,
    CancellationReason, CommandContext, CommandJournal, CommandToken, ExecutionReport,
    JournalError, Outcome, PreparedCommand,
};
pub use maintenance::{
    MaintenanceError, MaintenanceObservation, MaintenanceOutcome, MaintenancePolicy,
    MaintenanceReport,
};
#[cfg(feature = "s3")]
pub use orphans::{OrphanError, OrphanGracePolicy, OrphanPreparation};
pub use progress::{MetadataProgressError, MetadataProgressObservation};
pub use protection::{
    ProtectionError, UseAuthorization, UsePinSnapshot, UseSource, VersionSnapshot, VersionState,
};
pub use pruning::{PruningError, PruningOutcome};
pub use reclamation::ReclamationError;
pub use references::{ReferenceChange, ReferenceError, ReferenceId, ReferenceSnapshot};
pub use tables::{EnrolledTables, EnrollmentError};
pub use uploads::{
    AttemptSnapshot, AttemptState, IntentSnapshot, IntentState, NamespaceSnapshot,
    PublicationError, StagedSnapshot, TransferAuthorization, UploadConstraints, UploadError,
    UploadTarget,
};
