//! Regional DynamoDB integration components.
//!
//! These request-boundary components do not yet constitute a complete coordinator.
//! Application expressions remain native; they are not parsed or evaluated here.

mod actions;
mod journal;
mod maintenance;
mod protection;
mod pruning;
mod reclamation;
mod references;
mod tables;
mod uploads;

pub use actions::{ActionError, ApplicationActions, ApplicationInspection};
pub use journal::{
    ActionOrigin, ApplicationItemObservation, AttemptFailure, CancellationReason, CommandContext,
    CommandJournal, CommandToken, ExecutionReport, JournalError, Outcome, PreparedCommand,
};
pub use maintenance::{
    MaintenanceError, MaintenanceObservation, MaintenanceOutcome, MaintenancePolicy,
    MaintenanceReport,
};
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
