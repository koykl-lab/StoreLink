//! Persisted upload admission, selection and staged publication/lifetimes.
//! No S3 requests, use-pin runtime, inventory or GC engine. These components do
//! not constitute a complete protocol; see the selection child modules.
//!
//! Namespace configuration is supplied by the qualified fresh-enrollment flow. Reading `ACTIVE` configuration does not verify the S3 provider profile,
//! lifecycle, fresh scope or exclusive ownership. Physical binding is immutable
//! for a configuration revision; enrollment must never reuse revisions.
//!
//! StoreId has a permanent domain-local physical binding, created or checked in
//! the namespace enrollment transaction. This bounded configuration is never
//! retired or compacted with namespaces, claims or command outcomes.
//!
//! Flat native attributes, `sk = M`: store `pk = prefix#STORE#hex(id)`,
//! namespace `pk = prefix#NS#hex(id)`, intent
//! `pk = prefix#INTENT#incarnation#generation#uuid`, attempt analogously ATTEMPT.
//! Full identities accompany every address (hashes are not authority). Records
//! are bounded to 4000 DynamoDB item bytes, conservatively counting numeric text,
//! with 64 extra bytes reserved for state/deadline growth. Future VersionID,
//! selected-attempt and staging identities MUST live in separate records, not
//! grow these admitted rows. No retry-attempt list is stored on an intent.
//!
//! Snapshots and historical journal results are not transfer authority. Only
//! `authorize_transfer` reads all four authoritative rows coherently. Its handle
//! is a point-in-time admission, not a promise to cancel already-issued I/O;
//! the S3 adapter must freshly check its deadline at actual I/O and reauthorize
//! every further operation, including after suspension. No raw-tuple/Deserialize
//! constructor can manufacture a handle. SELECTED cancellation must use the
//! separate staging transition so protection accounting is released atomically.

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Get, Put, TransactGetItem, TransactWriteItem,
};
use thiserror::Error;
use uuid::Uuid;

use super::journal::{control_key, parse_control, prefix};
use super::{ApplicationActions, CommandContext, CommandJournal, JournalError, PreparedCommand};
use crate::claim::{ClaimError, ClaimPolicy, Renewal};
use crate::clock::{ClockError, ClockObservation, TimestampMs};
use crate::identity::{DomainId, IdentityError, NamespaceId, ResourceId, Scope, StoreId};

mod abandoned;
mod selection;
pub use selection::{PublicationError, StagedSnapshot};
pub(super) use selection::{decode_staged, encode_staged, selected_binding};

type Item = HashMap<String, AttributeValue>;
const PROFILE: &str = "aws-s3-versioned-lifecycle-v0";
const MAX_RECORD_BYTES: usize = 4000;
const GROWTH_RESERVE: usize = 64;

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("native abandoned-upload conditional conflict; reread authoritative rows")]
    AbandonedConflict,
    #[error("native abandoned-upload outcome UNKNOWN; reread, never infer rollback")]
    AbandonedOutcomeUnknown,
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Claim(#[from] ClaimError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error("invalid upload constraints")]
    Constraints,
    #[error("exclusive key prefix exceeds 1024 UTF-8 bytes; empty means the whole bucket")]
    PrefixLength,
    #[error("upload identity, namespace, configuration or target binding mismatch")]
    Binding,
    #[error("upload record is terminal or unsupported")]
    Terminal,
    #[error("required admission record is missing; no authority inferred")]
    Missing,
    #[error("invalid flat upload record; no authority inferred")]
    Corrupt,
    #[error("upload record exceeds the 4000-byte bound including growth reserve")]
    RecordSize,
    #[error("attempt deadline exceeds its fixed maximum MPU-use deadline")]
    MpuDeadline,
    #[error("native upload read failed; no absence inferred")]
    ReadFailed,
    #[error("native upload action construction failed")]
    ActionBuild,
    #[error(
        "native staged cleanup conditional conflict; reread native rows, not a historical abort result"
    )]
    StagedConflict,
    #[error(
        "native staged cleanup outcome UNKNOWN; reread native rows, do not infer rollback: {0}"
    )]
    StagedOutcomeUnknown(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadConstraints {
    max_length: u64,
    expected_length: Option<u64>,
    full_sha256: Option<[u8; 32]>,
}
impl UploadConstraints {
    pub fn new(
        max_length: u64,
        expected_length: Option<u64>,
        full_sha256: Option<[u8; 32]>,
    ) -> Result<Self, UploadError> {
        if max_length > i64::MAX as u64 || expected_length.is_some_and(|n| n > max_length) {
            return Err(UploadError::Constraints);
        }
        Ok(Self {
            max_length,
            expected_length,
            full_sha256,
        })
    }
    pub fn max_length(&self) -> u64 {
        self.max_length
    }
    pub fn expected_length(&self) -> Option<u64> {
        self.expected_length
    }
    pub fn full_sha256(&self) -> Option<&[u8; 32]> {
        self.full_sha256.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadTarget {
    store: StoreId,
    key: String,
}
impl UploadTarget {
    pub fn new(store: StoreId, key: impl Into<String>) -> Result<Self, IdentityError> {
        let key = key.into();
        bounded(&key, "object key", 1024)?;
        Ok(Self { store, key })
    }
    pub fn store(&self) -> &StoreId {
        &self.store
    }
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Checked configuration observation, NOT backend qualification or enrollment.
/// MPU age and lifecycle margin are explicit operator policy, with no defaults.
/// Future S3 qualification must assess actual lifecycle coverage/eligibility with
/// these same values: eligibility must exceed supported age by the margin.
/// All fields are immutable within the non-reused configuration revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceSnapshot {
    domain: DomainId,
    incarnation: Uuid,
    id: NamespaceId,
    configuration_revision: Uuid,
    store: StoreId,
    bucket: String,
    expected_owner: String,
    region: String,
    key_prefix: String,
    mpu_max_age_ms: u64,
    mpu_lifecycle_margin_ms: u64,
}
impl NamespaceSnapshot {
    pub fn id(&self) -> &NamespaceId {
        &self.id
    }
    pub fn domain(&self) -> &DomainId {
        &self.domain
    }
    pub fn incarnation(&self) -> Uuid {
        self.incarnation
    }
    pub fn configuration_revision(&self) -> Uuid {
        self.configuration_revision
    }
    pub fn store(&self) -> &StoreId {
        &self.store
    }
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn expected_owner(&self) -> &str {
        &self.expected_owner
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }
    pub fn provider_profile(&self) -> &str {
        PROFILE
    }
    pub fn mpu_max_age_ms(&self) -> u64 {
        self.mpu_max_age_ms
    }
    pub fn mpu_lifecycle_margin_ms(&self) -> u64 {
        self.mpu_lifecycle_margin_ms
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Admission {
    id: ResourceId,
    namespace: NamespaceId,
    configuration_revision: Uuid,
    target: UploadTarget,
    constraints: UploadConstraints,
    revision: Uuid,
    deadline: TimestampMs,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentState {
    Open,
    Selected,
    Consumed,
    Cancelled,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptState {
    Active,
    Sealed,
}

/// Strong observation; preparation always rechecks it in the native transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentSnapshot {
    admission: Admission,
    state: IntentState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttemptSnapshot {
    admission: Admission,
    intent: ResourceId,
    state: AttemptState,
    mpu_admitted_lower_bound: TimestampMs,
    max_mpu_use_until: TimestampMs,
}
macro_rules! admission_getters {
    ($name:ident) => {
        impl $name {
            pub fn id(&self) -> &ResourceId {
                &self.admission.id
            }
            pub fn namespace_id(&self) -> &NamespaceId {
                &self.admission.namespace
            }
            pub fn configuration_revision(&self) -> Uuid {
                self.admission.configuration_revision
            }
            pub fn target(&self) -> &UploadTarget {
                &self.admission.target
            }
            pub fn constraints(&self) -> &UploadConstraints {
                &self.admission.constraints
            }
            pub fn revision(&self) -> Uuid {
                self.admission.revision
            }
            pub fn deadline(&self) -> TimestampMs {
                self.admission.deadline
            }
        }
    };
}
admission_getters!(IntentSnapshot);
admission_getters!(AttemptSnapshot);
impl IntentSnapshot {
    pub fn state(&self) -> IntentState {
        self.state
    }
}
impl AttemptSnapshot {
    pub fn state(&self) -> AttemptState {
        self.state
    }
    pub fn intent_id(&self) -> &ResourceId {
        &self.intent
    }
    /// Immutable conservative admission time, not an observed S3 initiation time.
    pub fn mpu_admitted_lower_bound(&self) -> TimestampMs {
        self.mpu_admitted_lower_bound
    }
    pub fn max_mpu_use_until(&self) -> TimestampMs {
        self.max_mpu_use_until
    }
}

/// Issued only after the coherent native four-row admission read. Not renewable
/// locally: obtain another handle for every further operation. The S3 adapter
/// must freshly check the deadline at actual I/O; this point-in-time admission
/// cannot cancel already-issued I/O. No S3 method.
#[derive(Debug)]
pub struct TransferAuthorization {
    namespace: NamespaceSnapshot,
    intent: IntentSnapshot,
    attempt: AttemptSnapshot,
}
impl TransferAuthorization {
    pub fn namespace(&self) -> &NamespaceSnapshot {
        &self.namespace
    }
    pub fn intent(&self) -> &IntentSnapshot {
        &self.intent
    }
    pub fn attempt(&self) -> &AttemptSnapshot {
        &self.attempt
    }
    pub fn valid_until(&self) -> TimestampMs {
        self.intent
            .deadline()
            .min(self.attempt.deadline())
            .min(self.attempt.max_mpu_use_until)
    }
}

impl CommandJournal {
    /// Prepare fresh namespace enrollment, never an upsert, rebinding or adoption.
    /// Consumes the fresh-scope observation and fixes the configuration revision
    /// to this command UUID. Only a successful journal commit persists enrollment.
    /// The v0 single-region profile requires the configured DynamoDB region to
    /// match the qualified S3 region; absent/mismatched regions return `Binding`.
    /// The same transaction creates or checks the permanent domain-local StoreId
    /// binding. Identical bindings may be shared; a different physical store needs
    /// a new StoreId. This point check does not prove prefix non-overlap.
    ///
    /// The operator must maintain exclusive scope ownership and the qualified
    /// provider/lifecycle profile continuously. Qualification observations and
    /// this metadata transaction are NOT cross-store atomicity or proof of
    /// exclusive ownership. This method performs no S3 I/O or bucket mutation.
    ///
    /// Resolve an unknown submission using the existing journal token/`resolve`
    /// flow (or retry the exact prepared command), not a new namespace identity
    /// or adoption. Idempotent bootstrap must read an existing binding and validate
    /// it separately; similar configuration never establishes ownership here.
    ///
    /// Existing-profile health is not fresh-enrollment authority:
    /// ```compile_fail,E0308
    /// use storelink::dynamodb::{CommandContext, CommandJournal};
    /// use storelink::s3::QualifiedNamespace;
    /// fn health_is_not_fresh(
    ///     journal: &CommandJournal,
    ///     context: CommandContext,
    ///     health: QualifiedNamespace,
    /// ) {
    ///     journal.prepare_enroll_namespace(context, health, Vec::new()).unwrap();
    /// }
    /// ```
    #[cfg(feature = "s3")]
    pub fn prepare_enroll_namespace(
        &self,
        context: CommandContext,
        fresh: crate::s3::QualifiedFreshNamespace,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        let configuration = fresh.into_configuration();
        if self.native_client().config().region().map(|r| r.as_ref())
            != Some(configuration.region())
        {
            return Err(UploadError::Binding);
        }
        prefix_bound(configuration.key_prefix())?;
        let namespace = NamespaceSnapshot {
            domain: self.domain_id().clone(),
            incarnation: context.id().scope().incarnation(),
            id: configuration.id().clone(),
            configuration_revision: context.id().uuid(),
            store: configuration.store().clone(),
            bucket: configuration.bucket().to_owned(),
            expected_owner: configuration.expected_owner().to_owned(),
            region: configuration.region().to_owned(),
            key_prefix: configuration.key_prefix().to_owned(),
            mpu_max_age_ms: configuration.mpu_max_age_ms(),
            mpu_lifecycle_margin_ms: configuration.mpu_lifecycle_margin_ms(),
        };
        self.upload_command(
            context,
            vec![
                store_binding_action(self.coordination_arn(), &namespace)?,
                create(self.coordination_arn(), encode_namespace(&namespace)?)?,
            ],
            result,
        )
    }

    pub(super) async fn read_upload_item(&self, key: Item) -> Result<Option<Item>, UploadError> {
        Ok(self
            .native_client()
            .get_item()
            .table_name(self.coordination_arn())
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await
            .map_err(|_| UploadError::ReadFailed)?
            .item)
    }
    pub async fn read_namespace(
        &self,
        id: &NamespaceId,
    ) -> Result<Option<NamespaceSnapshot>, UploadError> {
        self.read_upload_item(namespace_key(self.domain_id(), id))
            .await?
            .as_ref()
            .map(|item| decode_namespace(self.domain_id(), id, item))
            .transpose()
    }
    pub async fn read_intent(
        &self,
        id: &ResourceId,
    ) -> Result<Option<IntentSnapshot>, UploadError> {
        check_domain(self.domain_id(), id)?;
        self.read_upload_item(resource_key("INTENT", id))
            .await?
            .as_ref()
            .map(|item| decode_intent(id, item))
            .transpose()
    }
    pub async fn read_attempt(
        &self,
        id: &ResourceId,
    ) -> Result<Option<AttemptSnapshot>, UploadError> {
        check_domain(self.domain_id(), id)?;
        self.read_upload_item(resource_key("ATTEMPT", id))
            .await?
            .as_ref()
            .map(|item| decode_attempt(id, item))
            .transpose()
    }

    /// Initial revision is the creating command UUID, fixed once, like the ID.
    /// No returned journal receipt authorizes transfer; commit then revalidate.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_begin_intent(
        &self,
        context: CommandContext,
        namespace: &NamespaceSnapshot,
        key: impl Into<String>,
        constraints: UploadConstraints,
        deadline: TimestampMs,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        let id = ResourceId::from_command(context.id());
        let intent = IntentSnapshot {
            admission: Admission {
                revision: id.uuid(),
                id,
                namespace: namespace.id.clone(),
                configuration_revision: namespace.configuration_revision,
                target: UploadTarget::new(namespace.store.clone(), key)?,
                constraints,
                deadline,
            },
            state: IntentState::Open,
        };
        self.existing_scope(&context, intent.id())?;
        binding(&intent.admission, namespace)?;
        new_deadline(deadline, clock, policy)?;
        self.upload_command(
            context,
            vec![
                namespace_guard(self.coordination_arn(), namespace)?,
                create(self.coordination_arn(), encode_intent(&intent)?)?,
            ],
            result,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_begin_attempt(
        &self,
        context: CommandContext,
        intent: &IntentSnapshot,
        namespace: &NamespaceSnapshot,
        deadline: TimestampMs,
        max_mpu_use_until: TimestampMs,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, intent.id())?;
        live_intent(intent, namespace, clock, policy)?;
        new_deadline(deadline, clock, policy)?;
        let mpu_admitted_lower_bound = clock.lower_bound()?;
        mpu_age_binding(
            mpu_admitted_lower_bound,
            deadline,
            max_mpu_use_until,
            namespace,
        )?;
        let id = ResourceId::from_command(context.id());
        let mut admission = intent.admission.clone();
        admission.revision = id.uuid();
        admission.id = id;
        admission.deadline = deadline;
        let attempt = AttemptSnapshot {
            admission,
            intent: intent.id().clone(),
            state: AttemptState::Active,
            mpu_admitted_lower_bound,
            max_mpu_use_until,
        };
        self.upload_command(
            context,
            vec![
                namespace_guard(self.coordination_arn(), namespace)?,
                guard(self.coordination_arn(), encode_intent(intent)?)?,
                create(self.coordination_arn(), encode_attempt(&attempt)?)?,
            ],
            result,
        )
    }

    /// Discovery read confers NO authority. The subsequent TransactGetItems
    /// re-reads attempt, parent, namespace and fence in one serializable view.
    /// Samples the trusted clock only AFTER that read, immediately before live
    /// validation. The synchronous sampler must obtain a fresh observation, not
    /// return a captured old one. It is not a business callback or replanner and
    /// never runs inside a database transaction. At actual I/O the S3 adapter must
    /// freshly check the deadline, and reauthorize every further operation.
    pub async fn authorize_transfer(
        &self,
        id: &ResourceId,
        sample_clock: impl FnOnce() -> Result<ClockObservation, ClockError>,
        policy: &ClaimPolicy,
    ) -> Result<TransferAuthorization, UploadError> {
        let discovery = self.read_attempt(id).await?.ok_or(UploadError::Missing)?;
        let gets = [
            control_key(self.domain_id()),
            namespace_key(self.domain_id(), discovery.namespace_id()),
            resource_key("INTENT", discovery.intent_id()),
            resource_key("ATTEMPT", id),
        ]
        .into_iter()
        .map(|key| {
            Get::builder()
                .table_name(self.coordination_arn())
                .set_key(Some(key))
                .build()
                .map(|get| TransactGetItem::builder().get(get).build())
                .map_err(|_| UploadError::ActionBuild)
        })
        .collect::<Result<Vec<_>, _>>()?;
        let response = self
            .native_client()
            .transact_get_items()
            .set_transact_items(Some(gets))
            .send()
            .await
            .map_err(|_| UploadError::ReadFailed)?;
        let rows = response.responses();
        if rows.len() != 4 {
            return Err(UploadError::Corrupt);
        }
        let fence = parse_control(rows[0].item.as_ref(), self.domain_id())?;
        let namespace = decode_namespace(
            self.domain_id(),
            discovery.namespace_id(),
            rows[1].item.as_ref().ok_or(UploadError::Missing)?,
        )?;
        let intent = decode_intent(
            discovery.intent_id(),
            rows[2].item.as_ref().ok_or(UploadError::Missing)?,
        )?;
        let attempt = decode_attempt(id, rows[3].item.as_ref().ok_or(UploadError::Missing)?)?;
        if fence.incarnation() != id.scope().incarnation()
            || id.scope().generation() > fence.current()
            || intent.id().scope().generation() > fence.current()
        {
            return Err(UploadError::Binding);
        }
        authorize_observed(namespace, intent, attempt, sample_clock, policy)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_renew_intent(
        &self,
        context: CommandContext,
        intent: &IntentSnapshot,
        namespace: &NamespaceSnapshot,
        renewal: &Renewal,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, intent.id())?;
        live_intent(intent, namespace, clock, policy)?;
        let mut next = intent.clone();
        renew(&mut next.admission, renewal, clock, policy)?;
        self.upload_command(
            context,
            vec![
                namespace_guard(self.coordination_arn(), namespace)?,
                replace(
                    self.coordination_arn(),
                    encode_intent(intent)?,
                    encode_intent(&next)?,
                )?,
            ],
            result,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_renew_attempt(
        &self,
        context: CommandContext,
        attempt: &AttemptSnapshot,
        intent: &IntentSnapshot,
        namespace: &NamespaceSnapshot,
        renewal: &Renewal,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, attempt.id())?;
        self.existing_scope(&context, intent.id())?;
        live_attempt(attempt, intent, namespace, clock, policy)?;
        let mut next = attempt.clone();
        renew(&mut next.admission, renewal, clock, policy)?;
        if next.deadline() > next.max_mpu_use_until {
            return Err(UploadError::MpuDeadline);
        }
        self.upload_command(
            context,
            vec![
                namespace_guard(self.coordination_arn(), namespace)?,
                guard(self.coordination_arn(), encode_intent(intent)?)?,
                replace(
                    self.coordination_arn(),
                    encode_attempt(attempt)?,
                    encode_attempt(&next)?,
                )?,
            ],
            result,
        )
    }

    /// ACTIVE -> SEALED at the observed revision. Works after expiry; does not
    /// abort multipart requests or claim to stop already-issued storage I/O.
    pub fn prepare_seal_attempt(
        &self,
        context: CommandContext,
        attempt: &AttemptSnapshot,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, attempt.id())?;
        if attempt.state != AttemptState::Active {
            return Err(UploadError::Terminal);
        }
        let mut next = attempt.clone();
        next.state = AttemptState::Sealed;
        self.upload_command(
            context,
            vec![replace(
                self.coordination_arn(),
                encode_attempt(attempt)?,
                encode_attempt(&next)?,
            )?],
            result,
        )
    }
    /// OPEN -> CANCELLED only. SELECTED cancellation must release counted
    /// staging protection and is not implemented. No S3 cancellation is performed.
    pub fn prepare_cancel_intent(
        &self,
        context: CommandContext,
        intent: &IntentSnapshot,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, intent.id())?;
        if intent.state != IntentState::Open {
            return Err(UploadError::Terminal);
        }
        let mut next = intent.clone();
        next.state = IntentState::Cancelled;
        self.upload_command(
            context,
            vec![replace(
                self.coordination_arn(),
                encode_intent(intent)?,
                encode_intent(&next)?,
            )?],
            result,
        )
    }
    pub(super) fn existing_scope(
        &self,
        context: &CommandContext,
        id: &ResourceId,
    ) -> Result<(), UploadError> {
        check_domain(self.domain_id(), id)?;
        let scope = context.id().scope();
        if scope.domain() != id.scope().domain()
            || scope.incarnation() != id.scope().incarnation()
            || id.scope().generation() > scope.generation()
        {
            return Err(UploadError::Binding);
        }
        Ok(())
    }
    fn upload_command(
        &self,
        context: CommandContext,
        internal: Vec<TransactWriteItem>,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        Ok(self.prepare_internal(
            context,
            ApplicationActions::new(vec![]).map_err(JournalError::from)?,
            internal,
            result,
        )?)
    }
}

// Only called after decoding the coherent read and checking its fence. Keeping
// the final sampling/live-validation boundary synchronous leaves no await gap.
fn authorize_observed(
    namespace: NamespaceSnapshot,
    intent: IntentSnapshot,
    attempt: AttemptSnapshot,
    sample_clock: impl FnOnce() -> Result<ClockObservation, ClockError>,
    policy: &ClaimPolicy,
) -> Result<TransferAuthorization, UploadError> {
    let clock = sample_clock()?;
    live_attempt(&attempt, &intent, &namespace, clock, policy)?;
    Ok(TransferAuthorization {
        namespace,
        intent,
        attempt,
    })
}

fn check_domain(domain: &DomainId, id: &ResourceId) -> Result<(), UploadError> {
    if id.scope().domain() != domain {
        return Err(UploadError::Binding);
    }
    Ok(())
}
fn bounded(value: &str, field: &'static str, max: usize) -> Result<(), IdentityError> {
    if value.is_empty() || value.len() > max {
        return Err(IdentityError::InvalidLength { field, max });
    }
    Ok(())
}
fn prefix_bound(prefix: &str) -> Result<(), UploadError> {
    if prefix.len() > 1024 {
        return Err(UploadError::PrefixLength);
    }
    Ok(())
}

fn upper(clock: ClockObservation, policy: &ClaimPolicy) -> Result<TimestampMs, UploadError> {
    clock.validate(policy.max_clock_uncertainty_ms())?;
    Ok(clock.upper_bound()?)
}
fn live(
    deadline: TimestampMs,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<(), UploadError> {
    if deadline <= upper(clock, policy)? {
        return Err(ClaimError::Expired.into());
    }
    Ok(())
}
fn new_deadline(
    deadline: TimestampMs,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<(), UploadError> {
    let upper = upper(clock, policy)?;
    if deadline <= upper {
        return Err(ClaimError::Expired.into());
    }
    if deadline > upper.checked_add(policy.max_lifetime_ms())? {
        return Err(ClaimError::LifetimeExceeded.into());
    }
    if deadline <= upper.checked_add(policy.renewal_margin_ms())? {
        return Err(ClaimError::RenewalMargin.into());
    }
    Ok(())
}
fn renew(
    admission: &mut Admission,
    renewal: &Renewal,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<(), UploadError> {
    if admission.revision != renewal.expected_revision() {
        return Err(ClaimError::RevisionMismatch.into());
    }
    live(admission.deadline, clock, policy)?;
    if admission.deadline <= upper(clock, policy)?.checked_add(policy.renewal_margin_ms())? {
        return Err(ClaimError::RenewalMargin.into());
    }
    if renewal.proposed_deadline() <= admission.deadline {
        return Err(ClaimError::DeadlineNotForward.into());
    }
    new_deadline(renewal.proposed_deadline(), clock, policy)?;
    admission.deadline = renewal.proposed_deadline();
    admission.revision = renewal.new_revision();
    Ok(())
}
fn binding(admission: &Admission, namespace: &NamespaceSnapshot) -> Result<(), UploadError> {
    if admission.id.scope().domain() != &namespace.domain
        || admission.id.scope().incarnation() != namespace.incarnation
        || admission.namespace != namespace.id
        || admission.configuration_revision != namespace.configuration_revision
        || admission.target.store != namespace.store
        || !admission.target.key.starts_with(&namespace.key_prefix)
    {
        return Err(UploadError::Binding);
    }
    Ok(())
}
fn live_intent(
    intent: &IntentSnapshot,
    namespace: &NamespaceSnapshot,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<(), UploadError> {
    if intent.state != IntentState::Open {
        return Err(UploadError::Terminal);
    }
    binding(&intent.admission, namespace)?;
    live(intent.deadline(), clock, policy)
}
fn mpu_policy(namespace: &NamespaceSnapshot) -> Result<(), UploadError> {
    if namespace.mpu_max_age_ms == 0 || namespace.mpu_lifecycle_margin_ms == 0 {
        return Err(UploadError::Corrupt);
    }
    TimestampMs::ZERO
        .checked_add(namespace.mpu_max_age_ms)?
        .checked_add(namespace.mpu_lifecycle_margin_ms)?;
    Ok(())
}
fn mpu_age_binding(
    admitted: TimestampMs,
    deadline: TimestampMs,
    ceiling: TimestampMs,
    namespace: &NamespaceSnapshot,
) -> Result<(), UploadError> {
    mpu_policy(namespace)?;
    if admitted >= deadline
        || deadline > ceiling
        || ceiling > admitted.checked_add(namespace.mpu_max_age_ms)?
    {
        return Err(UploadError::MpuDeadline);
    }
    Ok(())
}
fn live_attempt(
    attempt: &AttemptSnapshot,
    intent: &IntentSnapshot,
    namespace: &NamespaceSnapshot,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<(), UploadError> {
    live_intent(intent, namespace, clock, policy)?;
    if attempt.state != AttemptState::Active {
        return Err(UploadError::Terminal);
    }
    binding(&attempt.admission, namespace)?;
    if attempt.intent != *intent.id()
        || attempt.target() != intent.target()
        || attempt.constraints() != intent.constraints()
    {
        return Err(UploadError::Binding);
    }
    mpu_age_binding(
        attempt.mpu_admitted_lower_bound,
        attempt.deadline(),
        attempt.max_mpu_use_until,
        namespace,
    )?;
    if attempt.mpu_admitted_lower_bound > upper(clock, policy)? {
        return Err(UploadError::MpuDeadline);
    }
    live(attempt.deadline(), clock, policy)?;
    live(attempt.max_mpu_use_until, clock, policy)
}

pub(super) fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
pub(super) fn n(value: u64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}
fn key(pk: String) -> Item {
    HashMap::from([("pk".into(), s(pk)), ("sk".into(), s("M"))])
}
pub(super) fn namespace_key(domain: &DomainId, id: &NamespaceId) -> Item {
    let hex: String = id
        .as_str()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    key(format!("{}#NS#{hex}", prefix(domain)))
}
pub(super) fn resource_key(kind: &str, id: &ResourceId) -> Item {
    key(format!(
        "{}#{kind}#{}#{}#{}",
        prefix(id.scope().domain()),
        id.scope().incarnation(),
        id.scope().generation(),
        id.uuid()
    ))
}
pub(super) fn check_size(item: &Item) -> Result<(), UploadError> {
    // DynamoDB item size counts UTF-8 names/strings and raw binary, not JSON
    // quoting/base64 transport overhead. Decimal text is conservative for our
    // bounded unsigned integers (DynamoDB packed numbers use <= 11 bytes here).
    let mut size = GROWTH_RESERVE;
    for (name, value) in item {
        size += name.len()
            + match value {
                AttributeValue::S(v) => v.len(),
                AttributeValue::N(v) => v.len().max(2),
                AttributeValue::B(v) => v.as_ref().len(),
                _ => return Err(UploadError::Corrupt),
            };
    }
    if size > MAX_RECORD_BYTES {
        return Err(UploadError::RecordSize);
    }
    Ok(())
}
fn encode_namespace(ns: &NamespaceSnapshot) -> Result<Item, UploadError> {
    mpu_policy(ns)?;
    let mut item = namespace_key(&ns.domain, &ns.id);
    for (name, value) in [
        ("record", "namespace".to_owned()),
        ("domain", ns.domain.as_str().to_owned()),
        ("incarnation", ns.incarnation.to_string()),
        ("namespace_id", ns.id.as_str().to_owned()),
        (
            "configuration_revision",
            ns.configuration_revision.to_string(),
        ),
        ("state", "ACTIVE".to_owned()),
        ("store", ns.store.as_str().to_owned()),
        ("bucket", ns.bucket.clone()),
        ("expected_owner", ns.expected_owner.clone()),
        ("region", ns.region.clone()),
        ("key_prefix", ns.key_prefix.clone()),
        ("provider_profile", PROFILE.to_owned()),
    ] {
        item.insert(name.into(), s(value));
    }
    item.insert("mpu_max_age_ms".into(), n(ns.mpu_max_age_ms));
    item.insert(
        "mpu_lifecycle_margin_ms".into(),
        n(ns.mpu_lifecycle_margin_ms),
    );
    check_size(&item)?;
    Ok(item)
}
fn encode_admission(kind: &str, a: &Admission, state: &str) -> Item {
    let mut item = resource_key(kind, &a.id);
    for (name, value) in [
        ("record", kind.to_ascii_lowercase()),
        ("domain", a.id.scope().domain().as_str().to_owned()),
        ("incarnation", a.id.scope().incarnation().to_string()),
        ("resource_uuid", a.id.uuid().to_string()),
        ("namespace_id", a.namespace.as_str().to_owned()),
        (
            "configuration_revision",
            a.configuration_revision.to_string(),
        ),
        ("store", a.target.store.as_str().to_owned()),
        ("object_key", a.target.key.clone()),
        ("state", state.to_owned()),
        ("revision", a.revision.to_string()),
    ] {
        item.insert(name.into(), s(value));
    }
    for (name, value) in [
        ("generation", a.id.scope().generation()),
        ("deadline", a.deadline.as_millis()),
        ("max_length", a.constraints.max_length),
    ] {
        item.insert(name.into(), n(value));
    }
    if let Some(length) = a.constraints.expected_length {
        item.insert("expected_length".into(), n(length));
    }
    if let Some(hash) = a.constraints.full_sha256 {
        item.insert("full_sha256".into(), AttributeValue::B(Blob::new(hash)));
    }
    item
}
pub(super) fn encode_intent(intent: &IntentSnapshot) -> Result<Item, UploadError> {
    let item = encode_admission(
        "INTENT",
        &intent.admission,
        match intent.state {
            IntentState::Open => "OPEN",
            IntentState::Selected => "SELECTED",
            IntentState::Consumed => "CONSUMED",
            IntentState::Cancelled => "CANCELLED",
        },
    );
    check_size(&item)?;
    Ok(item)
}
fn encode_attempt(attempt: &AttemptSnapshot) -> Result<Item, UploadError> {
    let mut item = encode_admission(
        "ATTEMPT",
        &attempt.admission,
        match attempt.state {
            AttemptState::Active => "ACTIVE",
            AttemptState::Sealed => "SEALED",
        },
    );
    item.insert(
        "intent_domain".into(),
        s(attempt.intent.scope().domain().as_str()),
    );
    item.insert(
        "intent_incarnation".into(),
        s(attempt.intent.scope().incarnation().to_string()),
    );
    item.insert(
        "intent_generation".into(),
        n(attempt.intent.scope().generation()),
    );
    item.insert("intent_uuid".into(), s(attempt.intent.uuid().to_string()));
    item.insert(
        "mpu_admitted_lower_bound".into(),
        n(attempt.mpu_admitted_lower_bound.as_millis()),
    );
    item.insert(
        "max_mpu_use_until".into(),
        n(attempt.max_mpu_use_until.as_millis()),
    );
    check_size(&item)?;
    Ok(item)
}
pub(super) fn text<'a>(item: &'a Item, field: &str) -> Result<&'a str, UploadError> {
    match item.get(field) {
        Some(AttributeValue::S(v)) => Ok(v),
        _ => Err(UploadError::Corrupt),
    }
}
pub(super) fn number(item: &Item, field: &str) -> Result<u64, UploadError> {
    match item.get(field) {
        Some(AttributeValue::N(v)) => v
            .parse::<u64>()
            .ok()
            .filter(|n| n.to_string() == *v)
            .ok_or(UploadError::Corrupt),
        _ => Err(UploadError::Corrupt),
    }
}
pub(super) fn uuid(item: &Item, field: &str) -> Result<Uuid, UploadError> {
    let value = text(item, field)?;
    Uuid::parse_str(value)
        .ok()
        .filter(|id| !id.is_nil() && id.to_string() == value)
        .ok_or(UploadError::Corrupt)
}
pub(super) fn exact(item: &Item, encoded: Item) -> Result<(), UploadError> {
    check_size(item)?;
    if item != &encoded {
        return Err(UploadError::Corrupt);
    }
    Ok(())
}
pub(super) fn decode_namespace(
    domain: &DomainId,
    id: &NamespaceId,
    item: &Item,
) -> Result<NamespaceSnapshot, UploadError> {
    check_size(item)?;
    let ns = NamespaceSnapshot {
        domain: DomainId::new(text(item, "domain")?)?,
        incarnation: uuid(item, "incarnation")?,
        id: NamespaceId::new(text(item, "namespace_id")?)?,
        configuration_revision: uuid(item, "configuration_revision")?,
        store: StoreId::new(text(item, "store")?)?,
        bucket: text(item, "bucket")?.into(),
        expected_owner: text(item, "expected_owner")?.into(),
        region: text(item, "region")?.into(),
        key_prefix: text(item, "key_prefix")?.into(),
        mpu_max_age_ms: number(item, "mpu_max_age_ms")?,
        mpu_lifecycle_margin_ms: number(item, "mpu_lifecycle_margin_ms")?,
    };
    bounded(&ns.bucket, "bucket", 63)?;
    bounded(&ns.region, "region", 64)?;
    prefix_bound(&ns.key_prefix)?;
    if ns.expected_owner.len() != 12 || !ns.expected_owner.bytes().all(|b| b.is_ascii_digit()) {
        return Err(UploadError::Corrupt);
    }
    if &ns.domain != domain || &ns.id != id {
        return Err(UploadError::Binding);
    }
    exact(item, encode_namespace(&ns)?)?;
    Ok(ns)
}
fn decode_admission(id: &ResourceId, item: &Item) -> Result<Admission, UploadError> {
    check_size(item)?;
    let stored = ResourceId::new(
        Scope::new(
            DomainId::new(text(item, "domain")?)?,
            uuid(item, "incarnation")?,
            number(item, "generation")?,
        )?,
        uuid(item, "resource_uuid")?,
    )?;
    if &stored != id {
        return Err(UploadError::Binding);
    }
    let hash = match item.get("full_sha256") {
        None => None,
        Some(AttributeValue::B(v)) => Some(
            v.as_ref()
                .try_into()
                .map_err(|_| UploadError::Constraints)?,
        ),
        _ => return Err(UploadError::Constraints),
    };
    let expected = item
        .contains_key("expected_length")
        .then(|| number(item, "expected_length"))
        .transpose()?;
    Ok(Admission {
        id: stored,
        namespace: NamespaceId::new(text(item, "namespace_id")?)?,
        configuration_revision: uuid(item, "configuration_revision")?,
        target: UploadTarget::new(
            StoreId::new(text(item, "store")?)?,
            text(item, "object_key")?,
        )?,
        constraints: UploadConstraints::new(number(item, "max_length")?, expected, hash)?,
        revision: uuid(item, "revision")?,
        deadline: TimestampMs::new(number(item, "deadline")?)?,
    })
}
pub(super) fn decode_intent(id: &ResourceId, item: &Item) -> Result<IntentSnapshot, UploadError> {
    let intent = IntentSnapshot {
        admission: decode_admission(id, item)?,
        state: match text(item, "state")? {
            "OPEN" => IntentState::Open,
            "SELECTED" => IntentState::Selected,
            "CONSUMED" => IntentState::Consumed,
            "CANCELLED" => IntentState::Cancelled,
            _ => return Err(UploadError::Terminal),
        },
    };
    exact(item, encode_intent(&intent)?)?;
    Ok(intent)
}
pub(super) fn decode_attempt(id: &ResourceId, item: &Item) -> Result<AttemptSnapshot, UploadError> {
    let attempt = AttemptSnapshot {
        admission: decode_admission(id, item)?,
        intent: ResourceId::new(
            Scope::new(
                DomainId::new(text(item, "intent_domain")?)?,
                uuid(item, "intent_incarnation")?,
                number(item, "intent_generation")?,
            )?,
            uuid(item, "intent_uuid")?,
        )?,
        state: match text(item, "state")? {
            "ACTIVE" => AttemptState::Active,
            "SEALED" => AttemptState::Sealed,
            _ => return Err(UploadError::Terminal),
        },
        mpu_admitted_lower_bound: TimestampMs::new(number(item, "mpu_admitted_lower_bound")?)?,
        max_mpu_use_until: TimestampMs::new(number(item, "max_mpu_use_until")?)?,
    };
    if attempt.intent.scope().domain() != id.scope().domain()
        || attempt.intent.scope().incarnation() != id.scope().incarnation()
        || attempt.intent.scope().generation() > id.scope().generation()
    {
        return Err(UploadError::Binding);
    }
    if attempt.mpu_admitted_lower_bound >= attempt.deadline()
        || attempt.deadline() > attempt.max_mpu_use_until
    {
        return Err(UploadError::MpuDeadline);
    }
    exact(item, encode_attempt(&attempt)?)?;
    Ok(attempt)
}

// Native expression generation only; DynamoDB evaluates it. Every observed
// attribute (including the complete tuple, state, revision, lifetime and optional
// constraints) participates. No partial identity/hash comparison or upsert.
struct Condition {
    expression: String,
    names: HashMap<String, String>,
    values: Item,
}
fn expression_guard(item: &Item) -> Condition {
    let mut names = HashMap::new();
    let mut values = Item::new();
    let mut terms = Vec::new();
    for (i, (name, value)) in item
        .iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .enumerate()
    {
        let alias = format!("#f{i}");
        let parameter = format!(":v{i}");
        terms.push(format!("{alias} = {parameter}"));
        names.insert(alias, name.clone());
        values.insert(parameter, value.clone());
    }
    Condition {
        expression: terms.join(" AND "),
        names,
        values,
    }
}
fn condition(item: &Item) -> Condition {
    let mut c = expression_guard(item);
    let staged = item.get("record") == Some(&s("staged-verified-v1"));
    for field in ["expected_length", "full_sha256", "verified_full_sha256"] {
        if field == "verified_full_sha256" && !staged {
            continue;
        }
        if !item.contains_key(field) {
            let alias = format!("#missing_{field}");
            c.expression
                .push_str(&format!(" AND attribute_not_exists({alias})"));
            c.names.insert(alias, field.into());
        }
    }
    c
}

// Shared canonical store-binding schema for enrollment and read-only discovery.
// Physical field bounds match the persisted namespace codec; provider
// qualification remains the enrollment prerequisite, not a discovery operation.
fn encode_store_binding(
    domain: &DomainId,
    store: &StoreId,
    bucket: &str,
    expected_owner: &str,
    region: &str,
) -> Result<Item, UploadError> {
    let hex: String = store
        .as_str()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut item = key(format!("{}#STORE#{hex}", prefix(domain)));
    for (name, value) in [
        ("record", "store-binding-v1"),
        ("domain", domain.as_str()),
        ("store", store.as_str()),
        ("bucket", bucket),
        ("expected_owner", expected_owner),
        ("region", region),
        ("provider_profile", PROFILE),
    ] {
        item.insert(name.into(), s(value));
    }
    check_size(&item)?;
    bounded(bucket, "bucket", 63)?;
    bounded(region, "region", 64)?;
    if expected_owner.len() != 12 || !expected_owner.bytes().all(|b| b.is_ascii_digit()) {
        return Err(UploadError::Corrupt);
    }
    Ok(item)
}

pub(super) fn decode_store_binding(domain: &DomainId, item: &Item) -> Result<(), UploadError> {
    let stored_domain = DomainId::new(text(item, "domain")?)?;
    let store = StoreId::new(text(item, "store")?)?;
    if &stored_domain != domain {
        return Err(UploadError::Binding);
    }
    exact(
        item,
        encode_store_binding(
            &stored_domain,
            &store,
            text(item, "bucket")?,
            text(item, "expected_owner")?,
            text(item, "region")?,
        )?,
    )
}

/// A library-only, closed canonical schema: the condition checks every named
/// field, not absence of arbitrary extra attributes. Future authority schemas
/// MUST use a new discriminator and fence old writers, never append authority
/// fields to this kind. Preserving unknown fields with Update would not prove
/// compatibility. This Put is permanent bounded configuration, not a tombstone;
/// namespace retirement and claim/command compaction must never remove/rebind it.
#[cfg(any(feature = "s3", test))]
fn store_binding_action(
    table: &str,
    ns: &NamespaceSnapshot,
) -> Result<TransactWriteItem, UploadError> {
    // Production callers derive this snapshot only from QualifiedFreshNamespace.
    let item = encode_store_binding(
        &ns.domain,
        &ns.store,
        &ns.bucket,
        &ns.expected_owner,
        &ns.region,
    )?;
    let mut c = expression_guard(&item);
    c.names.insert("#pk".into(), "pk".into());
    let put = Put::builder()
        .table_name(table)
        .set_item(Some(item))
        .condition_expression(format!("attribute_not_exists(#pk) OR ({})", c.expression))
        .set_expression_attribute_names(Some(c.names))
        .set_expression_attribute_values(Some(c.values))
        .build()
        .map_err(|_| UploadError::ActionBuild)?;
    Ok(TransactWriteItem::builder().put(put).build())
}

fn item_key(item: &Item) -> Item {
    HashMap::from([
        ("pk".into(), item["pk"].clone()),
        ("sk".into(), item["sk"].clone()),
    ])
}
pub(super) fn namespace_guard(
    table: &str,
    ns: &NamespaceSnapshot,
) -> Result<TransactWriteItem, UploadError> {
    guard(table, encode_namespace(ns)?)
}
pub(super) fn guard(table: &str, item: Item) -> Result<TransactWriteItem, UploadError> {
    let c = condition(&item);
    let check = ConditionCheck::builder()
        .table_name(table)
        .set_key(Some(item_key(&item)))
        .condition_expression(c.expression)
        .set_expression_attribute_names(Some(c.names))
        .set_expression_attribute_values(Some(c.values))
        .build()
        .map_err(|_| UploadError::ActionBuild)?;
    Ok(TransactWriteItem::builder().condition_check(check).build())
}
pub(super) fn create(table: &str, item: Item) -> Result<TransactWriteItem, UploadError> {
    let put = Put::builder()
        .table_name(table)
        .set_item(Some(item))
        .condition_expression("attribute_not_exists(pk)")
        .build()
        .map_err(|_| UploadError::ActionBuild)?;
    Ok(TransactWriteItem::builder().put(put).build())
}
pub(super) fn replace(
    table: &str,
    old: Item,
    next: Item,
) -> Result<TransactWriteItem, UploadError> {
    let c = condition(&old);
    let put = Put::builder()
        .table_name(table)
        .set_item(Some(next))
        .condition_expression(c.expression)
        .set_expression_attribute_names(Some(c.names))
        .set_expression_attribute_values(Some(c.values))
        .build()
        .map_err(|_| UploadError::ActionBuild)?;
    Ok(TransactWriteItem::builder().put(put).build())
}

#[cfg(test)]
mod store_binding_tests {
    use super::*;

    // Private synthetic snapshot for native action-shape tests only: this is not
    // a QualifiedFreshNamespace factory or evidence of successful enrollment.
    fn namespace() -> NamespaceSnapshot {
        NamespaceSnapshot {
            domain: DomainId::new("domain").unwrap(),
            incarnation: Uuid::from_u128(1),
            id: NamespaceId::new("namespace").unwrap(),
            configuration_revision: Uuid::from_u128(2),
            store: StoreId::new("store").unwrap(),
            bucket: "test-bucket".into(),
            expected_owner: "123456789012".into(),
            region: "us-west-2".into(),
            key_prefix: "fresh/".into(),
            mpu_max_age_ms: 3_600_000,
            mpu_lifecycle_margin_ms: 3_600_000,
        }
    }

    fn put(table: &str, ns: &NamespaceSnapshot) -> Put {
        let action = store_binding_action(table, ns).unwrap();
        assert!(action.update.is_none());
        assert!(action.delete.is_none());
        assert!(action.condition_check.is_none());
        action.put.unwrap()
    }

    // Inspect SDK construction, never emulate DynamoDB expression evaluation.
    fn assert_full_predicate(put: &Put) {
        let fields = [
            "bucket",
            "domain",
            "expected_owner",
            "pk",
            "provider_profile",
            "record",
            "region",
            "sk",
            "store",
        ];
        assert_eq!(put.item().len(), fields.len());
        let names = put.expression_attribute_names().unwrap();
        let values = put.expression_attribute_values().unwrap();
        assert_eq!(names.len(), fields.len() + 1);
        assert_eq!(values.len(), fields.len());
        assert_eq!(names["#pk"], "pk");
        for (i, field) in fields.iter().enumerate() {
            assert_eq!(&names[&format!("#f{i}")], field);
            assert_eq!(&values[&format!(":v{i}")], &put.item()[*field]);
        }
        assert_eq!(
            put.condition_expression(),
            Some(
                "attribute_not_exists(#pk) OR (#f0 = :v0 AND #f1 = :v1 AND #f2 = :v2 AND #f3 = :v3 AND #f4 = :v4 AND #f5 = :v5 AND #f6 = :v6 AND #f7 = :v7 AND #f8 = :v8)"
            )
        );
    }

    fn canonical_bytes(item: &Item) -> Vec<u8> {
        let fields: BTreeMap<_, _> = item
            .iter()
            .map(|(name, value)| (name, value.as_s().unwrap()))
            .collect();
        serde_json::to_vec(&fields).unwrap()
    }

    #[test]
    fn closed_canonical_row_and_native_absent_or_full_equality() {
        let ns = namespace();
        let put = put("table", &ns);
        assert_eq!(put.table_name(), "table");
        assert_eq!(
            put.item(),
            &Item::from([
                (
                    "pk".into(),
                    s(format!("{}#STORE#73746f7265", prefix(&ns.domain)))
                ),
                ("sk".into(), s("M")),
                ("record".into(), s("store-binding-v1")),
                ("domain".into(), s("domain")),
                ("store".into(), s("store")),
                ("bucket".into(), s("test-bucket")),
                ("expected_owner".into(), s("123456789012")),
                ("region".into(), s("us-west-2")),
                (
                    "provider_profile".into(),
                    s("aws-s3-versioned-lifecycle-v0")
                ),
            ])
        );
        assert_full_predicate(&put);
        decode_store_binding(&ns.domain, put.item()).unwrap();
    }

    #[test]
    fn namespace_prefix_revision_and_lifetime_do_not_change_binding_bytes_or_action() {
        let ns = namespace();
        let original = put("table", &ns);
        let mut other = ns.clone();
        other.key_prefix = "independent/".into();
        other.configuration_revision = Uuid::from_u128(3);
        other.incarnation = Uuid::from_u128(4);
        other.mpu_max_age_ms *= 2;
        other.mpu_lifecycle_margin_ms *= 2;
        for id in [ns.id, NamespaceId::new("another-namespace").unwrap()] {
            other.id = id;
            let mut changed = put("another-table", &other);
            assert_eq!(
                canonical_bytes(original.item()),
                canonical_bytes(changed.item())
            );
            changed.table_name = original.table_name.clone();
            assert_eq!(original, changed);
        }
    }

    #[test]
    fn every_physical_field_is_guarded_and_future_kind_or_profile_is_not_accepted() {
        let ns = namespace();
        let original = put("table", &ns);
        for field in ["bucket", "expected_owner", "region"] {
            let mut other = ns.clone();
            match field {
                "bucket" => other.bucket = "another-bucket".into(),
                "expected_owner" => other.expected_owner = "210987654321".into(),
                "region" => other.region = "us-east-1".into(),
                _ => unreachable!(),
            }
            let changed = put("table", &other);
            assert_full_predicate(&changed);
            assert_ne!(original.item()[field], changed.item()[field]);
            assert_eq!(original.item()["pk"], changed.item()["pk"]);
            assert_ne!(
                original.expression_attribute_values(),
                changed.expression_attribute_values()
            );
        }
        // These immutable discriminator/profile constants cannot be selected by
        // a caller. Assert the native expected predicates, not fake evaluation of
        // an existing row. Real service conditional rejection is separate proof.
        let values = original.expression_attribute_values().unwrap();
        assert_eq!(values[":v5"], s("store-binding-v1"));
        assert_ne!(values[":v5"], s("store-binding-v2"));
        assert_eq!(values[":v4"], s("aws-s3-versioned-lifecycle-v0"));
        assert_ne!(values[":v4"], s("aws-s3-versioned-lifecycle-v1"));
        assert_full_predicate(&original);
    }

    #[test]
    fn addresses_separate_domains_and_stores_and_guard_full_identities() {
        let ns = namespace();
        let original = put("table", &ns);
        for field in ["domain", "store"] {
            let mut other = ns.clone();
            match field {
                "domain" => other.domain = DomainId::new("another-domain").unwrap(),
                "store" => other.store = StoreId::new("é#").unwrap(),
                _ => unreachable!(),
            }
            let changed = put("table", &other);
            assert_ne!(original.item()["pk"], changed.item()["pk"]);
            assert_ne!(original.item()[field], changed.item()[field]);
            assert_full_predicate(&changed);
            if field == "store" {
                assert_eq!(
                    changed.item()["pk"],
                    s(format!("{}#STORE#c3a923", prefix(&ns.domain)))
                );
                assert_eq!(changed.item()["store"], s("é#"));
            } else {
                assert_eq!(changed.item()["domain"], s("another-domain"));
            }
        }
    }

    #[test]
    fn maximum_width_binding_stays_under_four_kib_and_oversize_is_rejected() {
        let mut ns = namespace();
        ns.domain = DomainId::new("é".repeat(64)).unwrap();
        ns.store = StoreId::new("é".repeat(64)).unwrap();
        ns.bucket = "b".repeat(63);
        ns.region = format!("{}-x-1", "r".repeat(60));
        let put = put("table", &ns);
        assert_full_predicate(&put);
        let item_bytes: usize = put
            .item()
            .iter()
            .map(|(name, value)| name.len() + value.as_s().unwrap().len())
            .sum();
        assert!(item_bytes + GROWTH_RESERVE <= MAX_RECORD_BYTES);
        assert!(item_bytes < 4096);
        assert!(put.condition_expression().unwrap().len() < 4096);
        check_size(put.item()).unwrap();
        // Deliberately invalid private fixture proves the action invokes the
        // row-size gate; public qualified configuration cannot carry this value.
        ns.bucket = "b".repeat(4096);
        assert!(matches!(
            store_binding_action("table", &ns),
            Err(UploadError::RecordSize)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(n: u64) -> TimestampMs {
        TimestampMs::new(n).unwrap()
    }
    fn clock(n: u64) -> ClockObservation {
        ClockObservation::trusted(ts(n), 5).unwrap()
    }
    fn policy() -> ClaimPolicy {
        ClaimPolicy::new(2000, 50, 10).unwrap()
    }
    #[test]
    fn whole_bucket_scope_remains_valid_through_metadata_roundtrip() {
        let (mut namespace, intent, _) = fixtures();
        namespace.key_prefix.clear();
        let item = encode_namespace(&namespace).unwrap();
        assert_eq!(
            decode_namespace(&namespace.domain, &namespace.id, &item).unwrap(),
            namespace
        );
        binding(&intent.admission, &namespace).unwrap();
        assert!(prefix_bound("").is_ok());
        assert!(prefix_bound(&"é".repeat(512)).is_ok());
        assert!(matches!(
            prefix_bound(&"é".repeat(513)),
            Err(UploadError::PrefixLength)
        ));
    }

    fn fixtures() -> (NamespaceSnapshot, IntentSnapshot, AttemptSnapshot) {
        let domain = DomainId::new("opaque-domain").unwrap();
        let incarnation = Uuid::new_v4();
        let ns = NamespaceSnapshot {
            domain: domain.clone(),
            incarnation,
            id: NamespaceId::new("opaque-ns").unwrap(),
            configuration_revision: Uuid::new_v4(),
            store: StoreId::new("opaque-store").unwrap(),
            bucket: "test-bucket".into(),
            expected_owner: "123456789012".into(),
            region: "us-west-2".into(),
            key_prefix: "fresh/".into(),
            // Synthetic policy only, not production lifecycle qualification.
            mpu_max_age_ms: 2 * 60 * 60 * 1000,
            mpu_lifecycle_margin_ms: 60 * 60 * 1000,
        };
        let admission = Admission {
            id: ResourceId::new(Scope::new(domain, incarnation, 1).unwrap(), Uuid::new_v4())
                .unwrap(),
            namespace: ns.id.clone(),
            configuration_revision: ns.configuration_revision,
            target: UploadTarget::new(ns.store.clone(), "fresh/object").unwrap(),
            constraints: UploadConstraints::new(100, Some(10), Some([0; 32])).unwrap(),
            revision: Uuid::new_v4(),
            deadline: ts(1600),
        };
        let intent = IntentSnapshot {
            admission,
            state: IntentState::Open,
        };
        let mut admission = intent.admission.clone();
        admission.id = ResourceId::new(admission.id.scope().clone(), Uuid::new_v4()).unwrap();
        admission.revision = Uuid::new_v4();
        admission.deadline = ts(1500);
        let attempt = AttemptSnapshot {
            admission,
            intent: intent.id().clone(),
            state: AttemptState::Active,
            mpu_admitted_lower_bound: ts(995),
            max_mpu_use_until: ts(2100),
        };
        (ns, intent, attempt)
    }

    #[test]
    fn constraints_and_target_are_checked_without_deserialization_bypass() {
        assert!(UploadConstraints::new(i64::MAX as u64 + 1, None, None).is_err());
        assert!(UploadConstraints::new(0, Some(1), None).is_err());
        assert!(UploadConstraints::new(0, Some(0), Some([255; 32])).is_ok());
        for key in [String::new(), "x".repeat(1025)] {
            assert!(UploadTarget::new(StoreId::new("s").unwrap(), key).is_err());
        }
        assert!(UploadTarget::new(StoreId::new("s").unwrap(), "\"\\\n".repeat(341)).is_ok());
    }
    #[test]
    fn native_flat_codec_roundtrips_full_identities_and_rejects_aliases() {
        let (ns, intent, attempt) = fixtures();
        let namespace = encode_namespace(&ns).unwrap();
        assert_eq!(
            decode_namespace(&ns.domain, &ns.id, &namespace).unwrap(),
            ns
        );
        let item = encode_intent(&intent).unwrap();
        assert_eq!(decode_intent(intent.id(), &item).unwrap(), intent);
        assert_eq!(
            decode_attempt(attempt.id(), &encode_attempt(&attempt).unwrap()).unwrap(),
            attempt
        );
        for (field, value) in [
            ("pk", s("wrong-key")),
            ("sk", s("wrong-sort")),
            ("domain", s("other")),
            ("incarnation", s(Uuid::new_v4().to_string())),
            ("resource_uuid", s(Uuid::new_v4().to_string())),
            ("generation", n(2)),
            ("revision", s(Uuid::nil().to_string())),
            ("configuration_revision", s(Uuid::nil().to_string())),
            ("state", s("UNKNOWN")),
            ("record", s("attempt")),
            ("deadline", n(i64::MAX as u64 + 1)),
            ("max_length", n(i64::MAX as u64 + 1)),
            ("expected_length", n(101)),
            ("full_sha256", AttributeValue::B(Blob::new([0; 31]))),
            ("unknown", s("unsupported")),
        ] {
            let mut bad = item.clone();
            bad.insert(field.into(), value);
            assert!(
                decode_intent(intent.id(), &bad).is_err(),
                "accepted {field}"
            );
        }
        for (field, value) in [
            ("intent_domain", s("other")),
            ("intent_incarnation", s(Uuid::new_v4().to_string())),
            ("intent_generation", n(2)),
            ("state", s("UNKNOWN")),
            ("max_mpu_use_until", n(1499)),
            ("mpu_admitted_lower_bound", n(1500)),
            ("mpu_admitted_lower_bound", n(i64::MAX as u64 + 1)),
            ("mpu_admitted_lower_bound", s("995")),
        ] {
            let mut bad = encode_attempt(&attempt).unwrap();
            bad.insert(field.into(), value);
            assert!(
                decode_attempt(attempt.id(), &bad).is_err(),
                "accepted {field}"
            );
        }
        for (field, value) in [
            ("expected_owner", s("123")),
            ("expected_owner", s("12345678901x")),
            ("provider_profile", s("lifecycle-assessed-only")),
            ("state", s("INACTIVE")),
            ("key_prefix", s("x".repeat(1025))),
            ("configuration_revision", s(Uuid::nil().to_string())),
            ("domain", s("other")),
            ("namespace_id", s("other")),
            ("pk", s("alias")),
        ] {
            let mut bad = namespace.clone();
            bad.insert(field.into(), value);
            assert!(
                decode_namespace(&ns.domain, &ns.id, &bad).is_err(),
                "accepted namespace {field}"
            );
        }
    }
    #[test]
    fn mpu_policy_scalars_and_required_fields_fail_closed() {
        let (ns, _, attempt) = fixtures();
        let item = encode_namespace(&ns).unwrap();
        for field in ["mpu_max_age_ms", "mpu_lifecycle_margin_ms"] {
            let mut bad = item.clone();
            bad.remove(field);
            assert!(decode_namespace(&ns.domain, &ns.id, &bad).is_err());
            for value in [
                n(0),
                n(i64::MAX as u64),
                n(i64::MAX as u64 + 1),
                n(u64::MAX),
                s("1"),
                AttributeValue::N("01".into()),
                AttributeValue::N("-1".into()),
                AttributeValue::N("1.5".into()),
            ] {
                let mut bad = item.clone();
                bad.insert(field.into(), value);
                assert!(decode_namespace(&ns.domain, &ns.id, &bad).is_err());
            }
        }
        let mut bad = item;
        bad.insert("unrecognized_policy".into(), n(1));
        assert!(decode_namespace(&ns.domain, &ns.id, &bad).is_err());
        let mut boundary = ns.clone();
        boundary.mpu_max_age_ms = TimestampMs::MAX.as_millis() - 1;
        boundary.mpu_lifecycle_margin_ms = 1;
        assert!(encode_namespace(&boundary).is_ok());
        boundary.mpu_lifecycle_margin_ms = 2;
        assert!(encode_namespace(&boundary).is_err());
        let mut bad = encode_attempt(&attempt).unwrap();
        bad.remove("mpu_admitted_lower_bound");
        assert!(decode_attempt(attempt.id(), &bad).is_err());
    }
    #[test]
    fn hour_scale_mpu_age_is_independent_of_minute_claim_renewals() {
        let (ns, mut intent, mut attempt) = fixtures();
        let policy = ClaimPolicy::new(60_000, 50, 10).unwrap();
        let admitted = clock(1000).lower_bound().unwrap();
        let ceiling = admitted.checked_add(ns.mpu_max_age_ms()).unwrap();
        attempt.mpu_admitted_lower_bound = admitted;
        attempt.max_mpu_use_until = ceiling;
        intent.admission.deadline = ts(60_000);
        attempt.admission.deadline = ts(60_000);
        new_deadline(attempt.deadline(), clock(1000), &policy).unwrap();
        assert!(matches!(
            new_deadline(ceiling, clock(1000), &policy),
            Err(UploadError::Claim(ClaimError::LifetimeExceeded))
        ));
        mpu_age_binding(admitted, attempt.deadline(), ceiling, &ns).unwrap();
        assert!(
            mpu_age_binding(
                admitted,
                attempt.deadline(),
                ceiling.checked_add(1).unwrap(),
                &ns
            )
            .is_err()
        );
        assert!(
            mpu_age_binding(TimestampMs::MAX, TimestampMs::MAX, TimestampMs::MAX, &ns).is_err()
        );
        assert!(matches!(
            mpu_age_binding(
                ts(TimestampMs::MAX.as_millis() - 2),
                ts(TimestampMs::MAX.as_millis() - 1),
                TimestampMs::MAX,
                &ns
            ),
            Err(UploadError::Clock(ClockError::Overflow))
        ));
        // Continue across the initial minute horizon; each claim is independently
        // renewed while live, but the original MPU age binding never moves.
        for now in [40_000, 80_000, 120_000] {
            live_attempt(&attempt, &intent, &ns, clock(now), &policy).unwrap();
            for admission in [&mut intent.admission, &mut attempt.admission] {
                let renewal =
                    Renewal::new(admission.revision, ts(now + 60_000), Uuid::new_v4()).unwrap();
                renew(admission, &renewal, clock(now), &policy).unwrap();
            }
            let decoded = decode_attempt(attempt.id(), &encode_attempt(&attempt).unwrap()).unwrap();
            assert_eq!(decoded.mpu_admitted_lower_bound(), admitted);
            assert_eq!(decoded.max_mpu_use_until(), ceiling);
        }
        live_attempt(&attempt, &intent, &ns, clock(150_000), &policy).unwrap();
        let mut impossible = attempt.clone();
        impossible.max_mpu_use_until = ceiling.checked_add(1).unwrap();
        assert!(live_attempt(&impossible, &intent, &ns, clock(150_000), &policy).is_err());
        impossible = attempt.clone();
        impossible.mpu_admitted_lower_bound = ts(160_000);
        assert!(live_attempt(&impossible, &intent, &ns, clock(150_000), &policy).is_err());
    }
    #[test]
    fn authorization_samples_current_time_once_and_propagates_clock_failures() {
        let (ns, intent, attempt) = fixtures();
        // Models time advancing while the async caller reads: creation of the
        // sampler does not capture an observation. No fake database evaluator.
        let time = std::cell::Cell::new(1000);
        let samples = std::cell::Cell::new(0);
        let sampler = || {
            samples.set(samples.get() + 1);
            Ok(clock(time.get()))
        };
        time.set(1495); // deadline == fresh upper bound: expired, not authorized
        assert!(matches!(
            authorize_observed(
                ns.clone(),
                intent.clone(),
                attempt.clone(),
                sampler,
                &policy()
            ),
            Err(UploadError::Claim(ClaimError::Expired))
        ));
        assert_eq!(samples.get(), 1);
        for error in [
            ClockError::Untrusted,
            ClockError::Overflow,
            ClockError::ExcessiveUncertainty,
        ] {
            assert!(
                matches!(authorize_observed(ns.clone(), intent.clone(), attempt.clone(), || Err(error), &policy()),
                Err(UploadError::Clock(actual)) if actual == error)
            );
        }
        let owned_clock_state = Box::new(1000);
        let authorization = authorize_observed(
            ns,
            intent,
            attempt,
            move || {
                let now = *owned_clock_state;
                drop(owned_clock_state); // a genuinely FnOnce sampler
                Ok(clock(now))
            },
            &policy(),
        )
        .unwrap();
        assert_eq!(authorization.valid_until(), ts(1500));
    }
    #[test]
    fn live_bindings_reject_wrong_tuple_namespace_config_and_incarnation() {
        let (ns, intent, attempt) = fixtures();
        assert!(live_attempt(&attempt, &intent, &ns, clock(1000), &policy()).is_ok());
        for field in [
            "store",
            "key",
            "config",
            "namespace",
            "domain",
            "incarnation",
            "constraints",
            "parent",
        ] {
            let mut bad = attempt.clone();
            match field {
                "store" => bad.admission.target.store = StoreId::new("other").unwrap(),
                "key" => bad.admission.target.key = "fresh/other".into(),
                "config" => bad.admission.configuration_revision = Uuid::new_v4(),
                "namespace" => bad.admission.namespace = NamespaceId::new("other").unwrap(),
                "domain" | "incarnation" => {
                    bad.admission.id = ResourceId::new(
                        Scope::new(
                            if field == "domain" {
                                DomainId::new("other").unwrap()
                            } else {
                                ns.domain.clone()
                            },
                            if field == "incarnation" {
                                Uuid::new_v4()
                            } else {
                                ns.incarnation
                            },
                            1,
                        )
                        .unwrap(),
                        bad.id().uuid(),
                    )
                    .unwrap()
                }
                "constraints" => {
                    bad.admission.constraints = UploadConstraints::new(101, None, None).unwrap()
                }
                "parent" => {
                    bad.intent =
                        ResourceId::new(intent.id().scope().clone(), Uuid::new_v4()).unwrap()
                }
                _ => unreachable!(),
            }
            assert!(
                live_attempt(&bad, &intent, &ns, clock(1000), &policy()).is_err(),
                "accepted {field}"
            );
        }
        let mut outside = intent.clone();
        outside.admission.target.key = "historical/object".into();
        assert!(binding(&outside.admission, &ns).is_err());
    }
    #[test]
    fn conservative_lifetimes_independent_attempt_age_and_fixed_renewal() {
        let (ns, intent, mut attempt) = fixtures();
        for deadline in [1005, 1055, 3006] {
            assert!(new_deadline(ts(deadline), clock(1000), &policy()).is_err());
        }
        assert!(new_deadline(ts(1056), clock(1000), &policy()).is_ok());
        assert!(new_deadline(ts(3005), clock(1000), &policy()).is_ok());
        assert!(live_attempt(&attempt, &intent, &ns, clock(1495), &policy()).is_err());
        let renewal = Renewal::new(attempt.revision(), ts(1800), Uuid::new_v4()).unwrap();
        let mut first = attempt.admission.clone();
        let mut retry = first.clone();
        renew(&mut first, &renewal, clock(1000), &policy()).unwrap();
        renew(&mut retry, &renewal, clock(1100), &policy()).unwrap();
        assert_eq!(first, retry);
        assert_eq!(first.deadline, ts(1800));
        assert!(renew(&mut first, &renewal, clock(1100), &policy()).is_err());
        assert!(
            live_attempt(
                &attempt,
                &intent,
                &ns,
                ClockObservation::untrusted(ts(1000), 0),
                &policy()
            )
            .is_err()
        );
        assert!(
            live_attempt(
                &attempt,
                &intent,
                &ns,
                ClockObservation::trusted(ts(1000), 11).unwrap(),
                &policy()
            )
            .is_err()
        );
        attempt.admission.deadline = ts(2100);
        attempt.max_mpu_use_until = ts(1005);
        assert!(live_attempt(&attempt, &intent, &ns, clock(1000), &policy()).is_err());
        let mut cancelled = intent.clone();
        cancelled.state = IntentState::Cancelled;
        assert!(live_intent(&cancelled, &ns, clock(1000), &policy()).is_err());
        attempt.state = AttemptState::Sealed;
        assert!(live_attempt(&attempt, &intent, &ns, clock(1000), &policy()).is_err());
    }
    #[test]
    fn native_shapes_guard_full_records_and_absent_optional_constraints() {
        let (ns, mut intent, attempt) = fixtures();
        intent.admission.constraints = UploadConstraints::new(100, None, None).unwrap();
        let item = encode_intent(&intent).unwrap();
        let action = guard("table", item.clone()).unwrap();
        let check = action.condition_check.unwrap();
        assert!(
            check
                .condition_expression()
                .contains("attribute_not_exists(#missing_expected_length)")
        );
        assert!(
            check
                .condition_expression()
                .contains("attribute_not_exists(#missing_full_sha256)")
        );
        assert_eq!(
            check.expression_attribute_values().unwrap().len(),
            item.len()
        );
        let names = check.expression_attribute_names().unwrap();
        for field in item.keys() {
            assert!(names.values().any(|v| v == field));
        }
        let ns_guard = namespace_guard("table", &ns)
            .unwrap()
            .condition_check
            .unwrap();
        for field in [
            "bucket",
            "expected_owner",
            "region",
            "key_prefix",
            "provider_profile",
            "mpu_max_age_ms",
            "mpu_lifecycle_margin_ms",
            "configuration_revision",
            "state",
        ] {
            assert!(
                ns_guard
                    .expression_attribute_names()
                    .unwrap()
                    .values()
                    .any(|v| v == field)
            );
        }
        assert!(check.condition_expression().len() < 4096);
        let put = create("table", encode_attempt(&attempt).unwrap())
            .unwrap()
            .put
            .unwrap();
        assert_eq!(put.condition_expression(), Some("attribute_not_exists(pk)"));
        let mut sealed = attempt.clone();
        sealed.state = AttemptState::Sealed;
        let put = replace(
            "table",
            encode_attempt(&attempt).unwrap(),
            encode_attempt(&sealed).unwrap(),
        )
        .unwrap()
        .put
        .unwrap();
        assert_eq!(put.item().get("state"), Some(&s("SEALED")));
        for field in ["mpu_admitted_lower_bound", "max_mpu_use_until"] {
            assert_eq!(
                put.item().get(field),
                encode_attempt(&attempt).unwrap().get(field)
            );
            assert!(
                put.expression_attribute_names()
                    .unwrap()
                    .values()
                    .any(|v| v == field)
            );
        }
        assert!(put.condition_expression().unwrap().len() < 4096);
    }
    #[test]
    fn actual_native_bytes_include_names_keys_binary_and_growth_reserve() {
        let mut item = Item::from([(
            "x".into(),
            s("x".repeat(MAX_RECORD_BYTES - GROWTH_RESERVE - 1)),
        )]);
        assert!(check_size(&item).is_ok());
        item.insert("y".into(), s(""));
        assert!(matches!(check_size(&item), Err(UploadError::RecordSize)));
        let (mut ns, mut intent, mut attempt) = fixtures();
        ns.domain = DomainId::new("d".repeat(128)).unwrap();
        ns.id = NamespaceId::new("n".repeat(128)).unwrap();
        ns.store = StoreId::new("s".repeat(128)).unwrap();
        ns.bucket = "b".repeat(63);
        ns.region = "r".repeat(64);
        ns.key_prefix = "p".repeat(1024);
        ns.mpu_max_age_ms = TimestampMs::MAX.as_millis() / 2;
        ns.mpu_lifecycle_margin_ms = TimestampMs::MAX.as_millis() / 2;
        let namespace = encode_namespace(&ns).unwrap();
        assert_eq!(
            decode_namespace(&ns.domain, &ns.id, &namespace).unwrap(),
            ns
        );
        // Native strings do not suffer JSON-escape expansion. All supported
        // field maxima still fit; later selection payloads stay separate.
        let domain = DomainId::new("d".repeat(128)).unwrap();
        let scope = Scope::new(domain, intent.id().scope().incarnation(), u64::MAX).unwrap();
        intent.admission.id = ResourceId::new(scope.clone(), intent.id().uuid()).unwrap();
        intent.admission.target =
            UploadTarget::new(StoreId::new("s".repeat(128)).unwrap(), "\"".repeat(1024)).unwrap();
        intent.admission.namespace = NamespaceId::new("n".repeat(128)).unwrap();
        intent.admission.deadline = TimestampMs::MAX;
        intent.admission.constraints =
            UploadConstraints::new(i64::MAX as u64, Some(i64::MAX as u64), Some([255; 32]))
                .unwrap();
        attempt.admission = intent.admission.clone();
        attempt.admission.id = ResourceId::new(scope, Uuid::new_v4()).unwrap();
        attempt.intent = intent.id().clone();
        attempt.max_mpu_use_until = TimestampMs::MAX;
        attempt.mpu_admitted_lower_bound = ts(TimestampMs::MAX.as_millis() - 1);
        let item = encode_attempt(&attempt).unwrap();
        assert_eq!(decode_attempt(attempt.id(), &item).unwrap(), attempt);
        assert_eq!(
            decode_intent(intent.id(), &encode_intent(&intent).unwrap()).unwrap(),
            intent
        );
    }
}
