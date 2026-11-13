//! Finite, independent consuming-use protection (protocol §7).
//!
//! Snapshots are checked metadata observations, NOT usable S3 handles. Acquisition
//! guards application-supplied native permission/pointer conditions and a typed
//! library source. Renewal and terminal cleanup never depend on that old source.
//! authorize_use coherently reads pin/version/namespace/control and samples a
//! fresh trusted clock before issuing authority. No cancellation guarantee,
//! self-renewing task, S3 operation, scanner or metadata compactor is provided.

use std::collections::HashMap;

use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Get, TransactGetItem, TransactWriteItem, Update,
};
use thiserror::Error;
use uuid::Uuid;

use super::journal::{control_key, parse_control};
use super::references::{object_key, source_action, version_count_action};
use super::uploads::{
    check_size, create, decode_namespace, encode_intent, encode_staged, exact, guard, n,
    namespace_guard, namespace_key, number, replace, resource_key, s, selected_binding, text, uuid,
};
use super::{
    ApplicationActions, CommandContext, CommandJournal, IntentSnapshot, JournalError,
    NamespaceSnapshot, PreparedCommand, ReferenceError, ReferenceSnapshot, StagedSnapshot,
    UploadError,
};
use crate::claim::{Claim, ClaimError, ClaimPolicy, ClaimState, Renewal};
use crate::clock::{ClockError, ClockObservation, TimestampMs};
use crate::identity::{
    DomainId, IdentityError, NamespaceId, ResourceId, Scope, StoreId, VersionRef,
};

type Item = HashMap<String, AttributeValue>;

#[derive(Debug, Error)]
pub enum ProtectionError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Reference(#[from] ReferenceError),
    #[error(transparent)]
    Claim(#[from] ClaimError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error("use protection identity, namespace, routing or creation provenance mismatch")]
    Binding,
    #[error("version or use pin is terminal")]
    Terminal,
    #[error("invalid canonical protection record; no absence inferred")]
    Corrupt,
    #[error("native protection action construction failed")]
    ActionBuild,
    #[error("native use cleanup conditional conflict; reread rows, not a historical abort result")]
    Conflict,
    #[error("native use cleanup outcome UNKNOWN; reread rows, do not infer rollback: {0}")]
    OutcomeUnknown(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersionState {
    Ready,
    Retiring,
    Deleted,
}

// Immutable selection-created HISTORICAL provenance; pin records retain it
// independently of source removal and authorized domain-incarnation rotation.
// It is not current ephemeral authority. No public raw identity factory.
#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionIdentity {
    version: VersionRef,
    namespace: NamespaceId,
    configuration_revision: Uuid,
    creation: ResourceId,
}

/// Strong observation of an existing selection-created version, not authority.
/// Counts are observations, never caller-supplied deltas or CAS requirements.
/// ```compile_fail
/// use storelink::dynamodb::VersionSnapshot;
/// let _: VersionSnapshot = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSnapshot {
    identity: VersionIdentity,
    pub(super) state: VersionState,
    references: u64,
    claims: u64,
}
impl VersionSnapshot {
    pub fn version(&self) -> &VersionRef {
        &self.identity.version
    }
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.identity.namespace
    }
    pub fn configuration_revision(&self) -> Uuid {
        self.identity.configuration_revision
    }
    pub fn creation_id(&self) -> &ResourceId {
        &self.identity.creation
    }
    pub fn state(&self) -> VersionState {
        self.state
    }
    pub fn references(&self) -> u64 {
        self.references
    }
    pub fn claims(&self) -> u64 {
        self.claims
    }
}

/// Independently counted finite pin metadata. Not an I/O authorization. Pure
/// Claim mutations cannot mutate this opaque snapshot or persisted accounting.
/// ```compile_fail
/// use storelink::dynamodb::UsePinSnapshot;
/// let _: UsePinSnapshot = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsePinSnapshot {
    identity: VersionIdentity,
    claim: Claim,
}
impl UsePinSnapshot {
    pub fn claim(&self) -> &Claim {
        &self.claim
    }
    pub fn version(&self) -> &VersionRef {
        &self.identity.version
    }
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.identity.namespace
    }
    pub fn configuration_revision(&self) -> Uuid {
        self.identity.configuration_revision
    }
    pub fn version_creation_id(&self) -> &ResourceId {
        &self.identity.creation
    }
}

/// Only opaque observed rows are accepted; a tuple or verified upload receipt
/// cannot authorize acquiring protection. Application permission/pointer guards
/// remain the caller's responsibility in ApplicationActions.
/// ```compile_fail,E0308
/// use storelink::{dynamodb::UseSource, identity::VersionRef};
/// fn raw_tuple_is_not_authority(version: &VersionRef) {
///     let _ = UseSource::Reference(version);
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub enum UseSource<'a> {
    Reference(&'a ReferenceSnapshot),
    Staged {
        staged: &'a StagedSnapshot,
        intent: &'a IntentSnapshot,
    },
}

/// Opaque, freshly validated independent use authority, not a renewable handle.
/// Only `CommandJournal::authorize_use` constructs this after a coherent native
/// read. Consumers must revalidate time after suspension and periodically reread.
/// No source reference or staging lifetime is inherited by the acquired pin.
/// ```compile_fail
/// use storelink::dynamodb::UseAuthorization;
/// let _: UseAuthorization = serde_json::from_str("{}").unwrap();
/// ```
pub struct UseAuthorization {
    pin: UsePinSnapshot,
    version: VersionSnapshot,
    namespace: NamespaceSnapshot,
}
impl UseAuthorization {
    pub fn pin(&self) -> &UsePinSnapshot {
        &self.pin
    }
    pub fn version(&self) -> &VersionSnapshot {
        &self.version
    }
    pub fn namespace(&self) -> &NamespaceSnapshot {
        &self.namespace
    }
    pub fn valid_until(&self) -> TimestampMs {
        self.pin.claim.deadline()
    }

    /// Ignores only mutable claim deadlines/revisions and aggregate counts.
    #[cfg(feature = "s3")]
    pub(crate) fn same_binding(&self, other: &Self) -> bool {
        self.pin.claim.id() == other.pin.claim.id()
            && self.pin.identity == other.pin.identity
            && self.namespace == other.namespace
    }
}

impl CommandJournal {
    /// Discovery identifies keys only. The four-row TransactGet rereads control,
    /// pin, version and namespace in one serializable view. Missing, malformed or
    /// failed reads never grant authority. Closed creation generations remain
    /// usable; only the pin's incarnation (not historical version provenance)
    /// must be current. The synchronous sampler must return fresh trusted time.
    pub async fn authorize_use(
        &self,
        id: &ResourceId,
        sample_clock: impl FnOnce() -> Result<ClockObservation, ClockError>,
        policy: &ClaimPolicy,
    ) -> Result<UseAuthorization, ProtectionError> {
        let discovery = self.read_use_pin(id).await?.ok_or(UploadError::Missing)?;
        let gets = [
            control_key(self.domain_id()),
            resource_key("USE", id),
            object_key(self.domain_id(), &discovery.version().fingerprint()),
            namespace_key(self.domain_id(), discovery.namespace_id()),
        ]
        .into_iter()
        .map(|key| {
            Get::builder()
                .table_name(self.coordination_arn())
                .set_key(Some(key))
                .build()
                .map(|get| TransactGetItem::builder().get(get).build())
                .map_err(|_| ProtectionError::ActionBuild)
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
            return Err(ProtectionError::Corrupt);
        }
        let fence = parse_control(rows[0].item.as_ref(), self.domain_id())?;
        let pin = decode_pin(id, rows[1].item.as_ref().ok_or(UploadError::Missing)?)?;
        let version = decode_version(
            self.domain_id(),
            discovery.version(),
            rows[2].item.as_ref().ok_or(UploadError::Missing)?,
        )?;
        let namespace = decode_namespace(
            self.domain_id(),
            discovery.namespace_id(),
            rows[3].item.as_ref().ok_or(UploadError::Missing)?,
        )?;
        if fence.incarnation() != id.scope().incarnation()
            || id.scope().generation() > fence.current()
            || discovery.identity != pin.identity
            || pin.identity != version.identity
        {
            return Err(ProtectionError::Binding);
        }
        if version.state != VersionState::Ready || version.claims == 0 {
            return Err(ProtectionError::Terminal);
        }
        namespace_binding(
            &version.identity,
            self.domain_id(),
            self.native_client().config().region().map(|r| r.as_ref()),
            &namespace,
        )?;
        pin.claim.validate_use(sample_clock()?, policy)?;
        // No await between fresh time validation and issuing this opaque value.
        Ok(UseAuthorization {
            pin,
            version,
            namespace,
        })
    }

    /// Strong native read. Missing is distinct from corrupt or failed reads.
    pub async fn read_version(
        &self,
        version: &VersionRef,
    ) -> Result<Option<VersionSnapshot>, ProtectionError> {
        self.read_upload_item(object_key(self.domain_id(), &version.fingerprint()))
            .await?
            .as_ref()
            .map(|row| decode_version(self.domain_id(), version, row))
            .transpose()
    }

    /// The acquisition identity is ResourceId::from_command(prepared.token().id()).
    /// A read snapshot or historical successful result does not authorize S3 I/O.
    pub async fn read_use_pin(
        &self,
        id: &ResourceId,
    ) -> Result<Option<UsePinSnapshot>, ProtectionError> {
        if id.scope().domain() != self.domain_id() {
            return Err(ProtectionError::Binding);
        }
        self.read_upload_item(resource_key("USE", id))
            .await?
            .as_ref()
            .map(|row| decode_pin(id, row))
            .transpose()
    }

    /// Prepare one finite pin plus its ONE version claim increment, source and
    /// full namespace guards, native application actions and journal outcome.
    /// Pin ID/revision come from this acquiring command: no arbitrary reused ID.
    /// Retry the frozen PreparedCommand; never recalculate its absolute deadline.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_acquire_use(
        &self,
        context: CommandContext,
        application: ApplicationActions,
        version: &VersionSnapshot,
        namespace: &NamespaceSnapshot,
        source: UseSource<'_>,
        deadline: TimestampMs,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, ProtectionError> {
        // Durable version provenance can predate an authorized restore. The
        // builder checks its domain and same-incarnation generation ordering;
        // ephemeral staging sources must still belong to the current command.
        if let UseSource::Staged { staged, intent } = source {
            self.existing_scope(&context, intent.id())?;
            self.existing_scope(&context, staged.claim().id())?;
        }
        let actions = acquire_actions(
            self.coordination_arn(),
            self.domain_id(),
            self.native_client().config().region().map(|r| r.as_ref()),
            context.id(),
            version,
            namespace,
            source,
            deadline,
            clock,
            policy,
        )?;
        Ok(self.prepare_internal(context, application, actions, result)?)
    }

    /// Renew the independent pin at a fixed expected revision/deadline. The old
    /// reference may have been unlinked, or staging consumed/cancelled: neither
    /// source is rechecked. Namespace and READY version provenance still match.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_renew_use(
        &self,
        context: CommandContext,
        pin: &UsePinSnapshot,
        version: &VersionSnapshot,
        namespace: &NamespaceSnapshot,
        renewal: &Renewal,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, ProtectionError> {
        self.existing_scope(&context, pin.claim.id())?;
        let actions = renew_actions(
            self.coordination_arn(),
            self.domain_id(),
            self.native_client().config().region().map(|r| r.as_ref()),
            pin,
            version,
            namespace,
            renewal,
            clock,
            policy,
        )?;
        self.protection_command(context, actions, result)
    }

    /// ACTIVE -> RELEASED and claims -1 atomically. No clock, namespace or source
    /// liveness is required, including after expiry. A terminal pin cannot release
    /// twice; a stale ACTIVE snapshot fails the full expected-row condition.
    pub fn prepare_release_use(
        &self,
        context: CommandContext,
        pin: &UsePinSnapshot,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, ProtectionError> {
        self.existing_scope(&context, pin.claim.id())?;
        let actions = release_actions(self.coordination_arn(), self.domain_id(), pin)?;
        self.protection_command(context, actions, result)
    }

    /// Natural bounded cleanup: strong validated control read, full observed
    /// control/pin guards and READY version claims -1. No new receipt or ID.
    /// Closed generations and different VALID incarnations permit terminal
    /// cleanup; future generations in the same incarnation do not.
    /// One SDK transaction attempt. Any error requires rereading native rows;
    /// absence/errors never establish historical commit/rollback. Terminal rows
    /// await later compaction. This neither cancels issued I/O nor completes GC.
    pub async fn revoke_expired_use(
        &self,
        pin: &UsePinSnapshot,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<(), ProtectionError> {
        let control = self
            .read_upload_item(control_key(self.domain_id()))
            .await?
            .ok_or(JournalError::MissingControl)?;
        let actions = revoke_actions(
            self.coordination_arn(),
            self.domain_id(),
            control,
            pin,
            clock,
            policy,
        )?;
        self.native_client()
            .transact_write_items()
            .set_transact_items(Some(actions))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| revocation_error(e.as_service_error(), e.to_string()))?;
        Ok(())
    }

    fn protection_command(
        &self,
        context: CommandContext,
        actions: Vec<TransactWriteItem>,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, ProtectionError> {
        Ok(self.prepare_internal(
            context,
            ApplicationActions::new(vec![]).map_err(JournalError::from)?,
            actions,
            result,
        )?)
    }
}

fn namespace_binding(
    identity: &VersionIdentity,
    domain: &DomainId,
    region: Option<&str>,
    ns: &NamespaceSnapshot,
) -> Result<(), ProtectionError> {
    if identity.creation.scope().domain() != domain
        || ns.domain() != domain
        || &identity.namespace != ns.id()
        || identity.configuration_revision != ns.configuration_revision()
        || identity.version.store() != ns.store()
        || !identity.version.key().starts_with(ns.key_prefix())
        || region != Some(ns.region())
    {
        return Err(ProtectionError::Binding);
    }
    // Full native namespace equality below binds bucket/owner/region/profile to
    // this exact owning namespace/configuration, not any namespace sharing StoreId.
    // Enrollment's permanent StoreId binding prevents later physical rebinding.
    // Namespace incarnation may change under an authorized restore without
    // rewriting immutable version provenance. The full namespace guard retains
    // the observed incarnation; this is not a restore or migration operation.
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn acquire_actions(
    table: &str,
    domain: &DomainId,
    region: Option<&str>,
    command: &crate::identity::CommandId,
    version: &VersionSnapshot,
    namespace: &NamespaceSnapshot,
    source: UseSource<'_>,
    deadline: TimestampMs,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, ProtectionError> {
    let scope = command.scope();
    let creation = version.creation_id().scope();
    if scope.domain() != domain
        || scope.domain() != creation.domain()
        || (scope.incarnation() == creation.incarnation()
            && scope.generation() < creation.generation())
    {
        return Err(ProtectionError::Binding);
    }
    if version.state != VersionState::Ready {
        return Err(ProtectionError::Terminal);
    }
    namespace_binding(&version.identity, domain, region, namespace)?;
    let mut actions = vec![namespace_guard(table, namespace)?];
    match source {
        UseSource::Reference(reference) => {
            if reference.version() != version.version() {
                return Err(ProtectionError::Binding);
            }
            actions.push(source_action(table, domain, reference)?);
        }
        UseSource::Staged { staged, intent } => {
            selected_binding(domain, staged, intent)?;
            if staged.version() != version.version()
                || staged.namespace_id() != version.namespace_id()
                || staged.configuration_revision() != version.configuration_revision()
                || staged.claim().id() != version.creation_id()
            {
                return Err(ProtectionError::Binding);
            }
            staged.claim().validate_use(clock, policy)?;
            actions.push(guard(table, encode_intent(intent)?)?);
            actions.push(guard(table, encode_staged(staged)?)?);
        }
    }
    let pin = UsePinSnapshot {
        identity: version.identity.clone(),
        claim: Claim::new(
            ResourceId::from_command(command),
            version.version().clone(),
            command.uuid(),
            deadline,
            clock,
            policy,
        )?,
    };
    actions.push(create(table, encode_pin(&pin)?)?);
    actions.push(count_action(table, domain, &pin.identity, 1)?);
    Ok(actions)
}

#[allow(clippy::too_many_arguments)]
fn renew_actions(
    table: &str,
    domain: &DomainId,
    region: Option<&str>,
    pin: &UsePinSnapshot,
    version: &VersionSnapshot,
    ns: &NamespaceSnapshot,
    renewal: &Renewal,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, ProtectionError> {
    if pin.identity != version.identity {
        return Err(ProtectionError::Binding);
    }
    if version.state != VersionState::Ready {
        return Err(ProtectionError::Terminal);
    }
    namespace_binding(&pin.identity, domain, region, ns)?;
    let mut next = pin.clone();
    next.claim.renew(renewal, clock, policy)?;
    Ok(vec![
        namespace_guard(table, ns)?,
        replace(table, encode_pin(pin)?, encode_pin(&next)?)?,
        count_action(table, domain, &pin.identity, 0)?,
    ])
}

fn release_actions(
    table: &str,
    domain: &DomainId,
    pin: &UsePinSnapshot,
) -> Result<Vec<TransactWriteItem>, ProtectionError> {
    let mut next = pin.clone();
    if !next
        .claim
        .release(pin.claim.revision())?
        .accounting_must_change()
    {
        return Err(ProtectionError::Terminal);
    }
    terminal_actions(table, domain, pin, &next)
}
fn terminal_actions(
    table: &str,
    domain: &DomainId,
    pin: &UsePinSnapshot,
    next: &UsePinSnapshot,
) -> Result<Vec<TransactWriteItem>, ProtectionError> {
    Ok(vec![
        replace(table, encode_pin(pin)?, encode_pin(next)?)?,
        count_action(table, domain, &pin.identity, -1)?,
    ])
}
fn revoke_actions(
    table: &str,
    domain: &DomainId,
    control: Item,
    pin: &UsePinSnapshot,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, ProtectionError> {
    let fence = parse_control(Some(&control), domain)?;
    let scope = pin.claim.id().scope();
    if scope.domain() != domain
        || (fence.incarnation() == scope.incarnation() && scope.generation() > fence.current())
    {
        return Err(ProtectionError::Binding);
    }
    let mut next = pin.clone();
    if !next
        .claim
        .revoke(pin.claim.revision(), clock, policy)?
        .accounting_must_change()
    {
        return Err(ProtectionError::Terminal);
    }
    let mut actions = vec![guard(table, control)?];
    actions.extend(terminal_actions(table, domain, pin, &next)?);
    Ok(ApplicationActions::new(actions)
        .map_err(JournalError::from)?
        .into_native())
}
fn revocation_error(error: Option<&TransactWriteItemsError>, message: String) -> ProtectionError {
    if let Some(TransactWriteItemsError::TransactionCanceledException(e)) = error
        && e.cancellation_reasons()
            .iter()
            .any(|r| r.code() == Some("ConditionalCheckFailed"))
    {
        return ProtectionError::Conflict;
    }
    ProtectionError::OutcomeUnknown(message)
}

// Extend the shared counter engine's SINGLE native action, not a second action
// against the same item, nor an independent counter implementation. The original
// full tuple, READY and nonnegative/underflow conditions remain unchanged.
fn count_action(
    table: &str,
    domain: &DomainId,
    identity: &VersionIdentity,
    delta: i64,
) -> Result<TransactWriteItem, ProtectionError> {
    if identity.creation.scope().domain() != domain {
        return Err(ProtectionError::Binding);
    }
    let mut action = version_count_action(
        table,
        domain,
        &identity.version.fingerprint(),
        &identity.version,
        0,
        delta,
    )?;
    let (condition, names, values) = if let Some(u) = &action.update {
        (
            u.condition_expression().unwrap(),
            u.expression_attribute_names().unwrap(),
            u.expression_attribute_values().unwrap(),
        )
    } else {
        let c = action.condition_check.as_ref().unwrap();
        (
            c.condition_expression(),
            c.expression_attribute_names().unwrap(),
            c.expression_attribute_values().unwrap(),
        )
    };
    let (mut condition, mut names, mut values) =
        (condition.to_owned(), names.clone(), values.clone());
    let mut provenance = Item::new();
    insert_provenance(&mut provenance, identity);
    provenance.insert("kind".into(), s("VERSION"));
    let mut fields: Vec<_> = provenance.into_iter().collect();
    fields.sort_by(|a, b| a.0.cmp(&b.0));
    for (i, (field, value)) in fields.into_iter().enumerate() {
        let name = format!("#use{i}");
        let parameter = format!(":use{i}");
        condition.push_str(&format!(" AND {name} = {parameter}"));
        names.insert(name, field);
        values.insert(parameter, value);
    }
    if delta > 0 {
        // This private caller increments exactly one claim. Keep its result in
        // the checked u64 codec's range; native ADD is not Rust overflow checking.
        // Other writers retain their existing contracts. Out-of-range/corrupt
        // observations fail closed rather than being truncated or treated absent.
        condition.push_str(" AND #claims < :use_max_claims");
        values.insert(":use_max_claims".into(), n(u64::MAX));
    }
    if let Some(update) = action.update.take() {
        Ok(TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name(update.table_name)
                    .set_key(Some(update.key))
                    .update_expression(update.update_expression)
                    .set_return_values_on_condition_check_failure(
                        update.return_values_on_condition_check_failure,
                    )
                    .condition_expression(condition)
                    .set_expression_attribute_names(Some(names))
                    .set_expression_attribute_values(Some(values))
                    .build()
                    .map_err(|_| ProtectionError::ActionBuild)?,
            )
            .build())
    } else {
        let check = action.condition_check.take().unwrap();
        Ok(TransactWriteItem::builder()
            .condition_check(
                ConditionCheck::builder()
                    .table_name(check.table_name)
                    .set_key(Some(check.key))
                    .set_return_values_on_condition_check_failure(
                        check.return_values_on_condition_check_failure,
                    )
                    .condition_expression(condition)
                    .set_expression_attribute_names(Some(names))
                    .set_expression_attribute_values(Some(values))
                    .build()
                    .map_err(|_| ProtectionError::ActionBuild)?,
            )
            .build())
    }
}

fn insert_tuple(item: &mut Item, version: &VersionRef) {
    item.extend([
        ("store".into(), s(version.store().as_str())),
        ("object_key".into(), s(version.key())),
        ("version_id".into(), s(version.version())),
    ]);
}
fn insert_provenance(item: &mut Item, identity: &VersionIdentity) {
    let creation = &identity.creation;
    item.extend([
        ("namespace_id".into(), s(identity.namespace.as_str())),
        (
            "configuration_revision".into(),
            s(identity.configuration_revision.to_string()),
        ),
        (
            "creation_domain".into(),
            s(creation.scope().domain().as_str()),
        ),
        (
            "creation_incarnation".into(),
            s(creation.scope().incarnation().to_string()),
        ),
        (
            "creation_generation".into(),
            n(creation.scope().generation()),
        ),
        ("creation_uuid".into(), s(creation.uuid().to_string())),
    ]);
}
fn decode_identity(row: &Item) -> Result<VersionIdentity, ProtectionError> {
    Ok(VersionIdentity {
        version: VersionRef::new(
            StoreId::new(text(row, "store")?)?,
            text(row, "object_key")?,
            text(row, "version_id")?,
        )?,
        namespace: NamespaceId::new(text(row, "namespace_id")?)?,
        configuration_revision: uuid(row, "configuration_revision")?,
        creation: ResourceId::new(
            Scope::new(
                DomainId::new(text(row, "creation_domain")?)?,
                uuid(row, "creation_incarnation")?,
                number(row, "creation_generation")?,
            )?,
            uuid(row, "creation_uuid")?,
        )?,
    })
}
pub(super) fn encode_version(version: &VersionSnapshot) -> Result<Item, ProtectionError> {
    let mut row = object_key(
        version.creation_id().scope().domain(),
        &version.version().fingerprint(),
    );
    insert_tuple(&mut row, version.version());
    insert_provenance(&mut row, &version.identity);
    row.extend([
        ("record".into(), s("version")),
        ("kind".into(), s("VERSION")),
        (
            "state".into(),
            s(match version.state {
                VersionState::Ready => "READY",
                VersionState::Retiring => "RETIRING",
                VersionState::Deleted => "DELETED",
            }),
        ),
        ("references".into(), n(version.references)),
        ("claims".into(), n(version.claims)),
    ]);
    check_size(&row)?;
    Ok(row)
}
pub(super) fn decode_version(
    domain: &DomainId,
    version: &VersionRef,
    row: &Item,
) -> Result<VersionSnapshot, ProtectionError> {
    check_size(row)?;
    let snapshot = VersionSnapshot {
        identity: decode_identity(row)?,
        state: match text(row, "state")? {
            "READY" => VersionState::Ready,
            "RETIRING" => VersionState::Retiring,
            "DELETED" => VersionState::Deleted,
            _ => return Err(ProtectionError::Corrupt),
        },
        references: number(row, "references")?,
        claims: number(row, "claims")?,
    };
    if snapshot.version() != version || snapshot.creation_id().scope().domain() != domain {
        return Err(ProtectionError::Binding);
    }
    exact(row, encode_version(&snapshot)?)?;
    Ok(snapshot)
}
fn pin_binding(pin: &UsePinSnapshot) -> Result<(), ProtectionError> {
    let scope = pin.claim.id().scope();
    let creation = pin.identity.creation.scope();
    if pin.claim.version() != &pin.identity.version
        || scope.domain() != creation.domain()
        || (scope.incarnation() == creation.incarnation()
            && scope.generation() < creation.generation())
        || pin.identity.configuration_revision.is_nil()
    {
        return Err(ProtectionError::Binding);
    }
    Ok(())
}
fn encode_pin(pin: &UsePinSnapshot) -> Result<Item, ProtectionError> {
    pin_binding(pin)?;
    let id = pin.claim.id();
    let mut row = resource_key("USE", id);
    insert_tuple(&mut row, pin.version());
    insert_provenance(&mut row, &pin.identity);
    row.extend([
        ("record".into(), s("use")),
        ("kind".into(), s("USE")),
        ("domain".into(), s(id.scope().domain().as_str())),
        (
            "incarnation".into(),
            s(id.scope().incarnation().to_string()),
        ),
        ("generation".into(), n(id.scope().generation())),
        ("resource_uuid".into(), s(id.uuid().to_string())),
        (
            "state".into(),
            s(match pin.claim.state() {
                ClaimState::Active => "ACTIVE",
                ClaimState::Revoked => "REVOKED",
                ClaimState::Released => "RELEASED",
            }),
        ),
        ("revision".into(), s(pin.claim.revision().to_string())),
        ("deadline".into(), n(pin.claim.deadline().as_millis())),
    ]);
    check_size(&row)?;
    Ok(row)
}
pub(super) fn decode_pin(id: &ResourceId, row: &Item) -> Result<UsePinSnapshot, ProtectionError> {
    check_size(row)?;
    let stored = ResourceId::new(
        Scope::new(
            DomainId::new(text(row, "domain")?)?,
            uuid(row, "incarnation")?,
            number(row, "generation")?,
        )?,
        uuid(row, "resource_uuid")?,
    )?;
    if &stored != id {
        return Err(ProtectionError::Binding);
    }
    let identity = decode_identity(row)?;
    let state = match text(row, "state")? {
        "ACTIVE" => ClaimState::Active,
        "REVOKED" => ClaimState::Revoked,
        "RELEASED" => ClaimState::Released,
        _ => return Err(ProtectionError::Corrupt),
    };
    // Checked pure Claim reconstruction only. Persisted rows remain flat native
    // scalars and no public snapshot Deserialize/factory is exposed.
    let claim = serde_json::from_value(serde_json::json!({ "id": stored, "version": identity.version,
        "state": state, "revision": uuid(row, "revision")?, "deadline": TimestampMs::new(number(row, "deadline")?)? }))
        .map_err(|_| ProtectionError::Corrupt)?;
    let pin = UsePinSnapshot { identity, claim };
    exact(row, encode_pin(&pin)?)?;
    Ok(pin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamodb::{EnrolledTables, ReferenceId};
    use crate::identity::CommandId;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_dynamodb::types::{
        CancellationReason, ConditionCheck, error::TransactionCanceledException,
    };
    use serde_json::{Value, json};
    use std::{
        collections::VecDeque,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread::JoinHandle,
        time::Duration,
    };

    const TABLE: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
    fn ts(n: u64) -> TimestampMs {
        TimestampMs::new(n).unwrap()
    }
    fn clock(n: u64) -> ClockObservation {
        ClockObservation::trusted(ts(n), 5).unwrap()
    }
    fn policy() -> ClaimPolicy {
        ClaimPolicy::new(2000, 50, 10).unwrap()
    }
    fn domain() -> DomainId {
        DomainId::new("domain").unwrap()
    }
    fn id(generation: u64, unique: u128) -> ResourceId {
        ResourceId::new(
            Scope::new(domain(), Uuid::from_u128(1), generation).unwrap(),
            Uuid::from_u128(unique),
        )
        .unwrap()
    }
    fn version() -> VersionSnapshot {
        VersionSnapshot {
            identity: VersionIdentity {
                version: VersionRef::new(
                    StoreId::new("store").unwrap(),
                    "fresh/object",
                    "exact-version",
                )
                .unwrap(),
                namespace: NamespaceId::new("namespace").unwrap(),
                configuration_revision: Uuid::from_u128(2),
                creation: id(3, 5),
            },
            state: VersionState::Ready,
            references: 1,
            claims: 1,
        }
    }
    fn pin() -> UsePinSnapshot {
        let identity = version().identity;
        UsePinSnapshot {
            claim: Claim::new(
                id(4, 6),
                identity.version.clone(),
                Uuid::from_u128(6),
                ts(2500),
                clock(1000),
                &policy(),
            )
            .unwrap(),
            identity,
        }
    }
    fn control(incarnation: Uuid, current: u64, closed: u64, compacted: u64) -> Item {
        let mut row = control_key(&domain());
        row.extend([
            ("domain".into(), s("domain")),
            ("incarnation".into(), s(incarnation.to_string())),
            ("current_generation".into(), n(current)),
            ("closed_through".into(), n(closed)),
            ("compacted_through".into(), n(compacted)),
            ("revision".into(), n(1)),
        ]);
        row
    }
    fn current_control() -> Item {
        control(Uuid::from_u128(1), 4, 0, 0)
    }
    fn wire(row: &Item) -> Value {
        Value::Object(
            row.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        match v {
                            AttributeValue::S(s) => json!({"S":s}),
                            AttributeValue::N(n) => json!({"N":n}),
                            _ => panic!("unit fixture only emits flat native strings/numbers"),
                        },
                    )
                })
                .collect(),
        )
    }

    // Canned native SDK loopback observations, NOT expression evaluation or a
    // database implementation. No real credentials, cloud endpoints or retries.
    struct NativeFixture {
        address: std::net::SocketAddr,
        stop: Arc<AtomicBool>,
        replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
        requests: Arc<Mutex<Vec<Value>>>,
        worker: Option<JoinHandle<()>>,
    }
    impl NativeFixture {
        fn new() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let replies = Arc::new(Mutex::new(VecDeque::<(u16, Value)>::new()));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let (stopping, responses, captured) = (stop.clone(), replies.clone(), requests.clone());
            let worker = std::thread::spawn(move || {
                loop {
                    let (mut socket, _) = listener.accept().unwrap();
                    if stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    socket
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut headers = Vec::new();
                    let mut byte = [0];
                    while !headers.ends_with(b"\r\n\r\n") {
                        socket.read_exact(&mut byte).unwrap();
                        headers.push(byte[0]);
                        assert!(headers.len() < 32768);
                    }
                    let headers = String::from_utf8(headers).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|n| n.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    assert!(length < 1_000_000);
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).unwrap();
                    captured
                        .lock()
                        .unwrap()
                        .push(serde_json::from_slice(&body).unwrap());
                    let (status, body) = responses
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("unexpected native request");
                    let body = body.to_string();
                    write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
            });
            Self {
                address,
                stop,
                replies,
                requests,
                worker: Some(worker),
            }
        }
        fn reply(&self, value: Value) {
            self.replies.lock().unwrap().push_back((200, value));
        }
        fn item(&self, row: &Item) {
            self.reply(json!({"Item":wire(row)}));
        }
        fn client(&self, region: &str) -> aws_sdk_dynamodb::Client {
            aws_sdk_dynamodb::Client::from_conf(
                aws_sdk_dynamodb::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new(region.to_owned()))
                    .credentials_provider(Credentials::new(
                        "fixture", "fixture", None, None, "fixture",
                    ))
                    .endpoint_url(format!("http://{}", self.address))
                    .retry_config(RetryConfig::disabled())
                    .build(),
            )
        }
        async fn journal(&self, region: &str) -> CommandJournal {
            for name in ["coord", "app"] {
                self.reply(json!({"Table":{"TableName":name,"TableArn":format!("arn:aws:dynamodb:us-west-2:123456789012:table/{name}"),
                    "TableStatus":"ACTIVE", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
                    "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
            }
            let client = self.client(region);
            let tables = EnrolledTables::enroll(&client, "coord", &["app".into()])
                .await
                .unwrap();
            CommandJournal::new(client, tables, domain())
        }
        async fn context(&self, journal: &CommandJournal) -> CommandContext {
            self.item(&current_control());
            journal.command_context().await.unwrap()
        }
    }
    impl Drop for NativeFixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            let _ = self.worker.take().unwrap().join();
        }
    }
    struct Fixture {
        native: NativeFixture,
        journal: CommandJournal,
        ns: NamespaceSnapshot,
        intent: IntentSnapshot,
        staged: StagedSnapshot,
        reference: ReferenceSnapshot,
        version: VersionSnapshot,
    }
    fn namespace_observation(incarnation: Uuid) -> Value {
        json!({"Item": {
            "pk":{"S":format!("{}#NS#6e616d657370616365", crate::dynamodb::journal::prefix(&domain()))}, "sk":{"S":"M"},
            "record":{"S":"namespace"}, "domain":{"S":"domain"}, "incarnation":{"S":incarnation.to_string()},
            "namespace_id":{"S":"namespace"}, "configuration_revision":{"S":Uuid::from_u128(2).to_string()},
            "state":{"S":"ACTIVE"}, "store":{"S":"store"}, "bucket":{"S":"unit-not-qualified"}, "expected_owner":{"S":"123456789012"},
            "region":{"S":"us-west-2"}, "key_prefix":{"S":"fresh/"}, "provider_profile":{"S":"aws-s3-versioned-lifecycle-v0"},
            "mpu_max_age_ms":{"N":"10000"}, "mpu_lifecycle_margin_ms":{"N":"1000"}
        }})
    }
    impl Fixture {
        async fn new() -> Self {
            let native = NativeFixture::new();
            let journal = native.journal("us-west-2").await;
            let v = version();
            native.reply(namespace_observation(Uuid::from_u128(1)));
            let ns = journal
                .read_namespace(v.namespace_id())
                .await
                .unwrap()
                .unwrap();
            let mut intent_row = resource_key("INTENT", &id(1, 3));
            intent_row.extend([
                ("record".into(), s("intent")),
                ("domain".into(), s("domain")),
                ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
                ("resource_uuid".into(), s(Uuid::from_u128(3).to_string())),
                ("generation".into(), n(1)),
                ("namespace_id".into(), s("namespace")),
                (
                    "configuration_revision".into(),
                    s(Uuid::from_u128(2).to_string()),
                ),
                ("store".into(), s("store")),
                ("object_key".into(), s("fresh/object")),
                ("state".into(), s("SELECTED")),
                ("revision".into(), s(Uuid::from_u128(5).to_string())),
                ("deadline".into(), n(1500)),
                ("max_length".into(), n(100)),
            ]);
            native.item(&intent_row);
            let intent = journal.read_intent(&id(1, 3)).await.unwrap().unwrap();
            let mut staged_row = resource_key("STAGED", intent.id());
            insert_tuple(&mut staged_row, v.version());
            staged_row.extend([
                ("record".into(), s("staged")),
                ("kind".into(), s("STAGED")),
                ("domain".into(), s("domain")),
                ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
                ("intent_uuid".into(), s(Uuid::from_u128(3).to_string())),
                ("attempt_uuid".into(), s(Uuid::from_u128(4).to_string())),
                ("claim_uuid".into(), s(Uuid::from_u128(5).to_string())),
                ("namespace_id".into(), s("namespace")),
                (
                    "configuration_revision".into(),
                    s(Uuid::from_u128(2).to_string()),
                ),
                ("revision".into(), s(Uuid::from_u128(5).to_string())),
                ("state".into(), s("ACTIVE")),
                ("intent_generation".into(), n(1)),
                ("attempt_generation".into(), n(2)),
                ("claim_generation".into(), n(3)),
                ("deadline".into(), n(2500)),
            ]);
            native.item(&staged_row);
            let staged = journal.read_staged(intent.id()).await.unwrap().unwrap();
            native.reply(json!({"Item": {
                "pk":{"S":format!("{}#REF#736c6f74", crate::dynamodb::journal::prefix(&domain()))}, "sk":{"S":"M"}, "record":{"S":"reference"},
                "reference_id":{"S":"slot"}, "revision":{"S":Uuid::from_u128(7).to_string()},
                "store":{"S":"store"}, "object_key":{"S":"fresh/object"}, "version_id":{"S":"exact-version"}
            }}));
            let reference = journal
                .read_reference(&ReferenceId::new("slot").unwrap())
                .await
                .unwrap()
                .unwrap();
            native.item(&encode_version(&v).unwrap());
            let version = journal.read_version(v.version()).await.unwrap().unwrap();
            Self {
                native,
                journal,
                ns,
                intent,
                staged,
                reference,
                version,
            }
        }
        fn acquire(
            &self,
            source: UseSource<'_>,
            at: u64,
        ) -> Result<Vec<TransactWriteItem>, ProtectionError> {
            acquire_actions(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &CommandId::new(id(4, 6).scope().clone(), id(4, 6).uuid()).unwrap(),
                &self.version,
                &self.ns,
                source,
                ts(3000),
                clock(at),
                &policy(),
            )
        }
        fn staged_source(&self) -> UseSource<'_> {
            UseSource::Staged {
                staged: &self.staged,
                intent: &self.intent,
            }
        }
        async fn staged_with(&self, field: &str, value: AttributeValue) -> StagedSnapshot {
            let mut row = encode_staged(&self.staged).unwrap();
            row.insert(field.into(), value);
            self.native.item(&row);
            self.journal
                .read_staged(self.intent.id())
                .await
                .unwrap()
                .unwrap()
        }
        async fn intent_with(&self, state: &str) -> IntentSnapshot {
            let mut row = encode_intent(&self.intent).unwrap();
            row.insert("state".into(), s(state));
            self.native.item(&row);
            self.journal
                .read_intent(self.intent.id())
                .await
                .unwrap()
                .unwrap()
        }
    }

    fn assert_old_pin(action: &TransactWriteItem, pin: &UsePinSnapshot) {
        let put = action.put.as_ref().unwrap();
        let expected = guard(TABLE, encode_pin(pin).unwrap())
            .unwrap()
            .condition_check
            .unwrap();
        assert_eq!(
            put.condition_expression(),
            Some(expected.condition_expression())
        );
        assert_eq!(
            put.expression_attribute_names(),
            expected.expression_attribute_names()
        );
        assert_eq!(
            put.expression_attribute_values(),
            expected.expression_attribute_values()
        );
        assert!(put.condition_expression().unwrap().len() < 4096);
        for field in encode_pin(pin).unwrap().keys() {
            assert!(
                put.expression_attribute_names()
                    .unwrap()
                    .values()
                    .any(|v| v == field)
            );
        }
        check_size(put.item()).unwrap();
    }
    fn assert_count(action: &TransactWriteItem, identity: &VersionIdentity, delta: i64) {
        let (expression, names, values) = if delta == 0 {
            let c = action.condition_check.as_ref().unwrap();
            (
                c.condition_expression(),
                c.expression_attribute_names().unwrap(),
                c.expression_attribute_values().unwrap(),
            )
        } else {
            let u = action.update.as_ref().unwrap();
            assert_eq!(u.update_expression(), "ADD #claims :claim_delta");
            assert_eq!(
                u.expression_attribute_values().unwrap()[":claim_delta"],
                AttributeValue::N(delta.to_string())
            );
            (
                u.condition_expression().unwrap(),
                u.expression_attribute_names().unwrap(),
                u.expression_attribute_values().unwrap(),
            )
        };
        assert!(expression.contains("#state = :ready"));
        assert!(
            expression.contains("attribute_type(#references, :number) AND #references >= :zero")
        );
        assert!(expression.contains("attribute_type(#claims, :number) AND #claims >= :zero"));
        assert_eq!(expression.contains("#claims >= :minimum_claims"), delta < 0);
        assert_eq!(expression.contains("#claims < :use_max_claims"), delta > 0);
        assert_eq!(values[":ready"], s("READY"));
        assert_eq!(values[":record"], s("version"));
        assert_eq!(values[":store"], s(identity.version.store().as_str()));
        assert_eq!(values[":key"], s(identity.version.key()));
        assert_eq!(values[":version"], s(identity.version.version()));
        if delta < 0 {
            assert_eq!(values[":minimum_claims"], n(1));
        }
        if delta > 0 {
            assert_eq!(values[":use_max_claims"], n(u64::MAX));
        }
        let mut expected = Item::new();
        insert_provenance(&mut expected, identity);
        expected.insert("kind".into(), s("VERSION"));
        for (field, value) in expected {
            let alias = names.iter().find(|(_, name)| **name == field).unwrap().0;
            let parameter = alias.replacen('#', ":", 1);
            assert!(expression.contains(&format!("{alias} = {parameter}")));
            assert_eq!(values[&parameter], value);
        }
        assert!(expression.len() < 4096);
        assert!(action.put.is_none() && action.delete.is_none());
    }

    #[tokio::test]
    async fn acquire_guards_only_its_typed_source_and_one_ready_count() {
        let f = Fixture::new().await;
        for source in [UseSource::Reference(&f.reference), f.staged_source()] {
            let actions = f.acquire(source, 1000).unwrap();
            let staged = matches!(source, UseSource::Staged { .. });
            assert_eq!(actions.len(), if staged { 5 } else { 4 });
            assert_eq!(actions[0], namespace_guard(TABLE, &f.ns).unwrap());
            if staged {
                assert_eq!(
                    actions[1],
                    guard(TABLE, encode_intent(&f.intent).unwrap()).unwrap()
                );
                assert_eq!(
                    actions[2],
                    guard(TABLE, encode_staged(&f.staged).unwrap()).unwrap()
                );
            } else {
                assert_eq!(
                    actions[1],
                    source_action(TABLE, &domain(), &f.reference).unwrap()
                );
            }
            let put = actions[actions.len() - 2].put.as_ref().unwrap();
            assert_eq!(put.condition_expression(), Some("attribute_not_exists(pk)"));
            let pin = decode_pin(&id(4, 6), put.item()).unwrap();
            assert_eq!(pin.claim().id(), &id(4, 6));
            assert_eq!(pin.claim().revision(), Uuid::from_u128(6));
            assert_eq!(pin.claim().deadline(), ts(3000));
            assert_eq!(pin.version_creation_id(), f.version.creation_id());
            assert_count(actions.last().unwrap(), &f.version.identity, 1);
            assert_eq!(actions.iter().filter(|a| a.update.is_some()).count(), 1);
            assert_eq!(actions, f.acquire(source, 1100).unwrap());
        }
    }

    #[tokio::test]
    async fn expired_terminal_changed_and_mismatched_sources_fail_closed() {
        let mut f = Fixture::new().await;
        for state in ["OPEN", "CONSUMED", "CANCELLED"] {
            let intent = f.intent_with(state).await;
            assert!(
                f.acquire(
                    UseSource::Staged {
                        staged: &f.staged,
                        intent: &intent
                    },
                    1000
                )
                .is_err()
            );
        }
        for (field, value) in [
            ("state", s("RELEASED")),
            ("state", s("REVOKED")),
            ("deadline", n(1005)),
            ("namespace_id", s("other")),
            ("configuration_revision", s(Uuid::from_u128(99).to_string())),
            ("claim_uuid", s(Uuid::from_u128(99).to_string())),
            ("version_id", s("different-version")),
        ] {
            let staged = f.staged_with(field, value).await;
            assert!(
                f.acquire(
                    UseSource::Staged {
                        staged: &staged,
                        intent: &f.intent
                    },
                    1000
                )
                .is_err(),
                "accepted {field}"
            );
        }
        // A renewed row is valid new evidence but emits different full-row CAS:
        // stale snapshots cannot pass against it in DynamoDB. No local evaluator.
        let renewed = f
            .staged_with("revision", s(Uuid::from_u128(99).to_string()))
            .await;
        let old = f.acquire(f.staged_source(), 1000).unwrap();
        let new = f
            .acquire(
                UseSource::Staged {
                    staged: &renewed,
                    intent: &f.intent,
                },
                1000,
            )
            .unwrap();
        assert_ne!(old[2], new[2]);
        // SELECTED staging has independent lifetime, not the old upload deadline.
        assert!(f.acquire(f.staged_source(), 2000).is_ok());
        assert!(f.acquire(f.staged_source(), 2495).is_err());
        f.version.identity.version =
            VersionRef::new(StoreId::new("store").unwrap(), "fresh/object", "other").unwrap();
        assert!(f.acquire(UseSource::Reference(&f.reference), 1000).is_err());
    }

    #[tokio::test]
    async fn strong_public_reads_reject_corrupt_aliases_and_never_infer_absence() {
        let native = NativeFixture::new();
        let journal = native.journal("us-west-2").await;
        let v = version();
        let p = pin();
        native.item(&encode_version(&v).unwrap());
        assert_eq!(
            journal.read_version(v.version()).await.unwrap(),
            Some(v.clone())
        );
        native.item(&encode_pin(&p).unwrap());
        assert_eq!(
            journal.read_use_pin(p.claim.id()).await.unwrap(),
            Some(p.clone())
        );
        let requests = native.requests.lock().unwrap().clone();
        for (request, key) in requests[2..].iter().zip([
            object_key(&domain(), &v.version().fingerprint()),
            resource_key("USE", p.claim.id()),
        ]) {
            assert_eq!(request["ConsistentRead"], true);
            assert_eq!(request["TableName"], TABLE);
            assert_eq!(request["Key"], wire(&key));
        }
        for kind in ["version", "pin"] {
            native.reply(json!({}));
            if kind == "version" {
                assert!(journal.read_version(v.version()).await.unwrap().is_none());
            } else {
                assert!(journal.read_use_pin(p.claim.id()).await.unwrap().is_none());
            }
            let mut row = if kind == "version" {
                encode_version(&v).unwrap()
            } else {
                encode_pin(&p).unwrap()
            };
            row.insert("pk".into(), s("alias"));
            native.item(&row);
            if kind == "version" {
                assert!(journal.read_version(v.version()).await.is_err());
            } else {
                assert!(journal.read_use_pin(p.claim.id()).await.is_err());
            }
            native.replies.lock().unwrap().push_back((
                500,
                json!({"__type":"InternalServerError","message":"unit failure"}),
            ));
            if kind == "version" {
                assert!(journal.read_version(v.version()).await.is_err());
            } else {
                assert!(journal.read_use_pin(p.claim.id()).await.is_err());
            }
        }
        assert!(native.replies.lock().unwrap().is_empty());
    }

    #[test]
    fn canonical_codecs_check_every_field_and_decode_terminal_rows_without_authority() {
        let v = version();
        let p = pin();
        for original in [encode_version(&v).unwrap(), encode_pin(&p).unwrap()] {
            let decode = |row: &Item| {
                if text(&original, "record").unwrap() == "version" {
                    decode_version(&domain(), v.version(), row).map(|_| ())
                } else {
                    decode_pin(p.claim.id(), row).map(|_| ())
                }
            };
            for field in original.keys() {
                let mut row = original.clone();
                row.remove(field);
                assert!(decode(&row).is_err(), "missing {field}");
                let mut row = original.clone();
                row.insert(field.clone(), AttributeValue::Bool(true));
                assert!(decode(&row).is_err(), "type {field}");
            }
            for (field, value) in [
                ("pk", s("alias")),
                ("sk", s("alias")),
                ("record", s("unknown")),
                ("kind", s("unknown")),
                ("state", s("unknown")),
                ("configuration_revision", s(Uuid::nil().to_string())),
                ("creation_domain", s("other")),
                ("creation_incarnation", s("bad")),
                ("creation_uuid", s(Uuid::nil().to_string())),
                ("creation_generation", n(0)),
                ("creation_generation", AttributeValue::N("03".into())),
                ("version_id", s("null")),
                ("namespace_id", s("")),
                ("store", s("")),
                ("object_key", s("")),
                ("unknown", s("extra")),
            ] {
                let mut row = original.clone();
                row.insert(field.into(), value);
                assert!(decode(&row).is_err(), "accepted {field}");
            }
        }
        for field in ["references", "claims"] {
            for value in ["-1", "1.0", "01", "18446744073709551616"] {
                let mut row = encode_version(&v).unwrap();
                row.insert(field.into(), AttributeValue::N(value.into()));
                assert!(decode_version(&domain(), v.version(), &row).is_err());
            }
        }
        for state in ["READY", "RETIRING", "DELETED"] {
            let mut row = encode_version(&v).unwrap();
            row.insert("state".into(), s(state));
            let decoded = decode_version(&domain(), v.version(), &row).unwrap();
            assert_eq!(encode_version(&decoded).unwrap(), row);
        }
        for state in ["ACTIVE", "RELEASED", "REVOKED"] {
            let mut row = encode_pin(&p).unwrap();
            row.insert("state".into(), s(state));
            row.insert("deadline".into(), n(0));
            let decoded = decode_pin(p.claim.id(), &row).unwrap();
            assert_eq!(encode_pin(&decoded).unwrap(), row);
            assert!(decoded.claim.validate_use(clock(1000), &policy()).is_err());
        }
        for (field, value) in [
            ("incarnation", s(Uuid::from_u128(99).to_string())),
            ("resource_uuid", s(Uuid::from_u128(99).to_string())),
            ("generation", n(5)),
            ("creation_generation", n(5)),
            ("revision", s(Uuid::nil().to_string())),
            ("deadline", n(u64::MAX)),
        ] {
            let mut row = encode_pin(&p).unwrap();
            row.insert(field.into(), value);
            assert!(decode_pin(p.claim.id(), &row).is_err(), "accepted {field}");
        }
    }

    #[tokio::test]
    async fn renew_and_close_keep_no_old_source_dependency_and_contend_on_full_pin() {
        let f = Fixture::new().await;
        let p = pin();
        // Deliberately supply no source rows. Public renewal/release only read
        // command control here, even though the old source may no longer exist.
        let renewal = Renewal::new(p.claim.revision(), ts(3500), Uuid::from_u128(8)).unwrap();
        let actions = renew_actions(
            TABLE,
            &domain(),
            Some("us-west-2"),
            &p,
            &f.version,
            &f.ns,
            &renewal,
            clock(2000),
            &policy(),
        )
        .unwrap();
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0], namespace_guard(TABLE, &f.ns).unwrap());
        assert_old_pin(&actions[1], &p);
        assert_count(&actions[2], &p.identity, 0);
        let next = decode_pin(p.claim.id(), actions[1].put.as_ref().unwrap().item()).unwrap();
        assert_eq!(next.claim.deadline(), ts(3500));
        assert_eq!(next.claim.revision(), renewal.new_revision());
        let released = release_actions(TABLE, &domain(), &p).unwrap();
        assert_eq!(released.len(), 2);
        assert_old_pin(&released[0], &p);
        assert_count(&released[1], &p.identity, -1);
        let revoked = revoke_actions(
            TABLE,
            &domain(),
            current_control(),
            &p,
            clock(2505),
            &policy(),
        )
        .unwrap();
        assert_eq!(revoked.len(), 3);
        assert_old_pin(&revoked[1], &p);
        assert_count(&revoked[2], &p.identity, -1);
        for (action, state) in [
            (&released[0], ClaimState::Released),
            (&revoked[1], ClaimState::Revoked),
        ] {
            let terminal = decode_pin(p.claim.id(), action.put.as_ref().unwrap().item()).unwrap();
            assert_eq!(terminal.claim.state(), state);
            assert!(matches!(
                release_actions(TABLE, &domain(), &terminal),
                Err(ProtectionError::Terminal)
            ));
            assert!(matches!(
                revoke_actions(
                    TABLE,
                    &domain(),
                    current_control(),
                    &terminal,
                    ClockObservation::untrusted(ts(3000), 0),
                    &policy()
                ),
                Err(ProtectionError::Terminal)
            ));
        }
        assert!(matches!(
            revoke_actions(
                TABLE,
                &domain(),
                current_control(),
                &next,
                clock(2505),
                &policy()
            ),
            Err(ProtectionError::Claim(ClaimError::NotRevocable))
        ));
        assert!(matches!(
            renew_actions(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &next,
                &f.version,
                &f.ns,
                &renewal,
                clock(2100),
                &policy()
            ),
            Err(ProtectionError::Claim(ClaimError::RevisionMismatch))
        ));
        let context = f.native.context(&f.journal).await;
        assert_eq!(
            f.journal
                .prepare_renew_use(
                    context,
                    &p,
                    &f.version,
                    &f.ns,
                    &renewal,
                    clock(2000),
                    &policy(),
                    vec![]
                )
                .unwrap()
                .action_count(),
            5
        );
        let context = f.native.context(&f.journal).await;
        assert_eq!(
            f.journal
                .prepare_release_use(context, &p, vec![])
                .unwrap()
                .action_count(),
            4
        );
        let mut expired = encode_pin(&p).unwrap();
        expired.insert("deadline".into(), n(0));
        assert!(
            release_actions(
                TABLE,
                &domain(),
                &decode_pin(p.claim.id(), &expired).unwrap()
            )
            .is_ok()
        );
        assert!(f.native.replies.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn clocks_fixed_deadlines_terminal_version_and_namespace_binding_fail_closed() {
        let mut f = Fixture::new().await;
        let p = pin();
        let renewal = Renewal::new(p.claim.revision(), ts(3500), Uuid::from_u128(8)).unwrap();
        for at in [
            clock(2495),
            clock(2445),
            ClockObservation::untrusted(ts(2000), 0),
            ClockObservation::trusted(ts(2000), 11).unwrap(),
        ] {
            assert!(
                renew_actions(
                    TABLE,
                    &domain(),
                    Some("us-west-2"),
                    &p,
                    &f.version,
                    &f.ns,
                    &renewal,
                    at,
                    &policy()
                )
                .is_err()
            );
        }
        for at in [
            clock(2504),
            ClockObservation::untrusted(ts(3000), 0),
            ClockObservation::trusted(ts(3000), 11).unwrap(),
        ] {
            assert!(
                revoke_actions(TABLE, &domain(), current_control(), &p, at, &policy()).is_err()
            );
        }
        for region in [None, Some("us-east-1")] {
            assert!(namespace_binding(&p.identity, &domain(), region, &f.ns).is_err());
        }
        // The SAME StoreId is insufficient: provenance must identify its owner.
        for field in ["namespace", "config", "domain", "key", "store"] {
            let mut identity = p.identity.clone();
            match field {
                "namespace" => identity.namespace = NamespaceId::new("other").unwrap(),
                "config" => identity.configuration_revision = Uuid::from_u128(99),
                "domain" => {
                    identity.creation = ResourceId::new(
                        Scope::new(DomainId::new("other").unwrap(), Uuid::from_u128(1), 3).unwrap(),
                        Uuid::from_u128(5),
                    )
                    .unwrap()
                }
                "key" => {
                    identity.version = VersionRef::new(
                        StoreId::new("store").unwrap(),
                        "outside/object",
                        "exact-version",
                    )
                    .unwrap()
                }
                "store" => {
                    identity.version = VersionRef::new(
                        StoreId::new("other").unwrap(),
                        "fresh/object",
                        "exact-version",
                    )
                    .unwrap()
                }
                _ => unreachable!(),
            }
            assert!(
                namespace_binding(&identity, &domain(), Some("us-west-2"), &f.ns).is_err(),
                "accepted {field}"
            );
        }
        let ns_guard = namespace_guard(TABLE, &f.ns)
            .unwrap()
            .condition_check
            .unwrap();
        for field in [
            "bucket",
            "expected_owner",
            "region",
            "key_prefix",
            "provider_profile",
            "namespace_id",
            "configuration_revision",
            "domain",
            "incarnation",
            "store",
        ] {
            assert!(
                ns_guard
                    .expression_attribute_names()
                    .unwrap()
                    .values()
                    .any(|name| name == field)
            );
        }
        for state in [VersionState::Retiring, VersionState::Deleted] {
            f.version.state = state;
            assert!(f.acquire(UseSource::Reference(&f.reference), 1000).is_err());
            assert!(
                renew_actions(
                    TABLE,
                    &domain(),
                    Some("us-west-2"),
                    &p,
                    &f.version,
                    &f.ns,
                    &renewal,
                    clock(2000),
                    &policy()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn natural_cleanup_validates_rotation_and_freezes_entire_observed_control() {
        let p = pin();
        for row in [
            control(Uuid::from_u128(1), 5, 4, 4),
            control(Uuid::from_u128(99), 1, 0, 0),
        ] {
            let actions =
                revoke_actions(TABLE, &domain(), row.clone(), &p, clock(2505), &policy()).unwrap();
            assert_eq!(actions.len(), 3);
            assert_eq!(actions[0], guard(TABLE, row.clone()).unwrap());
            assert_old_pin(&actions[1], &p);
            assert_count(&actions[2], &p.identity, -1);
            for field in row.keys() {
                assert!(
                    actions[0]
                        .condition_check
                        .as_ref()
                        .unwrap()
                        .expression_attribute_names()
                        .unwrap()
                        .values()
                        .any(|name| name == field)
                );
            }
        }
        assert!(matches!(
            revoke_actions(
                TABLE,
                &domain(),
                control(Uuid::from_u128(1), 3, 0, 0),
                &p,
                clock(2505),
                &policy()
            ),
            Err(ProtectionError::Binding)
        ));
        for (field, value) in [
            ("incarnation", s("arbitrary-different-string")),
            ("incarnation", s(Uuid::nil().to_string())),
            ("incarnation", s("00000000-0000-0000-0000-00000000000A")),
            ("domain", s("other")),
            ("pk", s("alias")),
            ("current_generation", n(0)),
            ("closed_through", n(4)),
            ("compacted_through", n(1)),
            ("revision", n(0)),
        ] {
            let mut row = current_control();
            row.insert(field.into(), value);
            assert!(
                matches!(
                    revoke_actions(TABLE, &domain(), row, &p, clock(2505), &policy()),
                    Err(ProtectionError::Journal(JournalError::Corrupt(_)))
                ),
                "accepted {field}"
            );
        }
        for field in current_control().keys() {
            let mut row = current_control();
            row.remove(field);
            assert!(
                revoke_actions(TABLE, &domain(), row, &p, clock(2505), &policy()).is_err(),
                "missing {field}"
            );
        }
    }

    #[test]
    fn maximal_records_and_counter_widths_fit_reserved_native_bound() {
        let mut p = pin();
        let scope = Scope::new(
            DomainId::new("d".repeat(128)).unwrap(),
            Uuid::from_u128(1),
            u64::MAX,
        )
        .unwrap();
        p.identity.creation = ResourceId::new(scope.clone(), Uuid::from_u128(5)).unwrap();
        p.identity.namespace = NamespaceId::new("n".repeat(128)).unwrap();
        p.identity.version = VersionRef::new(
            StoreId::new("s".repeat(128)).unwrap(),
            "\\\"".repeat(512),
            "v".repeat(1024),
        )
        .unwrap();
        p.claim = Claim::new(
            ResourceId::new(scope, Uuid::from_u128(6)).unwrap(),
            p.identity.version.clone(),
            Uuid::from_u128(6),
            ts(2500),
            clock(1000),
            &policy(),
        )
        .unwrap();
        let mut row = encode_pin(&p).unwrap();
        row.insert("deadline".into(), n(TimestampMs::MAX.as_millis()));
        p = decode_pin(p.claim.id(), &row).unwrap();
        let mut v = version();
        v.identity = p.identity.clone();
        v.references = u64::MAX;
        v.claims = u64::MAX;
        for state in [
            VersionState::Ready,
            VersionState::Retiring,
            VersionState::Deleted,
        ] {
            v.state = state;
            let row = encode_version(&v).unwrap();
            check_size(&row).unwrap();
            assert_eq!(
                decode_version(v.creation_id().scope().domain(), v.version(), &row).unwrap(),
                v
            );
        }
        for state in ["ACTIVE", "REVOKED", "RELEASED"] {
            row.insert("state".into(), s(state));
            let decoded = decode_pin(p.claim.id(), &row).unwrap();
            check_size(&encode_pin(&decoded).unwrap()).unwrap();
            let bytes: usize = row
                .iter()
                .map(|(k, v)| {
                    k.len()
                        + match v {
                            AttributeValue::S(s) => s.len(),
                            AttributeValue::N(n) => n.len().max(2),
                            _ => unreachable!(),
                        }
                })
                .sum();
            println!("maximum USE {state}: {bytes} native bytes + 64 reserved <= 4000");
            assert!(bytes + 64 <= 4000);
        }
        for delta in [-1, 0, 1] {
            assert_count(
                &count_action(TABLE, p.claim.id().scope().domain(), &p.identity, delta).unwrap(),
                &p.identity,
                delta,
            );
        }
        let actions = release_actions(TABLE, p.claim.id().scope().domain(), &p).unwrap();
        assert_old_pin(&actions[0], &p);
        row.insert("version_id".into(), s("v".repeat(4000)));
        assert!(matches!(
            decode_pin(p.claim.id(), &row),
            Err(ProtectionError::Upload(UploadError::RecordSize))
        ));
    }

    #[tokio::test]
    async fn public_preparation_enforces_context_routing_and_complete_native_budget() {
        let f = Fixture::new().await;
        let application = |count: usize| {
            ApplicationActions::new(
                (0..count)
                    .map(|i| {
                        TransactWriteItem::builder()
                            .condition_check(
                                ConditionCheck::builder()
                                    .table_name("app")
                                    .key("pk", s(format!("pointer-{i}")))
                                    .key("sk", s("M"))
                                    .condition_expression("attribute_exists(pk)")
                                    .build()
                                    .unwrap(),
                            )
                            .build()
                    })
                    .collect(),
            )
            .unwrap()
        };
        for (source, capacity) in [
            (UseSource::Reference(&f.reference), 94),
            (f.staged_source(), 93),
        ] {
            let context = f.native.context(&f.journal).await;
            let id = context.id().clone();
            let prepared = f
                .journal
                .prepare_acquire_use(
                    context,
                    application(capacity),
                    &f.version,
                    &f.ns,
                    source,
                    ts(3000),
                    clock(1000),
                    &policy(),
                    vec![7],
                )
                .unwrap();
            assert_eq!(prepared.action_count(), 100);
            assert_eq!(prepared.token().id(), &id);
            let context = f.native.context(&f.journal).await;
            assert!(matches!(
                f.journal.prepare_acquire_use(
                    context,
                    application(capacity + 1),
                    &f.version,
                    &f.ns,
                    source,
                    ts(3000),
                    clock(1000),
                    &policy(),
                    vec![]
                ),
                Err(ProtectionError::Journal(JournalError::Budget {
                    count: 101
                }))
            ));
        }
        let p = pin();
        let renewal = Renewal::new(p.claim.revision(), ts(3500), Uuid::from_u128(8)).unwrap();
        // Same-incarnation future resources remain forbidden. Cross-incarnation
        // durable provenance is tested separately from ephemeral pin authority.
        for current in [1, 2] {
            let control = control(Uuid::from_u128(1), current, 0, 0);
            for operation in ["acquire", "renew", "release"] {
                f.native.item(&control);
                let context = f.journal.command_context().await.unwrap();
                let result = match operation {
                    "acquire" => f.journal.prepare_acquire_use(
                        context,
                        application(0),
                        &f.version,
                        &f.ns,
                        UseSource::Reference(&f.reference),
                        ts(3000),
                        clock(1000),
                        &policy(),
                        vec![],
                    ),
                    "renew" => f.journal.prepare_renew_use(
                        context,
                        &p,
                        &f.version,
                        &f.ns,
                        &renewal,
                        clock(2000),
                        &policy(),
                        vec![],
                    ),
                    "release" => f.journal.prepare_release_use(context, &p, vec![]),
                    _ => unreachable!(),
                };
                assert!(result.is_err(), "accepted {operation} wrong context");
            }
        }
        let wrong_region = f.native.journal("us-east-1").await;
        let context = f.native.context(&wrong_region).await;
        assert!(matches!(
            wrong_region.prepare_acquire_use(
                context,
                application(0),
                &f.version,
                &f.ns,
                UseSource::Reference(&f.reference),
                ts(3000),
                clock(1000),
                &policy(),
                vec![]
            ),
            Err(ProtectionError::Binding)
        ));
    }

    #[tokio::test]
    async fn retained_version_provenance_survives_rotation_but_ephemeral_sources_do_not() {
        let f = Fixture::new().await;
        let historical_version = encode_version(&f.version).unwrap();
        let incarnation_b = Uuid::from_u128(99);
        // B's generation 1 is intentionally less than historical A's generation
        // 3. Generations in different incarnations have no ordering relation.
        let rotated = control(incarnation_b, 1, 0, 0);
        f.native.reply(namespace_observation(incarnation_b));
        let ns_b = f.journal.read_namespace(f.ns.id()).await.unwrap().unwrap();
        assert_eq!(ns_b.configuration_revision(), f.ns.configuration_revision());
        assert_eq!(ns_b.incarnation(), incarnation_b);
        assert_ne!(
            ns_b.incarnation(),
            f.version.creation_id().scope().incarnation()
        );
        namespace_binding(&f.version.identity, &domain(), Some("us-west-2"), &ns_b).unwrap();
        // An otherwise identical namespace retaining the old incarnation also
        // binds the same durable provenance; its exact observed row is guarded.
        namespace_binding(&f.version.identity, &domain(), Some("us-west-2"), &f.ns).unwrap();
        assert_ne!(
            namespace_guard(TABLE, &ns_b).unwrap(),
            namespace_guard(TABLE, &f.ns).unwrap()
        );

        f.native.item(&rotated);
        let context = f.journal.command_context().await.unwrap();
        let command = context.id().clone();
        let pin_id = ResourceId::from_command(&command);
        let actions = acquire_actions(
            TABLE,
            &domain(),
            Some("us-west-2"),
            &command,
            &f.version,
            &ns_b,
            UseSource::Reference(&f.reference),
            ts(3000),
            clock(1000),
            &policy(),
        )
        .unwrap();
        assert_eq!(actions.len(), 4);
        assert_eq!(actions[0], namespace_guard(TABLE, &ns_b).unwrap());
        assert_eq!(
            actions[1],
            source_action(TABLE, &domain(), &f.reference).unwrap()
        );
        assert_count(&actions[3], &f.version.identity, 1);
        assert_eq!(actions.iter().filter(|a| a.update.is_some()).count(), 1);
        let pin_row = actions[2].put.as_ref().unwrap().item().clone();
        let prepared = f
            .journal
            .prepare_acquire_use(
                context,
                ApplicationActions::new(vec![]).unwrap(),
                &f.version,
                &ns_b,
                UseSource::Reference(&f.reference),
                ts(3000),
                clock(1000),
                &policy(),
                vec![7],
            )
            .unwrap();
        assert_eq!(prepared.action_count(), 6);
        // Official SDK submission to a canned loopback response. Inspect native
        // guards/actions, not their evaluation; this is not restore/DB proof.
        f.native.reply(json!({}));
        let report = f.journal.execute(&prepared).await.unwrap();
        assert_eq!(report.outcome, crate::dynamodb::Outcome::Committed(vec![7]));
        let request = f.native.requests.lock().unwrap().last().unwrap().clone();
        let writes = request["TransactItems"].as_array().unwrap();
        assert_eq!(writes.len(), 6);
        assert_eq!(
            writes.iter().filter(|a| a.get("Update").is_some()).count(),
            1
        );
        assert_eq!(writes[3]["Put"]["Item"], wire(&pin_row));
        assert_eq!(
            writes[4]["Update"]["Key"],
            wire(&object_key(&domain(), &f.version.version().fingerprint()))
        );
        let admission_values = writes[0]["ConditionCheck"]["ExpressionAttributeValues"]
            .as_object()
            .unwrap();
        assert!(
            admission_values
                .values()
                .any(|v| v == &json!({"S":incarnation_b.to_string()}))
        );

        // Public checked reconstruction retains both B pin identity and unchanged
        // A version provenance; no bulk remapping or row migration is involved.
        f.native.item(&pin_row);
        let pin_b = f.journal.read_use_pin(&pin_id).await.unwrap().unwrap();
        assert_eq!(pin_b.claim.id().scope().incarnation(), incarnation_b);
        assert_eq!(pin_b.claim.id().scope().generation(), 1);
        assert_eq!(pin_b.identity, f.version.identity);
        assert_eq!(encode_pin(&pin_b).unwrap(), pin_row);
        let renewal = Renewal::new(pin_b.claim.revision(), ts(3500), Uuid::from_u128(100)).unwrap();
        let renewed = renew_actions(
            TABLE,
            &domain(),
            Some("us-west-2"),
            &pin_b,
            &f.version,
            &ns_b,
            &renewal,
            clock(2000),
            &policy(),
        )
        .unwrap();
        assert_eq!(renewed.len(), 3); // no old source row
        assert_old_pin(&renewed[1], &pin_b);
        assert_count(&renewed[2], &f.version.identity, 0);
        f.native.item(&rotated);
        let context = f.journal.command_context().await.unwrap();
        assert_eq!(
            f.journal
                .prepare_renew_use(
                    context,
                    &pin_b,
                    &f.version,
                    &ns_b,
                    &renewal,
                    clock(2000),
                    &policy(),
                    vec![]
                )
                .unwrap()
                .action_count(),
            5
        );
        let renewed_pin = decode_pin(&pin_id, renewed[1].put.as_ref().unwrap().item()).unwrap();
        assert_eq!(renewed_pin.claim.deadline(), ts(3500));
        let released = release_actions(TABLE, &domain(), &renewed_pin).unwrap();
        assert_eq!(released.len(), 2);
        assert_old_pin(&released[0], &renewed_pin);
        assert_count(&released[1], &f.version.identity, -1);
        f.native.item(&rotated);
        let context = f.journal.command_context().await.unwrap();
        assert_eq!(
            f.journal
                .prepare_release_use(context, &renewed_pin, vec![])
                .unwrap()
                .action_count(),
            4
        );

        // Rotation does NOT refresh old ephemeral pin or staging authority.
        let pin_a = pin();
        let old_renewal =
            Renewal::new(pin_a.claim.revision(), ts(3500), Uuid::from_u128(101)).unwrap();
        for operation in ["renew", "release", "staged-acquire"] {
            f.native.item(&rotated);
            let context = f.journal.command_context().await.unwrap();
            let result = match operation {
                "renew" => f.journal.prepare_renew_use(
                    context,
                    &pin_a,
                    &f.version,
                    &ns_b,
                    &old_renewal,
                    clock(2000),
                    &policy(),
                    vec![],
                ),
                "release" => f.journal.prepare_release_use(context, &pin_a, vec![]),
                "staged-acquire" => f.journal.prepare_acquire_use(
                    context,
                    ApplicationActions::new(vec![]).unwrap(),
                    &f.version,
                    &ns_b,
                    f.staged_source(),
                    ts(3000),
                    clock(1000),
                    &policy(),
                    vec![],
                ),
                _ => unreachable!(),
            };
            assert!(
                matches!(result, Err(ProtectionError::Upload(UploadError::Binding))),
                "accepted old A {operation}"
            );
        }
        let revoked = revoke_actions(
            TABLE,
            &domain(),
            rotated.clone(),
            &pin_a,
            clock(2505),
            &policy(),
        )
        .unwrap();
        assert_eq!(revoked.len(), 3);
        assert_eq!(revoked[0], guard(TABLE, rotated.clone()).unwrap());
        assert_old_pin(&revoked[1], &pin_a);
        assert_count(&revoked[2], &f.version.identity, -1);
        f.native.item(&rotated);
        f.native.reply(json!({}));
        f.journal
            .revoke_expired_use(&pin_a, clock(2505), &policy())
            .await
            .unwrap();
        assert_eq!(encode_version(&f.version).unwrap(), historical_version);

        // Removing incarnation equality must not relax domain boundaries.
        let wrong_domain = CommandId::new(
            Scope::new(DomainId::new("other").unwrap(), incarnation_b, 1).unwrap(),
            Uuid::from_u128(102),
        )
        .unwrap();
        assert!(matches!(
            acquire_actions(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &wrong_domain,
                &f.version,
                &ns_b,
                UseSource::Reference(&f.reference),
                ts(3000),
                clock(1000),
                &policy()
            ),
            Err(ProtectionError::Binding)
        ));
        let mut mismatched_pin = pin_b.clone();
        mismatched_pin.identity.creation = ResourceId::from_command(&wrong_domain);
        assert!(matches!(
            encode_pin(&mismatched_pin),
            Err(ProtectionError::Binding)
        ));
        assert!(f.native.replies.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn natural_cleanup_native_wire_is_one_attempt_without_receipt_or_new_id() {
        let native = NativeFixture::new();
        let journal = native.journal("us-west-2").await;
        let p = pin();
        let rotated = control(Uuid::from_u128(99), 1, 0, 0);
        native.item(&rotated);
        native.reply(json!({}));
        journal
            .revoke_expired_use(&p, clock(2505), &policy())
            .await
            .unwrap();
        let requests = native.requests.lock().unwrap().clone();
        assert_eq!(requests[2]["ConsistentRead"], true);
        assert_eq!(requests[2]["Key"], wire(&control_key(&domain())));
        assert_eq!(requests[3]["TransactItems"].as_array().unwrap().len(), 3);
        // The official SDK supplies its own transport idempotency token. This
        // is not a journal receipt or a newly minted resource identity.
        let writes = &requests[3]["TransactItems"];
        assert_eq!(
            writes[0]["ConditionCheck"]["Key"],
            wire(&control_key(&domain()))
        );
        let mut terminal = p.clone();
        terminal
            .claim
            .revoke(p.claim.revision(), clock(2505), &policy())
            .unwrap();
        assert_eq!(
            writes[1]["Put"]["Item"],
            wire(&encode_pin(&terminal).unwrap())
        );
        assert_eq!(
            writes[2]["Update"]["Key"],
            wire(&object_key(&domain(), &p.version().fingerprint()))
        );
        // Error is not historical rollback; a conditional report is distinguished
        // from unknown. Canned service responses test SDK boundaries only.
        for (code, expected_conflict) in [
            ("ConditionalCheckFailed", true),
            ("TransactionConflict", false),
        ] {
            native.item(&rotated);
            native.replies.lock().unwrap().push_back((400, json!({"__type":"TransactionCanceledException","CancellationReasons":[{"Code":code}]})));
            let result = journal.revoke_expired_use(&p, clock(2505), &policy()).await;
            assert!(if expected_conflict {
                matches!(result, Err(ProtectionError::Conflict))
            } else {
                matches!(result, Err(ProtectionError::OutcomeUnknown(_)))
            });
        }
        assert_eq!(native.requests.lock().unwrap().len(), 8);
        assert!(native.replies.lock().unwrap().is_empty());
    }

    #[test]
    fn native_error_mapping_preserves_unknown_outcomes() {
        assert!(matches!(
            revocation_error(None, "lost response".into()),
            ProtectionError::OutcomeUnknown(_)
        ));
        for code in [
            "ConditionalCheckFailed",
            "None",
            "ValidationError",
            "ThrottlingError",
            "TransactionConflict",
        ] {
            let error = TransactWriteItemsError::TransactionCanceledException(
                TransactionCanceledException::builder()
                    .cancellation_reasons(CancellationReason::builder().code(code).build())
                    .build(),
            );
            let result = revocation_error(Some(&error), code.into());
            assert!(if code == "ConditionalCheckFailed" {
                matches!(result, ProtectionError::Conflict)
            } else {
                matches!(result, ProtectionError::OutcomeUnknown(_))
            });
        }
    }
}
