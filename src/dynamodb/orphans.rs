//! Bounded inventory-to-RETIRING bridge, never adoption into READY.
//!
//! Immutable S3 observations are not perpetual metadata authority. One native
//! transaction read supplies current control, namespace, version and (when
//! attributed) exact admitting attempt/intent. A fresh clock is sampled after
//! that read. Selection impossibility and full control/namespace observations
//! are guarded in the ordinary journal transaction; there is no S3 mutation,
//! automatic replay, scan, cancellation loop or new kind of durable receipt.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Get, TransactGetItem, TransactWriteItem,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::journal::control_key;
use super::maintenance::{Control, FIELDS as CONTROL_FIELDS};
use super::protection::decode_version;
use super::references::object_key;
use super::uploads::{
    check_size, create, decode_attempt, decode_intent, decode_namespace, guard, n, namespace_guard,
    namespace_key, resource_key, s,
};
use super::{
    ApplicationActions, AttemptState, CommandContext, CommandJournal, IntentState, JournalError,
    MaintenanceError, NamespaceSnapshot, PreparedCommand, ProtectionError, UploadConstraints,
    UploadError, UploadTarget, VersionSnapshot,
};
use crate::clock::{ClockError, ClockObservation, TimestampMs};
use crate::identity::{CommandId, DomainFence, DomainId, FenceError, ResourceId, VersionRef};
use crate::s3::{ObservedOrphanObject, OrphanAttribution};

type Item = HashMap<String, AttributeValue>;

/// Explicit positive diagnostic grace and host clock uncertainty cap. No default
/// policy or deserialization bypass. Grace is used only for objects which cannot
/// satisfy the required schema-1 attribution checks in the publication API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrphanGracePolicy {
    grace_ms: u64,
    maximum_clock_uncertainty_ms: u64,
}
impl OrphanGracePolicy {
    pub fn new(grace_ms: u64, maximum_clock_uncertainty_ms: u64) -> Result<Self, OrphanError> {
        if grace_ms == 0 {
            return Err(OrphanError::InvalidPolicy);
        }
        TimestampMs::new(grace_ms)?;
        TimestampMs::new(maximum_clock_uncertainty_ms)?;
        Ok(Self {
            grace_ms,
            maximum_clock_uncertainty_ms,
        })
    }
    pub fn grace_ms(&self) -> u64 {
        self.grace_ms
    }
    pub fn maximum_clock_uncertainty_ms(&self) -> u64 {
        self.maximum_clock_uncertainty_ms
    }
}

/// Preparation is not commit or physical deletion. ExistingTracked is only a
/// checked current observation, never an outcome of this or an earlier command.
#[derive(Debug)]
pub enum OrphanPreparation {
    Prepared(PreparedCommand),
    ExistingTracked(VersionSnapshot),
}

#[derive(Debug, Error)]
pub enum OrphanError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Maintenance(#[from] MaintenanceError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Protection(#[from] ProtectionError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error("orphan grace must be explicitly positive")]
    InvalidPolicy,
    #[error("orphan observation, metadata, command or namespace binding differs")]
    Binding,
    #[error("selection is not proven impossible; seal/cancel first and re-observe")]
    SelectionPossible,
    #[error("trusted conservative time has not reached LastModified plus grace")]
    GracePending,
    #[error("incomplete or malformed coherent orphan read; no absence inferred")]
    Corrupt,
    #[error("native orphan read failed; no absence inferred")]
    ReadFailed,
    #[error("native orphan action construction failed")]
    ActionBuild,
}

// Private borrowed projection, not another public observation factory. Only the
// public entry point can obtain it from provider evidence. Unit fixtures exercise
// the pure native builder without manufacturing an ObservedOrphanObject.
#[derive(Clone, Copy)]
enum Attribution<'a> {
    Known {
        intent: &'a ResourceId,
        attempt: &'a ResourceId,
        constraints_sha256: &'a [u8; 32],
    },
    Unattributed,
}
#[derive(Clone, Copy)]
struct Candidate<'a> {
    namespace: &'a NamespaceSnapshot,
    version: &'a VersionRef,
    last_modified: TimestampMs,
    attribution: Attribution<'a>,
}
impl<'a> Candidate<'a> {
    fn observed(observation: &'a ObservedOrphanObject) -> Self {
        Self {
            namespace: observation.namespace(),
            version: observation.version(),
            last_modified: observation.last_modified(),
            attribution: match observation.attribution() {
                OrphanAttribution::Known(known) => Attribution::Known {
                    intent: known.intent_id(),
                    attempt: known.attempt_id(),
                    constraints_sha256: known.constraints_sha256(),
                },
                OrphanAttribution::Unattributed(_) => Attribution::Unattributed,
            },
        }
    }
    fn keys(&self, domain: &DomainId) -> Result<Vec<Item>, OrphanError> {
        if self.namespace.domain() != domain {
            return Err(OrphanError::Binding);
        }
        let mut keys = vec![
            control_key(domain),
            namespace_key(domain, self.namespace.id()),
            object_key(domain, &self.version.fingerprint()),
        ];
        if let Attribution::Known {
            intent, attempt, ..
        } = self.attribution
        {
            if intent.scope().domain() != domain
                || attempt.scope().domain() != domain
                || intent.scope().incarnation() != attempt.scope().incarnation()
                || intent.scope().generation() > attempt.scope().generation()
            {
                return Err(OrphanError::Binding);
            }
            keys.push(resource_key("ATTEMPT", attempt));
            keys.push(resource_key("INTENT", intent));
        }
        Ok(keys)
    }
}

impl CommandJournal {
    /// Consume a fresh opaque exact-version observation; perform one coherent
    /// native read (three or five rows), then prepare one ordinary journaled
    /// absent-only RETIRING registration or return a checked existing version.
    ///
    /// The synchronous sampler MUST obtain fresh trusted time, not return a
    /// captured observation. It runs after the coherent read, with no subsequent
    /// await before preparation. ACTIVE/OPEN deadlines are never treated as state
    /// transitions. Historical attribution may predate the current incarnation;
    /// this command must still be admitted by current control.
    ///
    /// Submit/retry only the frozen PreparedCommand, resolving unknown outcomes
    /// through the normal journal. After Expired there is no historical proof or
    /// automatic replay: a new decision requires a new observation and context.
    /// ExistingTracked writes nothing and does not authorize raw S3 deletion;
    /// the existing tracked-retirement adapter owns subsequent deletion.
    pub async fn prepare_orphan_retirement(
        &self,
        context: CommandContext,
        observation: ObservedOrphanObject,
        policy: &OrphanGracePolicy,
        sample_clock: &impl Fn() -> Result<ClockObservation, ClockError>,
        result: Vec<u8>,
    ) -> Result<OrphanPreparation, OrphanError> {
        self.prepare_observed_orphan(
            context,
            Candidate::observed(&observation),
            policy,
            sample_clock,
            result,
        )
        .await
    }

    async fn prepare_observed_orphan(
        &self,
        context: CommandContext,
        candidate: Candidate<'_>,
        policy: &OrphanGracePolicy,
        sample_clock: &impl Fn() -> Result<ClockObservation, ClockError>,
        result: Vec<u8>,
    ) -> Result<OrphanPreparation, OrphanError> {
        let gets = read_actions(self.coordination_arn(), self.domain_id(), candidate)?;
        let response = self
            .native_client()
            .transact_get_items()
            .set_transact_items(Some(gets))
            .send()
            .await
            .map_err(|_| OrphanError::ReadFailed)?;
        let rows: Vec<_> = response
            .responses()
            .iter()
            .map(|r| r.item.as_ref())
            .collect();
        let built = build(
            self.coordination_arn(),
            self.domain_id(),
            self.native_client().config().region().map(|r| r.as_ref()),
            context.id(),
            candidate,
            &rows,
            policy,
            sample_clock,
        )?;
        self.finish_orphan_preparation(context, built, result)
    }

    fn finish_orphan_preparation(
        &self,
        context: CommandContext,
        built: Built,
        result: Vec<u8>,
    ) -> Result<OrphanPreparation, OrphanError> {
        match built {
            Built::Existing(version) => Ok(OrphanPreparation::ExistingTracked(version)),
            Built::Create { control, internal } => {
                Ok(OrphanPreparation::Prepared(self.prepare_internal_guarded(
                    context,
                    ApplicationActions::new(vec![]).map_err(JournalError::from)?,
                    internal,
                    result,
                    Some(control),
                )?))
            }
        }
    }
}

fn read_actions(
    table: &str,
    domain: &DomainId,
    candidate: Candidate<'_>,
) -> Result<Vec<TransactGetItem>, OrphanError> {
    candidate
        .keys(domain)?
        .into_iter()
        .map(|key| {
            Get::builder()
                .table_name(table)
                .set_key(Some(key))
                .build()
                .map(|get| TransactGetItem::builder().get(get).build())
                .map_err(|_| OrphanError::ActionBuild)
        })
        .collect()
}

#[derive(Debug)]
enum Built {
    Existing(VersionSnapshot),
    Create {
        control: ConditionCheck,
        internal: Vec<TransactWriteItem>,
    },
}

#[allow(clippy::too_many_arguments)]
fn build(
    table: &str,
    domain: &DomainId,
    region: Option<&str>,
    command: &CommandId,
    candidate: Candidate<'_>,
    rows: &[Option<&Item>],
    policy: &OrphanGracePolicy,
    sample_clock: &impl Fn() -> Result<ClockObservation, ClockError>,
) -> Result<Built, OrphanError> {
    if rows.len() != candidate.keys(domain)?.len() {
        return Err(OrphanError::Corrupt);
    }
    let control_row = rows[0].ok_or(JournalError::MissingControl)?;
    let control = Control::parse(Some(control_row.clone()), domain, table)?;
    let fence = control.observation.fence();
    fence.admit_command(command)?;
    let namespace = decode_namespace(
        domain,
        candidate.namespace.id(),
        rows[1].ok_or(UploadError::Missing)?,
    )?;
    if &namespace != candidate.namespace
        || region != Some(namespace.region())
        || candidate.version.store() != namespace.store()
        || !candidate.version.key().starts_with(namespace.key_prefix())
    {
        return Err(OrphanError::Binding);
    }
    // Existing accounting ALWAYS wins, including READY with live claims and
    // RETIRING/DELETED. Do not require orphan proof or grace to preserve it.
    if let Some(row) = rows[2] {
        let version = decode_version(domain, candidate.version, row)?;
        if version.namespace_id() != namespace.id()
            || version.configuration_revision() != namespace.configuration_revision()
        {
            return Err(OrphanError::Binding);
        }
        historical_scope(fence, version.creation_id())?;
        return Ok(Built::Existing(version));
    }
    let mut internal = vec![namespace_guard(table, &namespace)?];
    match candidate.attribution {
        Attribution::Known {
            intent: intent_id,
            attempt: attempt_id,
            constraints_sha256,
        } => {
            historical_scope(fence, intent_id)?;
            historical_scope(fence, attempt_id)?;
            let attempt = rows[3].map(|r| decode_attempt(attempt_id, r)).transpose()?;
            let intent = rows[4].map(|r| decode_intent(intent_id, r)).transpose()?;
            // Check BOTH present admissions before choosing any sufficient proof.
            // A terminal sibling does not turn mismatched immutable metadata into
            // garbage. Historical incarnation is intentionally not current scope.
            if let Some(a) = &attempt {
                admission_binding(
                    candidate,
                    a.namespace_id(),
                    a.configuration_revision(),
                    a.target(),
                    a.constraints(),
                    constraints_sha256,
                )?;
                if a.intent_id() != intent_id {
                    return Err(OrphanError::Binding);
                }
            }
            if let Some(i) = &intent {
                admission_binding(
                    candidate,
                    i.namespace_id(),
                    i.configuration_revision(),
                    i.target(),
                    i.constraints(),
                    constraints_sha256,
                )?;
            }
            if attempt
                .as_ref()
                .is_some_and(|a| a.state() == AttemptState::Sealed)
            {
                internal.push(guard(table, rows[3].ok_or(OrphanError::Corrupt)?.clone())?);
            } else if intent.as_ref().is_some_and(|i| {
                matches!(i.state(), IntentState::Consumed | IntentState::Cancelled)
            }) {
                internal.push(guard(table, rows[4].ok_or(OrphanError::Corrupt)?.clone())?);
            } else if attempt.is_none()
                && (attempt_id.scope().incarnation() != fence.incarnation()
                    || attempt_id.scope().generation() <= fence.closed_through())
            {
                internal.push(
                    TransactWriteItem::builder()
                        .condition_check(
                            ConditionCheck::builder()
                                .table_name(table)
                                .set_key(Some(resource_key("ATTEMPT", attempt_id)))
                                .condition_expression("attribute_not_exists(pk)")
                                .build()
                                .map_err(|_| OrphanError::ActionBuild)?,
                        )
                        .build(),
                );
            } else {
                return Err(OrphanError::SelectionPossible);
            }
        }
        Attribution::Unattributed => {}
    }
    // Sample only after decoding all authoritative metadata. Known proofs do not
    // depend on age, but a broken host clock still fails new preparation closed.
    let clock = sample_clock()?;
    clock.validate(policy.maximum_clock_uncertainty_ms)?;
    if matches!(candidate.attribution, Attribution::Unattributed)
        && clock.lower_bound()? < candidate.last_modified.checked_add(policy.grace_ms)?
    {
        return Err(OrphanError::GracePending);
    }
    let version = retiring_row(domain, command, candidate)?;
    internal.push(create(table, version)?);
    Ok(Built::Create {
        control: control_guard(table, control_row)?,
        internal,
    })
}

fn historical_scope(fence: &DomainFence, id: &ResourceId) -> Result<(), OrphanError> {
    if id.scope().domain() != fence.domain()
        || (id.scope().incarnation() == fence.incarnation()
            && id.scope().generation() > fence.current())
    {
        return Err(OrphanError::Binding);
    }
    Ok(())
}

fn admission_binding(
    candidate: Candidate<'_>,
    namespace: &crate::identity::NamespaceId,
    configuration_revision: uuid::Uuid,
    target: &UploadTarget,
    constraints: &UploadConstraints,
    observed_digest: &[u8; 32],
) -> Result<(), OrphanError> {
    if namespace != candidate.namespace.id()
        || configuration_revision != candidate.namespace.configuration_revision()
        || target.store() != candidate.version.store()
        || target.key() != candidate.version.key()
        || &constraints_digest(constraints) != observed_digest
    {
        return Err(OrphanError::Binding);
    }
    Ok(())
}

// Canonical schema-1 digest, byte-for-byte with s3::verification attribution:
// max u64 BE, expected Option marker/value, full hash Option marker/value.
// This is metadata comparison, not verification of orphan content or its length.
fn constraints_digest(c: &UploadConstraints) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(c.max_length().to_be_bytes());
    match c.expected_length() {
        Some(n) => {
            hash.update([1]);
            hash.update(n.to_be_bytes());
        }
        None => hash.update([0]),
    }
    match c.full_sha256() {
        Some(value) => {
            hash.update([1]);
            hash.update(value);
        }
        None => hash.update([0]),
    }
    hash.finalize().into()
}

fn retiring_row(
    domain: &DomainId,
    command: &CommandId,
    candidate: Candidate<'_>,
) -> Result<Item, OrphanError> {
    let mut row = object_key(domain, &candidate.version.fingerprint());
    row.extend([
        ("record".into(), s("version")),
        ("kind".into(), s("VERSION")),
        ("store".into(), s(candidate.version.store().as_str())),
        ("object_key".into(), s(candidate.version.key())),
        ("version_id".into(), s(candidate.version.version())),
        ("namespace_id".into(), s(candidate.namespace.id().as_str())),
        (
            "configuration_revision".into(),
            s(candidate.namespace.configuration_revision().to_string()),
        ),
        (
            "creation_domain".into(),
            s(command.scope().domain().as_str()),
        ),
        (
            "creation_incarnation".into(),
            s(command.scope().incarnation().to_string()),
        ),
        (
            "creation_generation".into(),
            n(command.scope().generation()),
        ),
        ("creation_uuid".into(), s(command.uuid().to_string())),
        ("state".into(), s("RETIRING")),
        ("references".into(), n(0)),
        ("claims".into(), n(0)),
    ]);
    check_size(&row)?;
    // Keep exact compatibility with the single existing tracked-version codec.
    decode_version(domain, candidate.version, &row)?;
    Ok(row)
}

fn control_guard(table: &str, row: &Item) -> Result<ConditionCheck, OrphanError> {
    let mut check = guard(table, row.clone())?
        .condition_check
        .ok_or(OrphanError::ActionBuild)?;
    // Include the optional maintenance fields' ABSENCE, not merely equality for
    // fields present today. No second control action: journal merges this check.
    for (index, field) in CONTROL_FIELDS.iter().enumerate() {
        if !row.contains_key(*field) {
            let alias = format!("#orphan_absent{index}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamodb::{EnrolledTables, Outcome, VersionState};
    use crate::identity::{NamespaceId, Scope, StoreId};
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region, retry::RetryConfig};
    use serde_json::{Value, json};
    use std::{
        cell::Cell,
        collections::{HashSet, VecDeque},
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread::JoinHandle,
        time::Duration,
    };
    use uuid::Uuid;

    const TABLE: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
    fn domain() -> DomainId {
        DomainId::new("domain").unwrap()
    }
    fn ts(n: u64) -> TimestampMs {
        TimestampMs::new(n).unwrap()
    }
    fn clock() -> Result<ClockObservation, ClockError> {
        ClockObservation::trusted(ts(1105), 5)
    }
    fn policy() -> OrphanGracePolicy {
        OrphanGracePolicy::new(100, 5).unwrap()
    }
    fn id(inc: u128, generation: u64, uuid: u128) -> ResourceId {
        ResourceId::new(
            Scope::new(domain(), Uuid::from_u128(inc), generation).unwrap(),
            Uuid::from_u128(uuid),
        )
        .unwrap()
    }
    fn command() -> CommandId {
        CommandId::new(id(1, 4, 50).scope().clone(), Uuid::from_u128(50)).unwrap()
    }
    fn control() -> Item {
        let mut row = control_key(&domain());
        row.extend([
            ("domain".into(), s("domain")),
            ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
            ("current_generation".into(), n(4)),
            ("closed_through".into(), n(0)),
            ("compacted_through".into(), n(0)),
            ("revision".into(), n(1)),
        ]);
        row
    }
    fn ns_row() -> Item {
        let mut row = namespace_key(&domain(), &NamespaceId::new("namespace").unwrap());
        row.extend([
            ("record".into(), s("namespace")),
            ("domain".into(), s("domain")),
            ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
            ("namespace_id".into(), s("namespace")),
            (
                "configuration_revision".into(),
                s(Uuid::from_u128(2).to_string()),
            ),
            ("state".into(), s("ACTIVE")),
            ("store".into(), s("store")),
            ("bucket".into(), s("fixture-not-qualified")),
            ("expected_owner".into(), s("123456789012")),
            ("region".into(), s("us-west-2")),
            ("key_prefix".into(), s("fresh/")),
            (
                "provider_profile".into(),
                s("aws-s3-versioned-lifecycle-v0"),
            ),
            ("mpu_max_age_ms".into(), n(10000)),
            ("mpu_lifecycle_margin_ms".into(), n(1000)),
        ]);
        row
    }
    fn admission(kind: &str, resource: &ResourceId, state: &str) -> Item {
        let mut row = resource_key(kind, resource);
        row.extend([
            ("record".into(), s(kind.to_ascii_lowercase())),
            ("domain".into(), s(resource.scope().domain().as_str())),
            (
                "incarnation".into(),
                s(resource.scope().incarnation().to_string()),
            ),
            ("generation".into(), n(resource.scope().generation())),
            ("resource_uuid".into(), s(resource.uuid().to_string())),
            ("namespace_id".into(), s("namespace")),
            (
                "configuration_revision".into(),
                s(Uuid::from_u128(2).to_string()),
            ),
            ("store".into(), s("store")),
            ("object_key".into(), s("fresh/object")),
            ("state".into(), s(state)),
            ("revision".into(), s(resource.uuid().to_string())),
            ("deadline".into(), n(900)),
            ("max_length".into(), n(100)),
        ]);
        row
    }
    struct Fixture {
        ns: NamespaceSnapshot,
        version: VersionRef,
        intent: ResourceId,
        attempt: ResourceId,
        digest: [u8; 32],
        rows: Vec<Option<Item>>,
    }
    impl Fixture {
        fn new() -> Self {
            Self::incarnation(1)
        }
        fn incarnation(inc: u128) -> Self {
            let ns = decode_namespace(
                &domain(),
                &NamespaceId::new("namespace").unwrap(),
                &ns_row(),
            )
            .unwrap();
            let intent = id(inc, 1, 3);
            let attempt = id(inc, 2, 4);
            let mut a = admission("ATTEMPT", &attempt, "SEALED");
            a.extend([
                ("intent_domain".into(), s("domain")),
                (
                    "intent_incarnation".into(),
                    s(intent.scope().incarnation().to_string()),
                ),
                ("intent_generation".into(), n(1)),
                ("intent_uuid".into(), s(intent.uuid().to_string())),
                ("mpu_admitted_lower_bound".into(), n(100)),
                ("max_mpu_use_until".into(), n(1000)),
            ]);
            let rows = vec![
                Some(control()),
                Some(ns_row()),
                None,
                Some(a),
                Some(admission("INTENT", &intent, "OPEN")),
            ];
            Self {
                ns,
                version: VersionRef::new(
                    StoreId::new("store").unwrap(),
                    "fresh/object",
                    "exact-version",
                )
                .unwrap(),
                intent,
                attempt,
                digest: constraints_digest(&UploadConstraints::new(100, None, None).unwrap()),
                rows,
            }
        }
        fn candidate(&self) -> Candidate<'_> {
            Candidate {
                namespace: &self.ns,
                version: &self.version,
                last_modified: ts(1000),
                attribution: Attribution::Known {
                    intent: &self.intent,
                    attempt: &self.attempt,
                    constraints_sha256: &self.digest,
                },
            }
        }
        fn build(&self) -> Result<Built, OrphanError> {
            build(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &command(),
                self.candidate(),
                &self.rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
                &policy(),
                &clock,
            )
        }
        fn state(&mut self, index: usize, state: &str) {
            self.rows[index]
                .as_mut()
                .unwrap()
                .insert("state".into(), s(state));
        }
        fn closed(&mut self) {
            self.rows[0]
                .as_mut()
                .unwrap()
                .insert("closed_through".into(), n(2));
        }
    }
    fn created(built: Built) -> (ConditionCheck, Vec<TransactWriteItem>) {
        match built {
            Built::Create { control, internal } => (control, internal),
            _ => panic!("expected absent-only create"),
        }
    }
    fn assert_full(check: &ConditionCheck, row: &Item) {
        let names = check.expression_attribute_names().unwrap();
        let values = check.expression_attribute_values().unwrap();
        for (field, value) in row {
            let alias = names.iter().find(|(_, f)| *f == field).unwrap().0;
            let term = check
                .condition_expression()
                .split(" AND ")
                .find(|term| term.starts_with(&format!("{alias} = ")))
                .unwrap();
            let parameter = term.strip_prefix(&format!("{alias} = ")).unwrap();
            assert_eq!(&values[parameter], value, "{field}");
        }
    }

    #[test]
    fn all_selection_impossibility_branches_guard_the_selected_exact_proof() {
        for (attempt, intent, closed, proof_index) in [
            (Some("SEALED"), Some("OPEN"), false, Some(3)),
            (Some("SEALED"), None, false, Some(3)),
            (Some("SEALED"), Some("SELECTED"), false, Some(3)),
            (Some("ACTIVE"), Some("CONSUMED"), false, Some(4)),
            (Some("ACTIVE"), Some("CANCELLED"), false, Some(4)),
            (None, Some("CANCELLED"), false, Some(4)),
            (None, Some("OPEN"), true, None),
            (None, None, true, None),
        ] {
            let mut f = Fixture::new();
            for (index, state) in [(3, attempt), (4, intent)] {
                if let Some(state) = state {
                    f.state(index, state);
                } else {
                    f.rows[index] = None;
                }
            }
            if closed {
                f.closed();
            }
            let (control, actions) = created(f.build().unwrap());
            assert_eq!(actions.len(), 3);
            assert_full(&control, f.rows[0].as_ref().unwrap());
            assert_full(
                actions[0].condition_check.as_ref().unwrap(),
                f.rows[1].as_ref().unwrap(),
            );
            let proof = actions[1].condition_check.as_ref().unwrap();
            if let Some(index) = proof_index {
                assert_full(proof, f.rows[index].as_ref().unwrap());
            } else {
                assert_eq!(proof.condition_expression(), "attribute_not_exists(pk)");
                assert_eq!(proof.key(), &resource_key("ATTEMPT", &f.attempt));
            }
        }
    }

    #[test]
    fn open_epoch_absence_active_expired_and_selected_are_not_proofs() {
        for (attempt, intent, closed) in [
            (None, None, false),
            (None, Some("OPEN"), false),
            (Some("ACTIVE"), Some("OPEN"), false),
            (Some("ACTIVE"), Some("OPEN"), true),
            (Some("ACTIVE"), Some("SELECTED"), true),
            (Some("ACTIVE"), None, true),
        ] {
            let mut f = Fixture::new();
            for (index, state) in [(3, attempt), (4, intent)] {
                if let Some(state) = state {
                    f.state(index, state);
                } else {
                    f.rows[index] = None;
                }
            }
            if closed {
                f.closed();
            }
            assert!(matches!(f.build(), Err(OrphanError::SelectionPossible)));
        }
    }

    #[test]
    fn historical_attribution_is_not_current_command_authority() {
        let mut f = Fixture::incarnation(99);
        f.rows[3] = None;
        f.rows[4] = None;
        let (control, actions) = created(f.build().unwrap());
        assert_full(&control, f.rows[0].as_ref().unwrap());
        assert_eq!(
            actions[1].condition_check.as_ref().unwrap().key(),
            &resource_key("ATTEMPT", &f.attempt)
        );
        let old_command =
            CommandId::new(id(99, 4, 50).scope().clone(), Uuid::from_u128(50)).unwrap();
        assert!(matches!(
            build(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &old_command,
                f.candidate(),
                &f.rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
                &policy(),
                &clock
            ),
            Err(OrphanError::Fence(_))
        ));
        let mut f = Fixture::incarnation(99);
        // Historical SEALED is independently sufficient, but ACTIVE isn't.
        assert!(f.build().is_ok());
        f.state(3, "ACTIVE");
        assert!(matches!(f.build(), Err(OrphanError::SelectionPossible)));
        let future = id(1, 5, 4);
        f.attempt = future;
        assert!(matches!(f.build(), Err(OrphanError::Binding)));
    }

    #[test]
    fn existing_ready_retiring_deleted_are_preserved_without_orphan_proof_or_clock() {
        for (state, expected) in [
            ("READY", VersionState::Ready),
            ("RETIRING", VersionState::Retiring),
            ("DELETED", VersionState::Deleted),
        ] {
            let mut f = Fixture::new();
            let mut row = retiring_row(&domain(), &command(), f.candidate()).unwrap();
            row.insert("state".into(), s(state));
            if state == "READY" {
                row.insert("references".into(), n(7));
                row.insert("claims".into(), n(9));
            }
            f.rows[2] = Some(row.clone());
            f.rows[3] = Some(Item::new()); // Existing version wins; no admission proof needed.
            let Built::Existing(version) = build(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &command(),
                f.candidate(),
                &f.rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
                &policy(),
                &|| panic!("preservation needs no clock"),
            )
            .unwrap() else {
                panic!()
            };
            assert_eq!(version.state(), expected);
            assert_eq!(
                super::super::protection::encode_version(&version).unwrap(),
                row
            );
        }
    }

    #[test]
    fn all_present_admissions_must_match_even_when_the_other_is_terminal() {
        for index in [3, 4] {
            for (field, value) in [
                ("namespace_id", s("another")),
                ("configuration_revision", s(Uuid::from_u128(88).to_string())),
                ("store", s("another")),
                ("object_key", s("fresh/different")),
                ("max_length", n(101)),
                ("expected_length", n(50)),
                (
                    "full_sha256",
                    AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new([7; 32])),
                ),
                ("domain", s("foreign")),
                ("resource_uuid", s(Uuid::from_u128(90).to_string())),
                ("pk", s("wrong-address")),
            ] {
                let mut f = Fixture::new();
                f.state(4, "CANCELLED");
                f.rows[index].as_mut().unwrap().insert(field.into(), value);
                assert!(f.build().is_err(), "{index}: {field}");
            }
        }
        let mut f = Fixture::new();
        f.state(4, "CONSUMED");
        f.rows[3]
            .as_mut()
            .unwrap()
            .insert("intent_uuid".into(), s(Uuid::from_u128(89).to_string()));
        assert!(matches!(f.build(), Err(OrphanError::Binding)));
        let mut f = Fixture::new();
        f.digest[0] ^= 1;
        assert!(matches!(f.build(), Err(OrphanError::Binding)));
    }

    #[test]
    fn full_namespace_and_exact_existing_version_binding_fail_closed() {
        for (field, value) in [
            ("bucket", s("changed-bucket")),
            ("expected_owner", s("999999999999")),
            ("region", s("us-east-1")),
            ("key_prefix", s("fresh")),
            ("incarnation", s(Uuid::from_u128(7).to_string())),
            ("configuration_revision", s(Uuid::from_u128(8).to_string())),
            ("namespace_id", s("other")),
            ("domain", s("foreign")),
            ("mpu_max_age_ms", n(20000)),
            ("mpu_lifecycle_margin_ms", n(2000)),
            ("provider_profile", s("unrecognized")),
        ] {
            let mut f = Fixture::new();
            f.rows[1].as_mut().unwrap().insert(field.into(), value);
            assert!(f.build().is_err(), "{field}");
        }
        let f = Fixture::new();
        let rows: Vec<_> = f.rows.iter().map(Option::as_ref).collect();
        assert!(matches!(
            build(
                TABLE,
                &DomainId::new("foreign").unwrap(),
                Some("us-west-2"),
                &command(),
                f.candidate(),
                &rows,
                &policy(),
                &clock
            ),
            Err(OrphanError::Binding)
        ));
        assert!(matches!(
            build(
                TABLE,
                &domain(),
                Some("us-east-1"),
                &command(),
                f.candidate(),
                &rows,
                &policy(),
                &clock
            ),
            Err(OrphanError::Binding)
        ));
        for (field, value) in [
            ("namespace_id", s("other")),
            ("configuration_revision", s(Uuid::from_u128(8).to_string())),
            ("store", s("other")),
            ("object_key", s("fresh/other")),
            ("version_id", s("other")),
            ("creation_domain", s("foreign")),
            ("creation_generation", n(5)),
            (
                "references",
                AttributeValue::N("18446744073709551616".into()),
            ),
        ] {
            let mut f = Fixture::new();
            let mut row = retiring_row(&domain(), &command(), f.candidate()).unwrap();
            row.insert(field.into(), value);
            f.rows[2] = Some(row);
            assert!(f.build().is_err(), "{field}");
        }
    }

    #[test]
    fn explicit_grace_uses_only_fresh_trusted_lower_bound_and_checked_arithmetic() {
        assert!(OrphanGracePolicy::new(0, 5).is_err());
        assert!(OrphanGracePolicy::new(u64::MAX, 5).is_err());
        assert!(OrphanGracePolicy::new(1, u64::MAX).is_err());
        let f = Fixture::new();
        let mut candidate = f.candidate();
        candidate.attribution = Attribution::Unattributed;
        let rows: Vec<_> = f.rows[..3].iter().map(Option::as_ref).collect();
        let run = |c, clock: ClockObservation| {
            build(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &command(),
                c,
                &rows,
                &policy(),
                &|| Ok(clock),
            )
        };
        assert!(run(candidate, clock().unwrap()).is_ok()); // lower == 1100
        assert!(matches!(
            run(candidate, ClockObservation::trusted(ts(1104), 5).unwrap()),
            Err(OrphanError::GracePending)
        ));
        assert!(matches!(
            run(candidate, ClockObservation::trusted(ts(1105), 6).unwrap()),
            Err(OrphanError::Clock(ClockError::ExcessiveUncertainty))
        ));
        assert!(matches!(
            run(candidate, ClockObservation::untrusted(ts(9999), 0)),
            Err(OrphanError::Clock(ClockError::Untrusted))
        ));
        candidate.last_modified = ts(1200);
        assert!(matches!(
            run(candidate, clock().unwrap()),
            Err(OrphanError::GracePending)
        ));
        candidate.last_modified = TimestampMs::MAX;
        assert!(matches!(
            run(candidate, clock().unwrap()),
            Err(OrphanError::Clock(ClockError::Overflow))
        ));
        // Known proofs do not use age, but still sample and validate host time.
        let samples = Cell::new(0);
        assert!(
            build(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &command(),
                f.candidate(),
                &f.rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
                &policy(),
                &|| {
                    samples.set(samples.get() + 1);
                    clock()
                }
            )
            .is_ok()
        );
        assert_eq!(samples.get(), 1);
    }

    #[test]
    fn malformed_incomplete_reads_and_unknown_control_never_become_absence() {
        let mut f = Fixture::new();
        f.rows.pop();
        assert!(matches!(f.build(), Err(OrphanError::Corrupt)));
        for index in 0..5 {
            let mut f = Fixture::new();
            f.rows[index] = Some(Item::new());
            assert!(f.build().is_err(), "empty row {index}");
        }
        for index in 0..2 {
            let mut f = Fixture::new();
            f.rows[index] = None;
            assert!(f.build().is_err());
        }
        let mut f = Fixture::new();
        f.rows[0]
            .as_mut()
            .unwrap()
            .insert("unknown-authority".into(), s("x"));
        let samples = Cell::new(0);
        assert!(
            build(
                TABLE,
                &domain(),
                Some("us-west-2"),
                &command(),
                f.candidate(),
                &f.rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
                &policy(),
                &|| {
                    samples.set(1);
                    clock()
                }
            )
            .is_err()
        );
        assert_eq!(samples.get(), 0);
    }

    #[test]
    fn native_guards_absent_put_creator_and_digest_encoding_are_exact() {
        let f = Fixture::new();
        let (control, actions) = created(f.build().unwrap());
        let names = control.expression_attribute_names().unwrap();
        for field in CONTROL_FIELDS {
            if !f.rows[0].as_ref().unwrap().contains_key(field) {
                let alias = names
                    .iter()
                    .find(|(_, value)| value.as_str() == field)
                    .unwrap()
                    .0;
                assert!(
                    control
                        .condition_expression()
                        .contains(&format!("attribute_not_exists({alias})"))
                );
            }
        }
        let put = actions[2].put.as_ref().unwrap();
        assert_eq!(put.condition_expression(), Some("attribute_not_exists(pk)"));
        let snapshot = decode_version(&domain(), &f.version, put.item()).unwrap();
        assert_eq!(
            snapshot.creation_id(),
            &ResourceId::from_command(&command())
        );
        assert_ne!(snapshot.creation_id(), &f.attempt);
        assert_eq!(
            (snapshot.state(), snapshot.references(), snapshot.claims()),
            (VersionState::Retiring, 0, 0)
        );
        assert_eq!(put.item()["record"], s("version"));
        assert_eq!(put.item()["kind"], s("VERSION"));
        let proof = actions[1].condition_check.as_ref().unwrap();
        for field in ["expected_length", "full_sha256"] {
            assert!(
                proof
                    .condition_expression()
                    .contains(&format!("attribute_not_exists(#missing_{field})"))
            );
        }
        for expected in [None, Some(7)] {
            for hash in [None, Some([9; 32])] {
                let c = UploadConstraints::new(100, expected, hash).unwrap();
                let mut bytes = 100u64.to_be_bytes().to_vec();
                bytes.push(u8::from(expected.is_some()));
                if let Some(n) = expected {
                    bytes.extend(n.to_be_bytes());
                }
                bytes.push(u8::from(hash.is_some()));
                if let Some(h) = hash {
                    bytes.extend(h);
                }
                let expected: [u8; 32] = Sha256::digest(bytes).into();
                assert_eq!(constraints_digest(&c), expected);
            }
        }
        let gets = read_actions(TABLE, &domain(), f.candidate()).unwrap();
        assert_eq!(gets.len(), 5);
        for (get, key) in gets.iter().zip(f.candidate().keys(&domain()).unwrap()) {
            assert_eq!(get.get.as_ref().unwrap().table_name(), TABLE);
            assert_eq!(get.get.as_ref().unwrap().key(), &key);
        }
    }

    #[test]
    fn optional_constraints_and_control_window_are_guarded_in_full() {
        let mut f = Fixture::new();
        let c = UploadConstraints::new(100, Some(7), Some([9; 32])).unwrap();
        f.digest = constraints_digest(&c);
        for index in [3, 4] {
            let row = f.rows[index].as_mut().unwrap();
            row.insert("expected_length".into(), n(7));
            row.insert(
                "full_sha256".into(),
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new([9; 32])),
            );
        }
        f.closed();
        f.rows[0].as_mut().unwrap().extend([
            ("resolution_retention_ms".into(), n(100)),
            ("compaction_candidate".into(), n(1)),
            ("compaction_not_before_ms".into(), n(1500)),
        ]);
        let (control, internal) = created(f.build().unwrap());
        assert_full(&control, f.rows[0].as_ref().unwrap());
        assert_full(
            internal[1].condition_check.as_ref().unwrap(),
            f.rows[3].as_ref().unwrap(),
        );
        for generation in [2, 5] {
            let command =
                CommandId::new(id(1, generation, 50).scope().clone(), Uuid::from_u128(50)).unwrap();
            assert!(matches!(
                build(
                    TABLE,
                    &domain(),
                    Some("us-west-2"),
                    &command,
                    f.candidate(),
                    &f.rows.iter().map(Option::as_ref).collect::<Vec<_>>(),
                    &policy(),
                    &clock
                ),
                Err(OrphanError::Fence(_))
            ));
        }
    }

    #[test]
    fn largest_checked_tuple_still_uses_bounded_canonical_version_codec() {
        let mut f = Fixture::new();
        f.version = VersionRef::new(
            StoreId::new("s".repeat(128)).unwrap(),
            "k".repeat(1024),
            "v".repeat(1024),
        )
        .unwrap();
        let mut ns = ns_row();
        ns.extend(namespace_key(
            &domain(),
            &NamespaceId::new("n".repeat(128)).unwrap(),
        ));
        ns.insert("namespace_id".into(), s("n".repeat(128)));
        ns.insert("store".into(), s("s".repeat(128)));
        ns.insert("key_prefix".into(), s(""));
        f.ns =
            decode_namespace(&domain(), &NamespaceId::new("n".repeat(128)).unwrap(), &ns).unwrap();
        let row = retiring_row(&domain(), &command(), f.candidate()).unwrap();
        check_size(&row).unwrap();
        let mut oversized = row;
        oversized.insert("object_key".into(), s("x".repeat(4000)));
        assert!(matches!(
            check_size(&oversized),
            Err(UploadError::RecordSize)
        ));
        assert!(decode_version(&domain(), &f.version, &oversized).is_err());
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
                            _ => panic!("flat fixture fields only"),
                        },
                    )
                })
                .collect(),
        )
    }
    // Native SDK HTTP capture only. It neither evaluates conditions nor claims
    // service concurrency evidence. Fixed credentials, loopback, retries disabled.
    struct NativeFixture {
        address: std::net::SocketAddr,
        stop: Arc<AtomicBool>,
        replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
        requests: Arc<Mutex<Vec<(String, Value)>>>,
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
                    let headers = String::from_utf8(headers).unwrap().to_ascii_lowercase();
                    let length = headers
                        .lines()
                        .find_map(|l| {
                            l.strip_prefix("content-length: ")
                                .map(|n| n.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    assert!(length < 1_000_000);
                    let target = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("x-amz-target: "))
                        .unwrap()
                        .to_owned();
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).unwrap();
                    captured
                        .lock()
                        .unwrap()
                        .push((target, serde_json::from_slice(&body).unwrap()));
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
        fn reply(&self, body: Value) {
            self.replies.lock().unwrap().push_back((200, body));
        }
        fn rows(&self, rows: &[Option<Item>]) {
            self.reply(json!({"Responses": rows.iter().map(|r| r.as_ref().map_or(json!({}), |r| json!({"Item": wire(r)}))).collect::<Vec<_>>()}));
        }
        async fn journal(&self) -> CommandJournal {
            self.reply(json!({"Table":{"TableName":"coord","TableArn":TABLE,"TableStatus":"ACTIVE",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
            let client = aws_sdk_dynamodb::Client::from_conf(
                aws_sdk_dynamodb::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new("us-west-2"))
                    .credentials_provider(Credentials::new(
                        "fixture", "fixture", None, None, "fixture",
                    ))
                    .endpoint_url(format!("http://{}", self.address))
                    .retry_config(RetryConfig::disabled())
                    .build(),
            );
            let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
            CommandJournal::new(client, tables, domain())
        }
        async fn context(&self, journal: &CommandJournal) -> CommandContext {
            self.reply(json!({"Item":wire(&control())}));
            journal.command_context().await.unwrap()
        }
    }
    impl Drop for NativeFixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            self.worker.take().unwrap().join().unwrap();
        }
    }

    #[tokio::test]
    async fn native_read_fresh_clock_and_actual_journal_submission_have_one_control_action() {
        let native = NativeFixture::new();
        let journal = native.journal().await;
        let f = Fixture::new();
        let context = native.context(&journal).await;
        let creator = ResourceId::from_command(context.id());
        native.rows(&f.rows);
        let samples = Cell::new(0);
        let prepared = journal
            .prepare_observed_orphan(
                context,
                f.candidate(),
                &policy(),
                &|| {
                    assert!(
                        native
                            .requests
                            .lock()
                            .unwrap()
                            .last()
                            .unwrap()
                            .0
                            .ends_with("transactgetitems")
                    );
                    samples.set(samples.get() + 1);
                    clock()
                },
                b"retired".to_vec(),
            )
            .await
            .unwrap();
        let OrphanPreparation::Prepared(prepared) = prepared else {
            panic!()
        };
        assert_eq!(samples.get(), 1);
        assert_eq!(prepared.action_count(), 5);
        assert_eq!(prepared.token().id().uuid(), creator.uuid());
        native.reply(json!({}));
        assert_eq!(
            journal.execute(&prepared).await.unwrap().outcome,
            Outcome::Committed(b"retired".to_vec())
        );
        let requests = native.requests.lock().unwrap();
        let gets = requests
            .iter()
            .find(|(target, _)| target.ends_with("transactgetitems"))
            .unwrap()
            .1["TransactItems"]
            .as_array()
            .unwrap();
        assert_eq!(gets.len(), 5);
        for (get, key) in gets.iter().zip(f.candidate().keys(&domain()).unwrap()) {
            assert_eq!(get["Get"]["Key"], wire(&key));
        }
        let actions = requests.last().unwrap().1["TransactItems"]
            .as_array()
            .unwrap();
        assert_eq!(actions.len(), 5);
        let mut keys = HashSet::new();
        for action in actions {
            let inner = action.as_object().unwrap().values().next().unwrap();
            let row = inner.get("Key").or_else(|| inner.get("Item")).unwrap();
            assert!(keys.insert((row["pk"].to_string(), row["sk"].to_string())));
            assert!(action.get("Update").is_none() && action.get("Delete").is_none());
        }
        let control_actions: Vec<_> = actions
            .iter()
            .filter(|a| a["ConditionCheck"]["Key"] == wire(&control_key(&domain())))
            .collect();
        assert_eq!(control_actions.len(), 1);
        let check = &control_actions[0]["ConditionCheck"];
        let expression = check["ConditionExpression"].as_str().unwrap();
        assert!(expression.contains("#closed < :g") && expression.contains("#current >= :g"));
        let values = check["ExpressionAttributeValues"].as_object().unwrap();
        let names = check["ExpressionAttributeNames"].as_object().unwrap();
        for (field, value) in control() {
            let alias = names
                .iter()
                .find(|(alias, name)| alias.starts_with("#f") && name.as_str() == Some(&field))
                .unwrap()
                .0;
            let parameter = format!(":v{}", alias.strip_prefix("#f").unwrap());
            assert!(expression.contains(&format!("{alias} = {parameter}")));
            assert_eq!(
                values[&parameter],
                wire(&Item::from([(field.clone(), value)]))[&field]
            );
        }
        let put = &actions[3]["Put"];
        assert_eq!(put["ConditionExpression"], "attribute_not_exists(pk)");
        assert_eq!(
            put["Item"]["creation_uuid"],
            json!({"S":creator.uuid().to_string()})
        );
        assert_eq!(put["Item"]["state"], json!({"S":"RETIRING"}));
        assert_eq!(put["Item"]["references"], json!({"N":"0"}));
        assert_eq!(put["Item"]["claims"], json!({"N":"0"}));
        assert_eq!(
            actions[4]["Put"]["Item"]["status"],
            json!({"S":"COMMITTED"})
        );
    }

    #[tokio::test]
    async fn native_unattributed_read_is_three_rows_and_failures_do_not_sample_or_write() {
        let native = NativeFixture::new();
        let journal = native.journal().await;
        let f = Fixture::new();
        let mut candidate = f.candidate();
        candidate.attribution = Attribution::Unattributed;
        let context = native.context(&journal).await;
        native.rows(&f.rows[..3]);
        let OrphanPreparation::Prepared(prepared) = journal
            .prepare_observed_orphan(context, candidate, &policy(), &clock, vec![])
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(prepared.action_count(), 4);
        assert_eq!(
            native.requests.lock().unwrap().last().unwrap().1["TransactItems"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        let context = native.context(&journal).await;
        native.replies.lock().unwrap().push_back((
            500,
            json!({"__type":"InternalServerError","message":"fixture"}),
        ));
        assert!(matches!(
            journal
                .prepare_observed_orphan(
                    context,
                    candidate,
                    &policy(),
                    &|| panic!("failed read cannot sample"),
                    vec![]
                )
                .await,
            Err(OrphanError::ReadFailed)
        ));
        let context = native.context(&journal).await;
        native.reply(json!({"Responses":[{},{}]}));
        assert!(matches!(
            journal
                .prepare_observed_orphan(
                    context,
                    candidate,
                    &policy(),
                    &|| panic!("incomplete read cannot sample"),
                    vec![]
                )
                .await,
            Err(OrphanError::Corrupt)
        ));
        assert!(
            native
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|(target, _)| !target.ends_with("transactwriteitems"))
        );
    }
}
