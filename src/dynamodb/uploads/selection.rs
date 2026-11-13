//! Verified selection with staged publication and finite-claim transitions.
//!
//! The child modules reuse `Claim` and the shared version-count engine, atomically
//! guarding the staged row and changing its ONE counted claim. Renewing an intent
//! never renews this independent claim. Use pins, inventory, payload retirement
//! and metadata compaction still require implementation and acceptance.
//!
//! The STAGED row uses the original intent address, with a distinct resource
//! kind. Its immutable tuple is never added to the admitted intent/attempt rows.
//! Domain/incarnation are shared across the intent, attempt and selection-created
//! claim IDs; their individual generations and UUIDs remain explicit. All stored
//! fields are flat native scalars, not a serialized JSON record or fingerprint
//! authority. Version rows use the existing references::object_key and counter
//! schema (`record = version`), plus `kind = VERSION` and creation provenance.

use super::*;
use crate::claim::{Claim, ClaimState};
#[cfg(any(feature = "s3", test))]
use crate::identity::CommandId;
use crate::identity::VersionRef;

mod lifecycle;
pub(in crate::dynamodb) use lifecycle::selected_binding;
mod publication;
pub use publication::PublicationError;

/// Checked strong observation, NOT publication/use authority. There is no public
/// constructor or Deserialize implementation. A cloned Claim is only a pure
/// model; mutating it cannot change this snapshot or persisted protection.
///
/// Terminal claims decode for future cleanup, without automatic state changes.
/// ```compile_fail
/// use storelink::dynamodb::StagedSnapshot;
/// let _: StagedSnapshot = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedSnapshot {
    intent: ResourceId,
    attempt: ResourceId,
    namespace: NamespaceId,
    configuration_revision: Uuid,
    constraints: UploadConstraints,
    verified_length: u64,
    verified_full_sha256: Option<[u8; 32]>,
    claim: Claim,
}
impl StagedSnapshot {
    pub fn intent_id(&self) -> &ResourceId {
        &self.intent
    }
    pub fn attempt_id(&self) -> &ResourceId {
        &self.attempt
    }
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace
    }
    pub fn configuration_revision(&self) -> Uuid {
        self.configuration_revision
    }
    /// The claim's identity is derived from the selection command, not the
    /// intent's original creation command. Its lifetime is independent.
    pub fn claim(&self) -> &Claim {
        &self.claim
    }
    pub fn version(&self) -> &VersionRef {
        self.claim.version()
    }
    /// Actual byte length verified by the upload adapter, not an expected length.
    /// This immutable observation grants no read or publication authority.
    pub fn verified_length(&self) -> u64 {
        self.verified_length
    }
    /// Full-object SHA-256 actually verified by the adapter, if any. A caller's
    /// expected hash is never substituted for absent verification evidence.
    pub fn verified_full_sha256(&self) -> Option<&[u8; 32]> {
        self.verified_full_sha256.as_ref()
    }
}

impl CommandJournal {
    /// Strong read by ORIGINAL intent identity. Missing and malformed records
    /// are distinct; neither a snapshot nor a terminal claim authorizes use.
    pub async fn read_staged(
        &self,
        intent: &ResourceId,
    ) -> Result<Option<StagedSnapshot>, UploadError> {
        check_domain(self.domain_id(), intent)?;
        self.read_upload_item(resource_key("STAGED", intent))
            .await?
            .as_ref()
            .map(|item| decode_staged(intent, item))
            .transpose()
    }

    /// Select one verified exact version and create one finite staging claim.
    /// No caller application writes or counters are accepted. The complete
    /// journal transaction contains admission, full namespace guard, guarded
    /// OPEN -> SELECTED and ACTIVE -> SEALED replacements, absent-only READY
    /// version/staging puts, and terminal outcome. Existing version records are
    /// never adopted/replaced, including a previous selection of this tuple.
    ///
    /// The receipt's observations may be stale: native expected-row conditions
    /// require them STILL to match, including revisions, deadlines and existence.
    /// A concurrent renewal therefore requires fresh verification, not weakening
    /// the guards. The caller supplies a fresh trusted clock and fixed absolute
    /// deadline; a retry uses the same PreparedCommand, never `now + duration`.
    /// The transaction does not compare a server NOW or cancel in-flight S3 I/O.
    ///
    /// This method selects only; publication and staged lifetime changes are
    /// separate operations. Collection and full protocol acceptance remain separate.
    ///
    /// A raw exact-version tuple is not selection evidence:
    /// ```compile_fail,E0308
    /// use storelink::{dynamodb::{CommandContext, CommandJournal},
    ///     identity::VersionRef, clock::{ClockObservation, TimestampMs}, claim::ClaimPolicy};
    /// fn raw_tuple(journal: &CommandJournal, context: CommandContext,
    ///     version: VersionRef, deadline: TimestampMs, clock: ClockObservation, policy: &ClaimPolicy) {
    ///     journal.prepare_select_upload(context, version, deadline, clock, policy, vec![]).unwrap();
    /// }
    /// ```
    #[cfg(feature = "s3")]
    pub fn prepare_select_upload(
        &self,
        context: CommandContext,
        receipt: crate::s3::VerifiedUploadReceipt,
        staging_deadline: TimestampMs,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        let internal = selection_actions(
            self.coordination_arn(),
            self.domain_id(),
            context.id(),
            SelectionEvidence {
                namespace: receipt.namespace(),
                intent: receipt.intent(),
                attempt: receipt.attempt(),
                version: receipt.version(),
                length: receipt.length(),
                full_sha256: receipt.full_sha256(),
            },
            staging_deadline,
            clock,
            policy,
        )?;
        self.upload_command(context, internal, result)
    }
}

// Private action-builder input, NOT a receipt or a public unchecked factory.
// Unit fixtures below exercise this boundary without claiming S3 verification.
#[cfg(any(feature = "s3", test))]
struct SelectionEvidence<'a> {
    namespace: &'a NamespaceSnapshot,
    intent: &'a IntentSnapshot,
    attempt: &'a AttemptSnapshot,
    version: &'a VersionRef,
    length: u64,
    full_sha256: Option<&'a [u8; 32]>,
}

#[cfg(any(feature = "s3", test))]
#[allow(clippy::too_many_arguments)]
fn selection_actions(
    table: &str,
    domain: &DomainId,
    command: &CommandId,
    evidence: SelectionEvidence<'_>,
    deadline: TimestampMs,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, UploadError> {
    let SelectionEvidence {
        namespace,
        intent,
        attempt,
        version,
        length,
        full_sha256,
    } = evidence;
    // The journal adds its ownership/fence guards; these comparisons reject a
    // fresh command wrapping an unrelated or future resource before submission.
    for id in [intent.id(), attempt.id()] {
        check_domain(domain, id)?;
        if command.scope().domain() != id.scope().domain()
            || command.scope().incarnation() != id.scope().incarnation()
            || command.scope().generation() < id.scope().generation()
        {
            return Err(UploadError::Binding);
        }
    }
    live_attempt(attempt, intent, namespace, clock, policy)?;
    if intent.id().scope().generation() > attempt.id().scope().generation()
        || version.store() != attempt.target().store()
        || version.key() != attempt.target().key()
        || length > attempt.constraints().max_length()
        || attempt
            .constraints()
            .expected_length()
            .is_some_and(|n| n != length)
        || attempt
            .constraints()
            .full_sha256()
            .is_some_and(|hash| Some(hash) != full_sha256)
    {
        return Err(UploadError::Binding);
    }
    let staged = StagedSnapshot {
        intent: intent.id().clone(),
        attempt: attempt.id().clone(),
        namespace: namespace.id().clone(),
        configuration_revision: namespace.configuration_revision(),
        constraints: attempt.constraints().clone(),
        verified_length: length,
        verified_full_sha256: full_sha256.copied(),
        claim: Claim::new(
            ResourceId::from_command(command),
            version.clone(),
            command.uuid(),
            deadline,
            clock,
            policy,
        )?,
    };
    let mut selected = intent.clone();
    selected.state = IntentState::Selected;
    selected.admission.revision = command.uuid();
    let mut sealed = attempt.clone();
    sealed.state = AttemptState::Sealed;
    sealed.admission.revision = command.uuid();
    Ok(vec![
        namespace_guard(table, namespace)?,
        replace(table, encode_intent(intent)?, encode_intent(&selected)?)?,
        replace(table, encode_attempt(attempt)?, encode_attempt(&sealed)?)?,
        create(table, encode_version(&staged)?)?,
        create(table, encode_staged(&staged)?)?,
    ])
}

fn staged_binding(staged: &StagedSnapshot) -> Result<(), UploadError> {
    let scope = staged.intent.scope();
    let attempt = staged.attempt.scope();
    let selection = staged.claim.id().scope();
    if scope.domain() != attempt.domain()
        || scope.domain() != selection.domain()
        || scope.incarnation() != attempt.incarnation()
        || scope.incarnation() != selection.incarnation()
        || scope.generation() > attempt.generation()
        || attempt.generation() > selection.generation()
        || staged.configuration_revision.is_nil()
        || staged.verified_length > staged.constraints.max_length()
        || staged
            .constraints
            .expected_length()
            .is_some_and(|n| n != staged.verified_length)
        || staged
            .constraints
            .full_sha256()
            .is_some_and(|hash| Some(hash) != staged.verified_full_sha256())
    {
        return Err(UploadError::Binding);
    }
    Ok(())
}

fn insert_version(item: &mut Item, version: &VersionRef) {
    item.insert("store".into(), s(version.store().as_str()));
    item.insert("object_key".into(), s(version.key()));
    item.insert("version_id".into(), s(version.version()));
}

pub(in crate::dynamodb) fn encode_staged(staged: &StagedSnapshot) -> Result<Item, UploadError> {
    staged_binding(staged)?;
    let mut item = resource_key("STAGED", &staged.intent);
    for (name, value) in [
        ("record", "staged-verified-v1".to_owned()),
        ("kind", "STAGED".to_owned()),
        ("domain", staged.intent.scope().domain().as_str().to_owned()),
        (
            "incarnation",
            staged.intent.scope().incarnation().to_string(),
        ),
        ("intent_uuid", staged.intent.uuid().to_string()),
        ("attempt_uuid", staged.attempt.uuid().to_string()),
        ("claim_uuid", staged.claim.id().uuid().to_string()),
        ("namespace_id", staged.namespace.as_str().to_owned()),
        (
            "configuration_revision",
            staged.configuration_revision.to_string(),
        ),
        ("revision", staged.claim.revision().to_string()),
        (
            "state",
            match staged.claim.state() {
                ClaimState::Active => "ACTIVE",
                ClaimState::Revoked => "REVOKED",
                ClaimState::Released => "RELEASED",
            }
            .to_owned(),
        ),
    ] {
        item.insert(name.into(), s(value));
    }
    for (name, value) in [
        ("intent_generation", staged.intent.scope().generation()),
        ("attempt_generation", staged.attempt.scope().generation()),
        ("claim_generation", staged.claim.id().scope().generation()),
        ("deadline", staged.claim.deadline().as_millis()),
        ("max_length", staged.constraints.max_length()),
        ("verified_length", staged.verified_length),
    ] {
        item.insert(name.into(), n(value));
    }
    if let Some(length) = staged.constraints.expected_length() {
        item.insert("expected_length".into(), n(length));
    }
    for (field, hash) in [
        ("full_sha256", staged.constraints.full_sha256()),
        ("verified_full_sha256", staged.verified_full_sha256()),
    ] {
        if let Some(hash) = hash {
            item.insert(field.into(), AttributeValue::B(Blob::new(hash)));
        }
    }
    insert_version(&mut item, staged.version());
    check_size(&item)?;
    Ok(item)
}

#[cfg(any(feature = "s3", test))]
fn encode_version(staged: &StagedSnapshot) -> Result<Item, UploadError> {
    staged_binding(staged)?;
    let creation = staged.claim.id();
    let mut item = super::super::references::object_key(
        creation.scope().domain(),
        &staged.version().fingerprint(),
    );
    insert_version(&mut item, staged.version());
    for (name, value) in [
        ("record", "version".to_owned()),
        ("kind", "VERSION".to_owned()),
        ("state", "READY".to_owned()),
        ("namespace_id", staged.namespace.as_str().to_owned()),
        (
            "configuration_revision",
            staged.configuration_revision.to_string(),
        ),
        (
            "creation_domain",
            creation.scope().domain().as_str().to_owned(),
        ),
        (
            "creation_incarnation",
            creation.scope().incarnation().to_string(),
        ),
        ("creation_uuid", creation.uuid().to_string()),
    ] {
        item.insert(name.into(), s(value));
    }
    item.insert(
        "creation_generation".into(),
        n(creation.scope().generation()),
    );
    item.insert("references".into(), n(0));
    item.insert("claims".into(), n(1));
    check_size(&item)?;
    Ok(item)
}

pub(in crate::dynamodb) fn decode_staged(
    intent: &ResourceId,
    item: &Item,
) -> Result<StagedSnapshot, UploadError> {
    check_size(item)?;
    // Pre-release format boundary: old/unknown rows contain no trustworthy facts.
    if text(item, "record")? != "staged-verified-v1" {
        return Err(UploadError::Corrupt);
    }
    let hash = |field| -> Result<Option<[u8; 32]>, UploadError> {
        match item.get(field) {
            None => Ok(None),
            Some(AttributeValue::B(value)) => value
                .as_ref()
                .try_into()
                .map(Some)
                .map_err(|_| UploadError::Corrupt),
            _ => Err(UploadError::Corrupt),
        }
    };
    let constraints = UploadConstraints::new(
        number(item, "max_length")?,
        item.contains_key("expected_length")
            .then(|| number(item, "expected_length"))
            .transpose()?,
        hash("full_sha256")?,
    )?;
    let domain = DomainId::new(text(item, "domain")?)?;
    let incarnation = uuid(item, "incarnation")?;
    let id = |generation: &str, unique: &str| -> Result<ResourceId, UploadError> {
        Ok(ResourceId::new(
            Scope::new(domain.clone(), incarnation, number(item, generation)?)?,
            uuid(item, unique)?,
        )?)
    };
    let stored_intent = id("intent_generation", "intent_uuid")?;
    if &stored_intent != intent {
        return Err(UploadError::Binding);
    }
    let version = VersionRef::new(
        StoreId::new(text(item, "store")?)?,
        text(item, "object_key")?,
        text(item, "version_id")?,
    )?;
    let state = match text(item, "state")? {
        "ACTIVE" => ClaimState::Active,
        "REVOKED" => ClaimState::Revoked,
        "RELEASED" => ClaimState::Released,
        _ => return Err(UploadError::Corrupt),
    };
    // Reuse Claim's checked persisted-record reconstruction, which deliberately
    // accepts expired/terminal records. This in-memory serde bridge is NEVER a
    // stored JSON blob and does not make StagedSnapshot deserializable authority.
    let claim: Claim = serde_json::from_value(serde_json::json!({
        "id": id("claim_generation", "claim_uuid")?,
        "version": version,
        "state": state,
        "revision": uuid(item, "revision")?,
        "deadline": TimestampMs::new(number(item, "deadline")?)?,
    }))
    .map_err(|_| UploadError::Corrupt)?;
    let staged = StagedSnapshot {
        intent: stored_intent,
        attempt: id("attempt_generation", "attempt_uuid")?,
        namespace: NamespaceId::new(text(item, "namespace_id")?)?,
        configuration_revision: uuid(item, "configuration_revision")?,
        constraints,
        verified_length: number(item, "verified_length")?,
        verified_full_sha256: hash("verified_full_sha256")?,
        claim,
    };
    exact(item, encode_staged(&staged)?)?;
    Ok(staged)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(value: u64) -> TimestampMs {
        TimestampMs::new(value).unwrap()
    }
    fn clock(now: u64) -> ClockObservation {
        ClockObservation::trusted(ts(now), 5).unwrap()
    }
    fn policy() -> ClaimPolicy {
        ClaimPolicy::new(2000, 50, 10).unwrap()
    }
    fn resource(domain: &DomainId, incarnation: Uuid, generation: u64) -> ResourceId {
        ResourceId::new(
            Scope::new(domain.clone(), incarnation, generation).unwrap(),
            Uuid::new_v4(),
        )
        .unwrap()
    }

    // Synthetic UNIT-ONLY native action fixtures. These do NOT qualify S3,
    // perform verification, create a VerifiedUploadReceipt or evaluate DynamoDB
    // expressions. Real-cloud integration needs real qualified S3 evidence.
    struct Fixture {
        ns: NamespaceSnapshot,
        intent: IntentSnapshot,
        attempt: AttemptSnapshot,
        command: CommandId,
        version: VersionRef,
        length: u64,
        hash: Option<[u8; 32]>,
    }
    impl Fixture {
        fn new() -> Self {
            let domain = DomainId::new("selection-domain").unwrap();
            let incarnation = Uuid::new_v4();
            let ns = NamespaceSnapshot {
                domain: domain.clone(),
                incarnation,
                id: NamespaceId::new("selection-namespace").unwrap(),
                configuration_revision: Uuid::new_v4(),
                store: StoreId::new("selection-store").unwrap(),
                bucket: "unit-fixture-not-qualified".into(),
                expected_owner: "123456789012".into(),
                region: "us-west-2".into(),
                key_prefix: "fresh/".into(),
                mpu_max_age_ms: 10_000,
                mpu_lifecycle_margin_ms: 1000,
            };
            let intent = IntentSnapshot {
                admission: Admission {
                    id: resource(&domain, incarnation, 1),
                    namespace: ns.id.clone(),
                    configuration_revision: ns.configuration_revision,
                    target: UploadTarget::new(ns.store.clone(), "fresh/object").unwrap(),
                    constraints: UploadConstraints::new(100, Some(10), Some([7; 32])).unwrap(),
                    revision: Uuid::new_v4(),
                    deadline: ts(1800),
                },
                state: IntentState::Open,
            };
            let mut admission = intent.admission.clone();
            admission.id = resource(&domain, incarnation, 2);
            admission.revision = Uuid::new_v4();
            admission.deadline = ts(1600);
            let attempt = AttemptSnapshot {
                admission,
                intent: intent.id().clone(),
                state: AttemptState::Active,
                mpu_admitted_lower_bound: ts(995),
                max_mpu_use_until: ts(2000),
            };
            let command =
                CommandId::new(Scope::new(domain, incarnation, 3).unwrap(), Uuid::new_v4())
                    .unwrap();
            let version =
                VersionRef::new(ns.store.clone(), "fresh/object", "exact-version").unwrap();
            Self {
                ns,
                intent,
                attempt,
                command,
                version,
                length: 10,
                hash: Some([7; 32]),
            }
        }
        fn evidence(&self) -> SelectionEvidence<'_> {
            SelectionEvidence {
                namespace: &self.ns,
                intent: &self.intent,
                attempt: &self.attempt,
                version: &self.version,
                length: self.length,
                full_sha256: self.hash.as_ref(),
            }
        }
        fn build(
            &self,
            deadline: TimestampMs,
            clock: ClockObservation,
        ) -> Result<Vec<TransactWriteItem>, UploadError> {
            selection_actions(
                "coord-table",
                &self.ns.domain,
                &self.command,
                self.evidence(),
                deadline,
                clock,
                &policy(),
            )
        }
        fn actions(&self) -> Vec<TransactWriteItem> {
            self.build(ts(2500), clock(1000)).unwrap()
        }
        fn staged(&self) -> StagedSnapshot {
            decode_staged(
                self.intent.id(),
                self.actions()[4].put.as_ref().unwrap().item(),
            )
            .unwrap()
        }
    }

    // Check exact emitted conditions/aliases/values, not an invented expression
    // interpreter. Equality predicates on pk/sk and all required fields reject
    // absent old rows in native DynamoDB; these replacements are not upserts.
    fn assert_expected(
        expression: &str,
        names: &HashMap<String, String>,
        values: &Item,
        expected: &Item,
    ) {
        let generated = condition(expected);
        assert_eq!(expression, generated.expression);
        assert_eq!(names, &generated.names);
        assert_eq!(values, &generated.values);
        for (field, value) in expected {
            let alias = names.iter().find(|(_, name)| *name == field).unwrap().0;
            let parameter = format!(":v{}", alias.strip_prefix("#f").unwrap());
            assert!(expression.contains(&format!("{alias} = {parameter}")));
            assert_eq!(&values[&parameter], value);
        }
        assert!(expression.len() < 4096);
    }

    #[test]
    fn native_selection_is_five_actions_with_full_old_rows_and_absent_only_new_rows() {
        let f = Fixture::new();
        let actions = f.actions();
        assert_eq!(actions.len(), 5); // journal adds admission + terminal outcome
        let namespace = actions[0].condition_check.as_ref().unwrap();
        assert_eq!(namespace.key(), &namespace_key(&f.ns.domain, &f.ns.id));
        assert_expected(
            namespace.condition_expression(),
            namespace.expression_attribute_names().unwrap(),
            namespace.expression_attribute_values().unwrap(),
            &encode_namespace(&f.ns).unwrap(),
        );
        for (index, old) in [
            (1, encode_intent(&f.intent).unwrap()),
            (2, encode_attempt(&f.attempt).unwrap()),
        ] {
            let put = actions[index].put.as_ref().unwrap();
            assert_eq!(put.table_name(), "coord-table");
            assert_expected(
                put.condition_expression().unwrap(),
                put.expression_attribute_names().unwrap(),
                put.expression_attribute_values().unwrap(),
                &old,
            );
            assert_eq!(item_key(put.item()), item_key(&old));
            assert_eq!(put.item().len(), old.len()); // no selection tuple growth
            assert!(!put.item().contains_key("version_id"));
            assert!(!put.item().contains_key("claim_uuid"));
        }
        let selected =
            decode_intent(f.intent.id(), actions[1].put.as_ref().unwrap().item()).unwrap();
        assert_eq!(selected.state(), IntentState::Selected);
        assert_eq!(selected.revision(), f.command.uuid());
        let sealed =
            decode_attempt(f.attempt.id(), actions[2].put.as_ref().unwrap().item()).unwrap();
        assert_eq!(sealed.state(), AttemptState::Sealed);
        assert_eq!(sealed.max_mpu_use_until(), f.attempt.max_mpu_use_until());
        assert_eq!(
            sealed.mpu_admitted_lower_bound(),
            f.attempt.mpu_admitted_lower_bound()
        );
        for action in &actions[3..] {
            let put = action.put.as_ref().unwrap();
            assert_eq!(put.condition_expression(), Some("attribute_not_exists(pk)"));
            assert!(put.expression_attribute_names().is_none());
            assert!(put.expression_attribute_values().is_none());
        }
        let version = actions[3].put.as_ref().unwrap().item();
        assert_eq!(
            item_key(version),
            super::super::super::references::object_key(&f.ns.domain, &f.version.fingerprint())
        );
        for (name, value) in [
            ("record", s("version")),
            ("kind", s("VERSION")),
            ("state", s("READY")),
            ("store", s(f.version.store().as_str())),
            ("object_key", s(f.version.key())),
            ("version_id", s(f.version.version())),
            ("references", n(0)),
            ("claims", n(1)),
            ("namespace_id", s(f.ns.id.as_str())),
            (
                "configuration_revision",
                s(f.ns.configuration_revision.to_string()),
            ),
            ("creation_domain", s(f.command.scope().domain().as_str())),
            (
                "creation_incarnation",
                s(f.command.scope().incarnation().to_string()),
            ),
            ("creation_generation", n(f.command.scope().generation())),
            ("creation_uuid", s(f.command.uuid().to_string())),
        ] {
            assert_eq!(version.get(name), Some(&value), "{name}");
        }
        let staged_item = actions[4].put.as_ref().unwrap().item();
        assert_eq!(item_key(staged_item), resource_key("STAGED", f.intent.id()));
        assert_ne!(
            item_key(staged_item),
            resource_key("STAGED", &ResourceId::from_command(&f.command))
        );
        let staged = decode_staged(f.intent.id(), staged_item).unwrap();
        assert_eq!(staged.intent_id(), f.intent.id());
        assert_eq!(staged.attempt_id(), f.attempt.id());
        assert_eq!(staged.namespace_id(), f.ns.id());
        assert_eq!(
            staged.configuration_revision(),
            f.ns.configuration_revision()
        );
        assert_eq!(staged.version(), &f.version);
        assert_eq!(staged.verified_length(), f.length);
        assert_eq!(staged.verified_full_sha256(), f.hash.as_ref());
        assert_eq!(staged.claim().id(), &ResourceId::from_command(&f.command));
        assert_eq!(staged.claim().revision(), f.command.uuid());
        assert_eq!(staged.claim().state(), ClaimState::Active);
        assert_eq!(staged.claim().deadline(), ts(2500)); // independent of admission deadlines/ceiling
        assert!(
            actions
                .iter()
                .all(|a| a.update.is_none() && a.delete.is_none())
        );
    }

    #[test]
    fn missing_optional_constraints_are_also_expected_and_retry_never_moves_deadline() {
        let mut f = Fixture::new();
        f.intent.admission.constraints = UploadConstraints::new(100, None, None).unwrap();
        f.attempt.admission.constraints = f.intent.constraints().clone();
        f.hash = None;
        let first = f.actions();
        let staged = decode_staged(f.intent.id(), first[4].put.as_ref().unwrap().item()).unwrap();
        assert_eq!(staged.verified_length(), 10); // unknown expected length is NOT zero
        assert_eq!(staged.verified_full_sha256(), None);
        assert!(
            condition(&encode_staged(&staged).unwrap())
                .expression
                .contains("attribute_not_exists(#missing_verified_full_sha256)")
        );
        assert_eq!(first, f.build(ts(2500), clock(1100)).unwrap());
        for action in &first[1..3] {
            let expression = action.put.as_ref().unwrap().condition_expression().unwrap();
            for name in ["expected_length", "full_sha256"] {
                assert!(expression.contains(&format!("attribute_not_exists(#missing_{name})")));
            }
        }
        // Even a legitimate renewal cannot silently rewrite an old selection's
        // expected revision/deadline. New preparation emits different guards.
        let old = first[1]
            .put
            .as_ref()
            .unwrap()
            .expression_attribute_values()
            .unwrap();
        f.intent.admission.revision = Uuid::new_v4();
        f.intent.admission.deadline = ts(1900);
        let renewed = f.actions();
        assert_ne!(
            old,
            renewed[1]
                .put
                .as_ref()
                .unwrap()
                .expression_attribute_values()
                .unwrap()
        );
        assert_eq!(first[4], renewed[4]); // intent renewal does NOT keep staging alive
    }

    #[test]
    fn verified_hash_is_independent_of_optional_expected_hash() {
        let mut f = Fixture::new();
        f.intent.admission.constraints = UploadConstraints::new(100, None, None).unwrap();
        f.attempt.admission.constraints = f.intent.constraints().clone();
        for hash in [None, Some([7; 32])] {
            f.hash = hash;
            let staged = f.staged();
            assert_eq!(staged.verified_length(), 10);
            assert_eq!(staged.verified_full_sha256(), hash.as_ref());
            let row = encode_staged(&staged).unwrap();
            assert!(!row.contains_key("full_sha256"));
            assert_eq!(row.contains_key("verified_full_sha256"), hash.is_some());
            assert_eq!(decode_staged(f.intent.id(), &row).unwrap(), staged);
        }
    }

    #[test]
    fn terminal_states_decode_but_never_gain_open_authority() {
        let f = Fixture::new();
        for state in [
            IntentState::Open,
            IntentState::Selected,
            IntentState::Consumed,
            IntentState::Cancelled,
        ] {
            let mut intent = f.intent.clone();
            intent.state = state;
            assert_eq!(
                decode_intent(intent.id(), &encode_intent(&intent).unwrap()).unwrap(),
                intent
            );
            if state != IntentState::Open {
                assert!(matches!(
                    live_intent(&intent, &f.ns, clock(1000), &policy()),
                    Err(UploadError::Terminal)
                ));
                assert!(matches!(
                    authorize_observed(
                        f.ns.clone(),
                        intent.clone(),
                        f.attempt.clone(),
                        || Ok(clock(1000)),
                        &policy()
                    ),
                    Err(UploadError::Terminal)
                ));
                let evidence = SelectionEvidence {
                    intent: &intent,
                    ..f.evidence()
                };
                assert!(matches!(
                    selection_actions(
                        "t",
                        &f.ns.domain,
                        &f.command,
                        evidence,
                        ts(2500),
                        clock(1000),
                        &policy()
                    ),
                    Err(UploadError::Terminal)
                ));
            }
        }
        let mut sealed = Fixture::new();
        sealed.attempt.state = AttemptState::Sealed;
        assert!(matches!(
            sealed.build(ts(2500), clock(1000)),
            Err(UploadError::Terminal)
        ));
    }

    #[test]
    fn every_receipt_binding_and_command_scope_is_checked() {
        for field in [
            "command_domain",
            "command_incarnation",
            "command_generation",
            "intent_domain",
            "intent_incarnation",
            "intent_generation",
            "attempt_domain",
            "attempt_incarnation",
            "attempt_generation",
            "parent",
            "namespace",
            "config",
            "store",
            "key",
            "constraints",
            "namespace_config",
            "namespace_scope",
            "version_store",
            "version_key",
            "length",
            "hash",
            "missing_hash",
        ] {
            let mut f = Fixture::new();
            match field {
                "command_domain" | "command_incarnation" | "command_generation" => {
                    f.command = CommandId::new(
                        Scope::new(
                            if field == "command_domain" {
                                DomainId::new("other").unwrap()
                            } else {
                                f.ns.domain.clone()
                            },
                            if field == "command_incarnation" {
                                Uuid::new_v4()
                            } else {
                                f.ns.incarnation
                            },
                            if field == "command_generation" { 1 } else { 3 },
                        )
                        .unwrap(),
                        f.command.uuid(),
                    )
                    .unwrap();
                }
                "intent_domain" | "intent_incarnation" | "intent_generation" => {
                    f.intent.admission.id = resource(
                        &if field == "intent_domain" {
                            DomainId::new("other").unwrap()
                        } else {
                            f.ns.domain.clone()
                        },
                        if field == "intent_incarnation" {
                            Uuid::new_v4()
                        } else {
                            f.ns.incarnation
                        },
                        if field == "intent_generation" { 3 } else { 1 },
                    );
                    f.attempt.intent = f.intent.id().clone();
                }
                "attempt_domain" | "attempt_incarnation" | "attempt_generation" => {
                    f.attempt.admission.id = resource(
                        &if field == "attempt_domain" {
                            DomainId::new("other").unwrap()
                        } else {
                            f.ns.domain.clone()
                        },
                        if field == "attempt_incarnation" {
                            Uuid::new_v4()
                        } else {
                            f.ns.incarnation
                        },
                        if field == "attempt_generation" { 4 } else { 2 },
                    );
                }
                "parent" => f.attempt.intent = resource(&f.ns.domain, f.ns.incarnation, 1),
                "namespace" => f.attempt.admission.namespace = NamespaceId::new("other").unwrap(),
                "config" => f.attempt.admission.configuration_revision = Uuid::new_v4(),
                "store" => f.attempt.admission.target.store = StoreId::new("other").unwrap(),
                "key" => f.attempt.admission.target.key = "fresh/other".into(),
                "constraints" => {
                    f.attempt.admission.constraints =
                        UploadConstraints::new(101, Some(10), Some([7; 32])).unwrap()
                }
                "namespace_config" => f.ns.configuration_revision = Uuid::new_v4(),
                "namespace_scope" => f.ns.key_prefix = "elsewhere/".into(),
                "version_store" => {
                    f.version = VersionRef::new(
                        StoreId::new("other").unwrap(),
                        "fresh/object",
                        "exact-version",
                    )
                    .unwrap()
                }
                "version_key" => {
                    f.version =
                        VersionRef::new(f.ns.store.clone(), "fresh/other", "exact-version").unwrap()
                }
                "length" => f.length = 11,
                "hash" => f.hash = Some([8; 32]),
                "missing_hash" => f.hash = None,
                _ => unreachable!(),
            }
            assert!(f.build(ts(2500), clock(1000)).is_err(), "accepted {field}");
        }
        let mut f = Fixture::new();
        f.intent.admission.constraints = UploadConstraints::new(100, None, None).unwrap();
        f.attempt.admission.constraints = f.intent.constraints().clone();
        f.length = 101;
        assert!(f.build(ts(2500), clock(1000)).is_err());
        f.length = 100;
        f.hash = None;
        assert!(f.build(ts(2500), clock(1000)).is_ok());
        // Selection uses the exact candidate ID, not an ETag/latest substitute.
        f.version =
            VersionRef::new(f.ns.store.clone(), "fresh/object", "another-exact-version").unwrap();
        assert_eq!(f.staged().version(), &f.version);
    }

    #[test]
    fn strict_clock_fixed_mpu_ceiling_and_claim_policy_fail_closed() {
        let f = Fixture::new();
        for observation in [
            clock(1595),
            clock(1795),
            clock(1995),
            ClockObservation::untrusted(ts(1000), 0),
            ClockObservation::trusted(ts(1000), 11).unwrap(),
        ] {
            assert!(f.build(ts(2500), observation).is_err());
        }
        for deadline in [1005, 1055, 3006] {
            assert!(f.build(ts(deadline), clock(1000)).is_err());
        }
        for deadline in [1056, 3005] {
            assert!(f.build(ts(deadline), clock(1000)).is_ok());
        }
        for field in [
            "future_admission",
            "beyond_ceiling",
            "beyond_age",
            "expired_intent",
        ] {
            let mut f = Fixture::new();
            match field {
                "future_admission" => f.attempt.mpu_admitted_lower_bound = ts(1006),
                "beyond_ceiling" => f.attempt.admission.deadline = ts(2001),
                "beyond_age" => f.attempt.max_mpu_use_until = ts(10996),
                "expired_intent" => f.intent.admission.deadline = ts(1005),
                _ => unreachable!(),
            }
            assert!(f.build(ts(2500), clock(1000)).is_err(), "accepted {field}");
        }
    }

    #[test]
    fn staged_checked_reconstruction_accepts_terminal_claims_without_revival() {
        let f = Fixture::new();
        let staged = f.staged();
        let original = encode_staged(&staged).unwrap();
        for state in ["ACTIVE", "REVOKED", "RELEASED"] {
            let mut item = original.clone();
            item.insert("state".into(), s(state));
            item.insert("deadline".into(), n(0)); // expired records remain readable
            let decoded = decode_staged(f.intent.id(), &item).unwrap();
            assert_eq!(
                text(&encode_staged(&decoded).unwrap(), "state").unwrap(),
                state
            );
            assert!(
                decoded
                    .claim()
                    .validate_use(clock(1000), &policy())
                    .is_err()
            );
        }
        for field in original.keys() {
            let mut item = original.clone();
            item.remove(field);
            if !["expected_length", "full_sha256"].contains(&field.as_str()) {
                assert!(
                    decode_staged(f.intent.id(), &item).is_err(),
                    "missing {field}"
                );
            }
            let mut item = original.clone();
            item.insert(field.clone(), AttributeValue::Bool(true));
            assert!(
                decode_staged(f.intent.id(), &item).is_err(),
                "wrong type {field}"
            );
        }
        for (field, value) in [
            ("pk", s("alias")),
            ("sk", s("alias")),
            ("record", s("intent")),
            ("record", s("staged")),
            ("record", s("staged-verified-v2")),
            ("verified_length", n(11)),
            ("verified_length", n(101)),
            ("verified_length", n(i64::MAX as u64 + 1)),
            ("verified_length", AttributeValue::N("-1".into())),
            ("verified_length", AttributeValue::N("1.5".into())),
            ("verified_length", AttributeValue::N("010".into())),
            (
                "verified_full_sha256",
                AttributeValue::B(Blob::new([7; 31])),
            ),
            (
                "verified_full_sha256",
                AttributeValue::B(Blob::new([7; 33])),
            ),
            (
                "verified_full_sha256",
                AttributeValue::B(Blob::new([8; 32])),
            ),
            ("full_sha256", AttributeValue::B(Blob::new([8; 32]))),
            ("expected_length", n(11)),
            ("max_length", n(9)),
            ("kind", s("INTENT")),
            ("domain", s("other")),
            ("incarnation", s(Uuid::new_v4().to_string())),
            ("intent_uuid", s(Uuid::new_v4().to_string())),
            ("intent_generation", n(2)),
            ("attempt_generation", n(0)),
            ("attempt_generation", n(4)),
            ("claim_generation", n(1)),
            ("attempt_uuid", s(Uuid::nil().to_string())),
            ("claim_uuid", s(Uuid::nil().to_string())),
            ("configuration_revision", s(Uuid::nil().to_string())),
            ("revision", s("bad")),
            ("state", s("UNKNOWN")),
            ("deadline", n(u64::MAX)),
            ("claim_generation", AttributeValue::N("03".into())),
            ("store", s("")),
            ("object_key", s("")),
            ("version_id", s("null")),
            ("version_id", s("v".repeat(1025))),
            ("namespace_id", s("")),
            ("unknown", s("extra")),
        ] {
            let mut item = original.clone();
            item.insert(field.into(), value);
            assert!(
                decode_staged(f.intent.id(), &item).is_err(),
                "accepted {field}"
            );
        }
        // Full tuple is retained and reconstructs independently of its hash.
        assert_eq!(decode_staged(f.intent.id(), &original).unwrap(), staged);
        assert!(decode_staged(&resource(&f.ns.domain, f.ns.incarnation, 1), &original).is_err());
        let mut released = staged.clone();
        assert!(
            released
                .claim
                .release(staged.claim.revision())
                .unwrap()
                .accounting_must_change()
        );
        assert_eq!(
            decode_staged(f.intent.id(), &encode_staged(&released).unwrap()).unwrap(),
            released
        );
        let mut revoked = staged.clone();
        assert!(
            revoked
                .claim
                .revoke(staged.claim.revision(), clock(2505), &policy())
                .unwrap()
                .accounting_must_change()
        );
        assert_eq!(
            decode_staged(f.intent.id(), &encode_staged(&revoked).unwrap()).unwrap(),
            revoked
        );
        assert_eq!(staged.claim.state(), ClaimState::Active); // no persisted automatic mutation
    }

    fn raw_size(item: &Item) -> usize {
        item.iter()
            .map(|(name, value)| {
                name.len()
                    + match value {
                        AttributeValue::S(v) => v.len(),
                        AttributeValue::N(v) => v.len().max(2),
                        AttributeValue::B(v) => v.as_ref().len(),
                        _ => panic!("not flat"),
                    }
            })
            .sum()
    }

    #[test]
    fn maximum_id_key_version_and_numeric_widths_fit_with_64_byte_growth_reserve() {
        let mut f = Fixture::new();
        f.ns.domain = DomainId::new("d".repeat(128)).unwrap();
        f.ns.id = NamespaceId::new("n".repeat(128)).unwrap();
        f.ns.store = StoreId::new("s".repeat(128)).unwrap();
        f.ns.key_prefix = "\"".repeat(1024);
        f.ns.bucket = "b".repeat(63);
        f.ns.region = "r".repeat(64);
        f.ns.mpu_max_age_ms = TimestampMs::MAX.as_millis() / 2;
        f.ns.mpu_lifecycle_margin_ms = TimestampMs::MAX.as_millis() / 2;
        f.intent.admission.id = resource(&f.ns.domain, f.ns.incarnation, u64::MAX);
        f.intent.admission.namespace = f.ns.id.clone();
        f.intent.admission.target =
            UploadTarget::new(f.ns.store.clone(), f.ns.key_prefix.clone()).unwrap();
        f.intent.admission.constraints =
            UploadConstraints::new(i64::MAX as u64, Some(i64::MAX as u64), Some([7; 32])).unwrap();
        f.attempt.admission = f.intent.admission.clone();
        f.attempt.admission.id = resource(&f.ns.domain, f.ns.incarnation, u64::MAX);
        f.attempt.intent = f.intent.id().clone();
        f.command = CommandId::new(f.intent.id().scope().clone(), Uuid::new_v4()).unwrap();
        f.version = VersionRef::new(
            f.ns.store.clone(),
            f.ns.key_prefix.clone(),
            "\\".repeat(1024),
        )
        .unwrap();
        f.length = i64::MAX as u64;
        let mut staged = f.staged();
        // Structural reconstruction covers maximum stored deadlines independently
        // of acquisition-time policy. No new authority is created by this read.
        let mut row = encode_staged(&staged).unwrap();
        row.insert("deadline".into(), n(TimestampMs::MAX.as_millis()));
        staged = decode_staged(f.intent.id(), &row).unwrap();
        f.intent.admission.deadline = TimestampMs::MAX;
        f.attempt.admission.deadline = TimestampMs::MAX;
        f.attempt.mpu_admitted_lower_bound = ts(TimestampMs::MAX.as_millis() - 1);
        f.attempt.max_mpu_use_until = TimestampMs::MAX;
        for state in [
            IntentState::Open,
            IntentState::Selected,
            IntentState::Consumed,
            IntentState::Cancelled,
        ] {
            f.intent.state = state;
            assert_eq!(
                decode_intent(f.intent.id(), &encode_intent(&f.intent).unwrap()).unwrap(),
                f.intent
            );
        }
        let version = encode_version(&staged).unwrap();
        let staged_item = encode_staged(&staged).unwrap();
        for (name, item) in [
            ("namespace", encode_namespace(&f.ns).unwrap()),
            ("intent", encode_intent(&f.intent).unwrap()),
            ("attempt", encode_attempt(&f.attempt).unwrap()),
            ("staged", staged_item.clone()),
            ("version", version.clone()),
        ] {
            let bytes = raw_size(&item);
            println!(
                "maximum {name}: {bytes} native bytes + {GROWTH_RESERVE} reserved <= {MAX_RECORD_BYTES}"
            );
            assert!(
                bytes + GROWTH_RESERVE <= MAX_RECORD_BYTES,
                "{name}: {bytes}"
            );
            check_size(&item).unwrap();
        }
        assert_eq!(decode_staged(f.intent.id(), &staged_item).unwrap(), staged);
        for state in ["REVOKED", "RELEASED"] {
            let mut terminal = staged_item.clone();
            terminal.insert("state".into(), s(state));
            assert!(raw_size(&terminal) <= raw_size(&staged_item) + GROWTH_RESERVE);
            decode_staged(f.intent.id(), &terminal).unwrap();
        }
        // Bounded unsigned counter-width growth plus terminal-state spelling
        // fits the reserved bytes; this is layout evidence, not a new counter
        // engine or a simulated retirement transition.
        for state in ["RETIRING", "DELETED"] {
            let mut grown = version.clone();
            grown.insert("references".into(), n(u64::MAX));
            grown.insert("claims".into(), n(u64::MAX));
            grown.insert("state".into(), s(state));
            assert!(raw_size(&grown) <= raw_size(&version) + GROWTH_RESERVE);
            check_size(&grown).unwrap();
        }
        let mut oversized = staged_item;
        oversized.insert("version_id".into(), s("v".repeat(4000)));
        assert!(matches!(
            decode_staged(f.intent.id(), &oversized),
            Err(UploadError::RecordSize)
        ));
    }
}
