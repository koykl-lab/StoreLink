//! Atomic slot transitions for versions ALREADY tracked as READY.
//!
//! Source authority is an existing library reference snapshot, revalidated in
//! the same commit. READY plus a raw tuple (including a stale/cancelled upload
//! receipt) is NOT authority. Independent use holds and selected-staging
//! consumption are not implemented: unlinked data held only by those mechanisms
//! cannot be attached through this API. There is no published flag or adoption
//! escape hatch. Application semantic authorization remains caller-owned.
//!
//! This is not upload selection, registration, namespace enrollment, claims or
//! garbage collection. The domain must already exclusively own its enrolled
//! managed object scope; a READY row is not permission for cross-domain adoption.
//!
//! Schema (all keys are strings, `sk = M`): reference `pk` is the journal domain
//! prefix plus `#REF#` and lowercase hex of the exact opaque ID bytes. Its only
//! other fields are `record = reference`, `reference_id`, `revision` (canonical
//! non-nil UUID), `store`, `object_key`, `version_id`. Version `pk` is that prefix
//! plus `#OBJ#` and lowercase hex of `VersionRef::fingerprint()`. Version actions
//! require `record = version`, the full tuple, `state = READY`, and native numeric
//! nonnegative `references` and `claims`. No action creates a missing version.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Delete, Put, TransactWriteItem, Update,
};
use thiserror::Error;
use uuid::Uuid;

use super::journal::prefix;
use super::{ApplicationActions, CommandContext, CommandJournal, JournalError, PreparedCommand};
use crate::identity::{DomainId, IdentityError, StoreId, VersionRef};

type Item = HashMap<String, AttributeValue>;

/// Opaque slot identity, not a filesystem path. Exact UTF-8 bytes are preserved.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReferenceId(String);
impl ReferenceId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(IdentityError::InvalidLength {
                field: "ReferenceId",
                max: 128,
            });
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for ReferenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReferenceId").finish_non_exhaustive()
    }
}

/// Immutable observation from a strong native read, not a current capability.
/// Publication rechecks this exact slot identity, tuple and revision atomically.
#[derive(Clone, PartialEq, Eq)]
pub struct ReferenceSnapshot {
    id: ReferenceId,
    version: VersionRef,
    revision: Uuid,
}
impl ReferenceSnapshot {
    pub fn id(&self) -> &ReferenceId {
        &self.id
    }
    pub fn version(&self) -> &VersionRef {
        &self.version
    }
    pub fn revision(&self) -> Uuid {
        self.revision
    }
}
impl fmt::Debug for ReferenceSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReferenceSnapshot").finish_non_exhaustive()
    }
}

/// One complete slot transition, never a caller-supplied numerical delta.
#[derive(Clone)]
pub struct ReferenceChange {
    id: ReferenceId,
    expected: Option<ReferenceSnapshot>,
    desired: Option<VersionRef>,
    new_revision: Option<Uuid>,
}
impl ReferenceChange {
    /// `expected = None` means authoritative expected absence. `desired = None`
    /// detaches the slot and requires no new revision. Otherwise supply a fixed,
    /// fresh non-nil revision, never reused across the slot's history. Local
    /// checks reject reuse of observed revisions, not unobservable history.
    /// Absent-to-absent changes are rejected.
    pub fn new(
        id: ReferenceId,
        expected: Option<ReferenceSnapshot>,
        desired: Option<VersionRef>,
        new_revision: Option<Uuid>,
    ) -> Result<Self, ReferenceError> {
        if expected.as_ref().is_some_and(|old| old.id != id) {
            return Err(ReferenceError::SnapshotIdMismatch);
        }
        if expected.is_none() && desired.is_none() {
            return Err(ReferenceError::EmptyChange);
        }
        if desired.is_some() != new_revision.is_some() {
            return Err(ReferenceError::RevisionRequired);
        }
        if let Some(revision) = new_revision
            && (revision.is_nil()
                || expected
                    .as_ref()
                    .is_some_and(|old| old.revision == revision))
        {
            return Err(ReferenceError::InvalidRevision);
        }
        Ok(Self {
            id,
            expected,
            desired,
            new_revision,
        })
    }
    pub fn id(&self) -> &ReferenceId {
        &self.id
    }
    pub fn expected(&self) -> Option<&ReferenceSnapshot> {
        self.expected.as_ref()
    }
    pub fn desired(&self) -> Option<&VersionRef> {
        self.desired.as_ref()
    }
    pub fn new_revision(&self) -> Option<Uuid> {
        self.new_revision
    }
}
impl fmt::Debug for ReferenceChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReferenceChange").finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum ReferenceError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("expected snapshot belongs to a different reference ID")]
    SnapshotIdMismatch,
    #[error("absent-to-absent reference change is not supported")]
    EmptyChange,
    #[error("a new revision is required exactly when a desired version is present")]
    RevisionRequired,
    #[error("proposed reference revision is nil or reused")]
    InvalidRevision,
    #[error("reference change list exceeds native transaction capacity")]
    TooManyChanges,
    #[error("reference source list exceeds native transaction capacity")]
    TooManySources,
    #[error(
        "desired version lacks an existing library reference source; other source kinds are unsupported"
    )]
    MissingSource,
    #[error("contradictory snapshots or expected absence for one source slot")]
    ContradictorySource,
    #[error("duplicate reference slot changes")]
    DuplicateSlot,
    #[error("duplicate staged claim or a non-active/cross-domain staged source")]
    InvalidStagedSource,
    #[error("unequal full version identities have the same object address")]
    AddressCollision,
    #[error("reference delta exceeds the supported integer bound")]
    DeltaOverflow,
    #[error("malformed reference record (no absence inferred)")]
    CorruptRecord,
    #[error("native reference read failed (no absence inferred)")]
    ReadFailed,
    #[error("native reference action construction failed")]
    ActionBuild,
}

impl CommandJournal {
    /// Strong native read. Only an authoritative missing item returns `None`;
    /// malformed records and service/transport errors are errors, never absence.
    pub async fn read_reference(
        &self,
        id: &ReferenceId,
    ) -> Result<Option<ReferenceSnapshot>, ReferenceError> {
        let response = self
            .native_client()
            .get_item()
            .table_name(self.coordination_arn())
            .set_key(Some(reference_key(self.domain_id(), id)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|_| ReferenceError::ReadFailed)?;
        response
            .item
            .as_ref()
            .map(|item| decode_reference(self.domain_id(), id, item))
            .transpose()
    }

    /// Compose complete application pointer changes and library-owned reference
    /// effects in ONE immutable journal command. No application pointer write may
    /// happen outside this commit. Callers must declare EVERY affected managed
    /// pointer and provide native authorization/concurrency guards themselves.
    ///
    /// The application owns its schema and authoritative owner identity: owner
    /// IDs MUST NOT be reused, and application revision guards MUST prevent an
    /// empty-slot ABA (create, delete, stale absent-slot create). StoreLink cannot
    /// infer these guards from opaque application actions or an absent slot.
    /// Proposed slot revisions must also never be reused, including past history.
    ///
    /// Only existing READY versions in this domain's exclusively enrolled scope
    /// are eligible. This API neither verifies uploads nor adopts arbitrary
    /// versions. Each desired full tuple requires an expected snapshot in
    /// `changes` or an explicit `sources` snapshot from `read_reference`.
    /// All explicit sources are conditions, even if unused. Equal duplicates
    /// merge; a changed source must equal that slot's expected snapshot, whose
    /// guarded write also serves as its source check. A detached source can
    /// authorize an atomic move. Unchanged sources get native ConditionChecks.
    /// Bare tuples, staging receipts and independent use holds are unsupported
    /// authority here; no source means rejection before journal preparation.
    ///
    /// Native conditions, receipt/digest/cancellation semantics and the
    /// FULL transaction budget remain owned by the journal; plans never split.
    pub fn prepare_reference_update(
        &self,
        context: CommandContext,
        application: ApplicationActions,
        changes: Vec<ReferenceChange>,
        sources: Vec<ReferenceSnapshot>,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, ReferenceError> {
        let internal = reference_actions(
            self.coordination_arn(),
            self.domain_id(),
            &changes,
            &sources,
        )?;
        Ok(self.prepare_internal(context, application, internal, result)?)
    }
}

fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn key(pk: String) -> Item {
    HashMap::from([("pk".into(), s(pk)), ("sk".into(), s("M"))])
}
fn reference_key(domain: &DomainId, id: &ReferenceId) -> Item {
    key(format!(
        "{}#REF#{}",
        prefix(domain),
        hex(id.as_str().as_bytes())
    ))
}
pub(crate) fn object_key(domain: &DomainId, address: &[u8; 32]) -> Item {
    key(format!("{}#OBJ#{}", prefix(domain), hex(address)))
}
fn insert_tuple(item: &mut Item, version: &VersionRef) {
    item.insert("store".into(), s(version.store().as_str()));
    item.insert("object_key".into(), s(version.key()));
    item.insert("version_id".into(), s(version.version()));
}
fn reference_item(domain: &DomainId, snapshot: &ReferenceSnapshot) -> Item {
    let mut item = reference_key(domain, &snapshot.id);
    item.insert("record".into(), s("reference"));
    item.insert("reference_id".into(), s(snapshot.id.as_str()));
    item.insert("revision".into(), s(snapshot.revision.to_string()));
    insert_tuple(&mut item, &snapshot.version);
    item
}
fn text<'a>(item: &'a Item, name: &str) -> Result<&'a str, ReferenceError> {
    match item.get(name) {
        Some(AttributeValue::S(value)) => Ok(value),
        _ => Err(ReferenceError::CorruptRecord),
    }
}
fn decode_reference(
    domain: &DomainId,
    id: &ReferenceId,
    item: &Item,
) -> Result<ReferenceSnapshot, ReferenceError> {
    // Exact field count plus mandatory reads reject ALL unknown/missing fields.
    if item.len() != 8
        || reference_key(domain, id)
            .iter()
            .any(|(k, v)| item.get(k) != Some(v))
        || text(item, "record")? != "reference"
        || text(item, "reference_id")? != id.as_str()
    {
        return Err(ReferenceError::CorruptRecord);
    }
    let revision_text = text(item, "revision")?;
    let revision = Uuid::parse_str(revision_text).map_err(|_| ReferenceError::CorruptRecord)?;
    if revision.is_nil() || revision.to_string() != revision_text {
        return Err(ReferenceError::CorruptRecord);
    }
    let store = StoreId::new(text(item, "store")?).map_err(|_| ReferenceError::CorruptRecord)?;
    let version = VersionRef::new(store, text(item, "object_key")?, text(item, "version_id")?)
        .map_err(|_| ReferenceError::CorruptRecord)?;
    Ok(ReferenceSnapshot {
        id: id.clone(),
        version,
        revision,
    })
}

fn tuple_guard(version: &VersionRef, record: &str) -> (String, HashMap<String, String>, Item) {
    let names = HashMap::from([
        ("#record".into(), "record".into()),
        ("#store".into(), "store".into()),
        ("#key".into(), "object_key".into()),
        ("#version".into(), "version_id".into()),
    ]);
    let values = HashMap::from([
        (":record".into(), s(record)),
        (":store".into(), s(version.store().as_str())),
        (":key".into(), s(version.key())),
        (":version".into(), s(version.version())),
    ]);
    (
        "#record = :record AND #store = :store AND #key = :key AND #version = :version".into(),
        names,
        values,
    )
}
fn snapshot_guard(snapshot: &ReferenceSnapshot) -> (String, HashMap<String, String>, Item) {
    let (mut condition, mut names, mut values) = tuple_guard(&snapshot.version, "reference");
    condition.push_str(" AND #id = :id AND #revision = :revision");
    names.insert("#id".into(), "reference_id".into());
    names.insert("#revision".into(), "revision".into());
    values.insert(":id".into(), s(snapshot.id.as_str()));
    values.insert(":revision".into(), s(snapshot.revision.to_string()));
    (condition, names, values)
}
pub(super) fn source_action(
    table: &str,
    domain: &DomainId,
    source: &ReferenceSnapshot,
) -> Result<TransactWriteItem, ReferenceError> {
    let (condition, names, values) = snapshot_guard(source);
    let check = ConditionCheck::builder()
        .table_name(table)
        .set_key(Some(reference_key(domain, &source.id)))
        .condition_expression(condition)
        .set_expression_attribute_names(Some(names))
        .set_expression_attribute_values(Some(values))
        .build()
        .map_err(|_| ReferenceError::ActionBuild)?;
    Ok(TransactWriteItem::builder().condition_check(check).build())
}
fn slot_action(
    table: &str,
    domain: &DomainId,
    change: &ReferenceChange,
) -> Result<TransactWriteItem, ReferenceError> {
    let (condition, names, values) = if let Some(old) = &change.expected {
        let (condition, names, values) = snapshot_guard(old);
        (condition, names, Some(values))
    } else {
        (
            "attribute_not_exists(#pk)".into(),
            HashMap::from([("#pk".into(), "pk".into())]),
            None,
        )
    };
    if let Some(desired) = &change.desired {
        let snapshot = ReferenceSnapshot {
            id: change.id.clone(),
            version: desired.clone(),
            revision: change.new_revision.expect("checked ReferenceChange"),
        };
        let put = Put::builder()
            .table_name(table)
            .set_item(Some(reference_item(domain, &snapshot)))
            .condition_expression(condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(values)
            .build()
            .map_err(|_| ReferenceError::ActionBuild)?;
        Ok(TransactWriteItem::builder().put(put).build())
    } else {
        let delete = Delete::builder()
            .table_name(table)
            .set_key(Some(reference_key(domain, &change.id)))
            .condition_expression(condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(values)
            .build()
            .map_err(|_| ReferenceError::ActionBuild)?;
        Ok(TransactWriteItem::builder().delete(delete).build())
    }
}

// Aggregate by the FULL tuple first. Address checking is a separate step so a
// hash collision can never combine counts for unequal immutable identities.
fn add_delta(
    deltas: &mut HashMap<VersionRef, i64>,
    version: &VersionRef,
    delta: i64,
) -> Result<(), ReferenceError> {
    let value = deltas.entry(version.clone()).or_default();
    *value = value
        .checked_add(delta)
        .ok_or(ReferenceError::DeltaOverflow)?;
    Ok(())
}
fn insert_address(
    objects: &mut BTreeMap<[u8; 32], (VersionRef, i64)>,
    address: [u8; 32],
    version: VersionRef,
    delta: i64,
) -> Result<(), ReferenceError> {
    if let Some((prior, _)) = objects.get(&address)
        && prior != &version
    {
        return Err(ReferenceError::AddressCollision);
    }
    objects.insert(address, (version, delta));
    Ok(())
}
fn reference_actions(
    table: &str,
    domain: &DomainId,
    changes: &[ReferenceChange],
    sources: &[ReferenceSnapshot],
) -> Result<Vec<TransactWriteItem>, ReferenceError> {
    reference_actions_with_staged(table, domain, changes, sources, &[])
}

/// Publication must add full SELECTED intent and ACTIVE staged-row guards and
/// terminal replacements in the SAME plan. These opaque observations supply
/// source identity and exactly one consumed claim each, never raw caller deltas.
pub(super) fn reference_actions_with_staged(
    table: &str,
    domain: &DomainId,
    changes: &[ReferenceChange],
    sources: &[ReferenceSnapshot],
    staged: &[&super::StagedSnapshot],
) -> Result<Vec<TransactWriteItem>, ReferenceError> {
    // At least one version action plus the journal's admission/outcome actions
    // are needed for any nonempty change set. Reject before building large plans;
    // prepare_internal still checks the exact full application+library budget.
    if changes.len() > 97 {
        return Err(ReferenceError::TooManyChanges);
    }
    if sources.len() > 97 || staged.len() > 97 {
        return Err(ReferenceError::TooManySources);
    }
    let mut staged_ids = HashSet::new();
    for source in staged {
        if source.claim().state() != crate::claim::ClaimState::Active
            || source.intent_id().scope().domain() != domain
            || source.claim().id().scope().domain() != domain
            || !staged_ids.insert(source.claim().id())
        {
            return Err(ReferenceError::InvalidStagedSource);
        }
    }
    // Validate all authority before constructing actions. Do not sum the two
    // input lengths: source conditions can merge with each other and slot writes.
    let mut slots = HashMap::new();
    let mut authority = BTreeMap::new();
    for change in changes {
        if slots.insert(&change.id, change).is_some() {
            return Err(ReferenceError::DuplicateSlot);
        }
        if let Some(old) = &change.expected {
            authority.insert(&old.id, old);
        }
    }
    for source in sources {
        if let Some(change) = slots.get(&source.id)
            && change.expected.as_ref() != Some(source)
        {
            return Err(ReferenceError::ContradictorySource);
        }
        if let Some(prior) = authority.insert(&source.id, source)
            && prior != source
        {
            return Err(ReferenceError::ContradictorySource);
        }
    }
    let versions: HashSet<_> = authority
        .values()
        .map(|source| &source.version)
        .chain(staged.iter().map(|source| source.version()))
        .collect();
    if changes
        .iter()
        .filter_map(|change| change.desired.as_ref())
        .any(|desired| !versions.contains(desired))
    {
        return Err(ReferenceError::MissingSource);
    }
    let mut deltas = HashMap::new();
    let mut actions = Vec::new();
    for change in changes {
        if let Some(old) = &change.expected {
            add_delta(&mut deltas, &old.version, -1)?;
        }
        if let Some(new) = &change.desired {
            add_delta(&mut deltas, new, 1)?;
        }
        actions.push(slot_action(table, domain, change)?);
    }
    for (id, source) in authority {
        if !slots.contains_key(id) {
            actions.push(source_action(table, domain, source)?);
        }
    }
    let mut objects = BTreeMap::new();
    for (version, delta) in deltas {
        insert_address(&mut objects, version.fingerprint(), version, delta)?;
    }
    let mut claim_deltas = HashMap::new();
    for source in staged {
        let version = source.version();
        // Also admit the address with zero reference delta so unused staging
        // consumption still releases its protection through the same engine.
        let address = version.fingerprint();
        let references = objects.get(&address).map(|(_, delta)| *delta).unwrap_or(0);
        insert_address(&mut objects, address, version.clone(), references)?;
        add_delta(&mut claim_deltas, version, -1)?;
    }
    for (address, (version, delta)) in objects {
        let claim_delta = claim_deltas.get(&version).copied().unwrap_or(0);
        actions.push(version_count_action(
            table,
            domain,
            &address,
            &version,
            delta,
            claim_delta,
        )?);
    }
    Ok(actions)
}
/// One native action per exact version, shared by references and finite claims.
/// Callers must guard the authoritative edge/claim transition in the same plan;
/// this internal helper is not permission to supply raw accounting deltas.
/// Both deltas must already be consolidated before constructing the action.
pub(super) fn version_count_action(
    table: &str,
    domain: &DomainId,
    address: &[u8; 32],
    version: &VersionRef,
    delta: i64,
    claim_delta: i64,
) -> Result<TransactWriteItem, ReferenceError> {
    let (mut condition, mut names, mut values) = tuple_guard(version, "version");
    condition.push_str(" AND #state = :ready AND attribute_type(#references, :number) AND #references >= :zero AND attribute_type(#claims, :number) AND #claims >= :zero");
    names.extend([
        ("#state".into(), "state".into()),
        ("#references".into(), "references".into()),
        ("#claims".into(), "claims".into()),
    ]);
    values.extend([
        (":ready".into(), s("READY")),
        (":number".into(), s("N")),
        (":zero".into(), AttributeValue::N("0".into())),
    ]);
    if delta < 0 {
        condition.push_str(" AND #references >= :minimum");
        values.insert(
            ":minimum".into(),
            AttributeValue::N(delta.unsigned_abs().to_string()),
        );
    }
    if claim_delta < 0 {
        condition.push_str(" AND #claims >= :minimum_claims");
        values.insert(
            ":minimum_claims".into(),
            AttributeValue::N(claim_delta.unsigned_abs().to_string()),
        );
    }
    if delta == 0 && claim_delta == 0 {
        let check = ConditionCheck::builder()
            .table_name(table)
            .set_key(Some(object_key(domain, address)))
            .condition_expression(condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .build()
            .map_err(|_| ReferenceError::ActionBuild)?;
        Ok(TransactWriteItem::builder().condition_check(check).build())
    } else {
        let mut additions = Vec::with_capacity(2);
        if delta != 0 {
            values.insert(":delta".into(), AttributeValue::N(delta.to_string()));
            additions.push("#references :delta");
        }
        if claim_delta != 0 {
            values.insert(
                ":claim_delta".into(),
                AttributeValue::N(claim_delta.to_string()),
            );
            additions.push("#claims :claim_delta");
        }
        let update = Update::builder()
            .table_name(table)
            .set_key(Some(object_key(domain, address)))
            .update_expression(format!("ADD {}", additions.join(", ")))
            .condition_expression(condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .build()
            .map_err(|_| ReferenceError::ActionBuild)?;
        Ok(TransactWriteItem::builder().update(update).build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn domain() -> DomainId {
        DomainId::new("domain").unwrap()
    }
    fn version(v: &str) -> VersionRef {
        VersionRef::new(StoreId::new("store").unwrap(), "exact/key", v).unwrap()
    }
    fn id(v: &str) -> ReferenceId {
        ReferenceId::new(v).unwrap()
    }
    fn snapshot(slot: &str, v: &str) -> ReferenceSnapshot {
        ReferenceSnapshot {
            id: id(slot),
            version: version(v),
            revision: Uuid::new_v4(),
        }
    }
    #[test]
    fn combined_reference_and_claim_accounting_is_one_guarded_action() {
        let version = version("selected");
        for (references, claims) in [(1, -1), (0, -1), (-2, 1), (0, 1), (0, 0)] {
            let action = version_count_action(
                "table",
                &domain(),
                &version.fingerprint(),
                &version,
                references,
                claims,
            )
            .unwrap();
            if references == 0 && claims == 0 {
                assert!(action.condition_check.is_some());
                continue;
            }
            let update = action.update.unwrap();
            let condition = update.condition_expression.as_deref().unwrap();
            assert!(condition.contains("#state = :ready"));
            assert!(condition.contains("attribute_type(#references, :number)"));
            assert!(condition.contains("attribute_type(#claims, :number)"));
            let expression = update.update_expression.as_str();
            assert_eq!(expression.contains("#references :delta"), references != 0);
            assert_eq!(expression.contains("#claims :claim_delta"), claims != 0);
            assert_eq!(
                condition.contains("#references >= :minimum"),
                references < 0
            );
            assert_eq!(condition.contains("#claims >= :minimum_claims"), claims < 0);
            let values = update.expression_attribute_values.unwrap();
            if references != 0 {
                assert_eq!(values[":delta"], AttributeValue::N(references.to_string()));
            }
            if claims != 0 {
                assert_eq!(
                    values[":claim_delta"],
                    AttributeValue::N(claims.to_string())
                );
            }
        }
    }

    fn attach(slot: &str, v: &str) -> ReferenceChange {
        ReferenceChange::new(id(slot), None, Some(version(v)), Some(Uuid::new_v4())).unwrap()
    }
    fn detach(old: ReferenceSnapshot) -> ReferenceChange {
        ReferenceChange::new(old.id.clone(), Some(old), None, None).unwrap()
    }
    fn replace(old: ReferenceSnapshot, v: &str) -> ReferenceChange {
        ReferenceChange::new(
            old.id.clone(),
            Some(old),
            Some(version(v)),
            Some(Uuid::new_v4()),
        )
        .unwrap()
    }
    fn build(changes: &[ReferenceChange], sources: &[ReferenceSnapshot]) -> Vec<TransactWriteItem> {
        reference_actions("coord-arn", &domain(), changes, sources).unwrap()
    }
    fn assert_object_guard(
        condition: &str,
        names: &HashMap<String, String>,
        values: &Item,
        v: &VersionRef,
    ) {
        assert!(condition.contains("#record = :record"));
        assert!(condition.contains("#store = :store AND #key = :key AND #version = :version"));
        assert!(condition.contains("#state = :ready"));
        assert!(
            condition.contains("attribute_type(#references, :number) AND #references >= :zero")
        );
        assert!(condition.contains("attribute_type(#claims, :number) AND #claims >= :zero"));
        for (alias, name) in [
            ("#record", "record"),
            ("#store", "store"),
            ("#key", "object_key"),
            ("#version", "version_id"),
            ("#references", "references"),
            ("#claims", "claims"),
            ("#state", "state"),
        ] {
            assert_eq!(names.get(alias).map(String::as_str), Some(name));
        }
        assert_eq!(values.get(":record"), Some(&s("version")));
        assert_eq!(values.get(":store"), Some(&s(v.store().as_str())));
        assert_eq!(values.get(":key"), Some(&s(v.key())));
        assert_eq!(values.get(":version"), Some(&s(v.version())));
        assert_eq!(values.get(":ready"), Some(&s("READY")));
        assert_eq!(values.get(":number"), Some(&s("N")));
        assert_eq!(values.get(":zero"), Some(&AttributeValue::N("0".into())));
    }
    #[test]
    fn checked_ids_preserve_exact_utf8_and_hide_debug_payload() {
        assert!(ReferenceId::new("").is_err());
        assert!(ReferenceId::new("é".repeat(65)).is_err());
        assert!(ReferenceId::new("é".repeat(64)).is_ok());
        let opaque = id("../é/secret-marker");
        assert_eq!(opaque.as_str(), "../é/secret-marker");
        assert!(
            text(&reference_key(&domain(), &id("é")), "pk")
                .unwrap()
                .ends_with("#REF#c3a9")
        );
        assert_ne!(
            reference_key(&domain(), &id("é")),
            reference_key(&domain(), &id("e\u{301}"))
        );
        assert!(!format!("{opaque:?}").contains("secret-marker"));
    }
    #[test]
    fn attach_native_absence_put_and_guarded_numeric_add() {
        let change = attach("slot", "v1");
        let source = snapshot("source", "v1");
        let actions = build(std::slice::from_ref(&change), std::slice::from_ref(&source));
        assert_eq!(actions.len(), 3);
        let check = actions[1].condition_check.as_ref().unwrap();
        assert_eq!(check.key(), &reference_key(&domain(), &source.id));
        let (condition, names, values) = snapshot_guard(&source);
        assert_eq!(check.condition_expression(), condition);
        assert_eq!(check.expression_attribute_names(), Some(&names));
        assert_eq!(check.expression_attribute_values(), Some(&values));
        let put = actions[0].put.as_ref().unwrap();
        assert_eq!(put.table_name(), "coord-arn");
        assert_eq!(
            put.condition_expression(),
            Some("attribute_not_exists(#pk)")
        );
        let observed = decode_reference(&domain(), &id("slot"), put.item()).unwrap();
        assert_eq!(observed.version(), &version("v1"));
        assert_eq!(observed.revision(), change.new_revision().unwrap());
        let update = actions[2].update.as_ref().unwrap();
        assert_eq!(
            update.key(),
            &object_key(&domain(), &version("v1").fingerprint())
        );
        assert_eq!(update.update_expression(), "ADD #references :delta");
        assert_eq!(
            update.expression_attribute_values().unwrap().get(":delta"),
            Some(&AttributeValue::N("1".into()))
        );
        assert_object_guard(
            update.condition_expression().unwrap(),
            update.expression_attribute_names().unwrap(),
            update.expression_attribute_values().unwrap(),
            &version("v1"),
        );
    }
    #[test]
    fn detach_native_delete_binds_old_identity_revision_and_prevents_underflow() {
        let old = snapshot("slot", "v1");
        let actions = build(&[detach(old.clone())], &[]);
        let delete = actions[0].delete.as_ref().unwrap();
        assert_eq!(delete.key(), &reference_key(&domain(), &old.id));
        assert!(
            delete
                .condition_expression()
                .unwrap()
                .contains("#id = :id AND #revision = :revision")
        );
        let values = delete.expression_attribute_values().unwrap();
        assert_eq!(values.get(":record"), Some(&s("reference")));
        assert_eq!(values.get(":id"), Some(&s("slot")));
        assert_eq!(values.get(":revision"), Some(&s(old.revision.to_string())));
        assert_eq!(values.get(":store"), Some(&s("store")));
        assert_eq!(values.get(":key"), Some(&s("exact/key")));
        assert_eq!(values.get(":version"), Some(&s("v1")));
        let update = actions[1].update.as_ref().unwrap();
        assert!(
            update
                .condition_expression()
                .unwrap()
                .contains("#references >= :minimum")
        );
        assert_eq!(
            update.expression_attribute_values().unwrap().get(":delta"),
            Some(&AttributeValue::N("-1".into()))
        );
        assert_eq!(
            update
                .expression_attribute_values()
                .unwrap()
                .get(":minimum"),
            Some(&AttributeValue::N("1".into()))
        );
        assert_object_guard(
            update.condition_expression().unwrap(),
            update.expression_attribute_names().unwrap(),
            update.expression_attribute_values().unwrap(),
            &old.version,
        );
    }
    #[test]
    fn replace_and_same_version_use_one_guarded_object_action_each() {
        let old = snapshot("slot", "v1");
        let actions = build(&[replace(old.clone(), "v2")], &[snapshot("source", "v2")]);
        assert_eq!(actions.len(), 4);
        let put = actions[0].put.as_ref().unwrap();
        assert_eq!(put.item().get("version_id"), Some(&s("v2")));
        assert_eq!(
            put.expression_attribute_values().unwrap().get(":version"),
            Some(&s("v1"))
        );
        assert_eq!(
            put.expression_attribute_values().unwrap().get(":revision"),
            Some(&s(old.revision.to_string()))
        );
        let deltas: HashMap<_, _> = actions[2..]
            .iter()
            .map(|a| {
                let values = a
                    .update
                    .as_ref()
                    .unwrap()
                    .expression_attribute_values()
                    .unwrap();
                (
                    text(values, ":version").unwrap(),
                    values.get(":delta").unwrap().clone(),
                )
            })
            .collect();
        assert_eq!(deltas["v1"], AttributeValue::N("-1".into()));
        assert_eq!(deltas["v2"], AttributeValue::N("1".into()));
        let same = build(&[replace(old, "v1")], &[]);
        assert_eq!(same.len(), 2);
        let check = same[1].condition_check.as_ref().unwrap();
        assert_object_guard(
            check.condition_expression(),
            check.expression_attribute_names().unwrap(),
            check.expression_attribute_values().unwrap(),
            &version("v1"),
        );
        assert!(
            !check
                .expression_attribute_values()
                .unwrap()
                .contains_key(":delta")
        );
    }
    #[test]
    fn multiple_slots_consolidate_by_full_tuple_including_zero_net_moves() {
        let actions = build(
            &[attach("a", "v1"), attach("b", "v1")],
            &[snapshot("source", "v1")],
        );
        assert_eq!(actions.len(), 4);
        assert_eq!(
            actions[3]
                .update
                .as_ref()
                .unwrap()
                .expression_attribute_values()
                .unwrap()
                .get(":delta"),
            Some(&AttributeValue::N("2".into()))
        );
        let moves = build(&[detach(snapshot("a", "v1")), attach("b", "v1")], &[]);
        assert_eq!(moves.len(), 3);
        assert!(moves[2].condition_check.is_some());
        let negative = build(
            &[detach(snapshot("a", "v1")), detach(snapshot("b", "v1"))],
            &[],
        );
        let values = negative[2]
            .update
            .as_ref()
            .unwrap()
            .expression_attribute_values()
            .unwrap();
        assert_eq!(values.get(":minimum"), Some(&AttributeValue::N("2".into())));
        assert_eq!(values.get(":delta"), Some(&AttributeValue::N("-2".into())));
        // Same version string with a different store/key remains a different tuple.
        let mut deltas = HashMap::new();
        add_delta(&mut deltas, &version("v1"), -1).unwrap();
        add_delta(
            &mut deltas,
            &VersionRef::new(StoreId::new("other").unwrap(), "exact/key", "v1").unwrap(),
            1,
        )
        .unwrap();
        assert_eq!(deltas.len(), 2);
    }
    #[test]
    fn duplicate_slots_collisions_and_reused_revisions_fail_closed() {
        let change = attach("slot", "v1");
        assert!(matches!(
            reference_actions("table", &domain(), &[change.clone(), change.clone()], &[]),
            Err(ReferenceError::DuplicateSlot)
        ));
        let reused = ReferenceChange::new(
            id("other"),
            None,
            Some(version("v2")),
            change.new_revision(),
        )
        .unwrap();
        // Revision identity is scoped to its slot. Distinct slots may use one
        // frozen operation revision; only reuse within a slot is forbidden.
        assert_eq!(
            reference_actions(
                "table",
                &domain(),
                &[change, reused],
                &[snapshot("s1", "v1"), snapshot("s2", "v2")]
            )
            .unwrap()
            .len(),
            6
        );
        let mut addresses = BTreeMap::new();
        insert_address(&mut addresses, [7; 32], version("v1"), 1).unwrap();
        assert!(matches!(
            insert_address(&mut addresses, [7; 32], version("v2"), -1),
            Err(ReferenceError::AddressCollision)
        ));
        assert_eq!(addresses[&[7; 32]].0, version("v1"));
        let old = snapshot("slot", "v1");
        assert!(matches!(
            ReferenceChange::new(id("other"), Some(old.clone()), None, None),
            Err(ReferenceError::SnapshotIdMismatch)
        ));
        for revision in [Uuid::nil(), old.revision] {
            assert!(matches!(
                ReferenceChange::new(
                    id("slot"),
                    Some(old.clone()),
                    Some(version("v2")),
                    Some(revision)
                ),
                Err(ReferenceError::InvalidRevision)
            ));
        }
        assert!(matches!(
            ReferenceChange::new(id("slot"), None, None, None),
            Err(ReferenceError::EmptyChange)
        ));
        assert!(matches!(
            ReferenceChange::new(id("slot"), None, Some(version("v1")), None),
            Err(ReferenceError::RevisionRequired)
        ));
        assert!(matches!(
            ReferenceChange::new(id("slot"), Some(old), None, Some(Uuid::new_v4())),
            Err(ReferenceError::RevisionRequired)
        ));
    }
    #[test]
    fn oversized_change_set_is_rejected_before_building_actions() {
        let changes: Vec<_> = (0..98)
            .map(|i| attach(&format!("slot-{i}"), "v1"))
            .collect();
        assert!(matches!(
            reference_actions("table", &domain(), &changes, &[]),
            Err(ReferenceError::TooManyChanges)
        ));
    }

    #[test]
    fn authority_is_exact_deduplicated_and_never_silently_ignored() {
        let source = snapshot("source", "v1");
        assert!(matches!(
            reference_actions("table", &domain(), &[attach("target", "v1")], &[]),
            Err(ReferenceError::MissingSource)
        ));
        assert!(matches!(
            reference_actions(
                "table",
                &domain(),
                &[attach("target", "v2")],
                std::slice::from_ref(&source)
            ),
            Err(ReferenceError::MissingSource)
        ));
        let mut wrong_tuple = source.clone();
        wrong_tuple.version =
            VersionRef::new(StoreId::new("other").unwrap(), "exact/key", "v1").unwrap();
        assert!(matches!(
            reference_actions(
                "table",
                &domain(),
                &[attach("target", "v1")],
                &[wrong_tuple.clone()]
            ),
            Err(ReferenceError::MissingSource)
        ));
        for contradictory in [snapshot("source", "v1"), wrong_tuple] {
            assert!(matches!(
                reference_actions(
                    "table",
                    &domain(),
                    &[],
                    &[source.clone(), contradictory.clone()]
                ),
                Err(ReferenceError::ContradictorySource)
            ));
            assert!(matches!(
                reference_actions(
                    "table",
                    &domain(),
                    &[detach(source.clone())],
                    &[contradictory]
                ),
                Err(ReferenceError::ContradictorySource)
            ));
        }
        assert!(matches!(
            reference_actions(
                "table",
                &domain(),
                &[attach("source", "v1")],
                std::slice::from_ref(&source)
            ),
            Err(ReferenceError::ContradictorySource)
        ));
        let duplicate = build(&[attach("target", "v1")], &[source.clone(), source.clone()]);
        assert_eq!(duplicate.len(), 3);
        let merged = build(
            &[detach(source.clone()), attach("target", "v1")],
            &[source.clone(), source.clone()],
        );
        assert_eq!(merged.len(), 3);
        assert!(merged[0].delete.is_some());
        assert!(merged[2].condition_check.is_some()); // zero-net READY guard
        let same = build(
            &[replace(source.clone(), "v1")],
            std::slice::from_ref(&source),
        );
        assert_eq!(same.len(), 2); // guarded Put, not Put + source ConditionCheck
        let extra = build(&[], std::slice::from_ref(&source));
        assert_eq!(extra.len(), 1); // unused explicit authority remains a condition
        assert!(extra[0].condition_check.is_some());
        assert!(matches!(
            reference_actions("table", &domain(), &[], &vec![source; 98]),
            Err(ReferenceError::TooManySources)
        ));
    }

    #[test]
    fn merged_source_input_lengths_do_not_impose_a_false_budget() {
        let sources: Vec<_> = (0..97)
            .map(|i| snapshot(&format!("slot-{i}"), "v1"))
            .collect();
        let changes: Vec<_> = sources.iter().cloned().map(detach).collect();
        let actions = build(&changes, &sources);
        assert_eq!(actions.len(), 98); // + journal admission/receipt = exactly 100
        assert_eq!(actions.iter().filter(|a| a.delete.is_some()).count(), 97);
    }

    #[test]
    fn malformed_reference_records_never_decode_as_absence() {
        let original = reference_item(&domain(), &snapshot("slot", "v1"));
        assert_eq!(original.len(), 8);
        for name in original.keys() {
            let mut item = original.clone();
            item.remove(name);
            assert!(decode_reference(&domain(), &id("slot"), &item).is_err());
            let mut item = original.clone();
            item.insert(name.clone(), AttributeValue::N("1".into()));
            assert!(decode_reference(&domain(), &id("slot"), &item).is_err());
        }
        for (field, value) in [
            ("pk", "wrong"),
            ("sk", "wrong"),
            ("record", "version"),
            ("reference_id", "different"),
            ("revision", "not-a-uuid"),
            ("revision", "00000000-0000-0000-0000-000000000000"),
            ("store", ""),
            ("object_key", ""),
            ("version_id", "null"),
            ("unknown", "extra"),
        ] {
            let mut item = original.clone();
            item.insert(field.into(), s(value));
            assert!(matches!(
                decode_reference(&domain(), &id("slot"), &item),
                Err(ReferenceError::CorruptRecord)
            ));
        }
    }
}
