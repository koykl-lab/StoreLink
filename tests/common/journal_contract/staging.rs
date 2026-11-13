//! Native DynamoDB transitions over explicitly SYNTHETIC TEST rows only.
//! Seeding SELECTED/ACTIVE records is NOT real S3 qualification, verification,
//! selection, namespace enrollment or complete reclamation acceptance. Opaque
//! snapshots are reconstructed by public strong reads, never fabricated. These
//! scenarios touch only the harness's two owned tables; resource lifecycle and
//! cleanup remain exclusively with the existing harness.

use super::*;
use aws_sdk_dynamodb::types::ConditionCheck;
use storelink::claim::{ClaimPolicy, ClaimState, Renewal};
use storelink::clock::{ClockObservation, TimestampMs};
use storelink::dynamodb::{
    IntentSnapshot, IntentState, NamespaceSnapshot, ProtectionError, StagedSnapshot, UploadError,
    UseSource,
};
use storelink::identity::NamespaceId;

fn ts(n: u64) -> TimestampMs {
    TimestampMs::new(n).unwrap()
}
fn clock(n: u64) -> ClockObservation {
    ClockObservation::trusted(ts(n), 5).unwrap()
}
fn policy() -> ClaimPolicy {
    // Explicit synthetic milliseconds, NOT production defaults.
    ClaimPolicy::new(2000, 50, 10).unwrap()
}
fn strings(row: &mut Item, fields: &[(&str, String)]) {
    for (name, value) in fields {
        row.insert((*name).into(), A::S(value.clone()));
    }
}
fn numbers(row: &mut Item, fields: &[(&str, u64)]) {
    for (name, value) in fields {
        row.insert((*name).into(), A::N(value.to_string()));
    }
}

struct Seed {
    domain: DomainId,
    journal: CommandJournal,
    namespace: NamespaceSnapshot,
    intent: IntentSnapshot,
    stage: StagedSnapshot,
    version: VersionRef,
}
impl Seed {
    async fn new(f: &Fixture) -> Self {
        Box::pin(Self::at_generation(f, 1)).await
    }
    async fn at_generation(f: &Fixture, generation: u64) -> Self {
        let domain = DomainId::new(format!("staging-test-{}", Uuid::new_v4())).unwrap();
        let journal = f.journal(&domain);
        let incarnation = journal.initialize_fresh_domain().await.unwrap();
        if generation != 1 {
            // Synthetic fixture setup only: admit ALL related IDs under the same
            // generation, preserving their native row/key codec bindings.
            let mut row = f.get(&f.coord, control(&domain)).await.unwrap();
            numbers(
                &mut row,
                &[("current_generation", generation), ("revision", 2)],
            );
            f.put(&f.coord, row).await;
        }
        let namespace_id = NamespaceId::new("synthetic-staging-test").unwrap();
        let configuration = Uuid::new_v4();
        let namespace_hex: String = namespace_id
            .as_str()
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let mut namespace_row = key(format!(
            "D#{:x}#NS#{namespace_hex}",
            Sha256::digest(domain.as_str().as_bytes())
        ));
        strings(
            &mut namespace_row,
            &[
                ("record", "namespace".into()),
                ("domain", domain.as_str().into()),
                ("incarnation", incarnation.to_string()),
                ("namespace_id", namespace_id.as_str().into()),
                ("configuration_revision", configuration.to_string()),
                ("state", "ACTIVE".into()),
                ("store", "synthetic-staging-store".into()),
                ("bucket", "fixture-not-s3-verified".into()),
                ("expected_owner", "123456789012".into()),
                ("region", "us-west-2".into()),
                ("key_prefix", "fresh-fixture/".into()),
                ("provider_profile", "aws-s3-versioned-lifecycle-v0".into()),
            ],
        );
        numbers(
            &mut namespace_row,
            &[
                ("mpu_max_age_ms", 7_200_000),
                ("mpu_lifecycle_margin_ms", 3_600_000),
            ],
        );
        f.put(&f.coord, namespace_row).await;
        let namespace = journal
            .read_namespace(&namespace_id)
            .await
            .unwrap()
            .unwrap();
        // Admitted identities only give these TEST records valid addresses; they
        // do not assert that the real selection protocol was exercised.
        let intent_id = ResourceId::from_command(journal.command_context().await.unwrap().id());
        let attempt_id = ResourceId::from_command(journal.command_context().await.unwrap().id());
        let claim_id = ResourceId::from_command(journal.command_context().await.unwrap().id());
        let version = VersionRef::new(
            StoreId::new("synthetic-staging-store").unwrap(),
            format!("fresh-fixture/{}", intent_id.uuid()),
            format!("synthetic-{}", Uuid::new_v4()),
        )
        .unwrap();
        let mut intent_row = upload_resource_key("INTENT", &intent_id);
        strings(
            &mut intent_row,
            &[
                ("record", "intent".into()),
                ("domain", domain.as_str().into()),
                ("incarnation", incarnation.to_string()),
                ("resource_uuid", intent_id.uuid().to_string()),
                ("namespace_id", namespace_id.as_str().into()),
                ("configuration_revision", configuration.to_string()),
                ("store", version.store().as_str().into()),
                ("object_key", version.key().into()),
                ("state", "SELECTED".into()),
                ("revision", claim_id.uuid().to_string()),
            ],
        );
        numbers(
            &mut intent_row,
            &[
                ("generation", intent_id.scope().generation()),
                // Former upload deadline is deliberately earlier than staging's.
                ("deadline", 1500),
                ("max_length", 100),
                ("expected_length", 10),
            ],
        );
        intent_row.insert("full_sha256".into(), A::B(Blob::new([7; 32])));
        f.put(&f.coord, intent_row).await;
        let mut stage_row = upload_resource_key("STAGED", &intent_id);
        strings(
            &mut stage_row,
            &[
                ("record", "staged".into()),
                ("kind", "STAGED".into()),
                ("domain", domain.as_str().into()),
                ("incarnation", incarnation.to_string()),
                ("intent_uuid", intent_id.uuid().to_string()),
                ("attempt_uuid", attempt_id.uuid().to_string()),
                ("claim_uuid", claim_id.uuid().to_string()),
                ("namespace_id", namespace_id.as_str().into()),
                ("configuration_revision", configuration.to_string()),
                ("revision", claim_id.uuid().to_string()),
                ("state", "ACTIVE".into()),
                ("store", version.store().as_str().into()),
                ("object_key", version.key().into()),
                ("version_id", version.version().into()),
            ],
        );
        numbers(
            &mut stage_row,
            &[
                ("intent_generation", intent_id.scope().generation()),
                ("attempt_generation", attempt_id.scope().generation()),
                ("claim_generation", claim_id.scope().generation()),
                ("deadline", 1800),
            ],
        );
        f.put(&f.coord, stage_row).await;
        // Same flat version schema as selection::encode_version and the shared
        // references count engine: exactly zero edges and ONE staging claim.
        let mut version_row = ready_version(&domain, &version);
        strings(
            &mut version_row,
            &[
                ("kind", "VERSION".into()),
                ("namespace_id", namespace_id.as_str().into()),
                ("configuration_revision", configuration.to_string()),
                ("creation_domain", domain.as_str().into()),
                ("creation_incarnation", incarnation.to_string()),
                ("creation_uuid", claim_id.uuid().to_string()),
            ],
        );
        numbers(
            &mut version_row,
            &[
                ("creation_generation", claim_id.scope().generation()),
                ("references", 0),
                ("claims", 1),
            ],
        );
        f.put(&f.coord, version_row).await;
        let intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
        let stage = journal.read_staged(&intent_id).await.unwrap().unwrap();
        assert_eq!(intent.state(), IntentState::Selected);
        assert_eq!(stage.claim().state(), ClaimState::Active);
        assert_eq!(stage.version(), &version);
        Self {
            domain,
            journal,
            namespace,
            intent,
            stage,
            version,
        }
    }
    fn slot(&self, label: &str) -> ReferenceId {
        ReferenceId::new(format!("{}-{label}", self.domain.as_str())).unwrap()
    }
    async fn counts(&self, f: &Fixture, references: u64, claims: u64) {
        let row = f
            .get(&f.coord, version_key(&self.domain, &self.version))
            .await
            .unwrap();
        assert_eq!(row.get("references"), Some(&A::N(references.to_string())));
        assert_eq!(row.get("claims"), Some(&A::N(claims.to_string())));
        assert_eq!(row.get("state"), Some(&A::S("READY".into())));
    }
    async fn states(&self, f: &Fixture, stage: &str, intent: &str) {
        for (kind, state) in [("STAGED", stage), ("INTENT", intent)] {
            let row = f
                .get(&f.coord, upload_resource_key(kind, self.intent.id()))
                .await
                .unwrap();
            assert_eq!(row.get("state"), Some(&A::S(state.into())));
        }
    }
    async fn publish(&self, f: &Fixture, slots: &[ReferenceId], result: &[u8]) -> PreparedCommand {
        let revision = Uuid::new_v4();
        self.journal
            .prepare_publish_staged(
                self.journal.command_context().await.unwrap(),
                actions(vec![pointer_put(
                    &f.app,
                    slots[0].as_str(),
                    &self.version,
                    None,
                    revision,
                )]),
                slots
                    .iter()
                    .map(|id| {
                        ReferenceChange::new(
                            id.clone(),
                            None,
                            Some(self.version.clone()),
                            Some(revision),
                        )
                        .unwrap()
                    })
                    .collect(),
                vec![],
                vec![(&self.stage, &self.intent)],
                clock(1000),
                &policy(),
                result.to_vec(),
            )
            .unwrap()
    }
}

// Native metadata-only pruning over this explicitly synthetic fixture. The
// DELETED state below is seeded only for the pruning predicate, NOT S3 deletion
// evidence (the separate object_aws suite verifies physical retirement).
async fn terminal_pruning(f: &Fixture) {
    use storelink::dynamodb::{
        MaintenancePolicy, PruningError, PruningOutcome, UploadConstraints, UseSource,
    };
    let s = Box::pin(Seed::new(f)).await;
    let j = &s.journal;
    let policy = policy();
    let version = j.read_version(&s.version).await.unwrap().unwrap();
    let context = j.command_context().await.unwrap();
    let pin_id = ResourceId::from_command(context.id());
    let acquire = j
        .prepare_acquire_use(
            context,
            actions(vec![]),
            &version,
            &s.namespace,
            UseSource::Staged {
                staged: &s.stage,
                intent: &s.intent,
            },
            ts(1800),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        j.execute(&acquire).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    assert!(matches!(
        j.prune_use_pin(&pin_id).await,
        Err(PruningError::Ineligible)
    ));
    let pin = j.read_use_pin(&pin_id).await.unwrap().unwrap();
    let release = j
        .prepare_release_use(j.command_context().await.unwrap(), &pin, vec![])
        .unwrap();
    assert!(matches!(
        j.execute(&release).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let cancel = j
        .prepare_cancel_staged(
            j.command_context().await.unwrap(),
            &s.stage,
            &s.intent,
            b"cancel-stage".to_vec(),
        )
        .unwrap();
    assert!(matches!(
        j.execute(&cancel).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    s.counts(f, 0, 0).await;

    // Produce an actual sealed attempt and cancelled OPEN intent through public
    // library methods; no S3 request is issued by any of these metadata methods.
    let context = j.command_context().await.unwrap();
    let intent_id = ResourceId::from_command(context.id());
    let begin = j
        .prepare_begin_intent(
            context,
            &s.namespace,
            "fresh-fixture/prune-attempt",
            UploadConstraints::new(100, None, None).unwrap(),
            ts(1800),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        j.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let intent = j.read_intent(&intent_id).await.unwrap().unwrap();
    let context = j.command_context().await.unwrap();
    let attempt_id = ResourceId::from_command(context.id());
    let begin = j
        .prepare_begin_attempt(
            context,
            &intent,
            &s.namespace,
            ts(1600),
            ts(2100),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        j.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    assert!(matches!(
        j.prune_upload_attempt(&attempt_id).await,
        Err(PruningError::Ineligible)
    ));
    let attempt = j.read_attempt(&attempt_id).await.unwrap().unwrap();
    let seal = j
        .prepare_seal_attempt(j.command_context().await.unwrap(), &attempt, vec![])
        .unwrap();
    assert!(matches!(
        j.execute(&seal).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let cancel_open = j
        .prepare_cancel_intent(j.command_context().await.unwrap(), &intent, vec![])
        .unwrap();
    assert!(matches!(
        j.execute(&cancel_open).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let never = j
        .prepare_metadata_only(
            j.command_context().await.unwrap(),
            actions(vec![]),
            b"never".to_vec(),
        )
        .unwrap();
    assert_eq!(
        j.cancel(never.token()).await.unwrap().outcome,
        Outcome::Cancelled
    );

    assert!(matches!(
        j.prune_use_pin(&pin_id).await,
        Err(PruningError::BeforeWatermark)
    ));
    assert!(matches!(
        j.prune_staged(s.intent.id()).await,
        Err(PruningError::BeforeWatermark)
    ));
    assert!(matches!(
        j.prune_upload_intent(&intent_id).await,
        Err(PruningError::BeforeWatermark)
    ));
    assert!(matches!(
        j.prune_upload_attempt(&attempt_id).await,
        Err(PruningError::BeforeWatermark)
    ));
    assert!(matches!(
        j.prune_command_outcome(cancel.token()).await,
        Err(PruningError::BeforeWatermark)
    ));
    let maintenance = MaintenancePolicy::new(1000, 10).unwrap();
    j.configure_compaction_policy(&maintenance).await.unwrap();
    j.advance_and_close_generation(&maintenance).await.unwrap();
    j.arm_compaction_window(&maintenance, &|| Ok(clock(1000)))
        .await
        .unwrap();
    j.publish_compaction_window(&maintenance, &|| Ok(clock(2010)))
        .await
        .unwrap();

    let counts_before = f
        .get(&f.coord, version_key(&s.domain, &s.version))
        .await
        .unwrap();
    assert_eq!(
        j.prune_use_pin(&pin_id).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(
        j.prune_use_pin(&pin_id).await.unwrap(),
        PruningOutcome::ObservationAbsent
    );
    assert_eq!(
        j.prune_staged(s.intent.id()).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(
        j.prune_upload_intent(s.intent.id()).await.unwrap(),
        PruningOutcome::Removed
    );
    // The terminal parent can disappear before its sealed child.
    assert_eq!(
        j.prune_upload_intent(&intent_id).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(
        j.prune_upload_attempt(&attempt_id).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(
        j.prune_upload_attempt(&attempt_id).await.unwrap(),
        PruningOutcome::ObservationAbsent
    );
    assert_eq!(
        f.get(&f.coord, version_key(&s.domain, &s.version)).await,
        Some(counts_before)
    );
    assert!(matches!(
        j.prune_deleted_version(&s.version).await,
        Err(PruningError::Ineligible)
    ));
    assert_eq!(
        j.resolve(cancel.token()).await.unwrap(),
        Outcome::Committed(b"cancel-stage".to_vec())
    );
    assert_eq!(
        j.prune_command_outcome(cancel.token()).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(j.resolve(cancel.token()).await.unwrap(), Outcome::Expired);
    assert_eq!(
        j.prune_command_outcome(cancel.token()).await.unwrap(),
        PruningOutcome::ObservationAbsent
    );
    assert_eq!(
        j.prune_command_outcome(never.token()).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(j.resolve(never.token()).await.unwrap(), Outcome::Expired);

    let unprotected = j.read_version(&s.version).await.unwrap().unwrap();
    j.retire_unprotected_version(&unprotected).await.unwrap();
    assert!(matches!(
        j.prune_deleted_version(&s.version).await,
        Err(PruningError::Ineligible)
    ));
    let address = version_key(&s.domain, &s.version);
    let mut tombstone = f.get(&f.coord, address.clone()).await.unwrap();
    tombstone.insert("state".into(), A::S("DELETED".into()));
    f.put(&f.coord, tombstone).await; // Synthetic predicate fixture, not physical deletion proof.
    assert_eq!(
        j.prune_deleted_version(&s.version).await.unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(
        j.prune_deleted_version(&s.version).await.unwrap(),
        PruningOutcome::ObservationAbsent
    );
    assert!(f.get(&f.coord, address).await.is_none());
}

pub(super) async fn exercise(f: &Fixture) {
    // Keep each scenario's large native-SDK future off its caller's poll frame.
    Box::pin(terminal_pruning(f)).await;
    Box::pin(publication_wins(f)).await;
    Box::pin(cancellation_wins(f)).await;
    Box::pin(renewal_then_revocation(f)).await;
    Box::pin(revocation_after_incarnation_rotation(f)).await;
    for malformed in ["negative-claims", "missing-claims", "missing-version"] {
        Box::pin(malformed_count(f, malformed)).await;
    }
    Box::pin(replacement(f)).await;
    Box::pin(use_pin_source_independence(f)).await;
    Box::pin(use_pin_restore_provenance(f)).await;
}

// Application-owned pointer CAS, using the same non-reused revision contract as
// pointer_put/pointer_delete. This is a native action, not an expression model.
fn use_pointer_guard(table: &str, owner: &str, observed_app_revision: Uuid) -> TransactWriteItem {
    TransactWriteItem::builder()
        .condition_check(
            ConditionCheck::builder()
                .table_name(table)
                .set_key(Some(key(owner)))
                .condition_expression("app_revision = :old")
                .expression_attribute_values(":old", A::S(observed_app_revision.to_string()))
                .build()
                .unwrap(),
        )
        .build()
}

// These are synthetic native-DDB accounting scenarios, NOT S3 qualification,
// retirement, restore-framework acceptance or full GC/reclamation proof.
async fn use_pin_source_independence(f: &Fixture) {
    let s = Box::pin(Seed::new(f)).await;
    s.counts(f, 0, 1).await;
    let version = s.journal.read_version(&s.version).await.unwrap().unwrap();
    let context = s.journal.command_context().await.unwrap();
    let pin_id = ResourceId::from_command(context.id());
    let acquire = s
        .journal
        .prepare_acquire_use(
            context,
            actions(vec![]),
            &version,
            &s.namespace,
            UseSource::Staged {
                staged: &s.stage,
                intent: &s.intent,
            },
            ts(2500),
            clock(1000),
            &policy(),
            b"use-acquired".to_vec(),
        )
        .unwrap();
    // Freeze the losing staged acquisition while its source is still ACTIVE.
    let stale_stage = s
        .journal
        .prepare_acquire_use(
            s.journal.command_context().await.unwrap(),
            actions(vec![]),
            &version,
            &s.namespace,
            UseSource::Staged {
                staged: &s.stage,
                intent: &s.intent,
            },
            ts(2500),
            clock(1000),
            &policy(),
            b"stale-stage".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&acquire).await.unwrap().outcome,
        Outcome::Committed(b"use-acquired".to_vec())
    );
    s.counts(f, 0, 2).await;
    let pin = s.journal.read_use_pin(&pin_id).await.unwrap().unwrap();
    assert_eq!(pin.claim().id(), &pin_id);
    assert_eq!(pin.claim().state(), ClaimState::Active);
    assert_eq!(pin.claim().deadline(), ts(2500));
    assert_eq!(pin.version_creation_id(), version.creation_id());
    let stale_release = s
        .journal
        .prepare_release_use(
            s.journal.command_context().await.unwrap(),
            &pin,
            b"stale-release".to_vec(),
        )
        .unwrap();

    let slot = s.slot("use-source");
    let publish = Box::pin(s.publish(f, std::slice::from_ref(&slot), b"use-published")).await;
    assert_eq!(
        s.journal.execute(&publish).await.unwrap().outcome,
        Outcome::Committed(b"use-published".to_vec())
    );
    s.counts(f, 1, 1).await;
    s.states(f, "RELEASED", "CONSUMED").await;
    let reference = s.journal.read_reference(&slot).await.unwrap().unwrap();
    assert_eq!(reference.version(), &s.version);
    let app = f.get(&f.app, key(slot.as_str())).await.unwrap();
    let observed_app_revision = Uuid::parse_str(app["app_revision"].as_s().unwrap()).unwrap();
    assert_eq!(observed_app_revision, reference.revision());
    assert_eq!(
        app.get("blob_version"),
        Some(&A::S(s.version.version().into()))
    );
    // Freeze both commands before detach wins: the old reference and app CAS
    // must be checked natively, not rejected by a refreshed Rust observation.
    let stale_reference = s
        .journal
        .prepare_acquire_use(
            s.journal.command_context().await.unwrap(),
            actions(vec![use_pointer_guard(
                &f.app,
                slot.as_str(),
                observed_app_revision,
            )]),
            &version,
            &s.namespace,
            UseSource::Reference(&reference),
            ts(2500),
            clock(1000),
            &policy(),
            b"stale-reference".to_vec(),
        )
        .unwrap();
    let detach = s
        .journal
        .prepare_reference_update(
            s.journal.command_context().await.unwrap(),
            actions(vec![pointer_delete(
                &f.app,
                slot.as_str(),
                observed_app_revision,
            )]),
            vec![ReferenceChange::new(slot.clone(), Some(reference), None, None).unwrap()],
            vec![],
            b"use-detached".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&detach).await.unwrap().outcome,
        Outcome::Committed(b"use-detached".to_vec())
    );
    s.counts(f, 0, 1).await;
    assert!(s.journal.read_reference(&slot).await.unwrap().is_none());
    assert!(f.get(&f.app, key(slot.as_str())).await.is_none());

    let current_version = s.journal.read_version(&s.version).await.unwrap().unwrap();
    assert_eq!(current_version.creation_id(), version.creation_id());
    let revision = Uuid::new_v4();
    let renewal = Renewal::new(pin.claim().revision(), ts(3400), revision).unwrap();
    let renew = s
        .journal
        .prepare_renew_use(
            s.journal.command_context().await.unwrap(),
            &pin,
            &current_version,
            &s.namespace,
            &renewal,
            clock(1700),
            &policy(),
            b"use-renewed".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&renew).await.unwrap().outcome,
        Outcome::Committed(b"use-renewed".to_vec())
    );
    s.counts(f, 0, 1).await;
    let renewed = s.journal.read_use_pin(&pin_id).await.unwrap().unwrap();
    assert_eq!(renewed.claim().id(), &pin_id);
    assert_eq!(renewed.claim().state(), ClaimState::Active);
    assert_eq!(renewed.claim().deadline(), ts(3400));
    assert_eq!(renewed.claim().revision(), revision);
    let renewed_row = f
        .get(&f.coord, upload_resource_key("USE", &pin_id))
        .await
        .unwrap();
    assert_eq!(renewed_row.get("deadline"), Some(&A::N("3400".into())));
    assert_eq!(
        renewed_row.get("revision"),
        Some(&A::S(revision.to_string()))
    );
    for rejected in [&stale_reference, &stale_stage, &stale_release] {
        assert_internal_rejection(&s.journal.execute(rejected).await.unwrap());
        s.counts(f, 0, 1).await;
        assert_eq!(
            f.get(&f.coord, upload_resource_key("USE", &pin_id)).await,
            Some(renewed_row.clone())
        );
        assert!(f.get(&f.coord, receipt(rejected.token())).await.is_none());
        assert!(s.journal.read_reference(&slot).await.unwrap().is_none());
        assert!(f.get(&f.app, key(slot.as_str())).await.is_none());
    }
    for rejected in [&stale_reference, &stale_stage] {
        let id = ResourceId::from_command(rejected.token().id());
        assert!(s.journal.read_use_pin(&id).await.unwrap().is_none());
        assert!(
            f.get(&f.coord, upload_resource_key("USE", &id))
                .await
                .is_none()
        );
    }
    s.states(f, "RELEASED", "CONSUMED").await;
    let release = s
        .journal
        .prepare_release_use(
            s.journal.command_context().await.unwrap(),
            &renewed,
            b"use-released".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&release).await.unwrap().outcome,
        Outcome::Committed(b"use-released".to_vec())
    );
    s.counts(f, 0, 0).await;
    let released = s.journal.read_use_pin(&pin_id).await.unwrap().unwrap();
    assert_eq!(released.claim().state(), ClaimState::Released);
    let released_row = f
        .get(&f.coord, upload_resource_key("USE", &pin_id))
        .await
        .unwrap();
    assert_eq!(released_row.get("state"), Some(&A::S("RELEASED".into())));
    let retry = s.journal.execute(&release).await.unwrap();
    assert_eq!(retry.outcome, Outcome::Committed(b"use-released".to_vec()));
    assert!(retry.attempt_failure.is_none());
    assert_eq!(
        s.journal.resolve(release.token()).await.unwrap(),
        retry.outcome
    );
    s.counts(f, 0, 0).await;
    assert_eq!(
        f.get(&f.coord, upload_resource_key("USE", &pin_id)).await,
        Some(released_row)
    );
    assert!(s.journal.read_reference(&slot).await.unwrap().is_none());
    assert!(f.get(&f.app, key(slot.as_str())).await.is_none());
}

async fn use_pin_restore_provenance(f: &Fixture) {
    let s = Box::pin(Seed::at_generation(f, 5)).await;
    let slot = s.slot("restore-use-source");
    let publish = Box::pin(s.publish(f, std::slice::from_ref(&slot), b"restore-published")).await;
    assert_eq!(
        s.journal.execute(&publish).await.unwrap().outcome,
        Outcome::Committed(b"restore-published".to_vec())
    );
    s.counts(f, 1, 0).await;
    s.states(f, "RELEASED", "CONSUMED").await;
    let version = s.journal.read_version(&s.version).await.unwrap().unwrap();
    let reference = s.journal.read_reference(&slot).await.unwrap().unwrap();
    let app = f.get(&f.app, key(slot.as_str())).await.unwrap();
    let observed_app_revision = Uuid::parse_str(app["app_revision"].as_s().unwrap()).unwrap();
    assert_eq!(reference.revision(), observed_app_revision);
    assert_eq!(reference.version(), &s.version);
    let context_a = s.journal.command_context().await.unwrap();
    let id_a = ResourceId::from_command(context_a.id());
    let incarnation_a = id_a.scope().incarnation();
    assert_eq!(id_a.scope().generation(), 5);
    assert_eq!(version.creation_id().scope().generation(), 5);
    let acquire_a = s
        .journal
        .prepare_acquire_use(
            context_a,
            actions(vec![use_pointer_guard(
                &f.app,
                slot.as_str(),
                observed_app_revision,
            )]),
            &version,
            &s.namespace,
            UseSource::Reference(&reference),
            ts(2500),
            clock(1000),
            &policy(),
            b"use-A".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&acquire_a).await.unwrap().outcome,
        Outcome::Committed(b"use-A".to_vec())
    );
    s.counts(f, 1, 1).await;
    let pin_a = s.journal.read_use_pin(&id_a).await.unwrap().unwrap();
    assert_eq!(pin_a.claim().state(), ClaimState::Active);
    assert_eq!(pin_a.version_creation_id(), version.creation_id());
    let pin_a_row = f
        .get(&f.coord, upload_resource_key("USE", &id_a))
        .await
        .unwrap();
    let original_version_row = f
        .get(&f.coord, version_key(&s.domain, &s.version))
        .await
        .unwrap();
    // Administrative TEST setup only: rotate VALID control, not namespace,
    // durable reference, or immutable version creation/configuration provenance.
    let incarnation_b = Uuid::new_v4();
    assert_ne!(incarnation_b, incarnation_a);
    let mut control_row = f.get(&f.coord, control(&s.domain)).await.unwrap();
    strings(
        &mut control_row,
        &[("incarnation", incarnation_b.to_string())],
    );
    numbers(
        &mut control_row,
        &[
            ("current_generation", 1),
            ("closed_through", 0),
            ("compacted_through", 0),
            ("revision", 3),
        ],
    );
    f.put(&f.coord, control_row.clone()).await;
    let namespace = s
        .journal
        .read_namespace(s.namespace.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(namespace, s.namespace); // Enrollment provenance intentionally stays A.
    let context_b = s.journal.command_context().await.unwrap();
    let id_b = ResourceId::from_command(context_b.id());
    assert_eq!(id_b.scope().incarnation(), incarnation_b);
    assert_eq!(id_b.scope().generation(), 1);
    let acquire_b = s
        .journal
        .prepare_acquire_use(
            context_b,
            actions(vec![use_pointer_guard(
                &f.app,
                slot.as_str(),
                observed_app_revision,
            )]),
            &version,
            &namespace,
            UseSource::Reference(&reference),
            ts(2500),
            clock(1000),
            &policy(),
            b"use-B".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&acquire_b).await.unwrap().outcome,
        Outcome::Committed(b"use-B".to_vec())
    );
    s.counts(f, 1, 2).await;
    let pin_b = s.journal.read_use_pin(&id_b).await.unwrap().unwrap();
    assert_eq!(pin_b.claim().id(), &id_b);
    assert_eq!(pin_b.claim().state(), ClaimState::Active);
    assert_eq!(pin_b.version_creation_id(), version.creation_id());
    assert_eq!(
        pin_b.version_creation_id().scope().incarnation(),
        incarnation_a
    );
    // Freeze old B release before renewal; A expiry likewise keeps its old
    // observed row, without minting any maintenance receipt/claim identity.
    let stale_release_b = s
        .journal
        .prepare_release_use(
            s.journal.command_context().await.unwrap(),
            &pin_b,
            b"stale-B".to_vec(),
        )
        .unwrap();
    let policy = policy();
    let stale_revoke_a = Box::pin(s.journal.revoke_expired_use(&pin_a, clock(2505), &policy));
    let revision_b = Uuid::new_v4();
    let renewal_b = Renewal::new(pin_b.claim().revision(), ts(3400), revision_b).unwrap();
    let current_version = s.journal.read_version(&s.version).await.unwrap().unwrap();
    let renew_b = s
        .journal
        .prepare_renew_use(
            s.journal.command_context().await.unwrap(),
            &pin_b,
            &current_version,
            &namespace,
            &renewal_b,
            clock(1700),
            &policy,
            b"renew-B".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&renew_b).await.unwrap().outcome,
        Outcome::Committed(b"renew-B".to_vec())
    );
    s.counts(f, 1, 2).await;
    let renewed_b = s.journal.read_use_pin(&id_b).await.unwrap().unwrap();
    assert_eq!(renewed_b.claim().deadline(), ts(3400));
    assert_eq!(renewed_b.claim().revision(), revision_b);
    assert_eq!(renewed_b.version_creation_id(), version.creation_id());
    assert_internal_rejection(&s.journal.execute(&stale_release_b).await.unwrap());
    s.counts(f, 1, 2).await;
    assert_eq!(
        s.journal.read_use_pin(&id_b).await.unwrap().unwrap(),
        renewed_b
    );
    assert!(
        f.get(&f.coord, receipt(stale_release_b.token()))
            .await
            .is_none()
    );
    let release_b = s
        .journal
        .prepare_release_use(
            s.journal.command_context().await.unwrap(),
            &renewed_b,
            b"release-B".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&release_b).await.unwrap().outcome,
        Outcome::Committed(b"release-B".to_vec())
    );
    s.counts(f, 1, 1).await;
    assert_eq!(
        s.journal
            .read_use_pin(&id_b)
            .await
            .unwrap()
            .unwrap()
            .claim()
            .state(),
        ClaimState::Released
    );

    let renewal_a = Renewal::new(pin_a.claim().revision(), ts(3400), Uuid::new_v4()).unwrap();
    assert!(matches!(
        s.journal.prepare_renew_use(
            s.journal.command_context().await.unwrap(),
            &pin_a,
            &current_version,
            &namespace,
            &renewal_a,
            clock(1700),
            &policy,
            b"forbidden-A-renewal".to_vec(),
        ),
        Err(ProtectionError::Upload(UploadError::Binding))
    ));
    s.counts(f, 1, 1).await;
    assert_eq!(
        f.get(&f.coord, upload_resource_key("USE", &id_a)).await,
        Some(pin_a_row.clone())
    );
    let staged_context_b = s.journal.command_context().await.unwrap();
    let forbidden_id = ResourceId::from_command(staged_context_b.id());
    assert!(matches!(
        s.journal.prepare_acquire_use(
            staged_context_b,
            actions(vec![]),
            &version,
            &namespace,
            UseSource::Staged {
                staged: &s.stage,
                intent: &s.intent
            },
            ts(2500),
            clock(1000),
            &policy,
            b"forbidden-A-stage".to_vec(),
        ),
        Err(ProtectionError::Upload(UploadError::Binding))
    ));
    s.counts(f, 1, 1).await;
    assert!(
        s.journal
            .read_use_pin(&forbidden_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.get(&f.coord, upload_resource_key("USE", &id_a)).await,
        Some(pin_a_row)
    );
    // now - uncertainty == A's deadline; generation 5 must NOT be compared
    // against B's generation 1. Only the valid current control guards cleanup.
    s.journal
        .revoke_expired_use(&pin_a, clock(2505), &policy)
        .await
        .unwrap();
    s.counts(f, 1, 0).await;
    let revoked_a = s.journal.read_use_pin(&id_a).await.unwrap().unwrap();
    assert_eq!(revoked_a.claim().state(), ClaimState::Revoked);
    assert_eq!(revoked_a.version_creation_id(), version.creation_id());
    let revoked_row = f
        .get(&f.coord, upload_resource_key("USE", &id_a))
        .await
        .unwrap();
    assert_eq!(revoked_row.get("state"), Some(&A::S("REVOKED".into())));
    assert!(matches!(
        stale_revoke_a.await,
        Err(ProtectionError::Conflict)
    ));
    s.counts(f, 1, 0).await;
    assert_eq!(
        f.get(&f.coord, upload_resource_key("USE", &id_a)).await,
        Some(revoked_row)
    );
    let final_version = s.journal.read_version(&s.version).await.unwrap().unwrap();
    assert_eq!(final_version.creation_id(), version.creation_id());
    assert_eq!(final_version.namespace_id(), version.namespace_id());
    assert_eq!(
        final_version.configuration_revision(),
        version.configuration_revision()
    );
    let mut expected_version_row = original_version_row;
    numbers(&mut expected_version_row, &[("claims", 0)]);
    assert_eq!(
        f.get(&f.coord, version_key(&s.domain, &s.version)).await,
        Some(expected_version_row)
    );
    assert_eq!(
        s.journal.read_reference(&slot).await.unwrap().unwrap(),
        reference
    );
    assert_eq!(f.get(&f.app, key(slot.as_str())).await, Some(app));
    assert_eq!(
        s.journal
            .read_namespace(namespace.id())
            .await
            .unwrap()
            .unwrap(),
        namespace
    );
    assert_eq!(f.get(&f.coord, control(&s.domain)).await, Some(control_row));
    s.states(f, "RELEASED", "CONSUMED").await;
}

async fn publication_wins(f: &Fixture) {
    let s = Box::pin(Seed::new(f)).await;
    let slots = [s.slot("a"), s.slot("b")];
    let publish = Box::pin(s.publish(f, &slots, b"published-two")).await;
    let cancel = s
        .journal
        .prepare_cancel_staged(
            s.journal.command_context().await.unwrap(),
            &s.stage,
            &s.intent,
            b"cancel".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&publish).await.unwrap().outcome,
        Outcome::Committed(b"published-two".to_vec())
    );
    s.counts(f, 2, 0).await;
    s.states(f, "RELEASED", "CONSUMED").await;
    for id in &slots {
        let reference = s.journal.read_reference(id).await.unwrap().unwrap();
        assert_eq!(reference.version(), &s.version);
    }
    let app = f.get(&f.app, key(slots[0].as_str())).await.unwrap();
    assert_eq!(
        app.get("blob_version"),
        Some(&A::S(s.version.version().into()))
    );
    let retry = s.journal.execute(&publish).await.unwrap();
    assert_eq!(retry.outcome, Outcome::Committed(b"published-two".to_vec()));
    assert!(retry.attempt_failure.is_none());
    assert_eq!(
        s.journal.resolve(publish.token()).await.unwrap(),
        retry.outcome
    );
    assert_internal_rejection(&s.journal.execute(&cancel).await.unwrap());
    s.counts(f, 2, 0).await;
    s.states(f, "RELEASED", "CONSUMED").await;
    assert_eq!(f.get(&f.app, key(slots[0].as_str())).await, Some(app));
    assert!(f.get(&f.coord, receipt(cancel.token())).await.is_none());
}

async fn cancellation_wins(f: &Fixture) {
    let s = Box::pin(Seed::new(f)).await;
    let slot = s.slot("cancel-target");
    let publish = Box::pin(s.publish(f, std::slice::from_ref(&slot), b"publish-loses")).await;
    let cancel = s
        .journal
        .prepare_cancel_staged(
            s.journal.command_context().await.unwrap(),
            &s.stage,
            &s.intent,
            b"cancel-wins".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&cancel).await.unwrap().outcome,
        Outcome::Committed(b"cancel-wins".to_vec())
    );
    s.counts(f, 0, 0).await;
    s.states(f, "RELEASED", "CANCELLED").await;
    assert_internal_rejection(&s.journal.execute(&publish).await.unwrap());
    assert!(f.get(&f.app, key(slot.as_str())).await.is_none());
    assert!(s.journal.read_reference(&slot).await.unwrap().is_none());
    assert!(f.get(&f.coord, receipt(publish.token())).await.is_none());
    s.counts(f, 0, 0).await;
    s.states(f, "RELEASED", "CANCELLED").await;
}

async fn renewal_then_revocation(f: &Fixture) {
    let s = Box::pin(Seed::new(f)).await;
    let cancel = s
        .journal
        .prepare_cancel_staged(
            s.journal.command_context().await.unwrap(),
            &s.stage,
            &s.intent,
            b"stale-cancel".to_vec(),
        )
        .unwrap();
    let policy = policy();
    // Maintenance has no prepare/receipt API. Freeze the OLD observations and
    // explicit expiry clock before the winner; poll the operation afterwards.
    let stale_revoke =
        Box::pin(
            s.journal
                .revoke_expired_staged(&s.stage, &s.intent, clock(1805), &policy),
        );
    let revision = Uuid::new_v4();
    let renewal = Renewal::new(s.stage.claim().revision(), ts(2400), revision).unwrap();
    let renew = s
        .journal
        .prepare_renew_staged(
            s.journal.command_context().await.unwrap(),
            &s.stage,
            &s.intent,
            &s.namespace,
            &renewal,
            clock(1600),
            &policy,
            b"renewed".to_vec(),
        )
        .unwrap();
    assert_eq!(
        s.journal.execute(&renew).await.unwrap().outcome,
        Outcome::Committed(b"renewed".to_vec())
    );
    let row = f
        .get(&f.coord, upload_resource_key("STAGED", s.intent.id()))
        .await
        .unwrap();
    assert_eq!(row.get("deadline"), Some(&A::N("2400".into())));
    assert_eq!(row.get("revision"), Some(&A::S(revision.to_string())));
    s.counts(f, 0, 1).await;
    assert_internal_rejection(&s.journal.execute(&cancel).await.unwrap());
    assert!(matches!(
        stale_revoke.await,
        Err(UploadError::StagedConflict)
    ));
    // A maintenance error is NOT rollback/history evidence: reread authoritative
    // state and counters after every failed attempt before further work.
    let renewed = s.journal.read_staged(s.intent.id()).await.unwrap().unwrap();
    let intent = s.journal.read_intent(s.intent.id()).await.unwrap().unwrap();
    assert_eq!(renewed.claim().deadline(), ts(2400));
    assert_eq!(renewed.claim().revision(), revision);
    s.counts(f, 0, 1).await;
    s.states(f, "ACTIVE", "SELECTED").await;
    assert_eq!(
        f.get(&f.coord, upload_resource_key("STAGED", s.intent.id()))
            .await,
        Some(row)
    );
    // now - uncertainty == deadline: the conservative lower bound is sufficient.
    s.journal
        .revoke_expired_staged(&renewed, &intent, clock(2405), &policy)
        .await
        .unwrap();
    s.counts(f, 0, 0).await;
    s.states(f, "REVOKED", "CANCELLED").await;
    assert!(matches!(
        s.journal
            .revoke_expired_staged(&renewed, &intent, clock(2405), &policy)
            .await,
        Err(UploadError::StagedConflict)
    ));
    let terminal = s.journal.read_staged(s.intent.id()).await.unwrap().unwrap();
    let terminal_intent = s.journal.read_intent(s.intent.id()).await.unwrap().unwrap();
    assert_eq!(terminal.claim().state(), ClaimState::Revoked);
    assert_eq!(terminal_intent.state(), IntentState::Cancelled);
    s.counts(f, 0, 0).await;
    s.states(f, "REVOKED", "CANCELLED").await;
}

async fn revocation_after_incarnation_rotation(f: &Fixture) {
    let s = Box::pin(Seed::at_generation(f, 5)).await;
    assert_eq!(s.stage.claim().id().scope().generation(), 5);
    assert_eq!(s.intent.id().scope().generation(), 5);
    let mut row = f.get(&f.coord, control(&s.domain)).await.unwrap();
    numbers(
        &mut row,
        &[
            ("current_generation", 1),
            ("closed_through", 0),
            ("compacted_through", 0),
            ("revision", 3),
        ],
    );
    f.put(&f.coord, row.clone()).await;
    // Same incarnation: a future creation generation is invalid, not cleanup
    // authority. This rejection precedes transaction submission.
    assert!(matches!(
        s.journal
            .revoke_expired_staged(&s.stage, &s.intent, clock(1805), &policy())
            .await,
        Err(UploadError::Binding)
    ));
    s.counts(f, 0, 1).await;
    s.states(f, "ACTIVE", "SELECTED").await;

    for incarnation in ["not-a-uuid".to_owned(), Uuid::nil().to_string()] {
        strings(&mut row, &[("incarnation", incarnation)]);
        f.put(&f.coord, row.clone()).await;
        // Native parse error proves this did not merely reach a write condition
        // failure; an arbitrary different string is not a valid incarnation.
        assert!(matches!(
            s.journal
                .revoke_expired_staged(&s.stage, &s.intent, clock(1805), &policy())
                .await,
            Err(UploadError::Journal(JournalError::Corrupt(_)))
        ));
        s.counts(f, 0, 1).await;
        s.states(f, "ACTIVE", "SELECTED").await;
    }

    let incarnation = Uuid::new_v4();
    assert_ne!(incarnation, s.stage.claim().id().scope().incarnation());
    strings(&mut row, &[("incarnation", incarnation.to_string())]);
    numbers(&mut row, &[("revision", 4)]);
    f.put(&f.coord, row.clone()).await;
    // Native current=1 must not be compared to old-incarnation claim generation=5.
    s.journal
        .revoke_expired_staged(&s.stage, &s.intent, clock(1805), &policy())
        .await
        .unwrap();
    s.counts(f, 0, 0).await;
    s.states(f, "REVOKED", "CANCELLED").await;
    assert!(matches!(
        s.journal
            .revoke_expired_staged(&s.stage, &s.intent, clock(1805), &policy())
            .await,
        Err(UploadError::StagedConflict)
    ));
    // Inspect native rows after the failed stale submission; never infer rollback.
    let terminal = s.journal.read_staged(s.intent.id()).await.unwrap().unwrap();
    let intent = s.journal.read_intent(s.intent.id()).await.unwrap().unwrap();
    assert_eq!(terminal.claim().state(), ClaimState::Revoked);
    assert_eq!(intent.state(), IntentState::Cancelled);
    s.counts(f, 0, 0).await;
    s.states(f, "REVOKED", "CANCELLED").await;
    assert_eq!(f.get(&f.coord, control(&s.domain)).await, Some(row));
}

async fn malformed_count(f: &Fixture, malformed: &str) {
    let s = Box::pin(Seed::new(f)).await;
    let slot = s.slot("malformed");
    let publish = Box::pin(s.publish(f, std::slice::from_ref(&slot), b"must-not-publish")).await;
    let address = version_key(&s.domain, &s.version);
    let mut row = f.get(&f.coord, address.clone()).await.unwrap();
    match malformed {
        "negative-claims" => {
            row.insert("claims".into(), A::N("-1".into()));
        }
        "missing-claims" => {
            row.remove("claims");
        }
        "missing-version" => {}
        _ => unreachable!(),
    }
    let expected = if malformed == "missing-version" {
        f.delete(&f.coord, address.clone()).await;
        None
    } else {
        f.put(&f.coord, row.clone()).await;
        Some(row)
    };
    assert_internal_rejection(&s.journal.execute(&publish).await.unwrap());
    assert_eq!(
        f.get(&f.coord, address).await,
        expected,
        "no count write/upsert on {malformed}"
    );
    assert!(f.get(&f.app, key(slot.as_str())).await.is_none());
    assert!(s.journal.read_reference(&slot).await.unwrap().is_none());
    assert!(f.get(&f.coord, receipt(publish.token())).await.is_none());
    s.states(f, "ACTIVE", "SELECTED").await;
}

async fn replacement(f: &Fixture) {
    let s = Box::pin(Seed::new(f)).await;
    let old_version = VersionRef::new(
        s.version.store().clone(),
        "fresh-fixture/old",
        "synthetic-old",
    )
    .unwrap();
    f.put(&f.coord, ready_version(&s.domain, &old_version))
        .await;
    let old = seed_source(f, &s.journal, &s.domain, &old_version).await;
    let owner = s.slot("replace-app");
    let old_revision = Uuid::new_v4();
    let old_put = pointer_put(&f.app, owner.as_str(), &old_version, None, old_revision);
    f.put(&f.app, old_put.put().unwrap().item().clone()).await;
    let new_revision = Uuid::new_v4();
    let replace = s
        .journal
        .prepare_publish_staged(
            s.journal.command_context().await.unwrap(),
            actions(vec![pointer_put(
                &f.app,
                owner.as_str(),
                &s.version,
                Some(old_revision),
                new_revision,
            )]),
            vec![
                ReferenceChange::new(
                    old.id().clone(),
                    Some(old.clone()),
                    Some(s.version.clone()),
                    Some(new_revision),
                )
                .unwrap(),
            ],
            vec![],
            vec![(&s.stage, &s.intent)],
            clock(1000),
            &policy(),
            b"replaced".to_vec(),
        )
        .unwrap();
    // A deliberately failing application CAS must reject the ENTIRE combined
    // count/claim/reference transition, not merely the application pointer.
    let wrong_app = s
        .journal
        .prepare_publish_staged(
            s.journal.command_context().await.unwrap(),
            actions(vec![pointer_put(
                &f.app,
                owner.as_str(),
                &s.version,
                Some(Uuid::new_v4()),
                Uuid::new_v4(),
            )]),
            vec![
                ReferenceChange::new(
                    old.id().clone(),
                    Some(old.clone()),
                    Some(s.version.clone()),
                    Some(Uuid::new_v4()),
                )
                .unwrap(),
            ],
            vec![],
            vec![(&s.stage, &s.intent)],
            clock(1000),
            &policy(),
            b"wrong-app-cas".to_vec(),
        )
        .unwrap();
    // A second frozen application CAS cannot succeed after the winner either.
    let stale = s
        .journal
        .prepare_publish_staged(
            s.journal.command_context().await.unwrap(),
            actions(vec![pointer_put(
                &f.app,
                owner.as_str(),
                &s.version,
                Some(old_revision),
                Uuid::new_v4(),
            )]),
            vec![
                ReferenceChange::new(
                    old.id().clone(),
                    Some(old.clone()),
                    Some(s.version.clone()),
                    Some(Uuid::new_v4()),
                )
                .unwrap(),
            ],
            vec![],
            vec![(&s.stage, &s.intent)],
            clock(1000),
            &policy(),
            b"stale-replacement".to_vec(),
        )
        .unwrap();
    let rejected = s.journal.execute(&wrong_app).await.unwrap();
    assert_eq!(rejected.outcome, Outcome::Unknown);
    match rejected.attempt_failure {
        Some(AttemptFailure::TransactionCancelled { reasons }) => assert!(
            reasons
                .iter()
                .any(|r| matches!(r.origin, ActionOrigin::Application { .. })
                    && r.code == "ConditionalCheckFailed")
        ),
        other => panic!("expected native application CAS rejection: {other:?}"),
    }
    assert_reference_count(f, &s.domain, &old_version, 1).await;
    s.counts(f, 0, 1).await;
    s.states(f, "ACTIVE", "SELECTED").await;
    assert_eq!(
        s.journal.read_reference(old.id()).await.unwrap().unwrap(),
        old
    );
    assert_eq!(
        f.get(&f.app, key(owner.as_str())).await.as_ref(),
        Some(old_put.put().unwrap().item())
    );
    assert!(f.get(&f.coord, receipt(wrong_app.token())).await.is_none());
    assert_eq!(
        s.journal.execute(&replace).await.unwrap().outcome,
        Outcome::Committed(b"replaced".to_vec())
    );
    assert_reference_count(f, &s.domain, &old_version, 0).await;
    s.counts(f, 1, 0).await;
    s.states(f, "RELEASED", "CONSUMED").await;
    let current = s.journal.read_reference(old.id()).await.unwrap().unwrap();
    assert_eq!(current.version(), &s.version);
    assert_eq!(current.revision(), new_revision);
    let app = f.get(&f.app, key(owner.as_str())).await.unwrap();
    assert_eq!(
        app.get("blob_version"),
        Some(&A::S(s.version.version().into()))
    );
    assert_eq!(
        app.get("app_revision"),
        Some(&A::S(new_revision.to_string()))
    );
    assert_internal_rejection(&s.journal.execute(&stale).await.unwrap());
    assert_reference_count(f, &s.domain, &old_version, 0).await;
    s.counts(f, 1, 0).await;
    assert_eq!(f.get(&f.app, key(owner.as_str())).await, Some(app));
}
