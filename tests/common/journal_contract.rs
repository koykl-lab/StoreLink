//! One native journal contract shared by explicit local and AWS fixtures.
//! This module neither provisions resources nor selects credentials/endpoints.
//! Upload namespace rows below are explicit native-test configuration fixtures,
//! NOT evidence that S3 lifecycle, fresh scope or exclusive ownership is verified.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue as A, Delete, Put, TransactWriteItem};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use storelink::dynamodb::{
    ActionOrigin, ApplicationActions, AttemptFailure, CommandJournal, CommandToken, EnrolledTables,
    JournalError, Outcome, PreparedCommand, ReferenceChange, ReferenceError, ReferenceId,
};
use storelink::identity::{DomainId, ResourceId, StoreId, VersionRef};
use tokio::sync::Barrier;
use uuid::Uuid;

type Item = HashMap<String, A>;
#[derive(Clone)]
pub struct Fixture {
    client: Client,
    coord: String,
    app: String,
    tables: EnrolledTables,
}
impl Fixture {
    /// Enrollment remains inside the wrapper's panic/cleanup boundary.
    pub async fn enroll(client: Client, coord: String, app: String) -> Self {
        let tables = EnrolledTables::enroll(&client, &coord, std::slice::from_ref(&app))
            .await
            .expect("native fixture enrollment");
        Self {
            client,
            coord,
            app,
            tables,
        }
    }
    fn journal(&self, domain: &DomainId) -> CommandJournal {
        CommandJournal::new(self.client.clone(), self.tables.clone(), domain.clone())
    }
    async fn get(&self, table: &str, key: Item) -> Option<Item> {
        self.client
            .get_item()
            .table_name(table)
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
    }
    async fn put(&self, table: &str, item: Item) {
        self.client
            .put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
    async fn delete(&self, table: &str, key: Item) {
        self.client
            .delete_item()
            .table_name(table)
            .set_key(Some(key))
            .send()
            .await
            .unwrap();
    }
    async fn prepare(
        &self,
        journal: &CommandJournal,
        name: &str,
        result: &[u8],
    ) -> PreparedCommand {
        journal
            .prepare_metadata_only(
                journal.command_context().await.unwrap(),
                actions(vec![put(&self.app, name, Some("attribute_not_exists(pk)"))]),
                result.to_vec(),
            )
            .unwrap()
    }
}
fn key(pk: impl Into<String>) -> Item {
    HashMap::from([
        ("pk".into(), A::S(pk.into())),
        ("sk".into(), A::S("M".into())),
    ])
}
fn control(domain: &DomainId) -> Item {
    key(format!(
        "D#{:x}#CONTROL",
        Sha256::digest(domain.as_str().as_bytes())
    ))
}
fn receipt(token: &CommandToken) -> Item {
    let id = token.id();
    key(format!(
        "D#{:x}#CMD#{}#{}#{}",
        Sha256::digest(id.scope().domain().as_str().as_bytes()),
        id.scope().incarnation(),
        id.scope().generation(),
        id.uuid()
    ))
}
fn put(table: &str, name: &str, condition: Option<&str>) -> TransactWriteItem {
    TransactWriteItem::builder()
        .put(
            Put::builder()
                .table_name(table)
                .set_item(Some(key(name)))
                .set_condition_expression(condition.map(str::to_owned))
                .build()
                .unwrap(),
        )
        .build()
}
fn actions(items: Vec<TransactWriteItem>) -> ApplicationActions {
    ApplicationActions::new(items).unwrap()
}

fn assert_native_validation(failure: Option<&AttemptFailure>) {
    match failure {
        Some(AttemptFailure::NativeValidation { code, message }) => {
            assert_eq!(code, "ValidationException");
            assert!(!message.trim().is_empty());
            assert_ne!(message, "service error", "native explanation was discarded");
        }
        Some(AttemptFailure::TransactionCancelled { reasons }) => {
            assert!(
                reasons.iter().any(|r| r.code == "ValidationError"
                    && r.message
                        .as_ref()
                        .is_some_and(|m| !m.trim().is_empty() && m != "service error")),
                "expected native validation explanation, got {reasons:?}"
            );
        }
        other => panic!("expected classified native validation, got {other:?}"),
    }
}

#[test]
fn scenario_future_layout_is_bounded() {
    fn size<A, F: std::future::Future>(_: fn(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let sizes = [
        ("suite", size(exercise)),
        ("journal", size::<&'static Fixture, _>(exercise_journal)),
        (
            "references",
            size::<&'static Fixture, _>(exercise_references),
        ),
        (
            "uploads",
            size::<&'static Fixture, _>(exercise_upload_admission),
        ),
    ];
    for (name, bytes) in sizes {
        eprintln!("native scenario future {name}: {bytes} bytes");
        assert!(bytes <= 128 * 1024, "oversized scenario future: {name}");
    }
}

#[path = "journal_contract/abandoned.rs"]
mod abandoned;
#[path = "journal_contract/maintenance.rs"]
mod maintenance;
#[path = "journal_contract/staging.rs"]
mod staging;

fn native_read_conflict_only(error: &JournalError) -> bool {
    let JournalError::Native(message) = error else {
        return false;
    };
    let Some(body) = message.strip_prefix("TransactionCanceledException: ") else {
        return false;
    };
    let Some((_, reasons)) = body.rsplit_once('[') else {
        return false;
    };
    let Some(reasons) = reasons.strip_suffix(']') else {
        return false;
    };
    let reasons: Vec<_> = reasons.split(',').map(str::trim).collect();
    reasons.len() == 2
        && reasons.contains(&"TransactionConflict")
        && reasons
            .iter()
            .all(|r| matches!(*r, "TransactionConflict" | "None"))
}

#[test]
fn only_native_two_item_read_conflicts_are_recoverable() {
    for (message, accepted) in [
        (
            "TransactionCanceledException: Transaction cancelled [TransactionConflict, None]",
            true,
        ),
        (
            "TransactionCanceledException: Transaction cancelled [None, TransactionConflict]",
            true,
        ),
        (
            "TransactionCanceledException: Transaction cancelled [TransactionConflict, ValidationError]",
            false,
        ),
        (
            "TransactionCanceledException: Transaction cancelled [None, None]",
            false,
        ),
        ("AccessDeniedException: TransactionConflict", false),
        (
            "TransactionCanceledException: Transaction cancelled [TransactionConflict]",
            false,
        ),
        (
            "TransactionCanceledException: Transaction cancelled [TransactionConflict, None] malformed",
            false,
        ),
    ] {
        assert_eq!(
            native_read_conflict_only(&JournalError::Native(message.into())),
            accepted
        );
    }
}

pub async fn exercise(f: Fixture) {
    // Separate scenario poll stacks. Do not nest the reference/admission suites
    // inside the journal suite's large debug-build poll frame.
    Box::pin(exercise_journal(&f)).await;
    Box::pin(exercise_references(&f)).await;
    Box::pin(exercise_upload_admission(&f)).await;
    Box::pin(staging::exercise(&f)).await;
    Box::pin(maintenance::exercise(&f)).await;
}

async fn exercise_journal(f: &Fixture) {
    let domain = DomainId::new(format!("fixture-{}", Uuid::new_v4())).unwrap();
    let journal = f.journal(&domain);
    assert!(matches!(
        journal.command_context().await,
        Err(JournalError::MissingControl)
    ));
    let incarnation = journal.initialize_fresh_domain().await.unwrap();
    assert!(journal.initialize_fresh_domain().await.is_err());
    let context = journal.command_context().await.unwrap();
    assert_eq!(context.id().scope().incarnation(), incarnation);
    assert_eq!(
        ResourceId::from_command(context.id()).uuid(),
        context.id().uuid()
    );
    let id = context.id().clone();
    let command = journal
        .prepare_metadata_only(
            context,
            actions(vec![put(
                &f.app,
                "atomic",
                Some("attribute_not_exists(pk)"),
            )]),
            vec![0, 255, 1],
        )
        .unwrap();
    assert_eq!(command.token().id(), &id);
    assert_eq!(command.action_count(), 3);
    let token: CommandToken =
        serde_json::from_slice(&serde_json::to_vec(command.token()).unwrap()).unwrap();
    assert_eq!(&token, command.token());
    assert_eq!(journal.resolve(&token).await.unwrap(), Outcome::Unknown);
    assert!(f.get(&f.coord, receipt(&token)).await.is_none());
    assert_eq!(
        journal.execute(&command).await.unwrap().outcome,
        Outcome::Committed(vec![0, 255, 1])
    );
    assert!(f.get(&f.app, key("atomic")).await.is_some());
    let stored = f.get(&f.coord, receipt(&token)).await.unwrap();
    assert_eq!(
        stored.get("result"),
        Some(&A::B(Blob::new(vec![0, 255, 1])))
    );
    assert_ne!(receipt(&token).get("pk"), control(&domain).get("pk"));
    let exact_retry = journal.execute(&command.clone()).await.unwrap();
    assert_eq!(exact_retry.outcome, Outcome::Committed(vec![0, 255, 1]));
    assert!(exact_retry.attempt_failure.is_none());
    // Historical result is independent of current application state. Drop the
    // original response, change app metadata, then resolve through a new journal.
    f.delete(&f.app, key("atomic")).await;
    assert_eq!(
        f.journal(&domain).resolve(&token).await.unwrap(),
        Outcome::Committed(vec![0, 255, 1])
    );
    assert_eq!(
        journal.cancel(&token).await.unwrap().outcome,
        Outcome::Committed(vec![0, 255, 1])
    );

    // App conditional rejection leaves no effects OR receipt. The diagnostic is
    // not allowed to turn absent/open into a historical NotCommitted assertion.
    f.put(&f.app, key("occupied")).await;
    let failed = journal
        .prepare_metadata_only(
            journal.command_context().await.unwrap(),
            actions(vec![
                put(&f.app, "untouched", None),
                put(&f.app, "occupied", Some("attribute_not_exists(pk)")),
            ]),
            vec![],
        )
        .unwrap();
    let report = journal.execute(&failed).await.unwrap();
    assert_eq!(report.outcome, Outcome::Unknown);
    if let Some(AttemptFailure::TransactionCancelled { reasons }) = report.attempt_failure {
        assert!(
            reasons
                .iter()
                .any(|r| r.origin == ActionOrigin::Application { index: 1 }
                    && r.code == "ConditionalCheckFailed")
        );
    } else {
        panic!("expected native conditional cancellation");
    }
    assert!(f.get(&f.app, key("untouched")).await.is_none());
    assert!(f.get(&f.coord, receipt(failed.token())).await.is_none());
    assert_eq!(
        journal.cancel(failed.token()).await.unwrap().outcome,
        Outcome::Cancelled
    );
    assert_eq!(
        journal.execute(&failed).await.unwrap().outcome,
        Outcome::Cancelled
    );

    // Cancel first with otherwise satisfiable effects: no unrelated false
    // application condition may mask a missing receipt/admission fence.
    let cancelled_first = f.prepare(&journal, "cancel-first", b"never-publish").await;
    assert!(f.get(&f.app, key("cancel-first")).await.is_none());
    assert_eq!(
        journal
            .cancel(cancelled_first.token())
            .await
            .unwrap()
            .outcome,
        Outcome::Cancelled
    );
    assert_eq!(
        journal.execute(&cancelled_first).await.unwrap().outcome,
        Outcome::Cancelled
    );
    assert!(f.get(&f.app, key("cancel-first")).await.is_none());

    // Native duplicate-item rejection, never a homemade expression evaluator.
    let duplicate = journal
        .prepare_metadata_only(
            journal.command_context().await.unwrap(),
            actions(vec![
                put(&f.app, "duplicate", None),
                put(&f.app, "duplicate", None),
            ]),
            vec![],
        )
        .unwrap();
    let rejected = journal.execute(&duplicate).await.unwrap();
    assert_eq!(rejected.outcome, Outcome::Unknown);
    assert_native_validation(rejected.attempt_failure.as_ref());
    assert!(f.get(&f.app, key("duplicate")).await.is_none());
    assert!(f.get(&f.coord, receipt(duplicate.token())).await.is_none());
    for (name, condition, result) in [
        ("bad-expression", Some("THIS IS NOT AN EXPRESSION"), vec![]),
        ("oversized-result", None, vec![0; 401 * 1024]),
    ] {
        let invalid = journal
            .prepare_metadata_only(
                journal.command_context().await.unwrap(),
                actions(vec![put(&f.app, name, condition)]),
                result,
            )
            .unwrap();
        let report = journal.execute(&invalid).await.unwrap();
        assert_eq!(report.outcome, Outcome::Unknown);
        assert_native_validation(report.attempt_failure.as_ref());
        assert!(f.get(&f.app, key(name)).await.is_none());
        assert!(f.get(&f.coord, receipt(invalid.token())).await.is_none());
    }

    // Barrier releases competing native transactions together. Service conflicts
    // may leave a losing attempt Unknown; bounded explicit resolution follows.
    for index in 0..8 {
        let name = format!("race-{index}");
        let command = f.prepare(&journal, &name, b"race-result").await;
        let barrier = Arc::new(Barrier::new(3));
        let j = journal.clone();
        let p = command.clone();
        let b = barrier.clone();
        let execute = tokio::spawn(async move {
            b.wait().await;
            j.execute(&p).await
        });
        let j = journal.clone();
        let t = command.token().clone();
        let b = barrier.clone();
        let cancel = tokio::spawn(async move {
            b.wait().await;
            j.cancel(&t).await
        });
        barrier.wait().await;
        // Settle both competitors before any assertion/unwrap can unwind into
        // fixture cleanup; neither JoinHandle may be detached on a peer failure.
        let (e, c) = tokio::join!(execute, cancel);
        let e = e.expect("execute task panicked");
        let c = c.expect("cancel task panicked");
        // The coherent recovery read can itself conflict with the other native
        // transaction. That is a failed observation, never a rollback result.
        let transient_conflict = native_read_conflict_only;
        for report in [&e, &c] {
            if let Err(error) = report {
                assert!(
                    transient_conflict(error),
                    "unexpected native error: {error}"
                );
            }
        }
        let mut final_outcome = Outcome::Unknown;
        for attempt in 0..3 {
            match journal.resolve(command.token()).await {
                Ok(outcome) => final_outcome = outcome,
                Err(error) => assert!(transient_conflict(&error), "{error}"),
            }
            if matches!(final_outcome, Outcome::Committed(_) | Outcome::Cancelled) {
                break;
            }
            // Exact frozen replay only; no new command, regenerated deadline or
            // inference from an absent receipt. This runs inside the bounded
            // background acceptance test, not a foreground polling loop.
            match journal.execute(&command).await {
                Ok(report) => final_outcome = report.outcome,
                Err(error) => assert!(transient_conflict(&error), "{error}"),
            }
            if matches!(final_outcome, Outcome::Committed(_) | Outcome::Cancelled) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt + 1))).await;
        }
        assert!(matches!(
            final_outcome,
            Outcome::Committed(_) | Outcome::Cancelled
        ));
        for report in [e, c].into_iter().flatten() {
            assert!(report.outcome == final_outcome || report.outcome == Outcome::Unknown);
        }
        assert_eq!(
            f.get(&f.app, key(name)).await.is_some(),
            matches!(final_outcome, Outcome::Committed(_))
        );
    }

    // Both canonical coordination ARN and bare name are reserved; unrelated
    // tables fail closed before an executable plan is built.
    for table in [&f.coord, f.tables.coordination_table_arn(), "not-enrolled"] {
        assert!(matches!(
            journal.prepare_metadata_only(
                journal.command_context().await.unwrap(),
                actions(vec![put(table, "forbidden", None)]),
                vec![]
            ),
            Err(JournalError::Enrollment(_))
        ));
    }
    let app_arn = f
        .client
        .describe_table()
        .table_name(&f.app)
        .send()
        .await
        .unwrap()
        .table
        .unwrap()
        .table_arn
        .unwrap();
    let arn_plan = journal
        .prepare_metadata_only(
            journal.command_context().await.unwrap(),
            actions(vec![put(&app_arn, "arn", None)]),
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&arn_plan).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    let hundred = journal
        .prepare_metadata_only(
            journal.command_context().await.unwrap(),
            actions(
                (0..98)
                    .map(|i| put(&f.app, &format!("budget-{i}"), None))
                    .collect(),
            ),
            vec![],
        )
        .unwrap();
    assert_eq!(hundred.action_count(), 100);
    assert_eq!(
        journal.execute(&hundred).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    assert!(matches!(
        journal.prepare_metadata_only(
            journal.command_context().await.unwrap(),
            actions(
                (0..99)
                    .map(|i| put(&f.app, &format!("over-{i}"), None))
                    .collect()
            ),
            vec![]
        ),
        Err(JournalError::Budget { count: 101 })
    ));

    // Token mutations cannot create executable plans; invalid resolver inputs
    // still fail closed, including wrong domain, future generation and digest.
    let mut wire = serde_json::to_value(&token).unwrap();
    wire["digest"][0] = serde_json::json!(token.digest()[0] ^ 1);
    let wrong: CommandToken = serde_json::from_value(wire).unwrap();
    assert!(matches!(
        journal.resolve(&wrong).await,
        Err(JournalError::DigestMismatch)
    ));
    let mut wire = serde_json::to_value(&token).unwrap();
    wire["id"]["scope"]["generation"] = serde_json::json!(2);
    assert!(matches!(
        journal
            .resolve(&serde_json::from_value(wire).unwrap())
            .await,
        Err(JournalError::FutureGeneration)
    ));
    let mut wire = serde_json::to_value(&token).unwrap();
    wire["id"]["scope"]["domain"] = serde_json::json!("wrong-domain");
    assert!(matches!(
        journal
            .resolve(&serde_json::from_value(wire).unwrap())
            .await,
        Err(JournalError::WrongDomain)
    ));

    let absent = f.prepare(&journal, "never-written", b"not-committed").await;
    let original_control = f.get(&f.coord, control(&domain)).await.unwrap();
    let mut fence = original_control.clone();
    fence.insert("current_generation".into(), A::N("2".into()));
    fence.insert("closed_through".into(), A::N("1".into()));
    fence.insert("revision".into(), A::N("2".into()));
    f.put(&f.coord, fence.clone()).await;
    assert_eq!(
        journal.resolve(absent.token()).await.unwrap(),
        Outcome::NotCommitted
    );
    let fenced = journal.execute(&absent).await.unwrap();
    assert_eq!(fenced.outcome, Outcome::NotCommitted);
    if let Some(AttemptFailure::TransactionCancelled { reasons }) = fenced.attempt_failure {
        assert!(
            reasons
                .iter()
                .any(|r| r.origin == ActionOrigin::Admission && r.code == "ConditionalCheckFailed")
        );
    } else {
        panic!("expected admission cancellation");
    }
    fence.insert("compacted_through".into(), A::N("1".into()));
    fence.insert("revision".into(), A::N("3".into()));
    f.put(&f.coord, fence.clone()).await;
    assert_eq!(
        journal.resolve(absent.token()).await.unwrap(),
        Outcome::Expired
    );
    assert_eq!(
        journal.resolve(&token).await.unwrap(),
        Outcome::Committed(vec![0, 255, 1])
    );
    f.delete(&f.coord, receipt(&token)).await;
    assert_eq!(journal.resolve(&token).await.unwrap(), Outcome::Expired);
    assert_eq!(
        journal.execute(&absent).await.unwrap().outcome,
        Outcome::Expired
    );
    assert!(f.get(&f.app, key("never-written")).await.is_none());
    fence.insert("incarnation".into(), A::S(Uuid::new_v4().to_string()));
    f.put(&f.coord, fence.clone()).await;
    assert_eq!(
        journal.resolve(absent.token()).await.unwrap(),
        Outcome::Expired
    );

    // Old-incarnation absence must expire even with OPEN/reset watermarks;
    // compaction must not accidentally provide the only rejection condition.
    let rotation_domain = DomainId::new(format!("rotation-{}", Uuid::new_v4())).unwrap();
    let rotation = f.journal(&rotation_domain);
    rotation.initialize_fresh_domain().await.unwrap();
    let present = f.prepare(&rotation, "rotation-present", b"original").await;
    rotation.execute(&present).await.unwrap();
    let missing = f
        .prepare(&rotation, "rotation-missing", b"not-executed")
        .await;
    let mut control_row = f.get(&f.coord, control(&rotation_domain)).await.unwrap();
    control_row.insert("incarnation".into(), A::S(Uuid::new_v4().to_string()));
    control_row.insert("current_generation".into(), A::N("1".into()));
    control_row.insert("closed_through".into(), A::N("0".into()));
    control_row.insert("compacted_through".into(), A::N("0".into()));
    f.put(&f.coord, control_row).await;
    assert_eq!(
        rotation.resolve(missing.token()).await.unwrap(),
        Outcome::Expired
    );
    assert_eq!(
        rotation.execute(&missing).await.unwrap().outcome,
        Outcome::Expired
    );
    assert!(f.get(&f.app, key("rotation-missing")).await.is_none());
    assert_eq!(
        rotation.resolve(present.token()).await.unwrap(),
        Outcome::Committed(b"original".to_vec())
    );

    // Deliberate corruption is confined to synthetic fixture records.
    for (field, value) in [
        ("domain", A::S("hash-collision-or-corruption".into())),
        ("compacted_through", A::N("99".into())),
        ("revision", A::N("0".into())),
        ("incarnation", A::S(Uuid::nil().to_string())),
    ] {
        let mut corrupt = fence.clone();
        corrupt.insert(field.into(), value);
        f.put(&f.coord, corrupt).await;
        assert!(matches!(
            journal.command_context().await,
            Err(JournalError::Corrupt(_))
        ));
        assert!(matches!(
            journal.resolve(absent.token()).await,
            Err(JournalError::Corrupt(_))
        ));
    }
    f.put(&f.coord, fence).await;
    let valid = f.prepare(&journal, "malformed-receipt", b"result").await;
    assert!(matches!(
        journal.execute(&valid).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let original = f.get(&f.coord, receipt(valid.token())).await.unwrap();
    for (field, value) in [
        ("command_uuid", A::S(Uuid::new_v4().to_string())),
        ("status", A::S("PENDING".into())),
        ("result", A::S("not-binary".into())),
        ("request_digest", A::B(Blob::new(vec![1]))),
    ] {
        let mut corrupt = original.clone();
        corrupt.insert(field.into(), value);
        f.put(&f.coord, corrupt).await;
        assert!(matches!(
            journal.resolve(valid.token()).await,
            Err(JournalError::Corrupt(_))
        ));
    }
}

// Synthetic fixtures ONLY. This documents the reserved v0 READY schema, not a
// production registration/adoption API. Future verified upload selection owns
// initial creation; explicit exclusive managed-scope enrollment is prerequisite.
fn version_key(domain: &DomainId, version: &VersionRef) -> Item {
    let address: String = version
        .fingerprint()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    key(format!(
        "D#{:x}#OBJ#{address}",
        Sha256::digest(domain.as_str().as_bytes())
    ))
}
fn ready_version(domain: &DomainId, version: &VersionRef) -> Item {
    let mut item = version_key(domain, version);
    for (name, value) in [
        ("record", "version"),
        ("store", version.store().as_str()),
        ("object_key", version.key()),
        ("version_id", version.version()),
        ("state", "READY"),
    ] {
        item.insert(name.into(), A::S(value.into()));
    }
    // Every synthetic initial version has ONE seeded library source edge.
    item.insert("references".into(), A::N("1".into()));
    item.insert("claims".into(), A::N("0".into()));
    item
}
// Fixture-only durable source/retention slot, never a production registration
// path. The snapshot MUST come back through the production strong native read.
async fn seed_source(
    f: &Fixture,
    journal: &CommandJournal,
    domain: &DomainId,
    version: &VersionRef,
) -> storelink::dynamodb::ReferenceSnapshot {
    let id = ReferenceId::new(format!("seed-{}", Uuid::new_v4())).unwrap();
    let encoded: String = id
        .as_str()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut row = key(format!(
        "D#{:x}#REF#{encoded}",
        Sha256::digest(domain.as_str().as_bytes())
    ));
    for (name, value) in [
        ("record", "reference"),
        ("reference_id", id.as_str()),
        ("store", version.store().as_str()),
        ("object_key", version.key()),
        ("version_id", version.version()),
    ] {
        row.insert(name.into(), A::S(value.into()));
    }
    row.insert("revision".into(), A::S(Uuid::new_v4().to_string()));
    f.put(&f.coord, row).await;
    journal.read_reference(&id).await.unwrap().unwrap()
}

// An arbitrary APPLICATION schema, with a non-reused owner key and native
// revision guard. These pointer writes participate in the very same transaction.
fn pointer_put(
    table: &str,
    owner: &str,
    version: &VersionRef,
    expected: Option<Uuid>,
    revision: Uuid,
) -> TransactWriteItem {
    let mut item = key(owner);
    for (name, value) in [
        ("blob_store", version.store().as_str()),
        ("blob_key", version.key()),
        ("blob_version", version.version()),
    ] {
        item.insert(name.into(), A::S(value.into()));
    }
    item.insert("app_revision".into(), A::S(revision.to_string()));
    let mut put = Put::builder().table_name(table).set_item(Some(item));
    put = if let Some(expected) = expected {
        put.condition_expression("app_revision = :old")
            .expression_attribute_values(":old", A::S(expected.to_string()))
    } else {
        put.condition_expression("attribute_not_exists(pk)")
    };
    TransactWriteItem::builder()
        .put(put.build().unwrap())
        .build()
}
fn pointer_delete(table: &str, owner: &str, revision: Uuid) -> TransactWriteItem {
    TransactWriteItem::builder()
        .delete(
            Delete::builder()
                .table_name(table)
                .set_key(Some(key(owner)))
                .condition_expression("app_revision = :old")
                .expression_attribute_values(":old", A::S(revision.to_string()))
                .build()
                .unwrap(),
        )
        .build()
}
async fn assert_reference_count(f: &Fixture, domain: &DomainId, version: &VersionRef, count: u64) {
    let row = f.get(&f.coord, version_key(domain, version)).await.unwrap();
    assert_eq!(row.get("references"), Some(&A::N(count.to_string())));
    assert_eq!(row.get("claims"), Some(&A::N("0".into())));
}
fn assert_internal_rejection(report: &storelink::dynamodb::ExecutionReport) {
    assert_eq!(report.outcome, Outcome::Unknown);
    match &report.attempt_failure {
        Some(AttemptFailure::TransactionCancelled { reasons }) => assert!(
            reasons
                .iter()
                .any(|r| matches!(r.origin, ActionOrigin::Internal { .. })
                    && r.code == "ConditionalCheckFailed")
        ),
        other => panic!("expected native library conditional rejection: {other:?}"),
    }
}

async fn exercise_references(f: &Fixture) {
    let domain = DomainId::new(format!("references-{}", Uuid::new_v4())).unwrap();
    let journal = f.journal(&domain);
    journal.initialize_fresh_domain().await.unwrap();
    let version = |v: &str| {
        VersionRef::new(
            StoreId::new("synthetic-store").unwrap(),
            "synthetic/exact-key",
            v,
        )
        .unwrap()
    };
    let v1 = version("v1");
    let v2 = version("v2");
    for v in [&v1, &v2] {
        f.put(&f.coord, ready_version(&domain, v)).await;
    }
    let source_v1 = seed_source(f, &journal, &domain, &v1).await;
    let source_v2 = seed_source(f, &journal, &domain, &v2).await;
    let slot = |name: &str| ReferenceId::new(format!("{}-{name}", domain.as_str())).unwrap();
    let a = slot("a");
    let b = slot("b");
    let c = slot("c");
    assert!(journal.read_reference(&a).await.unwrap().is_none());
    let ar = Uuid::new_v4();
    let br = Uuid::new_v4();
    let attach = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![
                pointer_put(&f.app, a.as_str(), &v1, None, ar),
                pointer_put(&f.app, b.as_str(), &v1, None, br),
            ]),
            vec![
                ReferenceChange::new(a.clone(), None, Some(v1.clone()), Some(ar)).unwrap(),
                ReferenceChange::new(b.clone(), None, Some(v1.clone()), Some(br)).unwrap(),
            ],
            vec![source_v1.clone(), source_v1.clone()], // exact duplicates merge
            b"two".to_vec(),
        )
        .unwrap();
    assert_eq!(attach.action_count(), 8); // admission + 2 app + 2 slot + source + ONE object + receipt
    assert_eq!(
        journal.execute(&attach).await.unwrap().outcome,
        Outcome::Committed(b"two".to_vec())
    );
    assert_eq!(
        journal.execute(&attach).await.unwrap().outcome,
        Outcome::Committed(b"two".to_vec())
    );
    assert_reference_count(f, &domain, &v1, 3).await; // seed + A + B
    let old_a = journal.read_reference(&a).await.unwrap().unwrap();
    let old_b = journal.read_reference(&b).await.unwrap().unwrap();
    assert_eq!(old_a.version(), &v1);
    assert_eq!(old_a.revision(), ar);

    // Move B to a fresh owner C while replacing A's version: v1 net -1, v2 +1.
    let ar2 = Uuid::new_v4();
    let cr = Uuid::new_v4();
    let move_replace = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![
                pointer_put(&f.app, a.as_str(), &v2, Some(ar), ar2),
                pointer_delete(&f.app, b.as_str(), br),
                pointer_put(&f.app, c.as_str(), &v1, None, cr),
            ]),
            vec![
                ReferenceChange::new(a.clone(), Some(old_a), Some(v2.clone()), Some(ar2)).unwrap(),
                ReferenceChange::new(b.clone(), Some(old_b.clone()), None, None).unwrap(),
                ReferenceChange::new(c.clone(), None, Some(v1.clone()), Some(cr)).unwrap(),
            ],
            vec![source_v2.clone(), old_b], // B's guarded Delete also checks the source
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&move_replace).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    assert_reference_count(f, &domain, &v1, 2).await; // seed + C
    assert_reference_count(f, &domain, &v2, 2).await; // seed + A
    assert!(journal.read_reference(&b).await.unwrap().is_none());
    assert!(f.get(&f.app, key(b.as_str())).await.is_none());
    assert_eq!(
        journal.read_reference(&c).await.unwrap().unwrap().version(),
        &v1
    );
    let stale_a = journal.read_reference(&a).await.unwrap().unwrap();

    // Same-version replacement still guards READY but must not increment counts.
    let ar3 = Uuid::new_v4();
    let same = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![pointer_put(&f.app, a.as_str(), &v2, Some(ar2), ar3)]),
            vec![
                ReferenceChange::new(
                    a.clone(),
                    Some(stale_a.clone()),
                    Some(v2.clone()),
                    Some(ar3),
                )
                .unwrap(),
            ],
            vec![stale_a.clone()], // same-row source merges into guarded Put
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&same).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    assert_reference_count(f, &domain, &v2, 2).await; // seed + A, unchanged
    let current_a = journal.read_reference(&a).await.unwrap().unwrap();
    let prior_app = f.get(&f.app, key(a.as_str())).await.unwrap();
    let ar4 = Uuid::new_v4();
    let wrong_revision = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![pointer_put(&f.app, a.as_str(), &v1, Some(ar3), ar4)]),
            vec![
                ReferenceChange::new(a.clone(), Some(stale_a), Some(v1.clone()), Some(ar4))
                    .unwrap(),
            ],
            vec![source_v1.clone()],
            vec![],
        )
        .unwrap();
    assert_internal_rejection(&journal.execute(&wrong_revision).await.unwrap());
    assert_eq!(f.get(&f.app, key(a.as_str())).await.unwrap(), prior_app);
    assert_eq!(
        journal.read_reference(&a).await.unwrap().unwrap(),
        current_a
    );
    assert_reference_count(f, &domain, &v1, 2).await;
    assert_reference_count(f, &domain, &v2, 2).await;
    assert!(
        f.get(&f.coord, receipt(wrong_revision.token()))
            .await
            .is_none()
    );
    assert_eq!(
        journal
            .cancel(wrong_revision.token())
            .await
            .unwrap()
            .outcome,
        Outcome::Cancelled
    );

    // A genuinely zero-net move still checks the shared READY row exactly once.
    let d = slot("d");
    let dr = Uuid::new_v4();
    let move_only = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![
                pointer_delete(&f.app, c.as_str(), cr),
                pointer_put(&f.app, d.as_str(), &v1, None, dr),
            ]),
            vec![
                ReferenceChange::new(
                    c.clone(),
                    journal.read_reference(&c).await.unwrap(),
                    None,
                    None,
                )
                .unwrap(),
                ReferenceChange::new(d.clone(), None, Some(v1.clone()), Some(dr)).unwrap(),
            ],
            vec![], // C's expected snapshot alone authorizes the move
            vec![],
        )
        .unwrap();
    assert_eq!(move_only.action_count(), 7);
    assert_eq!(
        journal.execute(&move_only).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    assert_reference_count(f, &domain, &v1, 2).await; // seed + D
    assert!(journal.read_reference(&c).await.unwrap().is_none());

    // Missing, RETIRING, malformed counters and a hash-address/full-tuple mismatch
    // all reject native attachment, atomically preserving absent app and slot.
    for case in [
        "missing",
        "retiring",
        "counter-type",
        "negative-claims",
        "missing-claims",
        "wrong-tuple",
    ] {
        let target = version(case);
        let mut row = ready_version(&domain, &target);
        match case {
            "retiring" => {
                row.insert("state".into(), A::S("RETIRING".into()));
            }
            "counter-type" => {
                row.insert("references".into(), A::S("0".into()));
            }
            "negative-claims" => {
                row.insert("claims".into(), A::N("-1".into()));
            }
            "missing-claims" => {
                row.remove("claims");
            }
            "wrong-tuple" => {
                row.insert("version_id".into(), A::S("different".into()));
            }
            _ => {}
        }
        if case != "missing" {
            f.put(&f.coord, row.clone()).await;
        }
        // Deliberately valid source syntax isolates the version-row guard even
        // for these administratively injected invalid/missing object states.
        let source = seed_source(f, &journal, &domain, &target).await;
        let id = slot(case);
        let revision = Uuid::new_v4();
        let plan = journal
            .prepare_reference_update(
                journal.command_context().await.unwrap(),
                actions(vec![pointer_put(
                    &f.app,
                    id.as_str(),
                    &target,
                    None,
                    revision,
                )]),
                vec![
                    ReferenceChange::new(id.clone(), None, Some(target.clone()), Some(revision))
                        .unwrap(),
                ],
                vec![source],
                vec![],
            )
            .unwrap();
        assert_internal_rejection(&journal.execute(&plan).await.unwrap());
        assert!(journal.read_reference(&id).await.unwrap().is_none());
        assert!(f.get(&f.app, key(id.as_str())).await.is_none());
        assert!(f.get(&f.coord, receipt(plan.token())).await.is_none());
        assert_eq!(
            f.get(&f.coord, version_key(&domain, &target)).await,
            (case != "missing").then_some(row)
        );
        assert_eq!(
            journal.cancel(plan.token()).await.unwrap().outcome,
            Outcome::Cancelled
        );
    }

    // A deterministic cancellation wins before otherwise-valid reference effects.
    let cancelled_id = slot("cancelled");
    let revision = Uuid::new_v4();
    let cancelled = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![pointer_put(
                &f.app,
                cancelled_id.as_str(),
                &v1,
                None,
                revision,
            )]),
            vec![
                ReferenceChange::new(cancelled_id.clone(), None, Some(v1.clone()), Some(revision))
                    .unwrap(),
            ],
            vec![source_v1.clone()],
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.cancel(cancelled.token()).await.unwrap().outcome,
        Outcome::Cancelled
    );
    assert_eq!(
        journal.execute(&cancelled).await.unwrap().outcome,
        Outcome::Cancelled
    );
    assert!(
        journal
            .read_reference(&cancelled_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(f.get(&f.app, key(cancelled_id.as_str())).await.is_none());
    assert_reference_count(f, &domain, &v1, 2).await; // seed + D

    // Full combined budget, not a fragment budget or implicit split.
    let budget_id = slot("budget");
    let over = journal.prepare_reference_update(
        journal.command_context().await.unwrap(),
        actions(
            (0..97)
                .map(|i| put(&f.app, &format!("reference-budget-{i}"), None))
                .collect(),
        ),
        vec![
            ReferenceChange::new(budget_id, None, Some(v1.clone()), Some(Uuid::new_v4())).unwrap(),
        ],
        vec![source_v1.clone()],
        vec![],
    );
    assert!(matches!(
        over,
        Err(ReferenceError::Journal(JournalError::Budget { count: 102 }))
    ));

    exercise_source_guards(f, &journal, &domain, &source_v1, &source_v2).await;

    // Race a copy against final-source detach, starting from ONE real edge and
    // references=1. Only afterwards may zero-count retirement win. This tests
    // native serialization, NOT a production GC implementation.
    let target = version("retirement-race");
    f.put(&f.coord, ready_version(&domain, &target)).await;
    let race_source = seed_source(f, &journal, &domain, &target).await;
    let race_id = slot("race");
    let revision = Uuid::new_v4();
    let race_plan = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![pointer_put(
                &f.app,
                race_id.as_str(),
                &target,
                None,
                revision,
            )]),
            vec![
                ReferenceChange::new(race_id.clone(), None, Some(target.clone()), Some(revision))
                    .unwrap(),
            ],
            vec![race_source.clone()],
            vec![],
        )
        .unwrap();
    let detach = journal
        .prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(vec![]),
            vec![
                ReferenceChange::new(
                    race_source.id().clone(),
                    Some(race_source.clone()),
                    None,
                    None,
                )
                .unwrap(),
            ],
            vec![],
            vec![],
        )
        .unwrap();
    // Settle both requests before assertions; no detached writers. Exact retry
    // is allowed for conflicts; Unknown must be terminalized before any replan.
    let (copied, detached) = tokio::join!(journal.execute(&race_plan), journal.execute(&detach));
    let detached = detached.unwrap();
    let detached_outcome = if detached.outcome == Outcome::Unknown {
        journal.execute(&detach).await.unwrap().outcome
    } else {
        detached.outcome
    };
    assert!(matches!(detached_outcome, Outcome::Committed(_)));
    assert!(
        journal
            .read_reference(race_source.id())
            .await
            .unwrap()
            .is_none()
    );
    let copied = copied.unwrap();
    let outcome = if copied.outcome == Outcome::Unknown {
        journal.cancel(race_plan.token()).await.unwrap().outcome
    } else {
        copied.outcome
    };
    let retirement = f.client.update_item().table_name(&f.coord).set_key(Some(version_key(&domain, &target)))
        .update_expression("SET #state = :retiring")
        .condition_expression("#record = :record AND #store = :store AND #key = :key AND #version = :version AND #state = :ready AND #references = :zero AND #claims = :zero")
        .expression_attribute_names("#record", "record").expression_attribute_names("#store", "store")
        .expression_attribute_names("#key", "object_key").expression_attribute_names("#version", "version_id")
        .expression_attribute_names("#state", "state").expression_attribute_names("#references", "references")
        .expression_attribute_names("#claims", "claims")
        .expression_attribute_values(":record", A::S("version".into()))
        .expression_attribute_values(":store", A::S(target.store().as_str().into()))
        .expression_attribute_values(":key", A::S(target.key().into()))
        .expression_attribute_values(":version", A::S(target.version().into()))
        .expression_attribute_values(":ready", A::S("READY".into()))
        .expression_attribute_values(":retiring", A::S("RETIRING".into()))
        .expression_attribute_values(":zero", A::N("0".into()));
    let retired = retirement.send().await;
    let row = f
        .get(&f.coord, version_key(&domain, &target))
        .await
        .unwrap();
    match outcome {
        Outcome::Committed(_) => {
            assert!(retired.is_err());
            assert_eq!(row.get("state"), Some(&A::S("READY".into())));
            assert_reference_count(f, &domain, &target, 1).await;
            assert_eq!(
                journal
                    .read_reference(&race_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .version(),
                &target
            );
            assert!(f.get(&f.app, key(race_id.as_str())).await.is_some());
        }
        Outcome::Cancelled => {
            retired.expect(
                "after final detach and cancelled copy, zero-count retirement must succeed",
            );
            assert_eq!(row.get("state"), Some(&A::S("RETIRING".into())));
            assert_reference_count(f, &domain, &target, 0).await;
            assert!(journal.read_reference(&race_id).await.unwrap().is_none());
            assert!(f.get(&f.app, key(race_id.as_str())).await.is_none());
            assert_eq!(
                journal.execute(&race_plan).await.unwrap().outcome,
                Outcome::Cancelled
            );
        }
        other => panic!("unexpected race outcome: {other:?}"),
    }
}

async fn exercise_source_guards(
    f: &Fixture,
    journal: &CommandJournal,
    domain: &DomainId,
    source_v1: &storelink::dynamodb::ReferenceSnapshot,
    source_v2: &storelink::dynamodb::ReferenceSnapshot,
) {
    let id = ReferenceId::new(format!("missing-authority-{}", Uuid::new_v4())).unwrap();
    let revision = Uuid::new_v4();
    let missing = journal.prepare_reference_update(
        journal.command_context().await.unwrap(),
        actions(vec![pointer_put(
            &f.app,
            id.as_str(),
            source_v1.version(),
            None,
            revision,
        )]),
        vec![
            ReferenceChange::new(
                id.clone(),
                None,
                Some(source_v1.version().clone()),
                Some(revision),
            )
            .unwrap(),
        ],
        vec![],
        vec![],
    );
    assert!(matches!(missing, Err(ReferenceError::MissingSource)));
    assert!(journal.read_reference(&id).await.unwrap().is_none());
    assert!(f.get(&f.app, key(id.as_str())).await.is_none());
    assert_reference_count(f, domain, source_v1.version(), 2).await;

    // 95 app + slot + object + one unique source + admission/receipt = 100.
    // Even 97 duplicate input sources fit. An UNUSED distinct source must count.
    for (sources, expected_count) in [
        (vec![source_v1.clone(); 97], 100),
        (vec![source_v1.clone(), source_v2.clone()], 101),
    ] {
        let plan = journal.prepare_reference_update(
            journal.command_context().await.unwrap(),
            actions(
                (0..95)
                    .map(|i| put(&f.app, &format!("source-budget-{i}"), None))
                    .collect(),
            ),
            vec![
                ReferenceChange::new(
                    id.clone(),
                    None,
                    Some(source_v1.version().clone()),
                    Some(revision),
                )
                .unwrap(),
            ],
            sources,
            vec![],
        );
        if expected_count == 100 {
            assert_eq!(plan.unwrap().action_count(), 100);
        } else {
            assert!(matches!(
                plan,
                Err(ReferenceError::Journal(JournalError::Budget { count: 101 }))
            ));
        }
    }

    // Mutating or deleting a source AFTER preparation must reject atomically.
    // Repeat with an UNUSED explicit source to prove intended guards aren't lost.
    for (delete, unused) in [(false, false), (true, false), (false, true), (true, true)] {
        let version = VersionRef::new(
            StoreId::new("synthetic-store").unwrap(),
            "source-guard",
            Uuid::new_v4().to_string(),
        )
        .unwrap();
        f.put(&f.coord, ready_version(domain, &version)).await;
        let source = seed_source(f, journal, domain, &version).await;
        let target = ReferenceId::new(format!("source-target-{}", Uuid::new_v4())).unwrap();
        let desired = if unused {
            source_v1.version()
        } else {
            &version
        };
        let revision = Uuid::new_v4();
        let mut sources = vec![source.clone()];
        if unused {
            sources.push(source_v1.clone());
        }
        let plan = journal
            .prepare_reference_update(
                journal.command_context().await.unwrap(),
                actions(vec![pointer_put(
                    &f.app,
                    target.as_str(),
                    desired,
                    None,
                    revision,
                )]),
                vec![
                    ReferenceChange::new(
                        target.clone(),
                        None,
                        Some(desired.clone()),
                        Some(revision),
                    )
                    .unwrap(),
                ],
                sources,
                vec![],
            )
            .unwrap();
        let mutation = journal
            .prepare_reference_update(
                journal.command_context().await.unwrap(),
                actions(vec![]), // synthetic fixture-owned retention source has no app pointer
                vec![
                    ReferenceChange::new(
                        source.id().clone(),
                        Some(source.clone()),
                        (!delete).then_some(version.clone()),
                        (!delete).then(Uuid::new_v4),
                    )
                    .unwrap(),
                ],
                vec![source.clone()], // source merges with either Delete or Put
                vec![],
            )
            .unwrap();
        assert_eq!(mutation.action_count(), 4); // admission + slot + object + receipt
        assert!(matches!(
            journal.execute(&mutation).await.unwrap().outcome,
            Outcome::Committed(_)
        ));
        assert_internal_rejection(&journal.execute(&plan).await.unwrap());
        assert!(journal.read_reference(&target).await.unwrap().is_none());
        assert!(f.get(&f.app, key(target.as_str())).await.is_none());
        assert!(f.get(&f.coord, receipt(plan.token())).await.is_none());
        assert_reference_count(f, domain, &version, if delete { 0 } else { 1 }).await;
        assert_reference_count(f, domain, source_v1.version(), 2).await;
        assert_eq!(
            journal.cancel(plan.token()).await.unwrap().outcome,
            Outcome::Cancelled
        );

        if !delete {
            let current = journal.read_reference(source.id()).await.unwrap().unwrap();
            assert_ne!(current, source);
            for changes in [
                vec![],
                vec![
                    ReferenceChange::new(current.id().clone(), Some(current.clone()), None, None)
                        .unwrap(),
                ],
            ] {
                let contradictory = journal.prepare_reference_update(
                    journal.command_context().await.unwrap(),
                    actions(vec![]),
                    changes,
                    vec![current.clone(), source.clone()],
                    vec![],
                );
                assert!(matches!(
                    contradictory,
                    Err(ReferenceError::ContradictorySource)
                ));
            }
        }
    }
}

fn upload_resource_key(kind: &str, id: &ResourceId) -> Item {
    key(format!(
        "D#{:x}#{kind}#{}#{}#{}",
        Sha256::digest(id.scope().domain().as_str().as_bytes()),
        id.scope().incarnation(),
        id.scope().generation(),
        id.uuid()
    ))
}

// Native regression: one-minute renewable claims with a two-hour MPU bound.
// These are explicit fixture values, not a recommendation or lifecycle proof.
async fn exercise_upload_mpu_age(
    f: &Fixture,
    journal: &CommandJournal,
    namespace: &storelink::dynamodb::NamespaceSnapshot,
) {
    use storelink::claim::{ClaimPolicy, Renewal};
    use storelink::clock::{ClockObservation, TimestampMs};
    use storelink::dynamodb::{UploadConstraints, UploadError};

    let ts = |n| TimestampMs::new(n).unwrap();
    let clock = |n| ClockObservation::trusted(ts(n), 5).unwrap();
    let policy = ClaimPolicy::new(60_000, 50, 10).unwrap();
    let context = journal.command_context().await.unwrap();
    let intent_id = ResourceId::from_command(context.id());
    let begin = journal
        .prepare_begin_intent(
            context,
            namespace,
            "fresh-fixture/long-mpu",
            UploadConstraints::new(100, None, None).unwrap(),
            ts(60_000),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    let mut intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
    let admitted = clock(1000).lower_bound().unwrap();
    let ceiling = admitted.checked_add(namespace.mpu_max_age_ms()).unwrap();
    assert!(matches!(
        journal.prepare_begin_attempt(
            journal.command_context().await.unwrap(),
            &intent,
            namespace,
            ts(60_000),
            ceiling.checked_add(1).unwrap(),
            clock(1000),
            &policy,
            vec![],
        ),
        Err(UploadError::MpuDeadline)
    ));
    let context = journal.command_context().await.unwrap();
    let attempt_id = ResourceId::from_command(context.id());
    let begin = journal
        .prepare_begin_attempt(
            context,
            &intent,
            namespace,
            ts(60_000),
            ceiling,
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    let mut attempt = journal.read_attempt(&attempt_id).await.unwrap().unwrap();
    for now in [40_000, 80_000, 120_000] {
        let next = ts(now + 60_000);
        let renewal = Renewal::new(intent.revision(), next, Uuid::new_v4()).unwrap();
        let plan = journal
            .prepare_renew_intent(
                journal.command_context().await.unwrap(),
                &intent,
                namespace,
                &renewal,
                clock(now),
                &policy,
                vec![],
            )
            .unwrap();
        assert_eq!(
            journal.execute(&plan).await.unwrap().outcome,
            Outcome::Committed(vec![])
        );
        intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
        let renewal = Renewal::new(attempt.revision(), next, Uuid::new_v4()).unwrap();
        let plan = journal
            .prepare_renew_attempt(
                journal.command_context().await.unwrap(),
                &attempt,
                &intent,
                namespace,
                &renewal,
                clock(now),
                &policy,
                vec![],
            )
            .unwrap();
        assert_eq!(
            journal.execute(&plan).await.unwrap().outcome,
            Outcome::Committed(vec![])
        );
        assert_eq!(
            journal.execute(&plan).await.unwrap().outcome,
            Outcome::Committed(vec![])
        );
        attempt = journal.read_attempt(&attempt_id).await.unwrap().unwrap();
        assert_eq!(attempt.deadline(), next);
        assert_eq!(attempt.mpu_admitted_lower_bound(), admitted);
        assert_eq!(attempt.max_mpu_use_until(), ceiling);
        assert!(
            journal
                .authorize_transfer(&attempt_id, || Ok(clock(now)), &policy)
                .await
                .is_ok()
        );
    }
    assert!(
        journal
            .authorize_transfer(&attempt_id, || Ok(clock(150_000)), &policy)
            .await
            .is_ok()
    );
    // The freshly sampled time, not the time the future was built, is decisive.
    let now = std::sync::atomic::AtomicU64::new(150_000);
    let authorization = journal.authorize_transfer(
        &attempt_id,
        || Ok(clock(now.load(std::sync::atomic::Ordering::SeqCst))),
        &policy,
    );
    now.store(180_000, std::sync::atomic::Ordering::SeqCst);
    assert!(authorization.await.is_err());
    let row = f
        .get(&f.coord, upload_resource_key("ATTEMPT", &attempt_id))
        .await
        .unwrap();
    assert_eq!(
        row.get("mpu_admitted_lower_bound"),
        Some(&A::N("995".into()))
    );
    assert_eq!(
        row.get("max_mpu_use_until"),
        Some(&A::N(ceiling.as_millis().to_string()))
    );
}

/// Exercises only native DynamoDB actions. Explicit test seeding does NOT
/// constitute qualified enrollment, assert any S3 profile verification, enroll
/// historical payloads, or issue S3 requests. Production enrollment is separate.
async fn exercise_upload_admission(f: &Fixture) {
    use storelink::claim::{ClaimPolicy, Renewal};
    use storelink::clock::{ClockObservation, TimestampMs};
    use storelink::dynamodb::{AttemptState, IntentState, UploadConstraints};
    use storelink::identity::NamespaceId;

    let ts = |n| TimestampMs::new(n).unwrap();
    let clock = |n| ClockObservation::trusted(ts(n), 5).unwrap();
    let policy = ClaimPolicy::new(2000, 50, 10).unwrap();
    let domain = DomainId::new(format!("upload-fixture-{}", Uuid::new_v4())).unwrap();
    let journal = f.journal(&domain);
    let incarnation = journal.initialize_fresh_domain().await.unwrap();
    let namespace_id = NamespaceId::new("opaque-upload-namespace").unwrap();
    assert!(
        journal
            .read_namespace(&namespace_id)
            .await
            .unwrap()
            .is_none()
    );
    let namespace_hex: String = namespace_id
        .as_str()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let namespace_key = key(format!(
        "D#{:x}#NS#{namespace_hex}",
        Sha256::digest(domain.as_str().as_bytes())
    ));
    let mut configuration = namespace_key.clone();
    for (name, value) in [
        ("record", "namespace".to_owned()),
        ("domain", domain.as_str().to_owned()),
        ("incarnation", incarnation.to_string()),
        ("namespace_id", namespace_id.as_str().to_owned()),
        ("configuration_revision", Uuid::new_v4().to_string()),
        ("state", "ACTIVE".to_owned()),
        ("store", "logical-test-store".to_owned()),
        ("bucket", "fixture-not-s3-verified".to_owned()),
        ("expected_owner", "123456789012".to_owned()),
        ("region", "us-west-2".to_owned()),
        ("key_prefix", "fresh-fixture/".to_owned()),
        (
            "provider_profile",
            "aws-s3-versioned-lifecycle-v0".to_owned(),
        ),
    ] {
        configuration.insert(name.into(), A::S(value));
    }
    // Explicit synthetic test values, NOT production timing defaults or S3 proof.
    configuration.insert("mpu_max_age_ms".into(), A::N("7200000".into()));
    configuration.insert("mpu_lifecycle_margin_ms".into(), A::N("3600000".into()));
    f.put(&f.coord, configuration.clone()).await;
    let namespace = journal
        .read_namespace(&namespace_id)
        .await
        .unwrap()
        .unwrap();
    let constraints = UploadConstraints::new(100, Some(10), Some([7; 32])).unwrap();
    exercise_upload_mpu_age(f, &journal, &namespace).await;
    Box::pin(abandoned::exercise(f, &journal, &namespace)).await;

    let context = journal.command_context().await.unwrap();
    let intent_id = ResourceId::from_command(context.id());
    let begin_intent = journal
        .prepare_begin_intent(
            context,
            &namespace,
            "fresh-fixture/object",
            constraints.clone(),
            ts(1600),
            clock(1000),
            &policy,
            b"intent".to_vec(),
        )
        .unwrap();
    assert_eq!(begin_intent.action_count(), 4);
    assert!(journal.read_intent(&intent_id).await.unwrap().is_none());
    assert_eq!(
        journal.execute(&begin_intent).await.unwrap().outcome,
        Outcome::Committed(b"intent".to_vec())
    );
    let intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
    assert_eq!(intent.id(), &intent_id);
    assert_eq!(intent.revision(), intent_id.uuid());
    assert_eq!(intent.state(), IntentState::Open);
    assert_eq!(intent.constraints(), &constraints);
    assert!(
        journal
            .prepare_begin_intent(
                journal.command_context().await.unwrap(),
                &namespace,
                "historical/not-enrolled",
                constraints.clone(),
                ts(1600),
                clock(1000),
                &policy,
                vec![]
            )
            .is_err()
    );

    let context = journal.command_context().await.unwrap();
    let attempt_id = ResourceId::from_command(context.id());
    let begin_attempt = journal
        .prepare_begin_attempt(
            context,
            &intent,
            &namespace,
            ts(1500),
            ts(2100),
            clock(1000),
            &policy,
            b"attempt".to_vec(),
        )
        .unwrap();
    assert_eq!(begin_attempt.action_count(), 5);
    assert!(journal.read_attempt(&attempt_id).await.unwrap().is_none());
    assert!(
        journal
            .authorize_transfer(
                &attempt_id,
                || panic!("sampled before discovery succeeded"),
                &policy
            )
            .await
            .is_err()
    );
    assert_eq!(
        journal.execute(&begin_attempt).await.unwrap().outcome,
        Outcome::Committed(b"attempt".to_vec())
    );
    assert_eq!(
        journal.execute(&begin_attempt).await.unwrap().outcome,
        Outcome::Committed(b"attempt".to_vec())
    );
    let authority = journal
        .authorize_transfer(&attempt_id, || Ok(clock(1000)), &policy)
        .await
        .unwrap();
    assert_eq!(authority.attempt().id(), &attempt_id);
    assert_eq!(authority.intent().id(), &intent_id);
    assert_eq!(authority.namespace(), &namespace);
    assert_eq!(authority.valid_until(), ts(1500));
    let attempt = journal.read_attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.state(), AttemptState::Active);
    assert_eq!(attempt.target(), intent.target());
    assert_eq!(attempt.constraints(), intent.constraints());
    assert_eq!(attempt.max_mpu_use_until(), ts(2100));
    assert_eq!(attempt.mpu_admitted_lower_bound(), ts(995));

    // Each lifetime is checked independently at the conservative upper bound.
    assert!(
        journal
            .authorize_transfer(&attempt_id, || Ok(clock(1495)), &policy)
            .await
            .is_err()
    );
    assert!(
        journal
            .authorize_transfer(
                &attempt_id,
                || Ok(ClockObservation::untrusted(ts(1000), 0)),
                &policy
            )
            .await
            .is_err()
    );
    let renewal = Renewal::new(intent.revision(), ts(1900), Uuid::new_v4()).unwrap();
    let renew_intent = journal
        .prepare_renew_intent(
            journal.command_context().await.unwrap(),
            &intent,
            &namespace,
            &renewal,
            clock(1100),
            &policy,
            b"intent-renewal".to_vec(),
        )
        .unwrap();
    let stale_intent_renewal = journal
        .prepare_renew_intent(
            journal.command_context().await.unwrap(),
            &intent,
            &namespace,
            &renewal,
            clock(1100),
            &policy,
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&renew_intent).await.unwrap().outcome,
        Outcome::Committed(b"intent-renewal".to_vec())
    );
    assert_eq!(
        journal.execute(&renew_intent).await.unwrap().outcome,
        Outcome::Committed(b"intent-renewal".to_vec())
    );
    assert_internal_rejection(&journal.execute(&stale_intent_renewal).await.unwrap());
    let intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
    assert_eq!(intent.deadline(), ts(1900));
    assert_eq!(intent.revision(), renewal.new_revision());
    assert_eq!(
        journal
            .read_attempt(&attempt_id)
            .await
            .unwrap()
            .unwrap()
            .deadline(),
        ts(1500),
        "intent renewal must not renew an abandoned attempt"
    );

    let renewal = Renewal::new(attempt.revision(), ts(1800), Uuid::new_v4()).unwrap();
    let renew_attempt = journal
        .prepare_renew_attempt(
            journal.command_context().await.unwrap(),
            &attempt,
            &intent,
            &namespace,
            &renewal,
            clock(1100),
            &policy,
            b"attempt-renewal".to_vec(),
        )
        .unwrap();
    let stale_seal = journal
        .prepare_seal_attempt(journal.command_context().await.unwrap(), &attempt, vec![])
        .unwrap();
    assert_eq!(
        journal.execute(&renew_attempt).await.unwrap().outcome,
        Outcome::Committed(b"attempt-renewal".to_vec())
    );
    assert_eq!(
        journal.execute(&renew_attempt).await.unwrap().outcome,
        Outcome::Committed(b"attempt-renewal".to_vec())
    );
    assert_internal_rejection(&journal.execute(&stale_seal).await.unwrap());
    let attempt = journal.read_attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.deadline(), ts(1800));
    assert_eq!(attempt.max_mpu_use_until(), ts(2100));
    assert_eq!(attempt.mpu_admitted_lower_bound(), ts(995));
    let too_long = Renewal::new(attempt.revision(), ts(2200), Uuid::new_v4()).unwrap();
    assert!(
        journal
            .prepare_renew_attempt(
                journal.command_context().await.unwrap(),
                &attempt,
                &intent,
                &namespace,
                &too_long,
                clock(1100),
                &policy,
                vec![]
            )
            .is_err()
    );
    assert!(
        journal
            .prepare_renew_attempt(
                journal.command_context().await.unwrap(),
                &attempt,
                &intent,
                &namespace,
                &too_long,
                clock(1800),
                &policy,
                vec![]
            )
            .is_err()
    );

    // Prepare old-generation creation, then close it. Existing live resources
    // remain usable, but the closed-generation command cannot create its ID.
    let context = journal.command_context().await.unwrap();
    let late_id = ResourceId::from_command(context.id());
    let late = journal
        .prepare_begin_attempt(
            context,
            &intent,
            &namespace,
            ts(1850),
            ts(2200),
            clock(1100),
            &policy,
            vec![],
        )
        .unwrap();
    let mut fence = f.get(&f.coord, control(&domain)).await.unwrap();
    fence.insert("current_generation".into(), A::N("2".into()));
    fence.insert("closed_through".into(), A::N("1".into()));
    fence.insert("revision".into(), A::N("2".into()));
    f.put(&f.coord, fence.clone()).await;
    assert_eq!(
        journal.execute(&late).await.unwrap().outcome,
        Outcome::NotCommitted
    );
    assert!(journal.read_attempt(&late_id).await.unwrap().is_none());
    assert!(
        journal
            .authorize_transfer(&attempt_id, || Ok(clock(1100)), &policy)
            .await
            .is_ok()
    );
    let context = journal.command_context().await.unwrap();
    let fresh_id = ResourceId::from_command(context.id());
    assert_eq!(fresh_id.scope().generation(), 2);
    assert_eq!(intent.id().scope().generation(), 1);
    let fresh = journal
        .prepare_begin_attempt(
            context,
            &intent,
            &namespace,
            ts(2000),
            ts(2300),
            clock(1100),
            &policy,
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&fresh).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1100)), &policy)
            .await
            .is_ok()
    );
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1895)), &policy)
            .await
            .is_err(),
        "live attempt cannot bypass expired parent"
    );
    let fresh_attempt = journal.read_attempt(&fresh_id).await.unwrap().unwrap();
    let renewal = Renewal::new(intent.revision(), ts(2100), Uuid::new_v4()).unwrap();
    let fresh_renewal = journal
        .prepare_renew_intent(
            journal.command_context().await.unwrap(),
            &intent,
            &namespace,
            &renewal,
            clock(1200),
            &policy,
            vec![],
        )
        .unwrap();
    assert_eq!(
        journal.execute(&fresh_renewal).await.unwrap().outcome,
        Outcome::Committed(vec![])
    );
    let intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
    assert_eq!(intent.deadline(), ts(2100));
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1895)), &policy)
            .await
            .is_ok()
    );

    // Test-only destructive fault injection and restoration. Absence/corruption
    // is never interpreted as authority. No production restore API is implied.
    for row_key in [
        namespace_key.clone(),
        upload_resource_key("INTENT", &intent_id),
        upload_resource_key("ATTEMPT", &fresh_id),
        control(&domain),
    ] {
        let row = f.get(&f.coord, row_key.clone()).await.unwrap();
        f.delete(&f.coord, row_key).await;
        // Missing parent/namespace/fence is discovered only by the coherent
        // four-row read, after successful attempt discovery. Never sample early.
        assert!(
            journal
                .authorize_transfer(
                    &fresh_id,
                    || panic!("sampled before coherent rows decoded"),
                    &policy
                )
                .await
                .is_err()
        );
        f.put(&f.coord, row).await;
        assert!(
            journal
                .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
                .await
                .is_ok()
        );
    }
    let row_key = upload_resource_key("ATTEMPT", &fresh_id);
    let row = f.get(&f.coord, row_key).await.unwrap();
    for (field, value) in [
        ("object_key", A::S("fresh-fixture/other".into())),
        ("store", A::S("wrong-store".into())),
        ("max_length", A::N("99".into())),
        ("mpu_admitted_lower_bound", A::N("2000".into())),
        ("max_mpu_use_until", A::N("7201096".into())),
        ("incarnation", A::S(Uuid::new_v4().to_string())),
        ("state", A::S("UNKNOWN".into())),
    ] {
        let mut corrupt = row.clone();
        corrupt.insert(field.into(), value);
        f.put(&f.coord, corrupt).await;
        assert!(
            journal
                .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
                .await
                .is_err(),
            "accepted {field}"
        );
        f.put(&f.coord, row.clone()).await;
    }

    // Both admission writes and current transfer authorization bind namespace
    // configuration revision; no historical receipt can bypass a changed one.
    let context = journal.command_context().await.unwrap();
    let changed_id = ResourceId::from_command(context.id());
    let stale_config_begin = journal
        .prepare_begin_attempt(
            context,
            &intent,
            &namespace,
            ts(2000),
            ts(2300),
            clock(1200),
            &policy,
            vec![],
        )
        .unwrap();
    // Even an illicit same-revision policy change must invalidate the full
    // namespace guard, not silently reinterpret an already prepared command.
    for field in ["mpu_max_age_ms", "mpu_lifecycle_margin_ms"] {
        let mut changed = configuration.clone();
        changed.insert(field.into(), A::N("1800000".into()));
        f.put(&f.coord, changed).await;
        assert_internal_rejection(&journal.execute(&stale_config_begin).await.unwrap());
        assert!(journal.read_attempt(&changed_id).await.unwrap().is_none());
    }
    let mut changed = configuration.clone();
    changed.insert(
        "configuration_revision".into(),
        A::S(Uuid::new_v4().to_string()),
    );
    f.put(&f.coord, changed).await;
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
    assert_internal_rejection(&journal.execute(&stale_config_begin).await.unwrap());
    assert!(journal.read_attempt(&changed_id).await.unwrap().is_none());
    let changed_namespace = journal
        .read_namespace(&namespace_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        journal
            .prepare_begin_attempt(
                journal.command_context().await.unwrap(),
                &intent,
                &changed_namespace,
                ts(2000),
                ts(2300),
                clock(1200),
                &policy,
                vec![]
            )
            .is_err()
    );
    f.put(&f.coord, configuration.clone()).await;
    let mut inactive = configuration.clone();
    inactive.insert("state".into(), A::S("INACTIVE".into()));
    f.put(&f.coord, inactive).await;
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
    f.put(&f.coord, configuration).await;

    // No upsert when an observed record vanishes between preparation and commit.
    let renewal = Renewal::new(fresh_attempt.revision(), ts(2200), Uuid::new_v4()).unwrap();
    let missing_renew = journal
        .prepare_renew_attempt(
            journal.command_context().await.unwrap(),
            &fresh_attempt,
            &intent,
            &namespace,
            &renewal,
            clock(1200),
            &policy,
            vec![],
        )
        .unwrap();
    let fresh_key = upload_resource_key("ATTEMPT", &fresh_id);
    let saved = f.get(&f.coord, fresh_key.clone()).await.unwrap();
    // Full renewal guards bind both immutable MPU timestamps, including when
    // injected changes remain individually well-formed and retain the revision.
    for (field, value) in [
        ("mpu_admitted_lower_bound", "1094"),
        ("max_mpu_use_until", "2301"),
    ] {
        let mut changed = saved.clone();
        changed.insert(field.into(), A::N(value.into()));
        f.put(&f.coord, changed).await;
        assert_internal_rejection(&journal.execute(&missing_renew).await.unwrap());
    }
    f.delete(&f.coord, fresh_key.clone()).await;
    assert_internal_rejection(&journal.execute(&missing_renew).await.unwrap());
    assert!(journal.read_attempt(&fresh_id).await.unwrap().is_none());
    f.put(&f.coord, saved).await;
    let seal = journal
        .prepare_seal_attempt(
            journal.command_context().await.unwrap(),
            &attempt,
            b"sealed".to_vec(),
        )
        .unwrap();
    assert_eq!(
        journal.execute(&seal).await.unwrap().outcome,
        Outcome::Committed(b"sealed".to_vec())
    );
    assert_eq!(
        journal
            .read_attempt(&attempt_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        AttemptState::Sealed
    );
    assert!(
        journal
            .authorize_transfer(&attempt_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
    assert_eq!(
        journal.resolve(begin_attempt.token()).await.unwrap(),
        Outcome::Committed(b"attempt".to_vec())
    );
    assert_eq!(
        journal.execute(&begin_attempt).await.unwrap().outcome,
        Outcome::Committed(b"attempt".to_vec())
    );
    assert!(
        journal
            .authorize_transfer(&attempt_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
            .await
            .is_ok()
    );

    // Current incarnation fences live attempts regardless of creation epoch;
    // historic receipts still resolve, without authorizing new transfer.
    let mut rotated = fence.clone();
    rotated.insert("incarnation".into(), A::S(Uuid::new_v4().to_string()));
    f.put(&f.coord, rotated).await;
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
    assert_eq!(
        journal.resolve(fresh.token()).await.unwrap(),
        Outcome::Committed(vec![])
    );
    assert!(
        journal
            .prepare_begin_attempt(
                journal.command_context().await.unwrap(),
                &intent,
                &namespace,
                ts(2000),
                ts(2300),
                clock(1200),
                &policy,
                vec![]
            )
            .is_err()
    );
    // Fixture restoration solely to independently exercise OPEN cancellation.
    f.put(&f.coord, fence).await;
    let cancel = journal
        .prepare_cancel_intent(
            journal.command_context().await.unwrap(),
            &intent,
            b"cancelled".to_vec(),
        )
        .unwrap();
    assert_eq!(
        journal.execute(&cancel).await.unwrap().outcome,
        Outcome::Committed(b"cancelled".to_vec())
    );
    let cancelled = journal.read_intent(&intent_id).await.unwrap().unwrap();
    assert_eq!(cancelled.state(), IntentState::Cancelled);
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
    assert!(
        journal
            .prepare_begin_attempt(
                journal.command_context().await.unwrap(),
                &cancelled,
                &namespace,
                ts(2000),
                ts(2300),
                clock(1200),
                &policy,
                vec![]
            )
            .is_err()
    );
    assert!(
        journal
            .prepare_renew_intent(
                journal.command_context().await.unwrap(),
                &cancelled,
                &namespace,
                &Renewal::new(cancelled.revision(), ts(2200), Uuid::new_v4()).unwrap(),
                clock(1200),
                &policy,
                vec![]
            )
            .is_err()
    );
    let intent_key = upload_resource_key("INTENT", &intent_id);
    let mut selected_row = f.get(&f.coord, intent_key).await.unwrap();
    selected_row.insert("state".into(), A::S("SELECTED".into()));
    f.put(&f.coord, selected_row).await;
    let selected = journal.read_intent(&intent_id).await.unwrap().unwrap();
    assert_eq!(selected.state(), IntentState::Selected);
    assert!(
        journal
            .prepare_cancel_intent(journal.command_context().await.unwrap(), &selected, vec![])
            .is_err(),
        "OPEN-only cancellation cannot bypass selected staging accounting"
    );
    assert!(
        journal
            .authorize_transfer(&fresh_id, || Ok(clock(1200)), &policy)
            .await
            .is_err()
    );
}
