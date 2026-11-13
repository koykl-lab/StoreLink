//! Native metadata-journal control conformance, NOT whole GC acceptance or a
//! wall-clock retention proof. All times below are explicit synthetic milliseconds.
//! No receipt pruner, scheduler, S3 operation, resource provisioning or cleanup is
//! introduced: reuse only the parent's two owned tables and cleanup boundary.

use super::*;
use storelink::clock::{ClockError, ClockObservation, TimestampMs};
use storelink::dynamodb::{
    MaintenanceError, MaintenanceObservation, MaintenanceOutcome, MaintenancePolicy,
};

fn policy() -> MaintenancePolicy {
    MaintenancePolicy::new(1000, 10).unwrap()
}
fn clock(now: u64) -> Result<ClockObservation, ClockError> {
    ClockObservation::trusted(TimestampMs::new(now)?, 5)
}
fn no_sample() -> Result<ClockObservation, ClockError> {
    panic!("control/policy/no-work validation must precede clock sampling")
}

/// Compare public getters AND an independent strong native read of the exact row.
/// Exact equality also checks optional-field removal and bounded control shape.
async fn assert_control(
    f: &Fixture,
    journal: &CommandJournal,
    observation: &MaintenanceObservation,
    domain: &DomainId,
    generations_revision: (u64, u64, u64, u64),
    retention: Option<u64>,
    window: Option<(u64, u64)>,
) {
    let (current, closed, compacted, revision) = generations_revision;
    assert_eq!(
        observation.coordination_arn(),
        f.tables.coordination_table_arn()
    );
    let fence = observation.fence();
    assert_eq!(fence.domain(), domain);
    assert_eq!(fence.current(), current);
    assert_eq!(fence.closed_through(), closed);
    assert_eq!(fence.compacted_through(), compacted);
    assert_eq!(observation.revision(), revision);
    assert_eq!(observation.resolution_retention_ms(), retention);
    assert_eq!(
        observation.compaction_window(),
        window.map(|(candidate, deadline)| (candidate, TimestampMs::new(deadline).unwrap()))
    );
    assert_eq!(
        &journal.read_maintenance_control().await.unwrap(),
        observation
    );
    let mut expected = control(domain);
    expected.insert("domain".into(), A::S(domain.as_str().into()));
    expected.insert("incarnation".into(), A::S(fence.incarnation().to_string()));
    for (name, value) in [
        ("current_generation", current),
        ("closed_through", closed),
        ("compacted_through", compacted),
        ("revision", revision),
    ] {
        expected.insert(name.into(), A::N(value.to_string()));
    }
    if let Some(retention) = retention {
        expected.insert(
            "resolution_retention_ms".into(),
            A::N(retention.to_string()),
        );
    }
    if let Some((candidate, deadline)) = window {
        expected.insert("compaction_candidate".into(), A::N(candidate.to_string()));
        expected.insert(
            "compaction_not_before_ms".into(),
            A::N(deadline.to_string()),
        );
    }
    assert_eq!(f.get(&f.coord, control(domain)).await, Some(expected));
}

pub(super) async fn exercise(f: &Fixture) {
    // Separate boxed poll frames, including on debug builds of the native suite.
    Box::pin(retention_and_replay(f)).await;
    Box::pin(invalid_control_never_samples(f)).await;
}

async fn retention_and_replay(f: &Fixture) {
    let domain = DomainId::new(format!("maintenance-test-{}", Uuid::new_v4())).unwrap();
    let journal = f.journal(&domain);
    let incarnation = journal.initialize_fresh_domain().await.unwrap();
    let initial = journal.read_maintenance_control().await.unwrap();
    assert_eq!(initial.fence().incarnation(), incarnation);
    assert_control(f, &journal, &initial, &domain, (1, 0, 0, 1), None, None).await;

    let present_key = format!("maintenance-present-{}", Uuid::new_v4());
    let absent_key = format!("maintenance-absent-{}", Uuid::new_v4());
    let committed = f
        .prepare(&journal, &present_key, b"maintenance-committed")
        .await;
    let frozen = f.prepare(&journal, &absent_key, b"never-submitted").await;
    assert_eq!(committed.token().id().scope().generation(), 1);
    assert_eq!(frozen.token().id().scope().generation(), 1);
    let committed_outcome = Outcome::Committed(b"maintenance-committed".to_vec());
    assert_eq!(
        journal.execute(&committed).await.unwrap().outcome,
        committed_outcome
    );
    let app_before = f.get(&f.app, key(&present_key)).await.unwrap();
    let receipt_before = f.get(&f.coord, receipt(committed.token())).await.unwrap();
    assert!(f.get(&f.coord, receipt(frozen.token())).await.is_none());
    assert!(f.get(&f.app, key(&absent_key)).await.is_none());

    // The consumer receives item and fence from one native transaction, without
    // reconstructing StoreLink's private control key or inferring command history.
    let observed = journal
        .observe_application_item(
            aws_sdk_dynamodb::types::Get::builder()
                .table_name(&f.app)
                .set_key(Some(key(&present_key)))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(observed.item(), Some(&app_before));
    assert_eq!(observed.fence().domain(), &domain);
    assert_eq!(observed.fence().current(), 1);
    let absent = journal
        .observe_application_item(
            aws_sdk_dynamodb::types::Get::builder()
                .table_name(&f.app)
                .set_key(Some(key(&absent_key)))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(absent.item().is_none());
    assert_eq!(absent.fence().current(), 1);
    assert!(matches!(
        journal
            .observe_application_item(
                aws_sdk_dynamodb::types::Get::builder()
                    .table_name(&f.coord)
                    .set_key(Some(control(&domain)))
                    .build()
                    .unwrap()
            )
            .await,
        Err(JournalError::Enrollment(
            storelink::dynamodb::EnrollmentError::ForbiddenRead
        ))
    ));

    let invalid_projection = journal
        .observe_application_item(
            aws_sdk_dynamodb::types::Get::builder()
                .table_name(&f.app)
                .set_key(Some(key(&present_key)))
                .projection_expression("(")
                .build()
                .unwrap(),
        )
        .await;
    match invalid_projection {
        Err(JournalError::Native(message)) => {
            assert!(message.contains("ValidationException"), "{message}");
            assert!(message.contains("ProjectionExpression"), "{message}");
        }
        _ => panic!("native invalid projection must retain its service diagnostic"),
    }

    let policy = policy();
    assert_eq!(policy.resolution_retention_ms(), 1000);
    assert_eq!(policy.maximum_clock_uncertainty_ms(), 10);
    let configured = journal.configure_compaction_policy(&policy).await.unwrap();
    assert_eq!(configured.outcome, MaintenanceOutcome::Configured);
    assert_control(
        f,
        &journal,
        &configured.observation,
        &domain,
        (1, 0, 0, 2),
        Some(1000),
        None,
    )
    .await;
    let repeated = journal.configure_compaction_policy(&policy).await.unwrap();
    assert_eq!(
        repeated.outcome,
        MaintenanceOutcome::PolicyAlreadyConfigured
    );
    assert_eq!(repeated.observation, configured.observation);
    assert!(matches!(
        journal
            .configure_compaction_policy(&MaintenancePolicy::new(1001, 10).unwrap())
            .await,
        Err(MaintenanceError::PolicyMismatch)
    ));
    let no_boundary = journal
        .arm_compaction_window(&policy, &no_sample)
        .await
        .unwrap();
    assert_eq!(no_boundary.outcome, MaintenanceOutcome::NoEligibleBoundary);
    assert_eq!(no_boundary.observation, configured.observation);
    assert_control(
        f,
        &journal,
        &no_boundary.observation,
        &domain,
        (1, 0, 0, 2),
        Some(1000),
        None,
    )
    .await;

    let closed = journal.advance_and_close_generation(&policy).await.unwrap();
    assert_eq!(closed.outcome, MaintenanceOutcome::AdvancedAndClosed);
    assert_control(
        f,
        &journal,
        &closed.observation,
        &domain,
        (2, 1, 0, 3),
        Some(1000),
        None,
    )
    .await;
    assert_eq!(
        journal.resolve(frozen.token()).await.unwrap(),
        Outcome::NotCommitted
    );
    let fenced = journal.execute(&frozen).await.unwrap();
    assert_eq!(fenced.outcome, Outcome::NotCommitted);
    assert!(matches!(
        fenced.attempt_failure,
        Some(AttemptFailure::TransactionCancelled { reasons })
            if reasons.iter().any(|r| r.origin == ActionOrigin::Admission && r.code == "ConditionalCheckFailed")
    ));
    assert!(f.get(&f.app, key(&absent_key)).await.is_none());
    assert!(f.get(&f.coord, receipt(frozen.token())).await.is_none());

    // The closure above is acknowledged before the sampler is invoked. These
    // synthetic samples test conservative arithmetic, not real elapsed time.
    let armed = journal
        .arm_compaction_window(&policy, &|| clock(1000))
        .await
        .unwrap();
    assert_eq!(armed.outcome, MaintenanceOutcome::Armed);
    assert_control(
        f,
        &journal,
        &armed.observation,
        &domain,
        (2, 1, 0, 4),
        Some(1000),
        Some((1, 2005)),
    )
    .await;
    let closed_again = journal.advance_and_close_generation(&policy).await.unwrap();
    assert_eq!(closed_again.outcome, MaintenanceOutcome::AdvancedAndClosed);
    assert_control(
        f,
        &journal,
        &closed_again.observation,
        &domain,
        (3, 2, 0, 5),
        Some(1000),
        Some((1, 2005)),
    )
    .await;
    let pending = journal
        .arm_compaction_window(&policy, &no_sample)
        .await
        .unwrap();
    assert_eq!(pending.outcome, MaintenanceOutcome::WindowPending);
    assert_eq!(pending.observation, closed_again.observation);

    assert_eq!(
        clock(2009).unwrap().lower_bound().unwrap().as_millis(),
        2004
    );
    let not_due = journal
        .publish_compaction_window(&policy, &|| clock(2009))
        .await
        .unwrap();
    assert_eq!(not_due.outcome, MaintenanceOutcome::NotDue);
    assert_eq!(not_due.observation, closed_again.observation);
    assert!(matches!(
        journal
            .publish_compaction_window(&policy, &|| {
                Ok(ClockObservation::untrusted(
                    TimestampMs::new(2010).unwrap(),
                    5,
                ))
            })
            .await,
        Err(MaintenanceError::Clock(ClockError::Untrusted))
    ));
    assert!(matches!(
        journal
            .publish_compaction_window(&policy, &|| {
                ClockObservation::trusted(TimestampMs::new(2010).unwrap(), 11)
            })
            .await,
        Err(MaintenanceError::Clock(ClockError::ExcessiveUncertainty))
    ));
    assert_control(
        f,
        &journal,
        &not_due.observation,
        &domain,
        (3, 2, 0, 5),
        Some(1000),
        Some((1, 2005)),
    )
    .await;

    assert_eq!(
        clock(2010).unwrap().lower_bound().unwrap().as_millis(),
        2005
    );
    let published = journal
        .publish_compaction_window(&policy, &|| clock(2010))
        .await
        .unwrap();
    assert_eq!(published.outcome, MaintenanceOutcome::Published);
    assert_control(
        f,
        &journal,
        &published.observation,
        &domain,
        (3, 2, 1, 6),
        Some(1000),
        None,
    )
    .await;
    assert_eq!(
        journal.resolve(frozen.token()).await.unwrap(),
        Outcome::Expired
    );
    assert_eq!(
        journal.resolve(committed.token()).await.unwrap(),
        committed_outcome
    );
    // Known command keys only, never table-wide counts (other domains may run
    // concurrently). The production maintenance path inspected for this contract
    // uses one conditional PutItem on CONTROL, with no command token/receipt.
    assert_eq!(
        f.get(&f.coord, receipt(committed.token())).await,
        Some(receipt_before)
    );
    assert!(f.get(&f.coord, receipt(frozen.token())).await.is_none());

    // Explicit TEST-ONLY receipt deletion demonstrates post-pruning resolution.
    // Publishing the watermark did NOT prune this receipt; no production receipt
    // pruning or whole metadata reclamation implementation is claimed here.
    f.delete(&f.coord, receipt(committed.token())).await;
    assert_eq!(
        journal.resolve(committed.token()).await.unwrap(),
        Outcome::Expired
    );
    // Native idempotency can still acknowledge the ORIGINAL committed request
    // from its SDK token cache. That is not reexecution: neither retry may
    // recreate the deleted receipt or change application state below.
    let retry = journal.execute(&committed).await.unwrap();
    assert!(retry.outcome == committed_outcome || retry.outcome == Outcome::Expired);
    assert_eq!(
        journal.execute(&frozen).await.unwrap().outcome,
        Outcome::Expired
    );
    assert_eq!(
        journal.resolve(committed.token()).await.unwrap(),
        Outcome::Expired
    );
    assert!(f.get(&f.coord, receipt(committed.token())).await.is_none());
    assert!(f.get(&f.coord, receipt(frozen.token())).await.is_none());
    assert_eq!(
        f.get(&f.app, key(&present_key)).await,
        Some(app_before.clone())
    );
    assert!(f.get(&f.app, key(&absent_key)).await.is_none());

    let no_window = journal
        .publish_compaction_window(&policy, &no_sample)
        .await
        .unwrap();
    assert_eq!(no_window.outcome, MaintenanceOutcome::NoPendingWindow);
    assert_eq!(no_window.observation, published.observation);
    let rearmed = journal
        .arm_compaction_window(&policy, &|| clock(3000))
        .await
        .unwrap();
    assert_eq!(rearmed.outcome, MaintenanceOutcome::Armed);
    assert_eq!(rearmed.observation.fence().incarnation(), incarnation);
    assert_control(
        f,
        &journal,
        &rearmed.observation,
        &domain,
        (3, 2, 1, 7),
        Some(1000),
        Some((2, 4005)),
    )
    .await;
    assert_eq!(f.get(&f.app, key(&present_key)).await, Some(app_before));
    assert!(f.get(&f.app, key(&absent_key)).await.is_none());
    assert!(f.get(&f.coord, receipt(committed.token())).await.is_none());
    assert!(f.get(&f.coord, receipt(frozen.token())).await.is_none());
}

async fn invalid_control_never_samples(f: &Fixture) {
    // Deliberately malformed state belongs to this fresh domain alone; it cannot
    // poison the normal scenario or any other suite using the shared tables.
    let domain = DomainId::new(format!("maintenance-invalid-{}", Uuid::new_v4())).unwrap();
    let journal = f.journal(&domain);
    let policy = policy();
    assert!(matches!(
        journal.read_maintenance_control().await,
        Err(MaintenanceError::Journal(JournalError::MissingControl))
    ));
    assert!(matches!(
        journal.arm_compaction_window(&policy, &no_sample).await,
        Err(MaintenanceError::Journal(JournalError::MissingControl))
    ));
    assert!(matches!(
        journal.publish_compaction_window(&policy, &no_sample).await,
        Err(MaintenanceError::Journal(JournalError::MissingControl))
    ));
    assert!(f.get(&f.coord, control(&domain)).await.is_none());

    journal.initialize_fresh_domain().await.unwrap();
    let initial = journal.read_maintenance_control().await.unwrap();
    assert!(matches!(
        journal.arm_compaction_window(&policy, &no_sample).await,
        Err(MaintenanceError::MissingPolicy)
    ));
    assert!(matches!(
        journal.publish_compaction_window(&policy, &no_sample).await,
        Err(MaintenanceError::MissingPolicy)
    ));
    assert!(matches!(
        journal.advance_and_close_generation(&policy).await,
        Err(MaintenanceError::MissingPolicy)
    ));
    assert_control(f, &journal, &initial, &domain, (1, 0, 0, 1), None, None).await;
    journal.configure_compaction_policy(&policy).await.unwrap();
    journal.advance_and_close_generation(&policy).await.unwrap();
    let wrong = MaintenancePolicy::new(1001, 10).unwrap();
    assert!(matches!(
        journal.arm_compaction_window(&wrong, &no_sample).await,
        Err(MaintenanceError::PolicyMismatch)
    ));
    assert!(matches!(
        journal.publish_compaction_window(&wrong, &no_sample).await,
        Err(MaintenanceError::PolicyMismatch)
    ));
    let valid = journal.read_maintenance_control().await.unwrap();
    assert_control(f, &journal, &valid, &domain, (2, 1, 0, 3), Some(1000), None).await;

    // Test-only partial candidate/deadline pair: rejected before sampling and
    // never silently repaired. The harness still owns all cleanup.
    let mut malformed = f.get(&f.coord, control(&domain)).await.unwrap();
    malformed.insert("compaction_candidate".into(), A::N("1".into()));
    f.put(&f.coord, malformed.clone()).await;
    assert!(matches!(
        journal.read_maintenance_control().await,
        Err(MaintenanceError::Corrupt)
    ));
    assert!(matches!(
        journal.arm_compaction_window(&policy, &no_sample).await,
        Err(MaintenanceError::Corrupt)
    ));
    assert!(matches!(
        journal.publish_compaction_window(&policy, &no_sample).await,
        Err(MaintenanceError::Corrupt)
    ));
    assert_eq!(f.get(&f.coord, control(&domain)).await, Some(malformed));
}
