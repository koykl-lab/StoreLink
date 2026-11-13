//! Native expiry transitions; no S3 I/O, orphan cleanup or scanner proof.
use super::*;
use storelink::claim::{ClaimPolicy, Renewal};
use storelink::clock::{ClockObservation, TimestampMs};
use storelink::dynamodb::{
    AttemptState, IntentState, NamespaceSnapshot, UploadConstraints, UploadError,
};
fn ts(n: u64) -> TimestampMs {
    TimestampMs::new(n).unwrap()
}
fn clock(n: u64) -> ClockObservation {
    ClockObservation::trusted(ts(n), 5).unwrap()
}

pub(super) async fn exercise(f: &Fixture, journal: &CommandJournal, ns: &NamespaceSnapshot) {
    let policy = ClaimPolicy::new(2000, 50, 10).unwrap();
    let context = journal.command_context().await.unwrap();
    let intent_id = ResourceId::from_command(context.id());
    let begin = journal
        .prepare_begin_intent(
            context,
            ns,
            "fresh-fixture/abandoned",
            UploadConstraints::new(100, None, None).unwrap(),
            ts(1500),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        journal.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let intent = journal.read_intent(&intent_id).await.unwrap().unwrap();
    let context = journal.command_context().await.unwrap();
    let attempt_id = ResourceId::from_command(context.id());
    let begin = journal
        .prepare_begin_attempt(
            context,
            &intent,
            ns,
            ts(1400),
            ts(2500),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        journal.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let attempt = journal.read_attempt(&attempt_id).await.unwrap().unwrap();
    let renewal = Renewal::new(intent.revision(), ts(2000), Uuid::new_v4()).unwrap();
    let renew = journal
        .prepare_renew_intent(
            journal.command_context().await.unwrap(),
            &intent,
            ns,
            &renewal,
            clock(1100),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        journal.execute(&renew).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    assert!(matches!(
        journal
            .cancel_expired_open_intent(&intent, clock(1505), &policy)
            .await,
        Err(UploadError::AbandonedConflict)
    ));
    assert_eq!(
        journal
            .read_intent(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        IntentState::Open
    );
    assert!(
        journal
            .seal_expired_attempt(&attempt, clock(1404), &policy)
            .await
            .is_err()
    );
    journal
        .seal_expired_attempt(&attempt, clock(1405), &policy)
        .await
        .unwrap();
    assert_eq!(
        journal
            .read_attempt(&attempt_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        AttemptState::Sealed
    );
    assert_eq!(
        journal
            .read_intent(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .deadline(),
        ts(2000)
    );
    assert!(matches!(
        journal
            .seal_expired_attempt(&attempt, clock(1405), &policy)
            .await,
        Err(UploadError::AbandonedConflict)
    ));
    let renewed = journal.read_intent(&intent_id).await.unwrap().unwrap();
    assert!(
        journal
            .cancel_expired_open_intent(&renewed, clock(2004), &policy)
            .await
            .is_err()
    );
    journal
        .cancel_expired_open_intent(&renewed, clock(2005), &policy)
        .await
        .unwrap();
    assert_eq!(
        journal
            .read_intent(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        IntentState::Cancelled
    );
    assert!(matches!(
        journal
            .cancel_expired_open_intent(&renewed, clock(2005), &policy)
            .await,
        Err(UploadError::AbandonedConflict)
    ));

    // Sealing an abandoned attempt needs no parent row: a terminal parent's
    // eventual pruning must not strand its remaining attempts.
    let context = journal.command_context().await.unwrap();
    let parent_id = ResourceId::from_command(context.id());
    let begin = journal
        .prepare_begin_intent(
            context,
            ns,
            "fresh-fixture/missing-parent",
            UploadConstraints::new(100, None, None).unwrap(),
            ts(1500),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        journal.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let parent = journal.read_intent(&parent_id).await.unwrap().unwrap();
    let context = journal.command_context().await.unwrap();
    let child_id = ResourceId::from_command(context.id());
    let begin = journal
        .prepare_begin_attempt(
            context,
            &parent,
            ns,
            ts(1600),
            ts(2500),
            clock(1000),
            &policy,
            vec![],
        )
        .unwrap();
    assert!(matches!(
        journal.execute(&begin).await.unwrap().outcome,
        Outcome::Committed(_)
    ));
    let child = journal.read_attempt(&child_id).await.unwrap().unwrap();
    journal
        .cancel_expired_open_intent(&parent, clock(1505), &policy)
        .await
        .unwrap();
    f.delete(&f.coord, upload_resource_key("INTENT", &parent_id))
        .await; // Synthetic missing-parent state, not production pruning proof.
    journal
        .seal_expired_attempt(&child, clock(1605), &policy)
        .await
        .unwrap();
    assert_eq!(
        journal
            .read_attempt(&child_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        AttemptState::Sealed
    );
    assert!(journal.read_intent(&parent_id).await.unwrap().is_none());
}
