//! Independent selected-staging lifetime and bounded terminal reclamation.
//!
//! Receipt-bearing renewal/cancellation use the journal. Natural expiry cleanup
//! is the protocol §§2/6 exception: one guarded native transaction, no per-cleanup
//! receipt or new resource identity. Terminal rows remain for later compaction;
//! this module alone does not implement eventual metadata/payload reclamation.

use super::*;
use crate::dynamodb::references::version_count_action;
use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;

impl CommandJournal {
    /// Renew only the independent ACTIVE staging claim at a fixed revision and
    /// absolute deadline. The SELECTED intent and namespace must still match.
    /// Its former OPEN upload deadline does NOT limit staging: selection ended
    /// upload admission, and neither this intent nor its attempt is renewed.
    ///
    /// Submit/retry the same PreparedCommand to retain the frozen deadline and
    /// journal outcome. No local snapshot or elapsed timestamp is DB authority.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_renew_staged(
        &self,
        context: CommandContext,
        staged: &StagedSnapshot,
        intent: &IntentSnapshot,
        namespace: &NamespaceSnapshot,
        renewal: &Renewal,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, intent.id())?;
        self.existing_scope(&context, staged.claim.id())?;
        let actions = renew_actions(
            self.coordination_arn(),
            self.domain_id(),
            staged,
            intent,
            namespace,
            renewal,
            clock,
            policy,
        )?;
        self.upload_command(context, actions, result)
    }

    /// Atomically cancel SELECTED and release its ONE ACTIVE staging claim.
    /// Works after expiry and without namespace liveness or healthy clocks.
    /// Published references and independent use pins are not released.
    ///
    /// Retry the same PreparedCommand/resolve its journal token. A fresh command
    /// with a stale snapshot cannot pass the expected-row guards, and a terminal
    /// snapshot cannot decrement again. This does not cancel in-flight S3 I/O.
    pub fn prepare_cancel_staged(
        &self,
        context: CommandContext,
        staged: &StagedSnapshot,
        intent: &IntentSnapshot,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, UploadError> {
        self.existing_scope(&context, intent.id())?;
        self.existing_scope(&context, staged.claim.id())?;
        let actions = cancel_actions(self.coordination_arn(), self.domain_id(), staged, intent)?;
        self.upload_command(context, actions, result)
    }

    /// Revoke an expired ACTIVE staging claim and cancel its SELECTED intent.
    /// The conservative clock lower bound must reach the fixed claim deadline.
    /// No command/receipt/resource ID is created; closed creation generations do
    /// not prevent this terminal cleanup, including after incarnation rotation.
    /// The current control must be valid and is frozen by a full-row guard.
    ///
    /// Returns Ok ONLY for an acknowledged native transaction commit. A
    /// StagedConflict is a conditional failure for this submission, not proof of
    /// historical abortion; StagedOutcomeUnknown never means rolled back. After
    /// ANY error, the reaper must reread native staged/intent rows before deciding
    /// further work, rather than blindly retrying a decrement. An already terminal
    /// snapshot returns Terminal, not a fabricated successful revocation.
    ///
    /// One SDK attempt, no automatic retry loop, worker, timer or S3 operation.
    /// Missing/changed rows fail their full expected-row conditions. Records are
    /// retained for later compaction; this is not complete GC.
    pub async fn revoke_expired_staged(
        &self,
        staged: &StagedSnapshot,
        intent: &IntentSnapshot,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<(), UploadError> {
        let control = self
            .read_upload_item(control_key(self.domain_id()))
            .await?
            .ok_or(JournalError::MissingControl)?;
        let actions = revoke_actions(
            self.coordination_arn(),
            self.domain_id(),
            control,
            staged,
            intent,
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
            .map_err(|error| revocation_error(error.as_service_error(), error.to_string()))?;
        Ok(())
    }
}

/// Shared selected-row identity checks, NOT time validation or use authority.
/// Publication must additionally validate the claim and guard these same rows.
pub(in crate::dynamodb) fn selected_binding(
    domain: &DomainId,
    staged: &StagedSnapshot,
    intent: &IntentSnapshot,
) -> Result<(), UploadError> {
    staged_binding(staged)?;
    check_domain(domain, &staged.intent)?;
    if staged.intent != *intent.id()
        || staged.namespace != *intent.namespace_id()
        || staged.configuration_revision != intent.configuration_revision()
        || staged.version().store() != intent.target().store()
        || staged.version().key() != intent.target().key()
    {
        return Err(UploadError::Binding);
    }
    if intent.state != IntentState::Selected {
        return Err(UploadError::Terminal);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn renew_actions(
    table: &str,
    domain: &DomainId,
    staged: &StagedSnapshot,
    intent: &IntentSnapshot,
    namespace: &NamespaceSnapshot,
    renewal: &Renewal,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, UploadError> {
    selected_binding(domain, staged, intent)?;
    // Deliberately NOT live_intent: SELECTED has its own lifetime owner.
    binding(&intent.admission, namespace)?;
    let mut next = staged.clone();
    next.claim.renew(renewal, clock, policy)?;
    Ok(vec![
        namespace_guard(table, namespace)?,
        guard(table, encode_intent(intent)?)?,
        replace(table, encode_staged(staged)?, encode_staged(&next)?)?,
        version_count_action(
            table,
            domain,
            &staged.version().fingerprint(),
            staged.version(),
            0,
            0,
        )
        .map_err(|_| UploadError::ActionBuild)?,
    ])
}

fn cancel_actions(
    table: &str,
    domain: &DomainId,
    staged: &StagedSnapshot,
    intent: &IntentSnapshot,
) -> Result<Vec<TransactWriteItem>, UploadError> {
    selected_binding(domain, staged, intent)?;
    let mut next = staged.clone();
    if !next
        .claim
        .release(staged.claim.revision())?
        .accounting_must_change()
    {
        return Err(UploadError::Terminal);
    }
    terminal_actions(table, domain, staged, intent, &next)
}

// Private pure builder: its actions cannot become caller-edited executable plans.
fn revoke_actions(
    table: &str,
    domain: &DomainId,
    control: Item,
    staged: &StagedSnapshot,
    intent: &IntentSnapshot,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, UploadError> {
    // Parse the strong-read native row before building any writes: an arbitrary
    // different string is not a valid replacement incarnation.
    let fence = parse_control(Some(&control), domain)?;
    selected_binding(domain, staged, intent)?;
    let scope = staged.claim.id().scope();
    if fence.incarnation() == scope.incarnation() && scope.generation() > fence.current() {
        return Err(UploadError::Binding);
    }
    let mut next = staged.clone();
    if !next
        .claim
        .revoke(staged.claim.revision(), clock, policy)?
        .accounting_must_change()
    {
        return Err(UploadError::Terminal);
    }
    // Cleanup mints no authority: closed/compacted generations are allowed, and
    // generation numbers in different incarnations are not comparable. Freeze
    // the entire validated control so concurrent rotation/watermark changes or
    // corruption cause a benign conflict rather than bypassing this read check.
    let mut actions = vec![guard(table, control)?];
    actions.extend(terminal_actions(table, domain, staged, intent, &next)?);
    // Exactly four bounded actions: control, two <=4000-byte record replacements,
    // one count update. No receipt, new item, namespace or attempt dependency.
    Ok(ApplicationActions::new(actions)
        .map_err(JournalError::from)?
        .into_native())
}

fn terminal_actions(
    table: &str,
    domain: &DomainId,
    staged: &StagedSnapshot,
    intent: &IntentSnapshot,
    next: &StagedSnapshot,
) -> Result<Vec<TransactWriteItem>, UploadError> {
    let mut cancelled = intent.clone();
    cancelled.state = IntentState::Cancelled;
    Ok(vec![
        replace(table, encode_intent(intent)?, encode_intent(&cancelled)?)?,
        replace(table, encode_staged(staged)?, encode_staged(next)?)?,
        version_count_action(
            table,
            domain,
            &staged.version().fingerprint(),
            staged.version(),
            0,
            -1,
        )
        .map_err(|_| UploadError::ActionBuild)?,
    ])
}

fn revocation_error(error: Option<&TransactWriteItemsError>, message: String) -> UploadError {
    if let Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) = error
        && cancelled
            .cancellation_reasons()
            .iter()
            .any(|reason| reason.code() == Some("ConditionalCheckFailed"))
    {
        return UploadError::StagedConflict;
    }
    // Conservative even for other service failures. No missing receipt or error
    // response is used as a historical noncommit assertion.
    UploadError::StagedOutcomeUnknown(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::{CancellationReason, error::TransactionCanceledException};

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
    fn id(domain: &DomainId, generation: u64, unique: u128) -> ResourceId {
        ResourceId::new(
            Scope::new(domain.clone(), Uuid::from_u128(1), generation).unwrap(),
            Uuid::from_u128(unique),
        )
        .unwrap()
    }
    struct Fixture {
        ns: NamespaceSnapshot,
        intent: IntentSnapshot,
        staged: StagedSnapshot,
    }
    impl Fixture {
        // Synthetic native-row fixtures, not S3 verification or DB evaluation.
        fn new() -> Self {
            let domain = DomainId::new("lifecycle-domain").unwrap();
            let ns = NamespaceSnapshot {
                domain: domain.clone(),
                incarnation: Uuid::from_u128(1),
                id: NamespaceId::new("namespace").unwrap(),
                configuration_revision: Uuid::from_u128(2),
                store: StoreId::new("store").unwrap(),
                bucket: "unit-not-qualified".into(),
                expected_owner: "123456789012".into(),
                region: "us-west-2".into(),
                key_prefix: "fresh/".into(),
                mpu_max_age_ms: 10_000,
                mpu_lifecycle_margin_ms: 1000,
            };
            let intent = IntentSnapshot {
                admission: Admission {
                    id: id(&domain, 1, 3),
                    namespace: ns.id.clone(),
                    configuration_revision: ns.configuration_revision,
                    target: UploadTarget::new(ns.store.clone(), "fresh/object").unwrap(),
                    constraints: UploadConstraints::new(100, None, None).unwrap(),
                    revision: Uuid::from_u128(5),
                    deadline: ts(1500),
                },
                state: IntentState::Selected,
            };
            let staged = StagedSnapshot {
                intent: intent.id().clone(),
                attempt: id(&domain, 2, 4),
                namespace: ns.id.clone(),
                configuration_revision: ns.configuration_revision,
                claim: Claim::new(
                    id(&domain, 3, 5),
                    VersionRef::new(ns.store.clone(), "fresh/object", "version").unwrap(),
                    Uuid::from_u128(5),
                    ts(2500),
                    clock(1000),
                    &policy(),
                )
                .unwrap(),
            };
            Self { ns, intent, staged }
        }
        fn renewal(&self) -> Renewal {
            Renewal::new(self.staged.claim.revision(), ts(3500), Uuid::from_u128(6)).unwrap()
        }
        fn renew(&self, at: ClockObservation) -> Result<Vec<TransactWriteItem>, UploadError> {
            renew_actions(
                TABLE,
                &self.ns.domain,
                &self.staged,
                &self.intent,
                &self.ns,
                &self.renewal(),
                at,
                &policy(),
            )
        }
        fn cancel(&self) -> Result<Vec<TransactWriteItem>, UploadError> {
            cancel_actions(TABLE, &self.ns.domain, &self.staged, &self.intent)
        }
        fn control(&self) -> Item {
            control_row(
                &self.ns.domain,
                self.staged.claim.id().scope().incarnation(),
                self.staged.claim.id().scope().generation(),
                0,
                0,
            )
        }
        fn revoke(&self, at: ClockObservation) -> Result<Vec<TransactWriteItem>, UploadError> {
            self.revoke_with(self.control(), at)
        }
        fn revoke_with(
            &self,
            control: Item,
            at: ClockObservation,
        ) -> Result<Vec<TransactWriteItem>, UploadError> {
            revoke_actions(
                TABLE,
                &self.ns.domain,
                control,
                &self.staged,
                &self.intent,
                at,
                &policy(),
            )
        }
    }
    fn control_row(
        domain: &DomainId,
        incarnation: Uuid,
        current: u64,
        closed: u64,
        compacted: u64,
    ) -> Item {
        let mut row = control_key(domain);
        row.extend([
            ("domain".into(), s(domain.as_str())),
            ("incarnation".into(), s(incarnation.to_string())),
            ("current_generation".into(), n(current)),
            ("closed_through".into(), n(closed)),
            ("compacted_through".into(), n(compacted)),
            ("revision".into(), n(1)),
        ]);
        row
    }
    fn assert_old_put(action: &TransactWriteItem, old: &Item) {
        let put = action.put.as_ref().unwrap();
        let expected = condition(old);
        assert_eq!(put.table_name(), TABLE);
        assert_eq!(
            put.condition_expression(),
            Some(expected.expression.as_str())
        );
        assert_eq!(put.expression_attribute_names(), Some(&expected.names));
        assert_eq!(put.expression_attribute_values(), Some(&expected.values));
        assert_eq!(item_key(put.item()), item_key(old));
        for field in ["pk", "sk", "state", "revision", "deadline"] {
            assert!(expected.names.values().any(|n| n == field));
        }
        assert!(expected.expression.len() < 4096);
        check_size(put.item()).unwrap();
    }
    fn staged_from(action: &TransactWriteItem, id: &ResourceId) -> StagedSnapshot {
        decode_staged(id, action.put.as_ref().unwrap().item()).unwrap()
    }
    fn assert_count(action: &TransactWriteItem, f: &Fixture, delta: i64) {
        assert_eq!(
            action,
            &version_count_action(
                TABLE,
                &f.ns.domain,
                &f.staged.version().fingerprint(),
                f.staged.version(),
                0,
                delta
            )
            .unwrap()
        );
        if delta < 0 {
            let update = action.update.as_ref().unwrap();
            assert_eq!(update.update_expression(), "ADD #claims :claim_delta");
            assert_eq!(
                update.expression_attribute_values().unwrap()[":claim_delta"],
                AttributeValue::N("-1".into())
            );
            assert!(
                update
                    .condition_expression()
                    .unwrap()
                    .contains("#claims >= :minimum_claims")
            );
            assert_eq!(
                update.expression_attribute_values().unwrap()[":minimum_claims"],
                n(1)
            );
        } else {
            assert!(action.condition_check.is_some());
        }
    }

    #[test]
    fn renewal_guards_four_rows_and_ignores_old_upload_deadline() {
        let f = Fixture::new();
        assert!(f.intent.deadline() < ts(2000));
        let actions = f.renew(clock(2000)).unwrap();
        assert_eq!(actions.len(), 4); // journal adds admission + outcome
        assert_eq!(actions[0], namespace_guard(TABLE, &f.ns).unwrap());
        assert_eq!(
            actions[1],
            guard(TABLE, encode_intent(&f.intent).unwrap()).unwrap()
        );
        assert_old_put(&actions[2], &encode_staged(&f.staged).unwrap());
        let next = staged_from(&actions[2], f.intent.id());
        assert_eq!(next.claim.deadline(), ts(3500));
        assert_eq!(next.claim.revision(), f.renewal().new_revision());
        assert_eq!(next.claim.state(), ClaimState::Active);
        assert_count(&actions[3], &f, 0);
        // Different sampling time does not shift the frozen deadline or request.
        assert_eq!(actions, f.renew(clock(2100)).unwrap());
        assert_eq!(actions.iter().filter(|a| a.put.is_some()).count(), 1);
        assert!(
            actions
                .iter()
                .all(|a| a.update.is_none() && a.delete.is_none())
        );
        assert_eq!(f.intent.deadline(), ts(1500));
        assert_eq!(f.staged.claim.deadline(), ts(2500));
    }

    #[test]
    fn cancellation_releases_only_one_claim_with_full_old_rows_and_no_clock() {
        let mut f = Fixture::new();
        f.intent.admission.deadline = ts(0);
        let mut row = encode_staged(&f.staged).unwrap();
        row.insert("deadline".into(), n(0));
        f.staged = decode_staged(f.intent.id(), &row).unwrap();
        let actions = f.cancel().unwrap();
        assert_eq!(actions.len(), 3); // journal adds admission + outcome
        assert_old_put(&actions[0], &encode_intent(&f.intent).unwrap());
        assert_old_put(&actions[1], &row);
        let cancelled =
            decode_intent(f.intent.id(), actions[0].put.as_ref().unwrap().item()).unwrap();
        assert_eq!(cancelled.state, IntentState::Cancelled);
        assert_eq!(cancelled.revision(), f.intent.revision());
        let released = staged_from(&actions[1], f.intent.id());
        assert_eq!(released.claim.state(), ClaimState::Released);
        assert_eq!(released.claim.revision(), f.staged.claim.revision());
        assert_eq!(released.claim.deadline(), ts(0));
        assert_count(&actions[2], &f, -1);
        assert_eq!(actions, f.cancel().unwrap());
        // No namespace/attempt row, reference edge, use pin, deletion or create.
        assert!(
            actions
                .iter()
                .all(|a| a.condition_check.is_none() && a.delete.is_none())
        );
    }

    #[test]
    fn natural_revocation_has_four_native_actions_no_receipt_or_open_generation_guard() {
        let f = Fixture::new();
        let actions = f.revoke(clock(2505)).unwrap();
        assert_eq!(actions.len(), 4);
        let control = actions[0].condition_check.as_ref().unwrap();
        assert_eq!(control.table_name(), TABLE);
        assert_eq!(control.key(), &control_key(&f.ns.domain));
        assert_eq!(actions[0], guard(TABLE, f.control()).unwrap());
        assert!(control.condition_expression().len() < 4096);
        for field in [
            "incarnation",
            "current_generation",
            "closed_through",
            "compacted_through",
            "revision",
        ] {
            assert!(
                control
                    .expression_attribute_names()
                    .unwrap()
                    .values()
                    .any(|n| n == field)
            );
        }
        assert_old_put(&actions[1], &encode_intent(&f.intent).unwrap());
        assert_old_put(&actions[2], &encode_staged(&f.staged).unwrap());
        assert_eq!(
            text(actions[1].put.as_ref().unwrap().item(), "state").unwrap(),
            "CANCELLED"
        );
        let revoked = staged_from(&actions[2], f.intent.id());
        assert_eq!(revoked.claim.state(), ClaimState::Revoked);
        assert_eq!(revoked.claim.id(), f.staged.claim.id());
        assert_eq!(revoked.claim.revision(), f.staged.claim.revision());
        assert_count(&actions[3], &f, -1);
        assert_eq!(actions, f.revoke(clock(3000)).unwrap());
        assert!(actions.iter().all(|a| a.delete.is_none()));
    }

    #[test]
    fn revocation_accepts_valid_rotation_and_closed_generations_but_freezes_control() {
        let f = Fixture::new();
        for row in [
            control_row(&f.ns.domain, f.ns.incarnation, 4, 3, 3),
            // Claim generation 3 exceeds current 1 in a DIFFERENT incarnation.
            control_row(&f.ns.domain, Uuid::from_u128(99), 1, 0, 0),
        ] {
            let actions = f.revoke_with(row.clone(), clock(2505)).unwrap();
            assert_eq!(actions.len(), 4);
            assert_eq!(actions[0], guard(TABLE, row.clone()).unwrap());
            assert_old_put(&actions[1], &encode_intent(&f.intent).unwrap());
            assert_old_put(&actions[2], &encode_staged(&f.staged).unwrap());
            assert_count(&actions[3], &f, -1);
            for (field, value) in [
                ("incarnation", s(Uuid::from_u128(100).to_string())),
                ("incarnation", s("not-a-uuid")),
                ("current_generation", n(5)),
                ("closed_through", n(0)),
                ("compacted_through", n(0)),
                ("revision", n(2)),
            ] {
                let mut changed = row.clone();
                changed.insert(field.into(), value);
                if changed != row {
                    // Compare native predicates, not a fake expression evaluator.
                    assert_ne!(actions[0], guard(TABLE, changed).unwrap());
                }
            }
        }
        assert!(matches!(
            f.revoke_with(
                control_row(&f.ns.domain, f.ns.incarnation, 2, 0, 0),
                clock(2505)
            ),
            Err(UploadError::Binding)
        ));
    }

    #[test]
    fn revocation_rejects_malformed_control_before_building_writes() {
        let f = Fixture::new();
        for (field, value) in [
            ("incarnation", s("arbitrary-different-string")),
            ("incarnation", s(Uuid::nil().to_string())),
            ("incarnation", s("00000000-0000-0000-0000-00000000000A")),
            ("domain", s("other-domain")),
            ("pk", s("other-key")),
            ("current_generation", n(0)),
            ("closed_through", n(3)),
            ("compacted_through", n(1)),
            ("revision", n(0)),
        ] {
            let mut row = f.control();
            row.insert(field.into(), value);
            assert!(
                matches!(
                    f.revoke_with(row, clock(2505)),
                    Err(UploadError::Journal(JournalError::Corrupt(_)))
                ),
                "accepted {field}"
            );
        }
        for field in [
            "domain",
            "incarnation",
            "current_generation",
            "closed_through",
            "compacted_through",
            "revision",
        ] {
            let mut row = f.control();
            row.remove(field);
            assert!(
                matches!(
                    f.revoke_with(row, clock(2505)),
                    Err(UploadError::Journal(JournalError::Corrupt(_)))
                ),
                "accepted missing {field}"
            );
        }
    }

    #[test]
    fn expiry_lower_bound_gap_and_untrusted_clock_fail_closed() {
        let f = Fixture::new();
        assert!(matches!(
            f.revoke(clock(2504)),
            Err(UploadError::Claim(ClaimError::NotRevocable))
        ));
        assert!(f.revoke(clock(2505)).is_ok());
        // upper == deadline stops renewal, but does not authorize revocation.
        assert!(matches!(
            f.renew(clock(2495)),
            Err(UploadError::Claim(ClaimError::Expired))
        ));
        assert!(matches!(
            f.revoke(clock(2495)),
            Err(UploadError::Claim(ClaimError::NotRevocable))
        ));
        assert!(matches!(
            f.renew(clock(2445)),
            Err(UploadError::Claim(ClaimError::RenewalMargin))
        ));
        for clock in [
            ClockObservation::untrusted(ts(3000), 0),
            ClockObservation::trusted(ts(3000), 11).unwrap(),
        ] {
            assert!(f.revoke(clock).is_err());
            assert!(f.renew(clock).is_err());
        }
        for (deadline, revision, expected) in [
            (3500, 99, ClaimError::RevisionMismatch),
            (2500, 5, ClaimError::DeadlineNotForward),
            (4006, 5, ClaimError::LifetimeExceeded),
        ] {
            let renewal =
                Renewal::new(Uuid::from_u128(revision), ts(deadline), Uuid::from_u128(6)).unwrap();
            assert!(
                matches!(renew_actions(TABLE, &f.ns.domain, &f.staged, &f.intent, &f.ns, &renewal, clock(2000), &policy()), Err(UploadError::Claim(error)) if error == expected)
            );
        }
    }

    #[test]
    fn terminal_claims_and_intents_never_renew_or_decrement_twice() {
        for state in [ClaimState::Released, ClaimState::Revoked] {
            let mut f = Fixture::new();
            if state == ClaimState::Released {
                f.staged.claim.release(f.staged.claim.revision()).unwrap();
            } else {
                f.staged
                    .claim
                    .revoke(f.staged.claim.revision(), clock(2505), &policy())
                    .unwrap();
            }
            assert!(matches!(
                f.renew(clock(2000)),
                Err(UploadError::Claim(ClaimError::Terminal))
            ));
            assert!(matches!(f.cancel(), Err(UploadError::Terminal)));
            assert!(matches!(
                f.revoke(ClockObservation::untrusted(ts(3000), 0)),
                Err(UploadError::Terminal)
            ));
        }
        for state in [
            IntentState::Open,
            IntentState::Consumed,
            IntentState::Cancelled,
        ] {
            let mut f = Fixture::new();
            f.intent.state = state;
            assert!(matches!(f.renew(clock(2000)), Err(UploadError::Terminal)));
            assert!(matches!(f.cancel(), Err(UploadError::Terminal)));
            assert!(matches!(f.revoke(clock(2505)), Err(UploadError::Terminal)));
        }
    }

    #[test]
    fn renewal_cancel_revoke_races_contend_on_identical_full_staged_row() {
        let f = Fixture::new();
        let renewed = f.renew(clock(2000)).unwrap();
        let cancelled = f.cancel().unwrap();
        let revoked = f.revoke(clock(2505)).unwrap();
        for action in [&renewed[2], &cancelled[1], &revoked[2]] {
            assert_old_put(action, &encode_staged(&f.staged).unwrap());
        }
        let next = staged_from(&renewed[2], f.intent.id());
        // Native predicates above compare the OLD revision/deadline/state. The
        // assertions compare their shape, not a made-up DynamoDB interpreter.
        assert_ne!(
            encode_staged(&next).unwrap(),
            encode_staged(&f.staged).unwrap()
        );
        assert!(matches!(
            revoke_actions(
                TABLE,
                &f.ns.domain,
                f.control(),
                &next,
                &f.intent,
                clock(2505),
                &policy()
            ),
            Err(UploadError::Claim(ClaimError::NotRevocable))
        ));
        assert!(matches!(
            renew_actions(
                TABLE,
                &f.ns.domain,
                &next,
                &f.intent,
                &f.ns,
                &f.renewal(),
                clock(2100),
                &policy()
            ),
            Err(UploadError::Claim(ClaimError::RevisionMismatch))
        ));
        let next_revoke = revoke_actions(
            TABLE,
            &f.ns.domain,
            f.control(),
            &next,
            &f.intent,
            clock(3505),
            &policy(),
        )
        .unwrap();
        assert_old_put(&next_revoke[2], &encode_staged(&next).unwrap());
        assert_ne!(
            next_revoke[2]
                .put
                .as_ref()
                .unwrap()
                .expression_attribute_values(),
            revoked[2]
                .put
                .as_ref()
                .unwrap()
                .expression_attribute_values()
        );
        let mut pure = next.claim.clone();
        assert!(matches!(
            pure.revoke(f.staged.claim.revision(), clock(3505), &policy()),
            Err(ClaimError::RevisionMismatch)
        ));
        assert!(matches!(
            pure.release(f.staged.claim.revision()),
            Err(ClaimError::RevisionMismatch)
        ));
    }

    #[test]
    fn selected_binding_rejects_cross_identity_namespace_config_and_target() {
        for field in [
            "domain",
            "intent",
            "incarnation",
            "generation",
            "namespace",
            "config",
            "store",
            "key",
            "attempt",
            "claim",
        ] {
            let mut f = Fixture::new();
            match field {
                "domain" => f.ns.domain = DomainId::new("other").unwrap(),
                "intent" => f.intent.admission.id = id(&f.ns.domain, 1, 99),
                "incarnation" => {
                    f.staged.attempt = ResourceId::new(
                        Scope::new(f.ns.domain.clone(), Uuid::from_u128(99), 2).unwrap(),
                        Uuid::from_u128(4),
                    )
                    .unwrap()
                }
                "generation" => f.staged.attempt = id(&f.ns.domain, 4, 4),
                "namespace" => f.staged.namespace = NamespaceId::new("other").unwrap(),
                "config" => f.staged.configuration_revision = Uuid::from_u128(99),
                "store" => f.intent.admission.target.store = StoreId::new("other").unwrap(),
                "key" => f.intent.admission.target.key = "fresh/other".into(),
                "attempt" => f.staged.attempt = id(&DomainId::new("other").unwrap(), 2, 4),
                "claim" => {
                    f.staged.claim = Claim::new(
                        id(&DomainId::new("other").unwrap(), 3, 5),
                        f.staged.version().clone(),
                        Uuid::from_u128(5),
                        ts(2500),
                        clock(1000),
                        &policy(),
                    )
                    .unwrap()
                }
                _ => unreachable!(),
            }
            assert!(f.renew(clock(2000)).is_err(), "renew accepted {field}");
            assert!(f.cancel().is_err(), "cancel accepted {field}");
            assert!(f.revoke(clock(2505)).is_err(), "revoke accepted {field}");
        }
        for field in ["namespace", "config", "incarnation", "prefix", "store"] {
            let mut f = Fixture::new();
            match field {
                "namespace" => f.ns.id = NamespaceId::new("other").unwrap(),
                "config" => f.ns.configuration_revision = Uuid::from_u128(99),
                "incarnation" => f.ns.incarnation = Uuid::from_u128(99),
                "prefix" => f.ns.key_prefix = "outside/".into(),
                "store" => f.ns.store = StoreId::new("other").unwrap(),
                _ => unreachable!(),
            }
            assert!(
                f.renew(clock(2000)).is_err(),
                "renew accepted namespace {field}"
            );
            // Cleanup does not depend on the current namespace configuration.
            assert!(f.cancel().is_ok());
            assert!(f.revoke(clock(2505)).is_ok());
        }
    }

    #[test]
    fn maximal_records_still_fit_for_renew_release_and_revoke() {
        let mut f = Fixture::new();
        f.ns.domain = DomainId::new("d".repeat(128)).unwrap();
        f.ns.id = NamespaceId::new("n".repeat(128)).unwrap();
        f.ns.store = StoreId::new("s".repeat(128)).unwrap();
        f.ns.key_prefix = "\\\"".repeat(512);
        f.ns.bucket = "b".repeat(63);
        f.ns.region = "r".repeat(64);
        f.ns.mpu_max_age_ms = TimestampMs::MAX.as_millis() / 2;
        f.ns.mpu_lifecycle_margin_ms = TimestampMs::MAX.as_millis() / 2;
        f.intent.admission.id = id(&f.ns.domain, u64::MAX, 3);
        f.intent.admission.namespace = f.ns.id.clone();
        f.intent.admission.target =
            UploadTarget::new(f.ns.store.clone(), f.ns.key_prefix.clone()).unwrap();
        f.intent.admission.constraints =
            UploadConstraints::new(i64::MAX as u64, Some(i64::MAX as u64), Some([255; 32]))
                .unwrap();
        f.intent.admission.deadline = TimestampMs::MAX;
        f.staged.intent = f.intent.id().clone();
        f.staged.attempt = id(&f.ns.domain, u64::MAX, 4);
        f.staged.namespace = f.ns.id.clone();
        f.staged.claim = Claim::new(
            id(&f.ns.domain, u64::MAX, 5),
            VersionRef::new(
                f.ns.store.clone(),
                f.ns.key_prefix.clone(),
                "v".repeat(1024),
            )
            .unwrap(),
            Uuid::from_u128(5),
            ts(2500),
            clock(1000),
            &policy(),
        )
        .unwrap();
        let mut row = encode_staged(&f.staged).unwrap();
        row.insert("deadline".into(), n(TimestampMs::MAX.as_millis() - 100));
        f.staged = decode_staged(f.intent.id(), &row).unwrap();
        let renewal = Renewal::new(
            f.staged.claim.revision(),
            TimestampMs::MAX,
            Uuid::from_u128(6),
        )
        .unwrap();
        let renewal_actions = renew_actions(
            TABLE,
            &f.ns.domain,
            &f.staged,
            &f.intent,
            &f.ns,
            &renewal,
            clock(TimestampMs::MAX.as_millis() - 300),
            &ClaimPolicy::new(295, 50, 10).unwrap(),
        )
        .unwrap();
        let cancelled = f.cancel().unwrap();
        let revoked = f.revoke(clock(TimestampMs::MAX.as_millis() - 95)).unwrap();
        for action in renewal_actions.iter().chain(&cancelled).chain(&revoked) {
            if let Some(put) = &action.put {
                check_size(put.item()).unwrap();
                assert!(put.condition_expression().unwrap().len() < 4096);
                assert_eq!(
                    put.item().get("revision").unwrap().as_s().unwrap().len(),
                    36
                );
            }
        }
        assert_eq!(
            staged_from(&renewal_actions[2], f.intent.id())
                .claim
                .deadline(),
            TimestampMs::MAX
        );
        assert_eq!(
            staged_from(&cancelled[1], f.intent.id()).claim.state(),
            ClaimState::Released
        );
        assert_eq!(
            staged_from(&revoked[2], f.intent.id()).claim.state(),
            ClaimState::Revoked
        );
        row.insert("version_id".into(), s("v".repeat(4000)));
        assert!(matches!(
            decode_staged(f.intent.id(), &row),
            Err(UploadError::RecordSize)
        ));
    }

    #[test]
    fn native_error_classification_never_infers_commit_or_rollback() {
        let error = |code: &str| {
            TransactWriteItemsError::TransactionCanceledException(
                TransactionCanceledException::builder()
                    .cancellation_reasons(CancellationReason::builder().code(code).build())
                    .build(),
            )
        };
        assert!(matches!(
            revocation_error(Some(&error("ConditionalCheckFailed")), "conditional".into()),
            UploadError::StagedConflict
        ));
        for code in [
            "None",
            "TransactionConflict",
            "ValidationError",
            "ThrottlingError",
        ] {
            assert!(matches!(
                revocation_error(Some(&error(code)), code.into()),
                UploadError::StagedOutcomeUnknown(_)
            ));
        }
        assert!(matches!(
            revocation_error(None, "transport response lost".into()),
            UploadError::StagedOutcomeUnknown(_)
        ));
        let no_reasons = TransactWriteItemsError::TransactionCanceledException(
            TransactionCanceledException::builder().build(),
        );
        assert!(matches!(
            revocation_error(Some(&no_reasons), "no reasons".into()),
            UploadError::StagedOutcomeUnknown(_)
        ));
    }
}
