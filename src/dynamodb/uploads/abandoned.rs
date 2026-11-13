//! Natural terminal transitions for abandoned OPEN uploads and ACTIVE attempts.
//! No new receipts, IDs, S3 calls or early multipart aborts. Discovery/scheduling
//! remains separate; terminal rows can later be pruned behind compaction fences.
use super::*;
use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;

impl CommandJournal {
    /// Cancel an OPEN intent only after its conservative expiry cutoff. A
    /// SELECTED intent must instead release its independently counted staging
    /// claim through revoke_expired_staged; this API cannot bypass accounting.
    pub async fn cancel_expired_open_intent(
        &self,
        intent: &IntentSnapshot,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<(), UploadError> {
        if intent.state != IntentState::Open {
            return Err(UploadError::Terminal);
        }
        expired(intent.deadline(), clock, policy)?;
        let control = self
            .read_upload_item(control_key(self.domain_id()))
            .await?
            .ok_or(JournalError::MissingControl)?;
        let mut next = intent.clone();
        next.state = IntentState::Cancelled;
        let actions = terminal_plan(
            self.coordination_arn(),
            self.domain_id(),
            control,
            intent.id(),
            encode_intent(intent)?,
            encode_intent(&next)?,
        )?;
        self.execute_abandoned_transition(actions).await
    }

    /// Seal an independently expired ACTIVE attempt even if its parent intent
    /// has been pruned. A renewed intent cannot retain an abandoned attempt.
    /// This does not cancel already-issued I/O; provider MPU lifecycle and late
    /// completed-version inventory remain required.
    pub async fn seal_expired_attempt(
        &self,
        attempt: &AttemptSnapshot,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<(), UploadError> {
        if attempt.state != AttemptState::Active {
            return Err(UploadError::Terminal);
        }
        expired(
            attempt.deadline().min(attempt.max_mpu_use_until()),
            clock,
            policy,
        )?;
        let control = self
            .read_upload_item(control_key(self.domain_id()))
            .await?
            .ok_or(JournalError::MissingControl)?;
        let mut next = attempt.clone();
        next.state = AttemptState::Sealed;
        let actions = terminal_plan(
            self.coordination_arn(),
            self.domain_id(),
            control,
            attempt.id(),
            encode_attempt(attempt)?,
            encode_attempt(&next)?,
        )?;
        self.execute_abandoned_transition(actions).await
    }

    async fn execute_abandoned_transition(
        &self,
        actions: Vec<TransactWriteItem>,
    ) -> Result<(), UploadError> {
        self.native_client()
            .transact_write_items()
            .set_transact_items(Some(actions))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|error| {
                if let Some(TransactWriteItemsError::TransactionCanceledException(e)) =
                    error.as_service_error()
                    && e.cancellation_reasons()
                        .iter()
                        .any(|r| r.code() == Some("ConditionalCheckFailed"))
                {
                    UploadError::AbandonedConflict
                } else {
                    UploadError::AbandonedOutcomeUnknown
                }
            })?;
        Ok(())
    }
}
fn expired(
    deadline: TimestampMs,
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<(), UploadError> {
    clock.validate(policy.max_clock_uncertainty_ms())?;
    if clock.lower_bound()? < deadline {
        return Err(ClaimError::NotRevocable.into());
    }
    Ok(())
}
fn terminal_plan(
    table: &str,
    domain: &DomainId,
    control: Item,
    id: &ResourceId,
    old: Item,
    next: Item,
) -> Result<Vec<TransactWriteItem>, UploadError> {
    check_domain(domain, id)?;
    let fence = parse_control(Some(&control), domain)?;
    if fence.incarnation() == id.scope().incarnation() && id.scope().generation() > fence.current()
    {
        return Err(UploadError::Binding);
    }
    Ok(vec![guard(table, control)?, replace(table, old, next)?])
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ts(n: u64) -> TimestampMs {
        TimestampMs::new(n).unwrap()
    }
    fn policy() -> ClaimPolicy {
        ClaimPolicy::new(1000, 20, 5).unwrap()
    }
    fn control(id: &ResourceId, incarnation: Uuid, generation: u64) -> Item {
        let mut row = control_key(id.scope().domain());
        for (name, value) in [
            ("domain", id.scope().domain().as_str().to_owned()),
            ("incarnation", incarnation.to_string()),
        ] {
            row.insert(name.into(), s(value));
        }
        for (name, value) in [
            ("current_generation", generation),
            ("closed_through", 0),
            ("compacted_through", 0),
            ("revision", 1),
        ] {
            row.insert(name.into(), n(value));
        }
        row
    }
    #[test]
    fn expiry_uses_lower_bound_and_fails_untrusted_clock() {
        assert!(
            expired(
                ts(100),
                ClockObservation::trusted(ts(104), 5).unwrap(),
                &policy()
            )
            .is_err()
        );
        expired(
            ts(100),
            ClockObservation::trusted(ts(105), 5).unwrap(),
            &policy(),
        )
        .unwrap();
        assert!(expired(ts(100), ClockObservation::untrusted(ts(1000), 0), &policy()).is_err());
        assert!(
            expired(
                ts(100),
                ClockObservation::trusted(ts(1000), 6).unwrap(),
                &policy()
            )
            .is_err()
        );
    }
    #[test]
    fn terminal_plan_guards_control_and_old_row_without_parent_or_receipt() {
        let id = ResourceId::new(
            Scope::new(DomainId::new("abandoned").unwrap(), Uuid::from_u128(1), 5).unwrap(),
            Uuid::from_u128(2),
        )
        .unwrap();
        let mut old = resource_key("ATTEMPT", &id);
        old.insert("state".into(), s("ACTIVE"));
        old.insert("revision".into(), s(Uuid::from_u128(3).to_string()));
        old.insert("deadline".into(), n(100));
        let mut next = old.clone();
        next.insert("state".into(), s("SEALED"));
        assert!(
            terminal_plan(
                "table",
                id.scope().domain(),
                control(&id, id.scope().incarnation(), 1),
                &id,
                old.clone(),
                next.clone()
            )
            .is_err()
        );
        let actions = terminal_plan(
            "table",
            id.scope().domain(),
            control(&id, Uuid::from_u128(99), 1),
            &id,
            old.clone(),
            next.clone(),
        )
        .unwrap();
        assert_eq!(actions.len(), 2);
        assert!(actions[0].condition_check.is_some());
        let put = actions[1].put.as_ref().unwrap();
        assert_eq!(put.item(), &next);
        let names = put.expression_attribute_names().unwrap();
        for key in ["state", "revision", "deadline"] {
            assert!(names.values().any(|v| v == key));
        }
        assert!(
            put.expression_attribute_values()
                .unwrap()
                .values()
                .any(|v| v == &s("ACTIVE"))
        );
        assert!(
            actions
                .iter()
                .all(|a| a.update.is_none() && a.delete.is_none())
        );
    }
}
