//! Finite claim records and guarded, in-memory state transitions.
//!
//! These are pure primitives, not database operations or proof of publication
//! authority. Adapters must atomically guard the record/revision/fence and apply
//! the reported accounting change in the SAME metadata transaction. Deserializing
//! a record does not authorize use; revalidate authoritative state and time.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::clock::{ClockError, ClockObservation, TimestampMs};
use crate::identity::{IdentityError, ResourceId, VersionRef, check_uuid};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClaimError {
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error(
        "policy requires positive lifetime and renewal margin, margin < lifetime, and bounded durations"
    )]
    InvalidPolicy,
    #[error("claim is terminal")]
    Terminal,
    #[error("expected revision does not match")]
    RevisionMismatch,
    #[error("new revision must differ from expected revision")]
    ReusedRevision,
    #[error("claim is no longer valid at the conservative clock upper bound")]
    Expired,
    #[error("insufficient time remains for the configured renewal margin")]
    RenewalMargin,
    #[error("proposed deadline must move forward")]
    DeadlineNotForward,
    #[error("deadline exceeds the configured lifetime horizon")]
    LifetimeExceeded,
    #[error("deadline has not elapsed at the conservative clock lower bound")]
    NotRevocable,
}

/// Explicit host policy; there are deliberately no production/test defaults.
///
/// `renewal_margin_ms` is the minimum remaining protection required to initiate
/// renewal (strictly more than the margin must remain). New claims must also
/// start with more than this margin. Ordinary use requires only positive validity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ClaimPolicyWire")]
pub struct ClaimPolicy {
    max_lifetime_ms: u64,
    renewal_margin_ms: u64,
    max_clock_uncertainty_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimPolicyWire {
    max_lifetime_ms: u64,
    renewal_margin_ms: u64,
    max_clock_uncertainty_ms: u64,
}

impl TryFrom<ClaimPolicyWire> for ClaimPolicy {
    type Error = ClaimError;

    fn try_from(value: ClaimPolicyWire) -> Result<Self, Self::Error> {
        Self::new(
            value.max_lifetime_ms,
            value.renewal_margin_ms,
            value.max_clock_uncertainty_ms,
        )
    }
}

impl ClaimPolicy {
    pub fn new(
        max_lifetime_ms: u64,
        renewal_margin_ms: u64,
        max_clock_uncertainty_ms: u64,
    ) -> Result<Self, ClaimError> {
        if max_lifetime_ms == 0
            || max_lifetime_ms > TimestampMs::MAX.as_millis()
            || renewal_margin_ms == 0
            || renewal_margin_ms >= max_lifetime_ms
            || max_clock_uncertainty_ms > TimestampMs::MAX.as_millis()
        {
            return Err(ClaimError::InvalidPolicy);
        }
        Ok(Self {
            max_lifetime_ms,
            renewal_margin_ms,
            max_clock_uncertainty_ms,
        })
    }

    pub fn max_lifetime_ms(self) -> u64 {
        self.max_lifetime_ms
    }
    pub fn renewal_margin_ms(self) -> u64 {
        self.renewal_margin_ms
    }
    pub fn max_clock_uncertainty_ms(self) -> u64 {
        self.max_clock_uncertainty_ms
    }

    fn upper_bound(self, clock: ClockObservation) -> Result<TimestampMs, ClaimError> {
        clock.validate(self.max_clock_uncertainty_ms)?;
        Ok(clock.upper_bound()?)
    }

    fn check_deadline(self, deadline: TimestampMs, upper: TimestampMs) -> Result<(), ClaimError> {
        if deadline <= upper {
            return Err(ClaimError::Expired);
        }
        if deadline > upper.checked_add(self.max_lifetime_ms)? {
            return Err(ClaimError::LifetimeExceeded);
        }
        Ok(())
    }

    fn check_margin(self, deadline: TimestampMs, upper: TimestampMs) -> Result<(), ClaimError> {
        if deadline <= upper.checked_add(self.renewal_margin_ms)? {
            return Err(ClaimError::RenewalMargin);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClaimState {
    Active,
    Revoked,
    Released,
}

/// Frozen renewal inputs. Replays never recompute a deadline from the retry time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RenewalWire")]
pub struct Renewal {
    expected_revision: Uuid,
    proposed_deadline: TimestampMs,
    new_revision: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenewalWire {
    expected_revision: Uuid,
    proposed_deadline: TimestampMs,
    new_revision: Uuid,
}

impl TryFrom<RenewalWire> for Renewal {
    type Error = ClaimError;

    fn try_from(value: RenewalWire) -> Result<Self, Self::Error> {
        Self::new(
            value.expected_revision,
            value.proposed_deadline,
            value.new_revision,
        )
    }
}

impl Renewal {
    pub fn new(
        expected_revision: Uuid,
        proposed_deadline: TimestampMs,
        new_revision: Uuid,
    ) -> Result<Self, ClaimError> {
        check_uuid(expected_revision, "expected revision")?;
        check_uuid(new_revision, "new revision")?;
        if new_revision == expected_revision {
            return Err(ClaimError::ReusedRevision);
        }
        Ok(Self {
            expected_revision,
            proposed_deadline,
            new_revision,
        })
    }

    pub fn expected_revision(&self) -> Uuid {
        self.expected_revision
    }
    pub fn proposed_deadline(&self) -> TimestampMs {
        self.proposed_deadline
    }
    pub fn new_revision(&self) -> Uuid {
        self.new_revision
    }
}

/// Describes a conditional terminal transition, not a caller-settable counter
/// delta. A retry that observes the same terminal record must not decrement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalTransition {
    Changed,
    AlreadyTerminal,
}

impl TerminalTransition {
    pub fn accounting_must_change(self) -> bool {
        self == Self::Changed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ClaimWire")]
pub struct Claim {
    id: ResourceId,
    version: VersionRef,
    state: ClaimState,
    revision: Uuid,
    deadline: TimestampMs,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimWire {
    id: ResourceId,
    version: VersionRef,
    state: ClaimState,
    revision: Uuid,
    deadline: TimestampMs,
}

impl TryFrom<ClaimWire> for Claim {
    type Error = ClaimError;

    fn try_from(value: ClaimWire) -> Result<Self, Self::Error> {
        check_uuid(value.revision, "revision")?;
        // Persisted records may legitimately be expired or terminal. Structural
        // validity is not current authority; validate_use/renew check it anew.
        Ok(Self {
            id: value.id,
            version: value.version,
            state: value.state,
            revision: value.revision,
            deadline: value.deadline,
        })
    }
}

impl Claim {
    /// Builds a finite ACTIVE record. The adapter still must verify the backing
    /// authority, exact READY version, creation admission and record absence.
    pub fn new(
        id: ResourceId,
        version: VersionRef,
        revision: Uuid,
        deadline: TimestampMs,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<Self, ClaimError> {
        check_uuid(revision, "revision")?;
        let upper = policy.upper_bound(clock)?;
        policy.check_deadline(deadline, upper)?;
        policy.check_margin(deadline, upper)?;
        Ok(Self {
            id,
            version,
            state: ClaimState::Active,
            revision,
            deadline,
        })
    }

    pub fn id(&self) -> &ResourceId {
        &self.id
    }
    pub fn version(&self) -> &VersionRef {
        &self.version
    }
    pub fn state(&self) -> ClaimState {
        self.state
    }
    pub fn revision(&self) -> Uuid {
        self.revision
    }
    pub fn deadline(&self) -> TimestampMs {
        self.deadline
    }

    pub fn validate_use(
        &self,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<(), ClaimError> {
        if self.state != ClaimState::Active {
            return Err(ClaimError::Terminal);
        }
        if self.deadline <= policy.upper_bound(clock)? {
            return Err(ClaimError::Expired);
        }
        Ok(())
    }

    /// Checks everything before mutating. UUID revisions must never be reused
    /// across a resource's history; the adapter/issuer owns that global uniqueness
    /// obligation (a bounded record cannot detect every historical UUID).
    pub fn renew(
        &mut self,
        renewal: &Renewal,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<(), ClaimError> {
        if self.state != ClaimState::Active {
            return Err(ClaimError::Terminal);
        }
        self.match_revision(renewal.expected_revision)?;
        let upper = policy.upper_bound(clock)?;
        if self.deadline <= upper {
            return Err(ClaimError::Expired);
        }
        policy.check_margin(self.deadline, upper)?;
        if renewal.proposed_deadline <= self.deadline {
            return Err(ClaimError::DeadlineNotForward);
        }
        policy.check_deadline(renewal.proposed_deadline, upper)?;
        self.deadline = renewal.proposed_deadline;
        self.revision = renewal.new_revision;
        Ok(())
    }

    fn match_revision(&self, expected: Uuid) -> Result<(), ClaimError> {
        check_uuid(expected, "expected revision")?;
        if self.revision != expected {
            return Err(ClaimError::RevisionMismatch);
        }
        Ok(())
    }

    /// Voluntary release does not depend on clock health. Preserve the last
    /// active revision so exact terminal repeats can be recognized without a
    /// second accounting change. A different terminal cause remains terminal.
    pub fn release(&mut self, expected_revision: Uuid) -> Result<TerminalTransition, ClaimError> {
        self.match_revision(expected_revision)?;
        if self.state != ClaimState::Active {
            return Ok(TerminalTransition::AlreadyTerminal);
        }
        self.state = ClaimState::Released;
        Ok(TerminalTransition::Changed)
    }

    pub fn revoke(
        &mut self,
        expected_revision: Uuid,
        clock: ClockObservation,
        policy: &ClaimPolicy,
    ) -> Result<TerminalTransition, ClaimError> {
        self.match_revision(expected_revision)?;
        // An already terminal record needs no new time-based authorization and
        // cannot change accounting, even if the current clock is unhealthy.
        if self.state != ClaimState::Active {
            return Ok(TerminalTransition::AlreadyTerminal);
        }
        clock.validate(policy.max_clock_uncertainty_ms)?;
        if clock.lower_bound()? < self.deadline {
            return Err(ClaimError::NotRevocable);
        }
        self.state = ClaimState::Revoked;
        Ok(TerminalTransition::Changed)
    }
}
