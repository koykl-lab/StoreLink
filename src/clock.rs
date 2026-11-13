//! Explicit, bounded UTC milliseconds. No implicit wall-clock reads or defaults.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ClockError {
    #[error("timestamp or arithmetic result exceeds i64::MAX milliseconds")]
    Overflow,
    #[error("clock observation is untrusted")]
    Untrusted,
    #[error("clock uncertainty exceeds configured maximum")]
    ExcessiveUncertainty,
}

/// Nonnegative UTC milliseconds, bounded for signed database representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct TimestampMs(u64);

impl TimestampMs {
    pub const MAX: Self = Self(i64::MAX as u64);
    pub const ZERO: Self = Self(0);

    pub fn new(value: u64) -> Result<Self, ClockError> {
        if value > Self::MAX.0 {
            return Err(ClockError::Overflow);
        }
        Ok(Self(value))
    }

    pub fn as_millis(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, milliseconds: u64) -> Result<Self, ClockError> {
        Self::new(
            self.0
                .checked_add(milliseconds)
                .ok_or(ClockError::Overflow)?,
        )
    }
}

impl TryFrom<u64> for TimestampMs {
    type Error = ClockError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TimestampMs> for u64 {
    fn from(value: TimestampMs) -> Self {
        value.0
    }
}

/// An observation supplied by a trusted clock integration, NOT a remotely
/// deserializable assertion of clock health. The constructor cannot itself prove
/// clock synchronization; the host owns that trust/uncertainty measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockObservation {
    now: TimestampMs,
    uncertainty_ms: u64,
    trusted: bool,
}

impl ClockObservation {
    pub fn trusted(now: TimestampMs, uncertainty_ms: u64) -> Result<Self, ClockError> {
        now.checked_add(uncertainty_ms)?;
        Ok(Self {
            now,
            uncertainty_ms,
            trusted: true,
        })
    }

    pub fn untrusted(now: TimestampMs, uncertainty_ms: u64) -> Self {
        Self {
            now,
            uncertainty_ms,
            trusted: false,
        }
    }

    pub fn now(self) -> TimestampMs {
        self.now
    }
    pub fn uncertainty_ms(self) -> u64 {
        self.uncertainty_ms
    }
    pub fn is_trusted(self) -> bool {
        self.trusted
    }

    pub fn validate(self, maximum_uncertainty_ms: u64) -> Result<(), ClockError> {
        if !self.trusted {
            return Err(ClockError::Untrusted);
        }
        if self.uncertainty_ms > maximum_uncertainty_ms {
            return Err(ClockError::ExcessiveUncertainty);
        }
        self.now.checked_add(self.uncertainty_ms)?;
        Ok(())
    }

    /// Acquisition/use must still be valid at this conservative upper bound.
    pub fn upper_bound(self) -> Result<TimestampMs, ClockError> {
        self.validate(TimestampMs::MAX.as_millis())?;
        self.now.checked_add(self.uncertainty_ms)
    }

    /// Revocation is eligible only at/after the deadline at this lower bound.
    /// Saturation at zero is conservative in the nonnegative timestamp domain.
    pub fn lower_bound(self) -> Result<TimestampMs, ClockError> {
        self.validate(TimestampMs::MAX.as_millis())?;
        Ok(TimestampMs(self.now.0.saturating_sub(self.uncertainty_ms)))
    }
}
