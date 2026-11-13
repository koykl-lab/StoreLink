//! Bounded exact-version reads under independent finite use protection (§7).
//!
//! The external owner alone renews/releases the pin. Dropping this handle drops
//! the body, without metadata writes, detached work or MPU abort. Cancellation
//! of `read` also drops the body and terminalizes the handle. Already issued I/O
//! is not a cancellation fence: uninterrupted I/O after protection loss is NOT
//! promised. Native SDK retries and request timeouts remain host-owned.

use super::QualifiedNamespace;
use crate::{
    claim::{ClaimError, ClaimPolicy},
    clock::{ClockError, ClockObservation},
    dynamodb::{CommandJournal, ProtectionError, UseAuthorization},
    identity::{ResourceId, VersionRef},
};
use std::{pin::Pin, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    time::Instant,
};

const READ_LIMIT: usize = 64 * 1024;
const MAX_CHECK_MS: u64 = 30_000;

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("fresh native use authorization failed")]
    Authorization(#[from] ProtectionError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error(transparent)]
    Claim(#[from] ClaimError),
    #[error("immutable pin, version or owning namespace differs")]
    Binding,
    #[error("invalid inclusive byte window or response Content-Range")]
    Range,
    #[error("S3 returned a missing or different exact VersionId")]
    Version,
    #[error("missing, negative or inconsistent body length")]
    Length,
    #[error("native exact-version S3 request failed")]
    Request,
    #[error("native body read failed")]
    Body,
    #[error("read handle is terminal after failure or cancellation")]
    Closed,
}

/// One checked inclusive window, not an HTTP multi-range parser. S3 must return
/// precisely this window (no clamping to EOF or whole-object fallback).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    start: u64,
    end_inclusive: u64,
}
impl ByteRange {
    pub fn new(start: u64, end_inclusive: u64) -> Result<Self, ReadError> {
        end_inclusive
            .checked_sub(start)
            .and_then(|n| n.checked_add(1))
            .ok_or(ReadError::Range)?;
        Ok(Self {
            start,
            end_inclusive,
        })
    }
    pub fn start(self) -> u64 {
        self.start
    }
    pub fn end_inclusive(self) -> u64 {
        self.end_inclusive
    }
    pub fn length(self) -> u64 {
        self.end_inclusive - self.start + 1
    }
}

fn binding(
    qualified: &QualifiedNamespace,
    auth: &UseAuthorization,
    old: Option<&UseAuthorization>,
) -> Result<(), ReadError> {
    let c = qualified.configuration();
    let ns = auth.namespace();
    if c.id() != ns.id()
        || c.store() != ns.store()
        || c.bucket() != ns.bucket()
        || c.expected_owner() != ns.expected_owner()
        || c.region() != ns.region()
        || c.key_prefix() != ns.key_prefix()
        || c.mpu_max_age_ms() != ns.mpu_max_age_ms()
        || c.mpu_lifecycle_margin_ms() != ns.mpu_lifecycle_margin_ms()
        || c.provider_profile() != ns.provider_profile()
        || old.is_some_and(|old| !old.same_binding(auth))
    {
        return Err(ReadError::Binding);
    }
    Ok(())
}

fn check_range(
    value: Option<&str>,
    range: Option<ByteRange>,
    length: u64,
) -> Result<(), ReadError> {
    let Some(range) = range else {
        return if value.is_none() {
            Ok(())
        } else {
            Err(ReadError::Range)
        };
    };
    let value = value
        .and_then(|s| s.strip_prefix("bytes "))
        .ok_or(ReadError::Range)?;
    let (window, total) = value.split_once('/').ok_or(ReadError::Range)?;
    let (start, end) = window.split_once('-').ok_or(ReadError::Range)?;
    let decimal = |s: &str| -> Result<u64, ReadError> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ReadError::Range);
        }
        s.parse().map_err(|_| ReadError::Range)
    };
    if decimal(start)? != range.start
        || decimal(end)? != range.end_inclusive
        || decimal(total)? <= range.end_inclusive
        || length != range.length()
    {
        return Err(ReadError::Range);
    }
    Ok(())
}

type Body = Pin<Box<dyn AsyncRead + Send>>;

/// An owned native stream plus finite, periodically revalidated authority.
/// Metadata getters describe the GET response, not continuing authorization.
/// Use `prepare_release_use` explicitly when finished; Drop cannot release
/// asynchronously. Crashed owners rely on finite pin expiry and reconciliation.
/// ```compile_fail
/// use storelink::s3::PinnedRead;
/// let _: PinnedRead<'_, fn() -> _> = serde_json::from_str("{}").unwrap();
/// ```
pub struct PinnedRead<'a, F: ?Sized> {
    journal: &'a CommandJournal,
    qualified: &'a QualifiedNamespace,
    id: ResourceId,
    policy: ClaimPolicy,
    sampler: &'a F,
    authorization: UseAuthorization,
    body: Option<Body>,
    length: u64,
    actual: u64,
    eof: bool,
    wake: Instant,
    clock_wake: u64,
}
impl<F> PinnedRead<'_, F>
where
    F: Fn() -> Result<ClockObservation, ClockError> + Sync + ?Sized,
{
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn version(&self) -> &VersionRef {
        self.authorization.version().version()
    }

    fn time(&self) -> Result<ClockObservation, ReadError> {
        let clock = (self.sampler)()?;
        clock.validate(self.policy.max_clock_uncertainty_ms())?;
        Ok(clock)
    }
    fn dispatch(&self) -> Result<(), ReadError> {
        self.authorization
            .pin()
            .claim()
            .validate_use(self.time()?, &self.policy)?;
        Ok(())
    }
    fn schedule(&mut self) -> Result<(), ReadError> {
        let clock = self.time()?;
        self.authorization
            .pin()
            .claim()
            .validate_use(clock, &self.policy)?;
        let upper = clock.upper_bound()?.as_millis();
        let left = self.authorization.valid_until().as_millis() - upper;
        let ms = if left > self.policy.renewal_margin_ms() {
            left - self.policy.renewal_margin_ms()
        } else {
            left
        }
        .min(MAX_CHECK_MS);
        self.wake = Instant::now() + Duration::from_millis(ms);
        self.clock_wake = upper + ms;
        Ok(())
    }
    fn due(&self) -> Result<bool, ReadError> {
        Ok(self.time()?.upper_bound()?.as_millis() >= self.clock_wake
            || Instant::now() >= self.wake)
    }
    async fn refresh(&mut self) -> Result<(), ReadError> {
        let auth = self
            .journal
            .authorize_use(&self.id, self.sampler, &self.policy)
            .await?;
        binding(self.qualified, &auth, Some(&self.authorization))?;
        self.authorization = auth;
        self.schedule()
    }

    /// Returns at most 64 KiB. Empty caller buffers are not EOF. A failed or
    /// cancelled call closes the stream permanently; errors never return bytes.
    /// No collection/spooling: only a fixed scratch buffer plus native buffering.
    pub async fn read(&mut self, output: &mut [u8]) -> Result<usize, ReadError> {
        // Taking ownership BEFORE any await makes cancellation terminal as well:
        // dropping this future drops the local body; self.body remains None.
        let mut body = self.body.take().ok_or(ReadError::Closed)?;
        let result = self.read_inner(&mut body, output).await;
        if result.is_ok() {
            self.body = Some(body);
        }
        result
    }
    async fn read_inner(&mut self, body: &mut Body, output: &mut [u8]) -> Result<usize, ReadError> {
        if self.due()? {
            self.refresh().await?;
        }
        self.dispatch()?;
        if output.is_empty() || self.eof {
            return Ok(0);
        }
        let mut buffer = [0u8; READ_LIMIT];
        let capacity = output.len().min(READ_LIMIT);
        loop {
            // AsyncReadExt::read is cancellation safe; a timer does not consume
            // a provider chunk. No timer or DB task outlives this read future.
            let n = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(self.wake) => {
                    self.time()?;
                    self.refresh().await?;
                    continue;
                }
                result = body.read(&mut buffer[..capacity]) => result.map_err(|_| ReadError::Body)?,
            };
            // A resumed body may have crossed the local deadline. Never return
            // that chunk under stale authority, even if the native read won.
            self.dispatch()?;
            if self.due()? {
                self.refresh().await?;
            }
            self.dispatch()?;
            let next = self.actual.checked_add(n as u64).ok_or(ReadError::Length)?;
            if next > self.length || (n == 0 && next != self.length) {
                return Err(ReadError::Length);
            }
            self.actual = next;
            self.eof = n == 0;
            output[..n].copy_from_slice(&buffer[..n]);
            return Ok(n);
        }
    }
}

/// Open using the SAME native client captured during qualification. Admission
/// precedes GET and is repeated after headers (an external renewal while waiting
/// must not falsely expire the old observation). Neither check consults the old
/// source reference/stage. The sampler must obtain fresh trusted time on demand.
/// This handle never owns renewal; no body survives an error/cancelled read.
pub async fn open_pinned_read<'a, F>(
    journal: &'a CommandJournal,
    qualified: &'a QualifiedNamespace,
    pin_id: &ResourceId,
    range: Option<ByteRange>,
    policy: &ClaimPolicy,
    sampler: &'a F,
) -> Result<PinnedRead<'a, F>, ReadError>
where
    F: Fn() -> Result<ClockObservation, ClockError> + Sync + ?Sized,
{
    let initial = journal.authorize_use(pin_id, sampler, policy).await?;
    binding(qualified, &initial, None)?;
    let config = qualified.configuration();
    let version = initial.version().version();
    initial.pin().claim().validate_use(sampler()?, policy)?;
    let response = qualified
        .native_client()
        .get_object()
        .bucket(config.bucket())
        .expected_bucket_owner(config.expected_owner())
        .key(version.key())
        .version_id(version.version())
        .set_range(range.map(|r| format!("bytes={}-{}", r.start, r.end_inclusive)))
        .send()
        .await
        .map_err(|_| ReadError::Request)?;
    if response.version_id() != Some(version.version()) {
        return Err(ReadError::Version);
    }
    let length = response
        .content_length()
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(ReadError::Length)?;
    check_range(response.content_range(), range, length)?;
    let authorization = journal.authorize_use(pin_id, sampler, policy).await?;
    binding(qualified, &authorization, Some(&initial))?;
    let mut handle = PinnedRead {
        journal,
        qualified,
        id: pin_id.clone(),
        policy: *policy,
        sampler,
        authorization,
        body: Some(Box::pin(response.body.into_async_read())),
        length,
        actual: 0,
        eof: false,
        wake: Instant::now(),
        clock_wake: 0,
    };
    handle.schedule()?;
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn window_arithmetic_and_response_validation() {
        assert!(ByteRange::new(2, 1).is_err());
        assert!(ByteRange::new(0, u64::MAX).is_err());
        let range = Some(ByteRange::new(2, 4).unwrap());
        assert!(check_range(Some("bytes 2-4/5"), range, 3).is_ok());
        for value in [
            None,
            Some("bytes 2-4/*"),
            Some("bytes 2-4/4"),
            Some("bytes 2-5/9"),
            Some("bytes +2-4/5"),
        ] {
            assert!(check_range(value, range, 3).is_err());
        }
        assert!(check_range(Some("bytes 2-4/5"), range, 2).is_err());
    }
}
