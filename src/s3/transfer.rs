//! Sequential native transfer; never selection or publication authority.
//!
//! Fixed 8 MiB parts and at most 10,000 parts impose an approximately 80 GiB
//! adapter bound (78.125 GiB), NOT an S3 object-size limit or lifetime policy.
//! One retryable part buffer plus one lookahead byte is retained; no disk spool.
//! SDK retries, credentials and timeouts remain the captured client's policy.
//!
//! No AbortMultipartUpload is issued, including on error, cancellation or Drop.
//! The continuously maintained qualified lifecycle owns incomplete-MPU cleanup,
//! including unknown creates and finite in-flight parts (the provider assumption
//! in protocol section 9). This adapter does not persist or expose UploadIds.
//! Already-issued I/O can still finish after authority is lost or this future is
//! dropped. Failed/invalid S3 responses are never evidence of rollback/absence.

use std::{fmt, time::Duration};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart},
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    time::Instant,
};

use super::{QualifiedNamespace, attribution_metadata, verification::check_authorization_binding};
use crate::{
    claim::ClaimPolicy,
    clock::{ClockError, ClockObservation},
    dynamodb::{CommandJournal, TransferAuthorization, UploadError},
    identity::{ResourceId, VersionRef},
};

const PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;
const MAX_CHECK_MS: u64 = 30_000;

/// Errors contain no source payload, native response, key or UploadId. Any
/// failure after issuing I/O can leave remote effects; no variant means rollback.
#[derive(Debug, Error)]
pub enum TransferError {
    #[error("fresh native upload admission failed")]
    Admission(#[from] UploadError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error("upload admission expired")]
    Expired,
    #[error("immutable namespace or upload binding differs")]
    Binding,
    #[error("required immutable attribution could not be constructed")]
    Attribution,
    #[error("upload source read failed")]
    Source,
    #[error("upload length violates declared constraints")]
    Length,
    #[error("computed full-body SHA-256 differs from the declared constraint")]
    Hash,
    #[error("fixed-size transfer exceeds the 10000-part adapter bound")]
    PartLimit,
    #[error("native S3 outcome UNKNOWN; a version or MPU may exist, not rollback")]
    OutcomeUnknown,
    #[error("S3 response lacks a valid exact VersionId; a version may exist")]
    Version,
    #[error("S3 response lacks a nonempty multipart completion token; MPU may exist")]
    MultipartToken,
}

/// Locally hashed transfer result, NOT verified observation, selection or live
/// publication authority. Pass `version()` to `verify_candidate` separately.
/// A response may arrive after the admission deadline; this immutable candidate
/// does not claim permission is still live. ETags are never version identities.
///
/// ```compile_fail
/// use storelink::s3::UploadCandidate;
/// let _: UploadCandidate = serde_json::from_str("{}").unwrap();
/// ```
pub struct UploadCandidate {
    version: VersionRef,
    length: u64,
    full_sha256: [u8; 32],
}
impl UploadCandidate {
    pub fn version(&self) -> &VersionRef {
        &self.version
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn full_sha256(&self) -> &[u8; 32] {
        &self.full_sha256
    }
}
impl fmt::Debug for UploadCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadCandidate")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

// This timer does not renew authority or recalculate a TTL. Only a new native
// admission can replace the fixed deadline. Its positive, <=30s schedule matches
// the verifier: check at the renewal margin, or at expiry when already inside it.
struct Admission<'a, F: ?Sized> {
    journal: &'a CommandJournal,
    qualified: &'a QualifiedNamespace,
    id: &'a ResourceId,
    initial: &'a TransferAuthorization,
    policy: &'a ClaimPolicy,
    clock: &'a F,
    deadline: u64,
    wake: Instant,
    clock_wake: u64,
}
impl<F> Admission<'_, F>
where
    F: Fn() -> Result<ClockObservation, ClockError> + Sync + ?Sized,
{
    fn time(&self) -> Result<u64, TransferError> {
        let clock = (self.clock)()?;
        clock.validate(self.policy.max_clock_uncertainty_ms())?;
        Ok(clock.upper_bound()?.as_millis())
    }
    fn dispatch(&self) -> Result<(), TransferError> {
        if self.time()? >= self.deadline {
            return Err(TransferError::Expired);
        }
        Ok(())
    }
    fn schedule(&mut self) -> Result<(), TransferError> {
        let upper = self.time()?;
        let left = self
            .deadline
            .checked_sub(upper)
            .filter(|n| *n > 0)
            .ok_or(TransferError::Expired)?;
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
    async fn refresh(&mut self) -> Result<(), TransferError> {
        let current = self
            .journal
            .authorize_transfer(self.id, self.clock, self.policy)
            .await?;
        check_authorization_binding(self.qualified, &current, Some(self.initial))
            .map_err(|_| TransferError::Binding)?;
        self.deadline = current.valid_until().as_millis();
        self.schedule()
    }
    async fn read<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        buffer: &mut [u8],
    ) -> Result<usize, TransferError> {
        loop {
            if self.time()? >= self.clock_wake || Instant::now() >= self.wake {
                self.refresh().await?;
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(self.wake) => { self.refresh().await?; }
                result = reader.read(buffer) => {
                    return result.map_err(|_| TransferError::Source);
                }
            }
            // AsyncReadExt::read is cancel-safe: losing to the timer does not
            // consume input. The same reader and partially filled buffer survive.
        }
    }
}

struct Input {
    length: u64,
    hash: Sha256,
    max: u64,
}
impl Input {
    fn observe(&mut self, bytes: &[u8]) -> Result<(), TransferError> {
        self.length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or(TransferError::Length)?;
        if self.length > self.max {
            return Err(TransferError::Length);
        }
        self.hash.update(bytes);
        Ok(())
    }
    fn finish(&self, initial: &TransferAuthorization) -> Result<[u8; 32], TransferError> {
        let c = initial.attempt().constraints();
        if c.expected_length().is_some_and(|n| n != self.length) {
            return Err(TransferError::Length);
        }
        let hash: [u8; 32] = self.hash.clone().finalize().into();
        if c.full_sha256().is_some_and(|h| h != &hash) {
            return Err(TransferError::Hash);
        }
        Ok(hash)
    }
    async fn fill<R, F>(
        &mut self,
        buffer: &mut Vec<u8>,
        reader: &mut R,
        admission: &mut Admission<'_, F>,
    ) -> Result<bool, TransferError>
    where
        R: AsyncRead + Unpin,
        F: Fn() -> Result<ClockObservation, ClockError> + Sync + ?Sized,
    {
        let mut filled = buffer.len();
        // Even a tiny max_length consumes at most one excess byte, never a
        // full part past that limit. No unbounded read_to_end/collect.
        let capacity =
            PART_SIZE.min(filled + (self.max - self.length).min(PART_SIZE as u64) as usize + 1);
        buffer.resize(capacity, 0);
        while filled < capacity {
            let end = capacity.min(filled + 64 * 1024);
            let n = admission.read(reader, &mut buffer[filled..end]).await?;
            if n == 0 {
                buffer.truncate(filled);
                return Ok(true);
            }
            self.observe(&buffer[filled..filled + n])?;
            filled += n;
            tokio::task::yield_now().await;
        }
        buffer.truncate(filled);
        Ok(false)
    }
}
fn part_number(index: usize) -> Result<i32, TransferError> {
    if !(1..=MAX_PARTS).contains(&index) {
        return Err(TransferError::PartLimit);
    }
    Ok(index as i32)
}
fn candidate(
    version: Option<&str>,
    initial: &TransferAuthorization,
    length: u64,
    full_sha256: [u8; 32],
) -> Result<UploadCandidate, TransferError> {
    let target = initial.attempt().target();
    let version = VersionRef::new(
        target.store().clone(),
        target.key(),
        version.ok_or(TransferError::Version)?,
    )
    .map_err(|_| TransferError::Version)?;
    Ok(UploadCandidate {
        version,
        length,
        full_sha256,
    })
}

/// Upload known or unknown-length input through the qualified native client.
/// No caller HTTP Content-Length is required; each buffered SDK body is replayable
/// (`ByteStream::from(Bytes)`) under that client's native retry policy.
///
/// Fresh native authority precedes each new S3 request and initial source read;
/// pending reads and CPU-ready chunks revalidate at positive bounded intervals.
/// The sampler must be fresh trusted time. Only the external owner renews claims.
/// Constraints and local full-body SHA-256 are checked BEFORE Put/Complete.
/// Dropping this future drops the reader without detached tasks or MPU aborts.
pub async fn transfer_upload<R, F>(
    journal: &CommandJournal,
    qualified: &QualifiedNamespace,
    attempt_id: &ResourceId,
    mut reader: R,
    policy: &ClaimPolicy,
    sample_clock: &F,
) -> Result<UploadCandidate, TransferError>
where
    R: AsyncRead + Unpin,
    F: Fn() -> Result<ClockObservation, ClockError> + Sync + ?Sized,
{
    let initial = journal
        .authorize_transfer(attempt_id, sample_clock, policy)
        .await?;
    check_authorization_binding(qualified, &initial, None).map_err(|_| TransferError::Binding)?;
    let metadata = attribution_metadata(&initial).map_err(|_| TransferError::Attribution)?;
    let config = qualified.configuration();
    let client = qualified.native_client();
    let key = initial.attempt().target().key();
    let mut admission = Admission {
        journal,
        qualified,
        id: attempt_id,
        initial: &initial,
        policy,
        clock: sample_clock,
        deadline: initial.valid_until().as_millis(),
        wake: Instant::now(),
        clock_wake: 0,
    };
    admission.schedule()?;
    let mut input = Input {
        length: 0,
        hash: Sha256::new(),
        max: initial.attempt().constraints().max_length(),
    };
    let mut buffer = Vec::new();
    let mut eof = input.fill(&mut buffer, &mut reader, &mut admission).await?;
    let mut lookahead = [0u8; 1];
    if !eof {
        let n = admission.read(&mut reader, &mut lookahead).await?;
        input.observe(&lookahead[..n])?;
        eof = n == 0;
    }
    if eof {
        let hash = input.finish(&initial)?;
        admission.refresh().await?;
        admission.dispatch()?;
        let output = client
            .put_object()
            .bucket(config.bucket())
            .expected_bucket_owner(config.expected_owner())
            .key(key)
            .set_metadata(Some(metadata))
            .body(ByteStream::from(Bytes::from(buffer)))
            .send()
            .await
            .map_err(|_| TransferError::OutcomeUnknown)?;
        return candidate(output.version_id(), &initial, input.length, hash);
    }

    admission.refresh().await?;
    admission.dispatch()?;
    // Match the pinned SDK's CRC32 default explicitly across the MPU. S3 can
    // otherwise choose a different initiation default. The native SDK computes
    // transport CRCs; our independent streaming SHA-256 remains the full hash.
    let created = client
        .create_multipart_upload()
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(ChecksumType::Composite)
        .bucket(config.bucket())
        .expected_bucket_owner(config.expected_owner())
        .key(key)
        .set_metadata(Some(metadata))
        .send()
        .await
        .map_err(|_| TransferError::OutcomeUnknown)?;
    let upload_id = created
        .upload_id()
        .filter(|id| !id.is_empty())
        .ok_or(TransferError::MultipartToken)?;
    let mut parts = Vec::new();
    loop {
        let number = part_number(parts.len() + 1)?;
        admission.refresh().await?;
        admission.dispatch()?;
        let output = client
            .upload_part()
            .bucket(config.bucket())
            .expected_bucket_owner(config.expected_owner())
            .key(key)
            .upload_id(upload_id)
            .part_number(number)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(ByteStream::from(Bytes::from(buffer)))
            // This MPU explicitly selected CRC32. The SDK's WhenRequired
            // policy otherwise omits UploadPart's optional checksum even when
            // checksum_algorithm is set. Require native calculation for this
            // operation only; do not mutate the captured client's policy.
            .customize()
            .config_override(
                aws_sdk_s3::config::Builder::new().request_checksum_calculation(
                    aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported,
                ),
            )
            .send()
            .await
            .map_err(|_| TransferError::OutcomeUnknown)?;
        let etag = output
            .e_tag()
            .filter(|s| !s.is_empty())
            .ok_or(TransferError::MultipartToken)?;
        // Native additional-checksum MPUs require the returned per-part
        // checksum alongside ETag in Complete. Neither token proves version
        // identity or replaces our local full-body SHA-256.
        let crc32 = output
            .checksum_crc32()
            .filter(|s| !s.is_empty())
            .ok_or(TransferError::MultipartToken)?;
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .e_tag(etag)
                .checksum_crc32(crc32)
                .build(),
        );
        if eof {
            break;
        }
        // An excess part is rejected before reading/issuing it. The lookahead
        // already proves it exists; do not silently complete a truncated object.
        part_number(parts.len() + 1)?;
        admission.refresh().await?;
        buffer = vec![lookahead[0]]; // Already counted and hashed exactly once.
        eof = input.fill(&mut buffer, &mut reader, &mut admission).await?;
        if !eof {
            let n = admission.read(&mut reader, &mut lookahead).await?;
            input.observe(&lookahead[..n])?;
            eof = n == 0;
        }
    }
    let hash = input.finish(&initial)?;
    admission.refresh().await?;
    admission.dispatch()?;
    let output = client
        .complete_multipart_upload()
        .checksum_type(ChecksumType::Composite)
        .bucket(config.bucket())
        .expected_bucket_owner(config.expected_owner())
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .map_err(|_| TransferError::OutcomeUnknown)?;
    // Includes embedded completion errors and NoSuchUpload after an ambiguous
    // SDK retry: never interpret either as proof that a version does not exist.
    candidate(output.version_id(), &initial, input.length, hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn part_index_bound_is_explicit_without_large_payloads() {
        assert_eq!(part_number(1).unwrap(), 1);
        assert_eq!(part_number(10_000).unwrap(), 10_000);
        for n in [0, 10_001, usize::MAX] {
            assert!(matches!(part_number(n), Err(TransferError::PartLimit)));
        }
    }
}
