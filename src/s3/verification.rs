//! Exact-version observation, not transfer, selection or publication authority.
//!
//! The operator maintains exclusive writers and the qualified lifecycle/profile.
//! Health qualification is not enrollment proof; native metadata admission is also
//! required. This module never renews metadata, mutates S3 or detaches tasks. SDK
//! retries/timeouts and credentials remain the supplied native client's policy.

use std::{collections::HashMap, fmt, time::Duration};

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{io::AsyncReadExt, time::Instant};

use super::QualifiedNamespace;
use crate::{
    claim::ClaimPolicy,
    clock::{ClockError, ClockObservation},
    dynamodb::{
        AttemptSnapshot, CommandJournal, IntentSnapshot, NamespaceSnapshot, TransferAuthorization,
        UploadError,
    },
    identity::{ResourceId, VersionRef},
};

const RESERVED: &str = "storelink-";
const METADATA_LIMIT: usize = 2048;
const MAX_CHECK_MS: u64 = 30_000;

/// Errors intentionally omit native request bodies, object keys and credentials.
#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("fresh native upload admission failed")]
    Admission(#[from] UploadError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error("upload admission expired")]
    Expired,
    #[error("immutable candidate, namespace or upload binding differs")]
    Binding,
    #[error(
        "required immutable attribution differs, is absent or contains unknown reserved fields"
    )]
    Attribution,
    #[error("caller metadata uses the reserved case-insensitive storelink- prefix")]
    ReservedMetadata,
    #[error("user metadata exceeds 2 KiB")]
    MetadataSize,
    #[error("exact-version S3 request failed")]
    Request,
    #[error("S3 returned a missing, null or different version")]
    Version,
    #[error("object length is absent, negative, inconsistent or outside declared constraints")]
    Length,
    #[error("exact-version body stream failed")]
    Body,
    #[error("computed full-body SHA-256 differs from the declared constraint")]
    Hash,
}

/// Immutable bytes evidence, NOT current publication/use authority. Selection
/// must recheck the backing namespace, intent and attempt states/revisions in its
/// transaction. A receipt can become stale immediately after its final read.
/// `full_sha256() == None` means only exact identity/attribution/declared length
/// were verified; no full-body hash claim is made.
///
/// ```compile_fail
/// use storelink::s3::VerifiedUploadReceipt;
/// let _: VerifiedUploadReceipt = serde_json::from_str("{}").unwrap();
/// ```
/// ```compile_fail
/// use storelink::s3::VerifiedUploadReceipt;
/// let receipt = VerifiedUploadReceipt { length: 3 };
/// ```
pub struct VerifiedUploadReceipt {
    namespace: NamespaceSnapshot,
    intent: IntentSnapshot,
    attempt: AttemptSnapshot,
    version: VersionRef,
    length: u64,
    full_sha256: Option<[u8; 32]>,
}
impl fmt::Debug for VerifiedUploadReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedUploadReceipt")
            .field("length", &self.length)
            .field("full_sha256_verified", &self.full_sha256.is_some())
            .finish_non_exhaustive()
    }
}
impl VerifiedUploadReceipt {
    pub fn namespace(&self) -> &NamespaceSnapshot {
        &self.namespace
    }
    pub fn intent(&self) -> &IntentSnapshot {
        &self.intent
    }
    pub fn attempt(&self) -> &AttemptSnapshot {
        &self.attempt
    }
    pub fn version(&self) -> &VersionRef {
        &self.version
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn full_sha256(&self) -> Option<&[u8; 32]> {
        self.full_sha256.as_ref()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Canonical schema 1 immutable user metadata for future SDK/presign callers.
/// This helper grants NO I/O authority: freshly revalidate before issuing every
/// instruction/request. Domain/namespace are hex of exact UTF-8; UUIDs use their
/// lowercase hyphenated representation and generations canonical decimal.
/// Target SHA-256 hashes store then key, each prefixed by its u64 BE byte length.
/// Constraints hash u64 BE max length, then expected-length option (0, or 1 +
/// u64 BE), then full-SHA option (0, or 1 + 32 bytes). No ETag/tag/hash assertion
/// substitutes for actual required full-body verification.
///
/// Use `attribution_metadata_with_extra` when merging caller metadata; never
/// allow case-insensitive reserved-prefix overrides. Deadlines and mutable
/// record revisions are deliberately absent so legitimate renewal is supported.
pub fn attribution_metadata(
    authorization: &TransferAuthorization,
) -> Result<HashMap<String, String>, VerificationError> {
    let ns = authorization.namespace();
    let intent = authorization.intent();
    let attempt = authorization.attempt();
    let mut target = Sha256::new();
    for part in [attempt.target().store().as_str(), attempt.target().key()] {
        target.update((part.len() as u64).to_be_bytes());
        target.update(part.as_bytes());
    }
    let c = attempt.constraints();
    let mut constraints = Sha256::new();
    constraints.update(c.max_length().to_be_bytes());
    match c.expected_length() {
        Some(n) => {
            constraints.update([1]);
            constraints.update(n.to_be_bytes());
        }
        None => constraints.update([0]),
    }
    match c.full_sha256() {
        Some(hash) => {
            constraints.update([1]);
            constraints.update(hash);
        }
        None => constraints.update([0]),
    }
    let metadata: HashMap<_, _> = [
        ("schema", "1".to_owned()),
        ("domain", hex(ns.domain().as_str().as_bytes())),
        ("incarnation", ns.incarnation().to_string()),
        ("namespace", hex(ns.id().as_str().as_bytes())),
        (
            "configuration-revision",
            ns.configuration_revision().to_string(),
        ),
        (
            "intent-generation",
            intent.id().scope().generation().to_string(),
        ),
        ("intent-uuid", intent.id().uuid().to_string()),
        (
            "attempt-generation",
            attempt.id().scope().generation().to_string(),
        ),
        ("attempt-uuid", attempt.id().uuid().to_string()),
        ("target-sha256", hex(&target.finalize())),
        ("constraints-sha256", hex(&constraints.finalize())),
    ]
    .into_iter()
    .map(|(k, v)| (format!("{RESERVED}{k}"), v))
    .collect();
    check_metadata_size(&metadata)?;
    Ok(metadata)
}

/// Merge unrelated user metadata without permitting even mixed-case reserved
/// overrides. The total key/value byte budget is S3's 2 KiB user-metadata limit.
pub fn attribution_metadata_with_extra(
    authorization: &TransferAuthorization,
    extra: &HashMap<String, String>,
) -> Result<HashMap<String, String>, VerificationError> {
    if extra
        .keys()
        .any(|k| k.to_ascii_lowercase().starts_with(RESERVED))
    {
        return Err(VerificationError::ReservedMetadata);
    }
    check_metadata_size(extra)?;
    let mut metadata = attribution_metadata(authorization)?;
    metadata.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    check_metadata_size(&metadata)?;
    Ok(metadata)
}
fn check_metadata_size(metadata: &HashMap<String, String>) -> Result<(), VerificationError> {
    if metadata
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>()
        > METADATA_LIMIT
    {
        return Err(VerificationError::MetadataSize);
    }
    Ok(())
}
fn check_attribution(
    observed: Option<&HashMap<String, String>>,
    expected: &HashMap<String, String>,
) -> Result<(), VerificationError> {
    let observed = observed.ok_or(VerificationError::Attribution)?;
    if expected.iter().any(|(k, v)| observed.get(k) != Some(v))
        || observed
            .keys()
            .any(|k| k.to_ascii_lowercase().starts_with(RESERVED) && !expected.contains_key(k))
    {
        return Err(VerificationError::Attribution);
    }
    Ok(())
}

fn check_binding(
    qualified: &QualifiedNamespace,
    candidate: &VersionRef,
    authorization: &TransferAuthorization,
    initial: Option<&TransferAuthorization>,
) -> Result<(), VerificationError> {
    check_authorization_binding(qualified, authorization, initial)?;
    let target = authorization.attempt().target();
    if candidate.store() != target.store() || candidate.key() != target.key() {
        return Err(VerificationError::Binding);
    }
    Ok(())
}

/// Shared native-admission binding, independent of any completed candidate.
/// Candidate exact identity remains the verifier's separate responsibility.
pub(super) fn check_authorization_binding(
    qualified: &QualifiedNamespace,
    authorization: &TransferAuthorization,
    initial: Option<&TransferAuthorization>,
) -> Result<(), VerificationError> {
    let c = qualified.configuration();
    let ns = authorization.namespace();
    let a = authorization.attempt();
    if c.id() != ns.id()
        || c.store() != ns.store()
        || c.bucket() != ns.bucket()
        || c.expected_owner() != ns.expected_owner()
        || c.region() != ns.region()
        || c.key_prefix() != ns.key_prefix()
        || c.mpu_max_age_ms() != ns.mpu_max_age_ms()
        || c.mpu_lifecycle_margin_ms() != ns.mpu_lifecycle_margin_ms()
        || c.provider_profile() != ns.provider_profile()
    {
        return Err(VerificationError::Binding);
    }
    if let Some(old) = initial {
        // Mutable deadlines and revisions may refresh; all immutable bindings,
        // including the fixed MPU age ceiling and admission time, must not.
        if old.namespace() != ns
            || old.intent().id() != authorization.intent().id()
            || old.attempt().id() != a.id()
            || old.attempt().intent_id() != a.intent_id()
            || old.attempt().target() != a.target()
            || old.attempt().constraints() != a.constraints()
            || old.attempt().mpu_admitted_lower_bound() != a.mpu_admitted_lower_bound()
            || old.attempt().max_mpu_use_until() != a.max_mpu_use_until()
        {
            return Err(VerificationError::Binding);
        }
    }
    Ok(())
}
fn remaining(
    authorization: &TransferAuthorization,
    policy: &ClaimPolicy,
    clock: ClockObservation,
) -> Result<(u64, u64), VerificationError> {
    clock.validate(policy.max_clock_uncertainty_ms())?;
    let upper = clock.upper_bound()?.as_millis();
    let left = authorization
        .valid_until()
        .as_millis()
        .checked_sub(upper)
        .filter(|n| *n > 0)
        .ok_or(VerificationError::Expired)?;
    Ok((upper, left))
}
fn next_check(
    authorization: &TransferAuthorization,
    policy: &ClaimPolicy,
    clock: ClockObservation,
) -> Result<(Instant, u64), VerificationError> {
    let (upper, left) = remaining(authorization, policy, clock)?;
    // Check upon entering the renewal margin, but don't spin when already in it.
    let ms = if left > policy.renewal_margin_ms() {
        left - policy.renewal_margin_ms()
    } else {
        left
    };
    let ms = ms.min(MAX_CHECK_MS);
    Ok((Instant::now() + Duration::from_millis(ms), upper + ms))
}
fn check_version(version: Option<&str>, candidate: &VersionRef) -> Result<(), VerificationError> {
    if version != Some(candidate.version()) {
        return Err(VerificationError::Version);
    }
    Ok(())
}
fn check_length(
    length: Option<i64>,
    auth: &TransferAuthorization,
) -> Result<u64, VerificationError> {
    let length = length
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(VerificationError::Length)?;
    let c = auth.attempt().constraints();
    if length > c.max_length() || c.expected_length().is_some_and(|n| n != length) {
        return Err(VerificationError::Length);
    }
    Ok(length)
}

/// Verify a candidate using the SAME native S3 client that qualified this health
/// observation. The sampler must return fresh trusted time, never a captured old
/// observation; admission samples after its coherent native metadata read.
/// Every new S3 operation has fresh admission, and streaming has a positive,
/// bounded timer even while a body read is pending. The external owner alone
/// renews metadata. Dropping this future drops the stream; no MPU abort is sent.
pub async fn verify_candidate<F>(
    journal: &CommandJournal,
    qualified: &QualifiedNamespace,
    attempt_id: &ResourceId,
    candidate: &VersionRef,
    policy: &ClaimPolicy,
    sample_clock: &F,
) -> Result<VerifiedUploadReceipt, VerificationError>
where
    F: Fn() -> Result<ClockObservation, ClockError> + Sync + ?Sized,
{
    let initial = journal
        .authorize_transfer(attempt_id, sample_clock, policy)
        .await?;
    check_binding(qualified, candidate, &initial, None)?;
    let metadata = attribution_metadata(&initial)?;
    let config = qualified.configuration();
    let client = qualified.native_client();
    remaining(&initial, policy, sample_clock()?)?;
    let head = client
        .head_object()
        .bucket(config.bucket())
        .expected_bucket_owner(config.expected_owner())
        .key(candidate.key())
        .version_id(candidate.version())
        .send()
        .await
        .map_err(|_| VerificationError::Request)?;
    check_version(head.version_id(), candidate)?;
    let length = check_length(head.content_length(), &initial)?;
    check_attribution(head.metadata(), &metadata)?;

    let full_sha256 = if let Some(expected) = initial.attempt().constraints().full_sha256() {
        let mut current = journal
            .authorize_transfer(attempt_id, sample_clock, policy)
            .await?;
        check_binding(qualified, candidate, &current, Some(&initial))?;
        remaining(&current, policy, sample_clock()?)?;
        let get = client
            .get_object()
            .bucket(config.bucket())
            .expected_bucket_owner(config.expected_owner())
            .key(candidate.key())
            .version_id(candidate.version())
            .send()
            .await
            .map_err(|_| VerificationError::Request)?;
        check_version(get.version_id(), candidate)?;
        if check_length(get.content_length(), &current)? != length {
            return Err(VerificationError::Length);
        }
        // Headers may arrive after the pre-GET deadline while the external
        // owner legitimately renewed. Refresh native authority after validating
        // the response, rather than rejecting on the stale pre-request snapshot.
        current = journal
            .authorize_transfer(attempt_id, sample_clock, policy)
            .await?;
        check_binding(qualified, candidate, &current, Some(&initial))?;
        // SDK StreamReader retains at most its provider-sized current chunk;
        // our fixed buffer never collects the body. AsyncReadExt::read is cancel
        // safe, preserving the ByteStream state when the timer wins.
        let mut body = get.body.into_async_read();
        let mut buffer = [0u8; 8192];
        let mut hash = Sha256::new();
        let mut count = 0u64;
        let (mut wake, mut clock_wake) = next_check(&current, policy, sample_clock()?)?;
        loop {
            let clock = sample_clock()?;
            clock.validate(policy.max_clock_uncertainty_ms())?;
            if clock.upper_bound()?.as_millis() >= clock_wake || Instant::now() >= wake {
                current = journal
                    .authorize_transfer(attempt_id, sample_clock, policy)
                    .await?;
                check_binding(qualified, candidate, &current, Some(&initial))?;
                (wake, clock_wake) = next_check(&current, policy, sample_clock()?)?;
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(wake) => {
                    current = journal.authorize_transfer(attempt_id, sample_clock, policy).await?;
                    check_binding(qualified, candidate, &current, Some(&initial))?;
                    (wake, clock_wake) = next_check(&current, policy, sample_clock()?)?;
                }
                read = body.read(&mut buffer) => {
                    let n = read.map_err(|_| VerificationError::Body)?;
                    if n == 0 { break; }
                    count = count.checked_add(n as u64).ok_or(VerificationError::Length)?;
                    if count > length || count > current.attempt().constraints().max_length() {
                        return Err(VerificationError::Length);
                    }
                    hash.update(&buffer[..n]);
                    // Bound CPU monopolization even for immediately-ready SDK chunks.
                    tokio::task::yield_now().await;
                }
            }
        }
        if count != length {
            return Err(VerificationError::Length);
        }
        let computed: [u8; 32] = hash.finalize().into();
        if &computed != expected {
            return Err(VerificationError::Hash);
        }
        Some(computed)
    } else {
        None
    };

    let latest = journal
        .authorize_transfer(attempt_id, sample_clock, policy)
        .await?;
    check_binding(qualified, candidate, &latest, Some(&initial))?;
    remaining(&latest, policy, sample_clock()?)?;
    Ok(VerifiedUploadReceipt {
        namespace: latest.namespace().clone(),
        intent: latest.intent().clone(),
        attempt: latest.attempt().clone(),
        version: candidate.clone(),
        length,
        full_sha256,
    })
}
