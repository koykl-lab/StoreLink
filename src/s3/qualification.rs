//! Read-only AWS S3 v0 qualification, independent of metadata enrollment.
//!
//! These observations are not an atomic snapshot, a permanent configuration
//! guarantee, proof of exclusive writers, or upload/publication authority. The
//! operator must maintain exclusive scope ownership (including IAM), monitor the
//! profile, and preserve lifecycle coverage even after metadata compaction. The
//! lifecycle in-flight-part reclamation property remains a provider assumption.
//! Optional Intelligent-Tiering configurations are not inspected: qualification
//! does not verify the absence of restore-required archive tiers. The operator
//! must not enable optional Archive Access or Deep Archive Access for managed objects.
//! The host supplies the native client, credentials, transport, retries/timeouts;
//! this module never loads default credentials or mutates S3 or metadata.

use std::time::Duration;

use aws_sdk_s3::{
    Client,
    error::ProvideErrorMetadata,
    operation::{
        list_multipart_uploads::ListMultipartUploadsOutput,
        list_object_versions::ListObjectVersionsOutput,
    },
    types::{EncodingType, ObjectLockEnabled},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{LifecycleError, LifecycleReport, MpuAgePolicy, assess_lifecycle};
use crate::identity::{NamespaceId, StoreId};

const PROFILE: &str = "aws-s3-versioned-lifecycle-v0";

/// Untrusted declarative input. Every field is explicit; this is not authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfigurationSpec {
    pub id: NamespaceId,
    pub store: StoreId,
    pub bucket: String,
    pub expected_owner: String,
    pub region: String,
    pub key_prefix: String,
    pub mpu_max_age_ms: u64,
    pub mpu_lifecycle_margin_ms: u64,
}

/// Checked, immutable configuration; no metadata row is needed to construct it.
#[derive(Clone, Debug)]
pub struct NamespaceConfiguration {
    spec: NamespaceConfigurationSpec,
    policy: MpuAgePolicy,
}
impl NamespaceConfiguration {
    pub fn new(spec: NamespaceConfigurationSpec) -> Result<Self, QualificationError> {
        if !valid_bucket(&spec.bucket) {
            return Err(QualificationError::InvalidConfiguration(
                "general-purpose bucket name",
            ));
        }
        if spec.expected_owner.len() != 12
            || !spec.expected_owner.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(QualificationError::InvalidConfiguration(
                "12-digit expected owner",
            ));
        }
        let components: Vec<_> = spec.region.split('-').collect();
        if spec.region.len() > 64
            || components.len() < 3
            || components.iter().any(|s| s.is_empty())
            || !spec
                .region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || !components
                .last()
                .is_some_and(|s| s.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(QualificationError::InvalidConfiguration(
                "regional S3 region",
            ));
        }
        if spec.key_prefix.len() > 1024 {
            return Err(QualificationError::InvalidConfiguration(
                "prefix exceeds 1024 UTF-8 bytes",
            ));
        }
        let policy = MpuAgePolicy::new(
            Duration::from_millis(spec.mpu_max_age_ms),
            Duration::from_millis(spec.mpu_lifecycle_margin_ms),
        )?;
        Ok(Self { spec, policy })
    }
    pub fn id(&self) -> &NamespaceId {
        &self.spec.id
    }
    pub fn store(&self) -> &StoreId {
        &self.spec.store
    }
    pub fn bucket(&self) -> &str {
        &self.spec.bucket
    }
    pub fn expected_owner(&self) -> &str {
        &self.spec.expected_owner
    }
    pub fn region(&self) -> &str {
        &self.spec.region
    }
    pub fn key_prefix(&self) -> &str {
        &self.spec.key_prefix
    }
    pub fn mpu_max_age_ms(&self) -> u64 {
        self.spec.mpu_max_age_ms
    }
    pub fn mpu_lifecycle_margin_ms(&self) -> u64 {
        self.spec.mpu_lifecycle_margin_ms
    }
    pub fn provider_profile(&self) -> &'static str {
        PROFILE
    }
}
impl TryFrom<NamespaceConfigurationSpec> for NamespaceConfiguration {
    type Error = QualificationError;
    fn try_from(spec: NamespaceConfigurationSpec) -> Result<Self, Self::Error> {
        Self::new(spec)
    }
}

fn valid_bucket(bucket: &str) -> bool {
    let bytes = bucket.as_bytes();
    (3..=63).contains(&bytes.len())
        && bytes.first().is_some_and(|b| b.is_ascii_alphanumeric())
        && bytes.last().is_some_and(|b| b.is_ascii_alphanumeric())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-'))
        && !bucket.contains("..")
        && !["xn--", "sthree-", "amzn-s3-demo-"]
            .iter()
            .any(|p| bucket.starts_with(p))
        && !["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|s| bucket.ends_with(s))
        && !(bucket.split('.').count() == 4
            && bucket
                .split('.')
                .all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())))
}

#[derive(Debug, Error)]
pub enum QualificationError {
    #[error("invalid namespace configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("configured client or observed bucket region differs from namespace region")]
    RegionMismatch,
    #[error("native {operation} failed; no absence or qualification inferred")]
    Request {
        operation: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error("v0 does not support enabled Object Lock")]
    ObjectLockEnabled,
    #[error("ambiguous or unsupported provider configuration: {0}")]
    InvalidObservation(&'static str),
    #[error("qualification lifecycle configuration exceeded its bounded rule budget")]
    ListingLimit,
    #[error("managed scope contains an object version, delete marker or multipart upload")]
    ScopeNotEmpty,
}
fn request_error(
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> QualificationError {
    QualificationError::Request {
        operation,
        source: Box::new(source),
    }
}

/// Opaque existing-profile health observation. Deliberately not usable as a fresh
/// enrollment token: an existing namespace may contain live data.
///
/// ```compile_fail
/// use storelink::s3::{QualifiedNamespace, QualifiedFreshNamespace};
/// fn register(_: QualifiedFreshNamespace) {}
/// fn health_is_not_fresh(health: QualifiedNamespace) { register(health); }
/// ```
pub struct QualifiedNamespace {
    configuration: NamespaceConfiguration,
    lifecycle: LifecycleReport,
    #[cfg_attr(not(feature = "dynamodb"), allow(dead_code))]
    client: Client,
}
impl std::fmt::Debug for QualifiedNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never format the native client (credentials/transport) or key prefix.
        f.debug_struct("QualifiedNamespace").finish_non_exhaustive()
    }
}
impl QualifiedNamespace {
    pub(super) fn native_client(&self) -> &Client {
        &self.client
    }
    pub fn configuration(&self) -> &NamespaceConfiguration {
        &self.configuration
    }
    pub fn lifecycle(&self) -> &LifecycleReport {
        &self.lifecycle
    }
}

/// Non-Clone, consumable fresh-scope observation; NOT persisted enrollment.
/// No own upload may begin before metadata enrollment, under the trusted exclusive
/// writer contract. Empty listings do not prove global exclusivity or drain late
/// requests. Requalification is required if that contract/profile is interrupted.
///
/// ```compile_fail
/// use storelink::s3::QualifiedFreshNamespace;
/// fn duplicate(token: QualifiedFreshNamespace) { let _ = token.clone(); }
/// ```
/// ```compile_fail
/// use storelink::s3::QualifiedFreshNamespace;
/// let _: QualifiedFreshNamespace = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Debug)]
pub struct QualifiedFreshNamespace {
    qualified: QualifiedNamespace,
}
impl QualifiedFreshNamespace {
    pub fn configuration(&self) -> &NamespaceConfiguration {
        self.qualified.configuration()
    }
    pub fn lifecycle(&self) -> &LifecycleReport {
        self.qualified.lifecycle()
    }

    /// Future metadata registration must consume this exact fresh token, not an
    /// existing-health result or a caller-supplied tuple. This does not register it.
    #[allow(dead_code)] // Seam for the metadata adapter, which owns enrollment.
    pub(crate) fn into_configuration(self) -> NamespaceConfiguration {
        self.qualified.configuration
    }
}

/// Observe versioning, lifecycle and Object Lock without assuming an enrolled DB
/// row or empty data. Optional Intelligent-Tiering configurations are not inspected;
/// the operator must keep restore-required optional archive tiers off managed objects.
/// Request retries and timeouts are the supplied client's bounded host policy.
pub async fn qualify_existing(
    client: &Client,
    configuration: &NamespaceConfiguration,
) -> Result<QualifiedNamespace, QualificationError> {
    if client.config().region().map(|r| r.as_ref()) != Some(configuration.region()) {
        return Err(QualificationError::RegionMismatch);
    }
    let bucket = configuration.bucket();
    let owner = configuration.expected_owner();
    let head = client
        .head_bucket()
        .bucket(bucket)
        .expected_bucket_owner(owner)
        .send()
        .await
        .map_err(|e| request_error("HeadBucket", e))?;
    // Never silently accept a redirected bucket in another region, or missing
    // region evidence. ExpectedBucketOwner is enforced by S3, not inferred locally.
    if head.bucket_region() != Some(configuration.region()) {
        return Err(QualificationError::RegionMismatch);
    }
    let versioning = client
        .get_bucket_versioning()
        .bucket(bucket)
        .expected_bucket_owner(owner)
        .send()
        .await
        .map_err(|e| request_error("GetBucketVersioning", e))?;
    let lifecycle = client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .expected_bucket_owner(owner)
        .send()
        .await
        .map_err(|e| request_error("GetBucketLifecycleConfiguration", e))?;
    if lifecycle.rules().len() > 1000 {
        return Err(QualificationError::ListingLimit);
    }
    let lifecycle = assess_lifecycle(
        configuration.key_prefix(),
        configuration.policy,
        &versioning,
        &lifecycle,
    )?;
    match client
        .get_object_lock_configuration()
        .bucket(bucket)
        .expected_bucket_owner(owner)
        .send()
        .await
    {
        Ok(output) => {
            if output
                .object_lock_configuration()
                .and_then(|c| c.object_lock_enabled())
                == Some(&ObjectLockEnabled::Enabled)
            {
                return Err(QualificationError::ObjectLockEnabled);
            }
            // Only the documented service error establishes an unconfigured lock.
            return Err(QualificationError::InvalidObservation(
                "Object Lock response",
            ));
        }
        Err(error)
            if error
                .raw_response()
                .is_some_and(|r| r.status().as_u16() == 404)
                && error.as_service_error().and_then(|e| e.code())
                    == Some("ObjectLockConfigurationNotFoundError") => {}
        Err(error) => return Err(request_error("GetObjectLockConfiguration", error)),
    }
    Ok(QualifiedNamespace {
        configuration: configuration.clone(),
        lifecycle,
        client: client.clone(),
    })
}

/// Qualify profile first, then reject *any* managed history or in-progress MPU.
/// Only two bounded first-page probes are needed to prove observed emptiness;
/// truncated/ambiguous results are rejection, never permission to adopt history.
pub async fn qualify_fresh(
    client: &Client,
    configuration: &NamespaceConfiguration,
) -> Result<QualifiedFreshNamespace, QualificationError> {
    let qualified = qualify_existing(client, configuration).await?;
    let versions = client
        .list_object_versions()
        .bucket(configuration.bucket())
        .expected_bucket_owner(configuration.expected_owner())
        .prefix(configuration.key_prefix())
        .max_keys(1)
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .map_err(|e| request_error("ListObjectVersions", e))?;
    assess_empty_versions(configuration, &versions)?;
    let uploads = client
        .list_multipart_uploads()
        .bucket(configuration.bucket())
        .expected_bucket_owner(configuration.expected_owner())
        .prefix(configuration.key_prefix())
        .max_uploads(1)
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .map_err(|e| request_error("ListMultipartUploads", e))?;
    assess_empty_uploads(configuration, &uploads)?;
    Ok(QualifiedFreshNamespace { qualified })
}

fn nonempty(value: Option<&str>) -> bool {
    value.is_some_and(|s| !s.is_empty())
}

// URL encoding is requested to keep arbitrary UTF-8/XML-control prefixes valid.
// Decode percent bytes only: '+' is a literal S3 key byte, not form whitespace.
fn prefix_matches(value: Option<&str>, encoding: Option<&EncodingType>, expected: &str) -> bool {
    if encoding != Some(&EncodingType::Url) {
        return false;
    }
    let value = value.unwrap_or("");
    if value.len() > 3072 {
        return false;
    }
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let hex = |b: u8| (b as char).to_digit(16).map(|n| n as u8);
            let Some(hi) = bytes.next().and_then(hex) else {
                return false;
            };
            let Some(lo) = bytes.next().and_then(hex) else {
                return false;
            };
            decoded.push(hi * 16 + lo);
        } else {
            decoded.push(byte);
        }
    }
    decoded == expected.as_bytes()
}
fn assess_empty_versions(
    configuration: &NamespaceConfiguration,
    output: &ListObjectVersionsOutput,
) -> Result<(), QualificationError> {
    if !output.versions().is_empty() || !output.delete_markers().is_empty() {
        return Err(QualificationError::ScopeNotEmpty);
    }
    if output.is_truncated() != Some(false)
        || output.name() != Some(configuration.bucket())
        || !prefix_matches(
            output.prefix(),
            output.encoding_type(),
            configuration.key_prefix(),
        )
        || output.max_keys() != Some(1)
        || !output.common_prefixes().is_empty()
        || nonempty(output.delimiter())
        || nonempty(output.key_marker())
        || nonempty(output.version_id_marker())
        || nonempty(output.next_key_marker())
        || nonempty(output.next_version_id_marker())
    {
        return Err(QualificationError::InvalidObservation(
            "incomplete/ambiguous version listing",
        ));
    }
    Ok(())
}
fn assess_empty_uploads(
    configuration: &NamespaceConfiguration,
    output: &ListMultipartUploadsOutput,
) -> Result<(), QualificationError> {
    if !output.uploads().is_empty() {
        return Err(QualificationError::ScopeNotEmpty);
    }
    if output.is_truncated() != Some(false)
        || output.bucket() != Some(configuration.bucket())
        || !prefix_matches(
            output.prefix(),
            output.encoding_type(),
            configuration.key_prefix(),
        )
        || output.max_uploads() != Some(1)
        || !output.common_prefixes().is_empty()
        || nonempty(output.delimiter())
        || nonempty(output.key_marker())
        || nonempty(output.upload_id_marker())
        || nonempty(output.next_key_marker())
        || nonempty(output.next_upload_id_marker())
    {
        return Err(QualificationError::InvalidObservation(
            "incomplete/ambiguous multipart listing",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_empty_listings_cannot_default_to_fresh() {
        let config = NamespaceConfiguration::new(NamespaceConfigurationSpec {
            id: NamespaceId::new("n").unwrap(),
            store: StoreId::new("s").unwrap(),
            bucket: "fixture".into(),
            expected_owner: "123456789012".into(),
            region: "us-west-2".into(),
            key_prefix: String::new(),
            mpu_max_age_ms: 1,
            mpu_lifecycle_margin_ms: 1,
        })
        .unwrap();
        assert!(
            assess_empty_versions(&config, &ListObjectVersionsOutput::builder().build()).is_err()
        );
        assert!(
            assess_empty_uploads(&config, &ListMultipartUploadsOutput::builder().build()).is_err()
        );
        // Check the crate-private consumption seam without granting public construction.
        let token = QualifiedFreshNamespace {
            qualified: QualifiedNamespace {
                client: Client::from_conf(
                    aws_sdk_s3::Config::builder()
                        .behavior_version_latest()
                        .region(aws_sdk_s3::config::Region::new("us-west-2"))
                        .build(),
                ),
                configuration: config,
                lifecycle: LifecycleReport {
                    covering_abort_days: 1,
                    earliest_overlapping_abort_days: 1,
                },
            },
        };
        assert_eq!(token.into_configuration().bucket(), "fixture");
    }
    #[test]
    fn prefix_echo_is_exact_utf8_not_path_or_form_normalization() {
        for (encoded, original) in [
            ("", ""),
            ("managed%2f", "managed/"),
            ("%C3%A9%01+%20%25", "é\u{1}+ %"),
            ("..%2F.%2F", ".././"),
        ] {
            assert!(prefix_matches(
                Some(encoded),
                Some(&EncodingType::Url),
                original
            ));
        }
        for encoded in ["%", "%0", "%GG", "%FF", "a+b"] {
            assert!(!prefix_matches(
                Some(encoded),
                Some(&EncodingType::Url),
                "a b"
            ));
        }
        assert!(!prefix_matches(Some(""), None, ""));
    }
}
