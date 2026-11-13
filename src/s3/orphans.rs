//! Read-only inventory and immutable exact-version observations, never retirement
//! authority. A later journal transaction must revalidate current metadata and
//! selection impossibility (or explicit trusted-lower-bound grace policy).

use std::collections::HashSet;

use aws_sdk_s3::{
    operation::list_object_versions::ListObjectVersionsOutput, primitives::DateTime,
    types::EncodingType,
};
use thiserror::Error;

use super::{NamespaceConfiguration, QualifiedNamespace};
use crate::{clock::TimestampMs, identity::VersionRef};

#[derive(Debug, Error)]
pub enum OrphanObservationError {
    #[error("page limit must be 1..=1000")]
    Limit,
    #[error("namespace, configuration, prefix or cursor binding differs")]
    Binding,
    #[error("native read failed; no physical absence inferred")]
    Request,
    #[error("missing or malformed native version listing")]
    Listing,
    #[error("missing, null, different or delete-marker object version")]
    Version,
    #[error("missing or invalid object length or LastModified")]
    ObjectMetadata,
    #[error("user metadata exceeds 2 KiB or was not completely returned")]
    Metadata,
    #[error("foreign or unsupported attribution requires operator investigation")]
    AttributionConflict,
    #[cfg(feature = "dynamodb")]
    #[error("checked enrolled namespace read failed")]
    Namespace(#[from] crate::dynamodb::UploadError),
}

/// Native continuation bound to the entire qualified configuration. No public
/// constructor or deserialization: callers cannot manufacture checked progress.
pub struct VersionPageCursor {
    configuration: NamespaceConfiguration,
    key: String,
    version: String,
}
impl std::fmt::Debug for VersionPageCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionPageCursor").finish_non_exhaustive()
    }
}
impl VersionPageCursor {
    pub fn key_marker(&self) -> &str {
        &self.key
    }
    pub fn version_id_marker(&self) -> &str {
        &self.version
    }

    /// Restore scheduling data only after the caller has coherently checked the
    /// enrolled namespace/configuration revision. Never public cursor forging.
    #[cfg(feature = "dynamodb")]
    pub(crate) fn restore(
        qualified: &QualifiedNamespace,
        key: &str,
        version: &str,
    ) -> Result<Self, OrphanObservationError> {
        let c = qualified.configuration();
        VersionRef::new(c.store().clone(), key, version)
            .map_err(|_| OrphanObservationError::Listing)?;
        if !key.starts_with(c.key_prefix()) {
            return Err(OrphanObservationError::Binding);
        }
        Ok(Self {
            configuration: c.clone(),
            key: key.into(),
            version: version.into(),
        })
    }
}

/// Inventory only, not evidence that anything is abandoned. Delete markers are
/// separate from content candidates. Even an empty final page is not a drain.
pub struct VersionPage {
    // Bind even an empty terminal page to the actual native request, not merely
    // to its returned versions or optional continuation.
    #[cfg_attr(not(feature = "dynamodb"), allow(dead_code))]
    configuration: NamespaceConfiguration,
    #[cfg_attr(not(feature = "dynamodb"), allow(dead_code))]
    input_markers: Option<(String, String)>,
    versions: Vec<VersionRef>,
    delete_markers: Vec<VersionRef>,
    next: Option<VersionPageCursor>,
}
impl std::fmt::Debug for VersionPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionPage")
            .field("version_count", &self.versions.len())
            .field("delete_marker_count", &self.delete_markers.len())
            .field("has_continuation", &self.next.is_some())
            .finish_non_exhaustive()
    }
}

impl VersionPage {
    pub fn versions(&self) -> &[VersionRef] {
        &self.versions
    }
    pub fn delete_markers(&self) -> &[VersionRef] {
        &self.delete_markers
    }
    pub fn next_cursor(&self) -> Option<&VersionPageCursor> {
        self.next.as_ref()
    }
    #[cfg(feature = "dynamodb")]
    pub(crate) fn matches_configuration(&self, c: &NamespaceConfiguration) -> bool {
        same_configuration(&self.configuration, c)
    }
    #[cfg(feature = "dynamodb")]
    pub(crate) fn input_markers(&self) -> Option<(&str, &str)> {
        self.input_markers
            .as_ref()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

fn same_configuration(a: &NamespaceConfiguration, b: &NamespaceConfiguration) -> bool {
    a.id() == b.id()
        && a.store() == b.store()
        && a.bucket() == b.bucket()
        && a.expected_owner() == b.expected_owner()
        && a.region() == b.region()
        && a.key_prefix() == b.key_prefix()
        && a.provider_profile() == b.provider_profile()
        && a.mpu_max_age_ms() == b.mpu_max_age_ms()
        && a.mpu_lifecycle_margin_ms() == b.mpu_lifecycle_margin_ms()
}

// S3 URL encoding is percent-byte encoding, NOT form/path normalization.
fn decode_key(value: &str) -> Result<String, OrphanObservationError> {
    if value.len() > 3072 {
        return Err(OrphanObservationError::Listing);
    }
    let mut result = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(b) = bytes.next() {
        result.push(if b == b'%' {
            let mut digit = || bytes.next().and_then(|b| (b as char).to_digit(16));
            let hi = digit().ok_or(OrphanObservationError::Listing)?;
            let lo = digit().ok_or(OrphanObservationError::Listing)?;
            (hi * 16 + lo) as u8
        } else {
            b
        });
    }
    let result = String::from_utf8(result).map_err(|_| OrphanObservationError::Listing)?;
    if result.len() > 1024 {
        return Err(OrphanObservationError::Listing);
    }
    Ok(result)
}

// Round UP sub-millisecond provider timestamps: a later grace comparison must
// never become eligible early by truncating LastModified. No local clock read.
fn timestamp(value: Option<&DateTime>) -> Result<TimestampMs, OrphanObservationError> {
    let value = value.ok_or(OrphanObservationError::ObjectMetadata)?;
    if value.secs() < 0 {
        return Err(OrphanObservationError::ObjectMetadata);
    }
    let millis = value
        .to_millis()
        .ok()
        .and_then(|n| u64::try_from(n).ok())
        .and_then(|n| n.checked_add(u64::from(value.subsec_nanos() % 1_000_000 != 0)))
        .ok_or(OrphanObservationError::ObjectMetadata)?;
    TimestampMs::new(millis).map_err(|_| OrphanObservationError::ObjectMetadata)
}

/// Exactly one native ListObjectVersions request (apart from host SDK retries),
/// at most 1000 entries, exact prefix/owner and checked native continuation. No
/// paginator, HEAD fanout, latest-key fallback, deletion or MPU operations.
pub async fn list_version_page(
    qualified: &QualifiedNamespace,
    cursor: Option<&VersionPageCursor>,
    limit: u16,
) -> Result<VersionPage, OrphanObservationError> {
    if !(1..=1000).contains(&limit) {
        return Err(OrphanObservationError::Limit);
    }
    let c = qualified.configuration();
    if cursor.is_some_and(|p| !same_configuration(c, &p.configuration)) {
        return Err(OrphanObservationError::Binding);
    }
    let output = qualified
        .native_client()
        .list_object_versions()
        .bucket(c.bucket())
        .expected_bucket_owner(c.expected_owner())
        .prefix(c.key_prefix())
        .encoding_type(EncodingType::Url)
        .max_keys(i32::from(limit))
        .set_key_marker(cursor.map(|p| p.key.clone()))
        .set_version_id_marker(cursor.map(|p| p.version.clone()))
        .send()
        .await
        .map_err(|_| OrphanObservationError::Request)?;
    check_page(c, cursor, limit, &output)
}

fn check_page(
    c: &NamespaceConfiguration,
    cursor: Option<&VersionPageCursor>,
    limit: u16,
    out: &ListObjectVersionsOutput,
) -> Result<VersionPage, OrphanObservationError> {
    let bad = || OrphanObservationError::Listing;
    let count = out
        .versions()
        .len()
        .checked_add(out.delete_markers().len())
        .ok_or_else(bad)?;
    if count > usize::from(limit)
        || count > 1000
        || out.name() != Some(c.bucket())
        || out.max_keys() != Some(i32::from(limit))
        || out.encoding_type() != Some(&EncodingType::Url)
        || decode_key(out.prefix().ok_or_else(bad)?)? != c.key_prefix()
        || !out.common_prefixes().is_empty()
        || out.delimiter().is_some_and(|v| !v.is_empty())
        || decode_key(out.key_marker().unwrap_or(""))? != cursor.map_or("", |p| p.key.as_str())
        || out.version_id_marker().unwrap_or("") != cursor.map_or("", |p| p.version.as_str())
    {
        return Err(bad());
    }
    let mut seen = HashSet::new();
    // Retained identifiers are bounded by count * (1024 key + 1024 version).
    // Enforce an explicit aggregate budget in addition to each checked identity.
    let mut budget = 2 * 1024 * 1024usize;
    let mut entry = |key: Option<&str>,
                     version: Option<&str>,
                     modified: Option<&DateTime>,
                     latest: Option<bool>| {
        let key = decode_key(key.ok_or_else(bad)?)?;
        let v =
            VersionRef::new(c.store().clone(), key, version.ok_or_else(bad)?).map_err(|_| bad())?;
        if !v.key().starts_with(c.key_prefix())
            || latest.is_none()
            || cursor.is_some_and(|p| v.key() < p.key.as_str())
            || !seen.insert((v.key().to_owned(), v.version().to_owned()))
        {
            return Err(bad());
        }
        timestamp(modified)?;
        budget = budget
            .checked_sub(v.key().len() + v.version().len())
            .ok_or_else(bad)?;
        Ok(v)
    };
    let mut versions = Vec::with_capacity(out.versions().len());
    for v in out.versions() {
        if !v.size().is_some_and(|n| n >= 0) {
            return Err(bad());
        }
        versions.push(entry(
            v.key(),
            v.version_id(),
            v.last_modified(),
            v.is_latest(),
        )?);
    }
    let mut delete_markers = Vec::with_capacity(out.delete_markers().len());
    for v in out.delete_markers() {
        delete_markers.push(entry(
            v.key(),
            v.version_id(),
            v.last_modified(),
            v.is_latest(),
        )?);
    }
    let next = match out.is_truncated() {
        // Native terminal classification is authoritative; optional next-marker
        // fields can still be populated and are not a continuation obligation.
        Some(false) => None,
        Some(true) => {
            let key = decode_key(out.next_key_marker().ok_or_else(bad)?)?;
            let version = out.next_version_id_marker().ok_or_else(bad)?.to_owned();
            VersionRef::new(c.store().clone(), key.clone(), version.clone()).map_err(|_| bad())?;
            // Service markers need not name a returned entry. Version IDs are
            // opaque; validate bounded scope and marker progress, not ordering
            // or membership among this page's returned versions.
            if !key.starts_with(c.key_prefix())
                || cursor.is_some_and(|p| key < p.key || (p.key == key && p.version == version))
            {
                return Err(bad());
            }
            Some(VersionPageCursor {
                configuration: c.clone(),
                key,
                version,
            })
        }
        None => return Err(bad()),
    };
    Ok(VersionPage {
        configuration: c.clone(),
        input_markers: cursor.map(|p| (p.key.clone(), p.version.clone())),
        versions,
        delete_markers,
        next,
    })
}

#[cfg(feature = "dynamodb")]
mod observation {
    use super::*;
    use crate::{
        dynamodb::{CommandJournal, NamespaceSnapshot},
        identity::{DomainId, NamespaceId, ResourceId, Scope},
    };
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use uuid::Uuid;

    const FIELDS: [&str; 11] = [
        "schema",
        "domain",
        "incarnation",
        "namespace",
        "configuration-revision",
        "intent-generation",
        "intent-uuid",
        "attempt-generation",
        "attempt-uuid",
        "target-sha256",
        "constraints-sha256",
    ];

    /// Checked schema-1 provenance only; constraints must still be compared to
    /// persisted intent/attempt constraints when those records exist.
    #[derive(Debug)]
    pub struct KnownOrphanAttribution {
        intent: ResourceId,
        attempt: ResourceId,
        constraints_sha256: [u8; 32],
    }
    impl KnownOrphanAttribution {
        pub fn intent_id(&self) -> &ResourceId {
            &self.intent
        }
        pub fn attempt_id(&self) -> &ResourceId {
            &self.attempt
        }
        pub fn constraints_sha256(&self) -> &[u8; 32] {
            &self.constraints_sha256
        }
    }
    /// Private construction prevents caller-manufactured checked classifications.
    /// Unattributed is NOT grace eligibility or deletion permission.
    #[derive(Debug)]
    pub struct UnattributedOrphan {
        _checked: (),
    }
    #[derive(Debug)]
    pub enum OrphanAttribution {
        Known(KnownOrphanAttribution),
        Unattributed(UnattributedOrphan),
    }
    /// Immutable provider evidence, not perpetual namespace/retirement authority.
    /// Deliberately neither publicly constructible nor deserializable.
    ///
    /// ```compile_fail
    /// let _: storelink::s3::ObservedOrphanObject = serde_json::from_str("{}").unwrap();
    /// ```
    pub struct ObservedOrphanObject {
        namespace: NamespaceSnapshot,
        version: VersionRef,
        last_modified: TimestampMs,
        length: u64,
        attribution: OrphanAttribution,
    }
    impl std::fmt::Debug for ObservedOrphanObject {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ObservedOrphanObject")
                .finish_non_exhaustive()
        }
    }
    impl ObservedOrphanObject {
        pub fn namespace(&self) -> &NamespaceSnapshot {
            &self.namespace
        }
        pub fn version(&self) -> &VersionRef {
            &self.version
        }
        /// Conservative millisecond representation (rounded up, never earlier).
        pub fn last_modified(&self) -> TimestampMs {
            self.last_modified
        }
        pub fn length(&self) -> u64 {
            self.length
        }
        pub fn attribution(&self) -> &OrphanAttribution {
            &self.attribution
        }
    }

    pub async fn observe_orphan_object(
        journal: &CommandJournal,
        qualified: &QualifiedNamespace,
        version: &VersionRef,
    ) -> Result<ObservedOrphanObject, OrphanObservationError> {
        let c = qualified.configuration();
        // read_namespace validates the journal's OWN domain and exact namespace.
        let ns = journal
            .read_namespace(c.id())
            .await?
            .ok_or(OrphanObservationError::Binding)?;
        if c.id() != ns.id()
            || c.store() != ns.store()
            || c.bucket() != ns.bucket()
            || c.expected_owner() != ns.expected_owner()
            || c.region() != ns.region()
            || c.key_prefix() != ns.key_prefix()
            || c.provider_profile() != ns.provider_profile()
            || c.mpu_max_age_ms() != ns.mpu_max_age_ms()
            || c.mpu_lifecycle_margin_ms() != ns.mpu_lifecycle_margin_ms()
            || version.store() != ns.store()
            || !version.key().starts_with(ns.key_prefix())
        {
            return Err(OrphanObservationError::Binding);
        }
        let head = qualified
            .native_client()
            .head_object()
            .bucket(c.bucket())
            .expected_bucket_owner(c.expected_owner())
            .key(version.key())
            .version_id(version.version())
            .send()
            .await
            .map_err(|_| OrphanObservationError::Request)?;
        if head.version_id() != Some(version.version()) || head.delete_marker() == Some(true) {
            return Err(OrphanObservationError::Version);
        }
        if head.missing_meta().is_some_and(|n| n != 0) {
            return Err(OrphanObservationError::Metadata);
        }
        let length = head
            .content_length()
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(OrphanObservationError::ObjectMetadata)?;
        let last_modified = timestamp(head.last_modified())?;
        let empty = HashMap::new();
        let attribution = classify(head.metadata().unwrap_or(&empty), &ns, version)?;
        Ok(ObservedOrphanObject {
            namespace: ns,
            version: version.clone(),
            last_modified,
            length,
            attribution,
        })
    }

    fn hex_decode(value: &str) -> Option<Vec<u8>> {
        if !value.len().is_multiple_of(2)
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return None;
        }
        value
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| {
                let d = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
                Some(d(p[0]) * 16 + d(p[1]))
            })
            .collect()
    }
    fn canonical_uuid(value: &str) -> Option<Uuid> {
        Uuid::parse_str(value)
            .ok()
            .filter(|u| !u.is_nil() && u.to_string() == value)
    }
    fn positive(value: &str) -> Option<u64> {
        value
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0 && n.to_string() == value)
    }
    fn label(value: &str) -> Option<String> {
        String::from_utf8(hex_decode(value)?).ok()
    }
    fn digest(value: &str) -> Option<[u8; 32]> {
        hex_decode(value)?.try_into().ok()
    }

    fn classify(
        m: &HashMap<String, String>,
        ns: &NamespaceSnapshot,
        version: &VersionRef,
    ) -> Result<OrphanAttribution, OrphanObservationError> {
        if m.iter()
            .try_fold(0usize, |n, (k, v)| {
                n.checked_add(k.len())?.checked_add(v.len())
            })
            .is_none_or(|n| n > 2048)
        {
            return Err(OrphanObservationError::Metadata);
        }
        // Inspect ALL reserved names before any missing/malformed-field fallback.
        for key in m.keys() {
            if key.to_ascii_lowercase().starts_with("storelink-")
                && !FIELDS.iter().any(|f| key == &format!("storelink-{f}"))
            {
                return Err(OrphanObservationError::AttributionConflict);
            }
        }
        let get = |name: &str| m.get(&format!("storelink-{name}")).map(String::as_str);
        if get("schema").is_some_and(|s| s != "1") {
            return Err(OrphanObservationError::AttributionConflict);
        }
        // A valid foreign scope never becomes garbage even alongside malformed or
        // missing known fields. Check canonical binding fields independently.
        let domain = get("domain")
            .and_then(label)
            .and_then(|s| DomainId::new(s).ok());
        let namespace = get("namespace")
            .and_then(label)
            .and_then(|s| NamespaceId::new(s).ok());
        let incarnation = get("incarnation").and_then(canonical_uuid);
        let revision = get("configuration-revision").and_then(canonical_uuid);
        let target = get("target-sha256").and_then(digest);
        let mut hash = Sha256::new();
        for p in [version.store().as_str(), version.key()] {
            hash.update((p.len() as u64).to_be_bytes());
            hash.update(p.as_bytes());
        }
        let expected: [u8; 32] = hash.finalize().into();
        if domain.as_ref().is_some_and(|d| d != ns.domain())
            || namespace.as_ref().is_some_and(|n| n != ns.id())
            || revision.is_some_and(|r| r != ns.configuration_revision())
            || target.is_some_and(|t| t != expected)
        {
            return Err(OrphanObservationError::AttributionConflict);
        }
        let known = (|| {
            if get("schema")? != "1" {
                return None;
            }
            let domain = domain?;
            namespace?;
            revision?;
            target?;
            let incarnation = incarnation?;
            let intent_generation = positive(get("intent-generation")?)?;
            let attempt_generation = positive(get("attempt-generation")?)?;
            if attempt_generation < intent_generation {
                return None;
            }
            let intent = ResourceId::new(
                Scope::new(domain.clone(), incarnation, intent_generation).ok()?,
                canonical_uuid(get("intent-uuid")?)?,
            )
            .ok()?;
            let attempt = ResourceId::new(
                Scope::new(domain, incarnation, attempt_generation).ok()?,
                canonical_uuid(get("attempt-uuid")?)?,
            )
            .ok()?;
            Some(KnownOrphanAttribution {
                intent,
                attempt,
                constraints_sha256: digest(get("constraints-sha256")?)?,
            })
        })();
        Ok(match known {
            Some(k) => OrphanAttribution::Known(k),
            None => OrphanAttribution::Unattributed(UnattributedOrphan { _checked: () }),
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn canonical_scalars() {
            for bad in ["0", "01", "+1", " 1", "18446744073709551616"] {
                assert!(positive(bad).is_none());
            }
            assert_eq!(positive("12"), Some(12));
            assert!(canonical_uuid("00000000-0000-0000-0000-000000000000").is_none());
            assert!(canonical_uuid("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA").is_none());
            assert!(canonical_uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").is_some());
            for bad in ["FF", "f", "gg", "é"] {
                assert!(hex_decode(bad).is_none());
            }
            assert_eq!(label("c3a9"), Some("é".into()));
            assert!(label("ff").is_none());
            assert!(digest(&"a".repeat(64)).is_some());
            assert!(digest(&"A".repeat(64)).is_none());
            assert!(digest(&"a".repeat(62)).is_none());
        }
    }
}
#[cfg(feature = "dynamodb")]
pub use observation::{
    KnownOrphanAttribution, ObservedOrphanObject, OrphanAttribution, UnattributedOrphan,
    observe_orphan_object,
};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timestamps_never_round_grace_early() {
        assert_eq!(
            timestamp(Some(&DateTime::from_secs_and_nanos(1, 1)))
                .unwrap()
                .as_millis(),
            1001
        );
        assert_eq!(
            timestamp(Some(&DateTime::from_secs(1)))
                .unwrap()
                .as_millis(),
            1000
        );
        assert!(timestamp(None).is_err());
        assert!(timestamp(Some(&DateTime::from_secs(-1))).is_err());
        assert!(timestamp(Some(&DateTime::from_secs(i64::MAX))).is_err());
    }
    #[test]
    fn percent_decoding_preserves_literal_plus_and_path_bytes() {
        assert_eq!(
            decode_key("managed%2F..%2F%00%C3%A9+%20").unwrap(),
            "managed/../\0é+ "
        );
        for bad in ["%", "%0", "%GG", "%ff"] {
            assert!(decode_key(bad).is_err());
        }
    }
}
