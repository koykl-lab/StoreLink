use std::time::Duration;

use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
use aws_sdk_s3::types::{BucketVersioningStatus, ExpirationStatus, LifecycleRule, MfaDeleteStatus};
use thiserror::Error;

const DAY_MS: u64 = 86_400_000;

/// Explicit supported-attempt age and provider eligibility margin, not a promise
/// about when AWS will physically finish cleanup. No production defaults.
#[derive(Clone, Copy, Debug)]
pub struct MpuAgePolicy {
    minimum_eligibility_ms: u64,
}
impl MpuAgePolicy {
    pub fn new(
        supported_age: Duration,
        eligibility_margin: Duration,
    ) -> Result<Self, LifecycleError> {
        let age = u64::try_from(supported_age.as_millis())
            .map_err(|_| LifecycleError::InvalidAgePolicy)?;
        let margin = u64::try_from(eligibility_margin.as_millis())
            .map_err(|_| LifecycleError::InvalidAgePolicy)?;
        if age == 0 || margin == 0 {
            return Err(LifecycleError::InvalidAgePolicy);
        }
        let minimum_eligibility_ms = age
            .checked_add(margin)
            .ok_or(LifecycleError::InvalidAgePolicy)?;
        Ok(Self {
            minimum_eligibility_ms,
        })
    }
}

/// Assessment of supplied observations only, not an atomic/current/future bucket
/// configuration guarantee or a capability accepted by an upload API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleReport {
    /// A finite complete-prefix rule exists; this is its smallest observed age.
    pub covering_abort_days: u32,
    /// Also accounts for earlier rules applying to only a subset of this scope.
    pub earliest_overlapping_abort_days: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LifecycleError {
    #[error("MPU supported age and eligibility margin must be positive and representable")]
    InvalidAgePolicy,
    #[error("managed prefix exceeds the 1024-byte S3 key bound")]
    InvalidPrefix,
    #[error("versioning must be enabled, not absent or suspended")]
    VersioningRequired,
    #[error("MFA Delete would require manual intervention in version reclamation")]
    MfaDelete,
    #[error("unknown provider configuration state")]
    UnknownState,
    #[error("ambiguous or unsupported lifecycle filter representation")]
    InvalidFilter,
    #[error("completed-object expiration overlaps managed scope")]
    CompletedObjectExpiration,
    #[error("restore-required or unknown storage transition overlaps managed scope")]
    UnavailableStorageTransition,
    #[error("incomplete multipart expiration must specify a positive day count")]
    InvalidAbortAge,
    #[error("an overlapping MPU rule can act before supported age plus margin")]
    AbortBeforeSupportedAge,
    #[error("no unconditional multipart-cleanup rule covers the entire managed prefix")]
    MissingMultipartCoverage,
}

struct FilterScope<'a> {
    prefix: &'a str,
    unconditional: bool,
}

#[allow(deprecated)] // Read native legacy Prefix responses; do not generate them.
fn filter_scope(rule: &LifecycleRule) -> Result<FilterScope<'_>, LifecycleError> {
    if rule.prefix.is_some() && rule.filter.is_some() {
        return Err(LifecycleError::InvalidFilter);
    }
    if let Some(prefix) = rule.prefix.as_deref() {
        return Ok(FilterScope {
            prefix,
            unconditional: true,
        });
    }
    let Some(filter) = rule.filter.as_ref() else {
        // Missing schema information cannot prove cleanup coverage. Assume
        // possible whole-bucket overlap when evaluating destructive actions.
        return Ok(FilterScope {
            prefix: "",
            unconditional: false,
        });
    };
    let count = usize::from(filter.prefix.is_some())
        + usize::from(filter.tag.is_some())
        + usize::from(filter.object_size_greater_than.is_some())
        + usize::from(filter.object_size_less_than.is_some())
        + usize::from(filter.and.is_some());
    if count > 1 {
        return Err(LifecycleError::InvalidFilter);
    }
    if let Some(and) = &filter.and {
        let predicates = usize::from(and.prefix.is_some())
            + usize::from(and.object_size_greater_than.is_some())
            + usize::from(and.object_size_less_than.is_some())
            + and.tags().len();
        let mut tag_keys = std::collections::BTreeSet::new();
        if predicates < 2
            || and
                .tags()
                .iter()
                .any(|tag| tag.key().is_empty() || !tag_keys.insert(tag.key()))
            || and.object_size_greater_than.is_some_and(|v| v < 0)
            || and.object_size_less_than.is_some_and(|v| v < 0)
            || matches!((and.object_size_greater_than, and.object_size_less_than), (Some(lower), Some(upper)) if lower >= upper)
        {
            return Err(LifecycleError::InvalidFilter);
        }
        return Ok(FilterScope {
            prefix: and.prefix.as_deref().unwrap_or(""),
            unconditional: false,
        });
    }
    Ok(FilterScope {
        prefix: filter.prefix.as_deref().unwrap_or(""),
        unconditional: filter.tag.is_none()
            && filter.object_size_greater_than.is_none()
            && filter.object_size_less_than.is_none(),
    })
}

fn overlaps(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}
fn instant_class(value: &str) -> bool {
    matches!(
        value,
        "STANDARD_IA" | "ONEZONE_IA" | "INTELLIGENT_TIERING" | "GLACIER_IR"
    )
}

/// Assess native configuration observations without inventing an application
/// retention policy. Every possibly overlapping expiration rule matters, not
/// merely the first matching rule. Tag/size predicates are not trusted exclusions:
/// an application may legitimately reference an object matching those predicates.
/// Optional Intelligent-Tiering archive configurations are not inspected by
/// qualification. Operators must keep restore-required optional tiers disabled;
/// this lifecycle report alone does not establish their absence.
pub fn assess_lifecycle(
    managed_prefix: &str,
    policy: MpuAgePolicy,
    versioning: &GetBucketVersioningOutput,
    lifecycle: &GetBucketLifecycleConfigurationOutput,
) -> Result<LifecycleReport, LifecycleError> {
    if managed_prefix.len() > 1024 {
        return Err(LifecycleError::InvalidPrefix);
    }
    if versioning.status() != Some(&BucketVersioningStatus::Enabled) {
        return Err(LifecycleError::VersioningRequired);
    }
    match versioning.mfa_delete() {
        None | Some(MfaDeleteStatus::Disabled) => {}
        Some(MfaDeleteStatus::Enabled) => return Err(LifecycleError::MfaDelete),
        _ => return Err(LifecycleError::UnknownState),
    }
    let mut covering: Option<u32> = None;
    let mut earliest: Option<u32> = None;
    for rule in lifecycle.rules() {
        match rule.status() {
            ExpirationStatus::Disabled => continue,
            ExpirationStatus::Enabled => {}
            _ => return Err(LifecycleError::UnknownState),
        }
        let scope = filter_scope(rule)?;
        if !overlaps(managed_prefix, scope.prefix) {
            continue;
        }
        if rule.noncurrent_version_expiration().is_some() {
            return Err(LifecycleError::CompletedObjectExpiration);
        }
        if let Some(expiration) = rule.expiration() {
            // Removing an otherwise empty delete marker cannot remove protected
            // version bytes. No current-object date/day expiration is accepted.
            if expiration.days().is_some()
                || expiration.date().is_some()
                || expiration.expired_object_delete_marker() != Some(true)
            {
                return Err(LifecycleError::CompletedObjectExpiration);
            }
        }
        for class in rule.transitions().iter().map(|v| v.storage_class()).chain(
            rule.noncurrent_version_transitions()
                .iter()
                .map(|v| v.storage_class()),
        ) {
            if !class.is_some_and(|v| instant_class(v.as_str())) {
                return Err(LifecycleError::UnavailableStorageTransition);
            }
        }
        if let Some(abort) = rule.abort_incomplete_multipart_upload() {
            let days = abort
                .days_after_initiation()
                .filter(|d| *d > 0)
                .ok_or(LifecycleError::InvalidAbortAge)? as u32;
            if u64::from(days) * DAY_MS < policy.minimum_eligibility_ms {
                return Err(LifecycleError::AbortBeforeSupportedAge);
            }
            earliest = Some(earliest.map_or(days, |old| old.min(days)));
            if scope.unconditional && managed_prefix.starts_with(scope.prefix) {
                covering = Some(covering.map_or(days, |old| old.min(days)));
            }
        }
    }
    let covering_abort_days = covering.ok_or(LifecycleError::MissingMultipartCoverage)?;
    Ok(LifecycleReport {
        covering_abort_days,
        earliest_overlapping_abort_days: earliest.expect("coverage also establishes overlap"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::{
        AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleRuleAndOperator,
        LifecycleRuleFilter, NoncurrentVersionExpiration, Tag, Transition, TransitionStorageClass,
    };

    fn policy() -> MpuAgePolicy {
        MpuAgePolicy::new(Duration::from_secs(3600), Duration::from_secs(3600)).unwrap()
    }
    fn versioning() -> GetBucketVersioningOutput {
        GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Enabled)
            .build()
    }
    fn abort(prefix: &str, days: i32) -> LifecycleRule {
        LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
            .abort_incomplete_multipart_upload(
                AbortIncompleteMultipartUpload::builder()
                    .days_after_initiation(days)
                    .build(),
            )
            .build()
            .unwrap()
    }
    fn assess(rules: Vec<LifecycleRule>) -> Result<LifecycleReport, LifecycleError> {
        assess_lifecycle(
            "managed/",
            policy(),
            &versioning(),
            &GetBucketLifecycleConfigurationOutput::builder()
                .set_rules(Some(rules))
                .build(),
        )
    }

    #[test]
    fn requires_whole_prefix_coverage_and_enabled_versioning() {
        assert_eq!(
            assess(vec![]),
            Err(LifecycleError::MissingMultipartCoverage)
        );
        assert_eq!(
            assess(vec![abort("managed/subset/", 3)]),
            Err(LifecycleError::MissingMultipartCoverage)
        );
        assert_eq!(assess(vec![abort("", 3)]).unwrap().covering_abort_days, 3);
        let disabled = GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Suspended)
            .build();
        assert_eq!(
            assess_lifecycle(
                "managed/",
                policy(),
                &disabled,
                &GetBucketLifecycleConfigurationOutput::builder()
                    .rules(abort("", 3))
                    .build()
            ),
            Err(LifecycleError::VersioningRequired)
        );
    }

    #[test]
    fn noncurrent_age_or_tag_is_never_proof_that_references_ended() {
        let mut expires = abort("managed/", 3);
        expires.noncurrent_version_expiration = Some(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(180)
                .build(),
        );
        assert_eq!(
            assess(vec![abort("", 3), expires.clone()]),
            Err(LifecycleError::CompletedObjectExpiration)
        );
        expires.filter = Some(
            LifecycleRuleFilter::builder()
                .tag(Tag::builder().key("old").value("yes").build().unwrap())
                .build(),
        );
        assert_eq!(
            assess(vec![abort("", 3), expires]),
            Err(LifecycleError::CompletedObjectExpiration)
        );
    }

    #[test]
    fn disjoint_expiration_does_not_constrain_managed_scope() {
        let mut other = abort("other/", 1);
        other.noncurrent_version_expiration = Some(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        );
        assert!(assess(vec![abort("managed/", 3), other]).is_ok());
    }

    #[test]
    fn overlapping_earlier_abort_cannot_hide_behind_covering_rule() {
        let policy =
            MpuAgePolicy::new(Duration::from_secs(2 * 86400), Duration::from_secs(86400)).unwrap();
        let lifecycle = GetBucketLifecycleConfigurationOutput::builder()
            .rules(abort("", 30))
            .rules(abort("managed/subset/", 1))
            .build();
        assert_eq!(
            assess_lifecycle("managed/", policy, &versioning(), &lifecycle),
            Err(LifecycleError::AbortBeforeSupportedAge)
        );
    }

    #[test]
    fn predicates_cannot_claim_universal_multipart_coverage() {
        let mut limited = abort("", 3);
        limited.filter = Some(
            LifecycleRuleFilter::builder()
                .and(
                    LifecycleRuleAndOperator::builder()
                        .prefix("managed/")
                        .object_size_greater_than(1024)
                        .build(),
                )
                .build(),
        );
        assert_eq!(
            assess(vec![limited]),
            Err(LifecycleError::MissingMultipartCoverage)
        );
        let mut malformed = abort("", 3);
        malformed.filter = Some(
            LifecycleRuleFilter::builder()
                .prefix("managed/")
                .object_size_less_than(1)
                .build(),
        );
        assert_eq!(assess(vec![malformed]), Err(LifecycleError::InvalidFilter));
    }

    #[test]
    fn malformed_and_cannot_hide_behind_another_covering_rule() {
        let tag = || Tag::builder().key("a").value("b").build().unwrap();
        let invalid = [
            LifecycleRuleAndOperator::builder().build(),
            LifecycleRuleAndOperator::builder()
                .prefix("managed/")
                .build(),
            LifecycleRuleAndOperator::builder().tags(tag()).build(),
            LifecycleRuleAndOperator::builder()
                .tags(tag())
                .tags(tag())
                .build(),
            LifecycleRuleAndOperator::builder()
                .prefix("managed/")
                .object_size_greater_than(-1)
                .build(),
            LifecycleRuleAndOperator::builder()
                .object_size_greater_than(10)
                .object_size_less_than(5)
                .build(),
        ];
        for and in invalid {
            let mut rule = abort("managed/", 3);
            rule.filter = Some(LifecycleRuleFilter::builder().and(and).build());
            assert_eq!(
                assess(vec![abort("", 3), rule]),
                Err(LifecycleError::InvalidFilter)
            );
        }
    }

    #[test]
    fn disabled_rules_and_empty_delete_markers_are_distinct_from_body_expiration() {
        let mut disabled = abort("", 1);
        disabled.status = ExpirationStatus::Disabled;
        disabled.noncurrent_version_expiration = Some(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        );
        assert!(assess(vec![abort("", 3), disabled]).is_ok());
        let mut markers = abort("", 3);
        markers.expiration = Some(
            LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        );
        assert!(assess(vec![markers.clone()]).is_ok());
        markers.expiration = Some(LifecycleExpiration::builder().days(1).build());
        assert_eq!(
            assess(vec![markers]),
            Err(LifecycleError::CompletedObjectExpiration)
        );
    }

    #[test]
    fn restore_required_transitions_and_mfa_delete_are_not_automatic_reclamation() {
        let mut rule = abort("", 3);
        rule.transitions = Some(vec![
            Transition::builder()
                .days(1)
                .storage_class(TransitionStorageClass::Glacier)
                .build(),
        ]);
        assert_eq!(
            assess(vec![rule]),
            Err(LifecycleError::UnavailableStorageTransition)
        );
        let locked = GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Enabled)
            .mfa_delete(MfaDeleteStatus::Enabled)
            .build();
        assert_eq!(
            assess_lifecycle(
                "managed/",
                policy(),
                &locked,
                &GetBucketLifecycleConfigurationOutput::builder()
                    .rules(abort("", 3))
                    .build()
            ),
            Err(LifecycleError::MfaDelete)
        );
    }

    #[test]
    fn ages_are_explicit_bounded_and_have_a_positive_margin() {
        assert!(MpuAgePolicy::new(Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(MpuAgePolicy::new(Duration::from_secs(1), Duration::ZERO).is_err());
        assert!(MpuAgePolicy::new(Duration::MAX, Duration::from_secs(1)).is_err());
        assert_eq!(
            assess(vec![abort("", 0)]),
            Err(LifecycleError::InvalidAbortAge)
        );
    }
}
