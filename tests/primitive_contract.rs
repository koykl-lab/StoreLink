use proptest::prelude::*;
use serde_json::{Value, json};
use storelink::claim::{Claim, ClaimError, ClaimPolicy, ClaimState, Renewal, TerminalTransition};
use storelink::clock::{ClockError, ClockObservation, TimestampMs};
use storelink::identity::{
    CommandId, DomainFence, DomainId, FenceError, NamespaceId, ResourceId, Scope, StoreId,
    VersionRef,
};
use uuid::Uuid;

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn ts(n: u64) -> TimestampMs {
    TimestampMs::new(n).unwrap()
}
fn clock(n: u64, uncertainty: u64) -> ClockObservation {
    ClockObservation::trusted(ts(n), uncertainty).unwrap()
}
fn policy() -> ClaimPolicy {
    ClaimPolicy::new(1_000, 10, 50).unwrap()
}
fn scope(generation: u64) -> Scope {
    Scope::new(DomainId::new("domain").unwrap(), uuid(1), generation).unwrap()
}
fn resource(generation: u64) -> ResourceId {
    ResourceId::from_command(&command(generation))
}
fn command(generation: u64) -> CommandId {
    CommandId::new(scope(generation), uuid(3)).unwrap()
}
fn version() -> VersionRef {
    VersionRef::new(StoreId::new("store").unwrap(), "a/../%2F/é", "v1").unwrap()
}
fn claim() -> Claim {
    Claim::new(
        resource(1),
        version(),
        uuid(4),
        ts(500),
        clock(100, 0),
        &policy(),
    )
    .unwrap()
}
fn fence(current: u64, closed: u64, compacted: u64) -> DomainFence {
    DomainFence::new(
        DomainId::new("domain").unwrap(),
        uuid(1),
        current,
        closed,
        compacted,
    )
    .unwrap()
}
fn roundtrip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        &serde_json::from_value::<T>(serde_json::to_value(value).unwrap()).unwrap(),
        value
    );
}

#[test]
fn opaque_labels_use_utf8_bytes_without_normalization() {
    for good in [
        "a".to_owned(),
        "é".repeat(64),
        "🦀".repeat(32),
        " ../%2f\0 ".to_owned(),
    ] {
        let domain = DomainId::new(&good).unwrap();
        assert_eq!(domain.as_str(), good);
        roundtrip(&domain);
        roundtrip(&StoreId::new(&good).unwrap());
        roundtrip(&NamespaceId::new(&good).unwrap());
    }
    for bad in [String::new(), "é".repeat(65), "a".repeat(129)] {
        assert!(DomainId::new(&bad).is_err());
        assert!(StoreId::new(&bad).is_err());
        assert!(NamespaceId::new(&bad).is_err());
        assert!(serde_json::from_value::<DomainId>(json!(bad)).is_err());
        assert!(serde_json::from_value::<StoreId>(json!(bad)).is_err());
        assert!(serde_json::from_value::<NamespaceId>(json!(bad)).is_err());
    }
}

#[test]
fn scope_and_distinct_ids_have_checked_serde() {
    roundtrip(&scope(1));
    roundtrip(&resource(1));
    roundtrip(&command(1));
    assert!(Scope::new(DomainId::new("d").unwrap(), Uuid::nil(), 1).is_err());
    assert!(Scope::new(DomainId::new("d").unwrap(), uuid(1), 0).is_err());
    assert!(ResourceId::new(scope(1), Uuid::nil()).is_err());
    assert!(CommandId::new(scope(1), Uuid::nil()).is_err());
    for (field, invalid) in [
        ("domain", json!("")),
        ("incarnation", json!(Uuid::nil())),
        ("generation", json!(0)),
    ] {
        let mut value = serde_json::to_value(scope(1)).unwrap();
        value[field] = invalid;
        assert!(serde_json::from_value::<Scope>(value.clone()).is_err());
        assert!(
            serde_json::from_value::<ResourceId>(json!({"scope": value, "uuid": uuid(2)})).is_err()
        );
        assert!(
            serde_json::from_value::<CommandId>(json!({"scope": value, "uuid": uuid(3)})).is_err()
        );
    }
    assert!(
        serde_json::from_value::<ResourceId>(json!({"scope": scope(1), "uuid": Uuid::nil()}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<CommandId>(json!({"scope": scope(1), "uuid": Uuid::nil()}))
            .is_err()
    );
}

#[test]
fn exact_version_limits_null_and_checked_serde() {
    let store = StoreId::new("store").unwrap();
    let boundary = VersionRef::new(store.clone(), "🦀".repeat(256), "é".repeat(512)).unwrap();
    roundtrip(&boundary);
    for invalid in ["".to_owned(), "a".repeat(1025), "é".repeat(513)] {
        assert!(VersionRef::new(store.clone(), &invalid, "v").is_err());
        assert!(VersionRef::new(store.clone(), "k", &invalid).is_err());
        assert!(
            serde_json::from_value::<VersionRef>(
                json!({"store":"store", "key":invalid, "version":"v"})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<VersionRef>(
                json!({"store":"store", "key":"k", "version":invalid})
            )
            .is_err()
        );
    }
    assert!(VersionRef::new(store.clone(), "k", "null").is_err());
    assert!(
        serde_json::from_value::<VersionRef>(json!({"store":"store", "key":"k", "version":"null"}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<VersionRef>(json!({"store":"", "key":"k", "version":"v"}))
            .is_err()
    );
    for accepted in ["NULL", " null", "null\0", "null "] {
        assert!(VersionRef::new(store.clone(), "k", accepted).is_ok());
    }
}

#[test]
fn fingerprint_is_stable_unambiguous_and_full_tuple_equality_is_retained() {
    let exact = version();
    assert_eq!(exact.key(), "a/../%2F/é");
    let fingerprint: String = exact
        .fingerprint()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    // Independently generated with Python hashlib and big-endian length prefixes.
    assert_eq!(
        fingerprint,
        "79e80e5840233e792b11c26bdc264fb94cc1f804d2a182f213adcbcc5359ba7a"
    );
    let a = VersionRef::new(StoreId::new("a").unwrap(), "bc", "d").unwrap();
    let b = VersionRef::new(StoreId::new("ab").unwrap(), "c", "d").unwrap();
    let c = VersionRef::new(StoreId::new("a").unwrap(), "b", "cd").unwrap();
    assert_ne!(a, b);
    assert_ne!(a.fingerprint(), b.fingerprint());
    assert_ne!(a.fingerprint(), c.fingerprint());
    let composed = VersionRef::new(StoreId::new("store").unwrap(), "é", "v").unwrap();
    let decomposed = VersionRef::new(StoreId::new("store").unwrap(), "e\u{301}", "v").unwrap();
    assert_ne!(composed, decomposed);
    assert_ne!(composed.fingerprint(), decomposed.fingerprint());
    for changed in [
        VersionRef::new(StoreId::new("other").unwrap(), exact.key(), exact.version()).unwrap(),
        VersionRef::new(exact.store().clone(), "normalized", exact.version()).unwrap(),
        VersionRef::new(exact.store().clone(), exact.key(), "v2").unwrap(),
    ] {
        assert_ne!(exact, changed);
    }
}

#[test]
fn fence_invariants_and_deserialization_cannot_be_bypassed() {
    roundtrip(&fence(3, 2, 1));
    for (current, closed, compacted) in [(0, 0, 0), (1, 1, 0), (2, 1, 2), (1, 2, 0)] {
        assert!(
            DomainFence::new(
                DomainId::new("domain").unwrap(),
                uuid(1),
                current,
                closed,
                compacted
            )
            .is_err()
        );
        assert!(serde_json::from_value::<DomainFence>(json!({"domain":"domain", "incarnation":uuid(1), "current":current, "closed_through":closed, "compacted_through":compacted})).is_err());
    }
    let mut value = serde_json::to_value(fence(1, 0, 0)).unwrap();
    value["incarnation"] = json!(Uuid::nil());
    assert!(serde_json::from_value::<DomainFence>(value).is_err());
}

#[test]
fn closure_and_compaction_are_monotonic_and_overflow_is_atomic() {
    let mut f = fence(1, 0, 0);
    assert_eq!(f.close_through(1), Err(FenceError::InvalidWatermarks));
    assert_eq!(f.advance_generation(), Ok(2));
    f.close_through(1).unwrap();
    f.close_through(1).unwrap();
    assert_eq!(f.compact_through(2), Err(FenceError::InvalidWatermarks));
    f.compact_through(1).unwrap();
    f.compact_through(1).unwrap();
    let snapshot = f.clone();
    assert_eq!(f.close_through(0), Err(FenceError::Regression));
    assert_eq!(f.compact_through(0), Err(FenceError::Regression));
    assert_eq!(f, snapshot);
    let mut maximum = fence(u64::MAX, u64::MAX - 1, u64::MAX - 1);
    let before = maximum.clone();
    assert_eq!(
        maximum.advance_generation(),
        Err(FenceError::GenerationOverflow)
    );
    assert_eq!(maximum, before);
}

#[test]
fn fresh_command_cannot_resurrect_missing_old_ids_but_can_operate_existing_live_claim() {
    let f = fence(3, 2, 2);
    let fresh = command(3);
    let old = resource(1);
    assert_eq!(
        f.admit_command(&command(1)),
        Err(FenceError::GenerationNotAdmitted)
    );
    assert_eq!(
        f.admit_command(&command(4)),
        Err(FenceError::GenerationNotAdmitted)
    );
    assert_eq!(
        f.admit_creation(&fresh, &old),
        Err(FenceError::CreationGenerationMismatch)
    );
    f.admit_creation(&fresh, &resource(3)).unwrap();
    f.admit_existing(&fresh, &old).unwrap();
    let mut existing = claim();
    existing.validate_use(clock(200, 0), &policy()).unwrap();
    existing
        .renew(
            &Renewal::new(uuid(4), ts(600), uuid(5)).unwrap(),
            clock(200, 0),
            &policy(),
        )
        .unwrap();
    existing.release(uuid(5)).unwrap();
    // Fence admission alone is not authority; terminal resource state still fails.
    f.admit_existing(&fresh, existing.id()).unwrap();
    assert_eq!(
        existing.validate_use(clock(200, 0), &policy()),
        Err(ClaimError::Terminal)
    );
    assert_eq!(
        f.admit_existing(&fresh, &resource(4)),
        Err(FenceError::FutureResource)
    );
    // Different still-open generations are also not creation aliases.
    assert_eq!(
        fence(3, 0, 0).admit_creation(&fresh, &old),
        Err(FenceError::CreationGenerationMismatch)
    );
}

#[test]
fn resource_factory_preserves_creator_scope_and_uuid_and_read_constructors() {
    let creator = command(2);
    let created = ResourceId::from_command(&creator);
    assert_eq!(created.scope(), creator.scope());
    assert_eq!(created.uuid(), creator.uuid());
    assert_eq!(
        created,
        ResourceId::new(creator.scope().clone(), creator.uuid()).unwrap()
    );
    roundtrip(&created);
    fence(2, 1, 0).admit_creation(&creator, &created).unwrap();
    // Stored IDs remain structurally readable independently of admission.
    let independent_nonce = ResourceId::new(creator.scope().clone(), uuid(99)).unwrap();
    roundtrip(&independent_nonce);
    assert_eq!(
        fence(2, 1, 0).admit_creation(&creator, &independent_nonce),
        Err(FenceError::CreatorMismatch)
    );
}

#[test]
fn different_command_cannot_resurrect_deleted_resource_in_same_open_generation() {
    let f = fence(2, 1, 0);
    let creator = command(2);
    let original = ResourceId::from_command(&creator);
    f.admit_creation(&creator, &original).unwrap();
    let later = CommandId::new(creator.scope().clone(), uuid(99)).unwrap();
    f.admit_command(&later).unwrap();
    // While the resource exists, another command may operate on it. After
    // natural terminal cleanup deletes it, the same ID must fail creation.
    f.admit_existing(&later, &original).unwrap();
    assert_eq!(
        f.admit_creation(&later, &original),
        Err(FenceError::CreatorMismatch)
    );
    f.admit_creation(&later, &ResourceId::from_command(&later))
        .unwrap();
    // Matching creator UUID does not bypass the creation-generation fence.
    let newer = CommandId::new(scope(3), creator.uuid()).unwrap();
    assert_eq!(
        fence(3, 1, 0).admit_creation(&newer, &original),
        Err(FenceError::CreationGenerationMismatch)
    );
}

#[test]
fn domain_and_incarnation_must_match_for_commands_and_resources() {
    let f = fence(2, 1, 0);
    for bad_scope in [
        Scope::new(DomainId::new("other").unwrap(), uuid(1), 2).unwrap(),
        Scope::new(DomainId::new("domain").unwrap(), uuid(9), 2).unwrap(),
    ] {
        let cmd = CommandId::new(bad_scope.clone(), uuid(3)).unwrap();
        let resource = ResourceId::new(bad_scope, uuid(2)).unwrap();
        assert_eq!(f.admit_command(&cmd), Err(FenceError::ScopeMismatch));
        assert_eq!(
            f.admit_creation(&command(2), &resource),
            Err(FenceError::ScopeMismatch)
        );
        assert_eq!(
            f.admit_existing(&command(2), &resource),
            Err(FenceError::ScopeMismatch)
        );
    }
}

#[test]
fn timestamps_check_signed_bound_overflow_and_serde() {
    roundtrip(&TimestampMs::ZERO);
    roundtrip(&TimestampMs::MAX);
    assert_eq!(TimestampMs::MAX.checked_add(0), Ok(TimestampMs::MAX));
    assert_eq!(TimestampMs::MAX.checked_add(1), Err(ClockError::Overflow));
    assert_eq!(ts(1).checked_add(u64::MAX), Err(ClockError::Overflow));
    assert!(TimestampMs::new(i64::MAX as u64 + 1).is_err());
    for invalid in [
        json!(-1),
        json!(i64::MAX as u64 + 1),
        json!(u64::MAX),
        json!(1.5),
        json!("100"),
    ] {
        assert!(serde_json::from_value::<TimestampMs>(invalid).is_err());
    }
    assert_eq!(
        ClockObservation::trusted(TimestampMs::MAX, 1),
        Err(ClockError::Overflow)
    );
}

#[test]
fn clocks_use_opposite_conservative_bounds_and_fail_closed_when_untrusted() {
    let c = clock(100, 20);
    assert_eq!(c.upper_bound().unwrap(), ts(120));
    assert_eq!(c.lower_bound().unwrap(), ts(80));
    assert_eq!(clock(0, 20).lower_bound().unwrap(), TimestampMs::ZERO);
    assert_eq!(c.validate(19), Err(ClockError::ExcessiveUncertainty));
    let bad = ClockObservation::untrusted(ts(100), u64::MAX);
    assert_eq!(bad.upper_bound(), Err(ClockError::Untrusted));
    assert_eq!(bad.lower_bound(), Err(ClockError::Untrusted));
    assert_eq!(bad.validate(u64::MAX), Err(ClockError::Untrusted));
}

#[test]
fn claim_policy_is_explicit_positive_bounded_and_checked_on_deserialization() {
    roundtrip(&policy());
    for (lifetime, margin, uncertainty) in [
        (0, 1, 0),
        (100, 0, 0),
        (100, 100, 0),
        (100, 101, 0),
        (u64::MAX, 1, 0),
        (100, 1, u64::MAX),
    ] {
        assert!(ClaimPolicy::new(lifetime, margin, uncertainty).is_err());
        assert!(serde_json::from_value::<ClaimPolicy>(json!({"max_lifetime_ms":lifetime,"renewal_margin_ms":margin,"max_clock_uncertainty_ms":uncertainty})).is_err());
    }
    assert!(ClaimPolicy::new(100, 1, 0).is_ok());
}

#[test]
fn checked_claim_and_renewal_deserialization_are_not_current_authority() {
    roundtrip(&claim());
    let renewal = Renewal::new(uuid(4), ts(600), uuid(5)).unwrap();
    roundtrip(&renewal);
    let mut record = serde_json::to_value(claim()).unwrap();
    assert_eq!(record["state"], "ACTIVE");
    for (field, bad) in [
        ("revision", json!(Uuid::nil())),
        ("deadline", json!(u64::MAX)),
        ("state", json!("MISSING")),
    ] {
        let mut mutated = record.clone();
        mutated[field] = bad;
        assert!(serde_json::from_value::<Claim>(mutated).is_err());
    }
    record["id"]["scope"]["generation"] = json!(0);
    assert!(serde_json::from_value::<Claim>(record).is_err());
    for (field, bad) in [
        ("expected_revision", json!(Uuid::nil())),
        ("new_revision", json!(Uuid::nil())),
        ("new_revision", json!(uuid(4))),
        ("proposed_deadline", json!(u64::MAX)),
    ] {
        let mut mutated = serde_json::to_value(&renewal).unwrap();
        mutated[field] = bad;
        assert!(serde_json::from_value::<Renewal>(mutated).is_err());
    }
    let restored: Claim = serde_json::from_value(serde_json::to_value(claim()).unwrap()).unwrap();
    assert_eq!(
        restored.validate_use(clock(500, 0), &policy()),
        Err(ClaimError::Expired)
    );
}

#[test]
fn acquisition_checks_revision_deadline_horizon_margin_and_clock() {
    let create = |revision, deadline, clock| {
        Claim::new(resource(1), version(), revision, deadline, clock, &policy())
    };
    assert!(create(Uuid::nil(), ts(500), clock(100, 0)).is_err());
    assert_eq!(
        create(uuid(4), ts(120), clock(100, 20)),
        Err(ClaimError::Expired)
    );
    assert_eq!(
        create(uuid(4), ts(130), clock(100, 20)),
        Err(ClaimError::RenewalMargin)
    );
    assert!(create(uuid(4), ts(131), clock(100, 20)).is_ok());
    assert!(create(uuid(4), ts(1120), clock(100, 20)).is_ok());
    assert_eq!(
        create(uuid(4), ts(1121), clock(100, 20)),
        Err(ClaimError::LifetimeExceeded)
    );
    assert_eq!(
        create(uuid(4), ts(500), ClockObservation::untrusted(ts(100), 0)),
        Err(ClaimError::Clock(ClockError::Untrusted))
    );
    assert_eq!(
        create(uuid(4), ts(500), clock(100, 51)),
        Err(ClaimError::Clock(ClockError::ExcessiveUncertainty))
    );
    assert_eq!(
        create(uuid(4), TimestampMs::MAX, clock(i64::MAX as u64 - 100, 0)),
        Err(ClaimError::Clock(ClockError::Overflow))
    );
}

#[test]
fn use_and_revocation_have_a_safe_uncertainty_gap() {
    let mut c = claim();
    assert!(c.validate_use(clock(479, 20), &policy()).is_ok());
    assert_eq!(
        c.validate_use(clock(480, 20), &policy()),
        Err(ClaimError::Expired)
    );
    assert_eq!(
        c.revoke(uuid(4), clock(519, 20), &policy()),
        Err(ClaimError::NotRevocable)
    );
    assert_eq!(c.state(), ClaimState::Active); // time passing is not a transition
    assert_eq!(
        c.revoke(uuid(4), clock(520, 20), &policy()),
        Ok(TerminalTransition::Changed)
    );
    assert_eq!(c.state(), ClaimState::Revoked);
}

#[test]
fn untrusted_or_excess_uncertainty_cannot_authorize_use_renew_or_revoke() {
    for observation in [ClockObservation::untrusted(ts(200), 0), clock(200, 51)] {
        let mut c = claim();
        let before = c.clone();
        assert!(c.validate_use(observation, &policy()).is_err());
        assert!(
            c.renew(
                &Renewal::new(uuid(4), ts(600), uuid(5)).unwrap(),
                observation,
                &policy()
            )
            .is_err()
        );
        assert!(c.revoke(uuid(4), observation, &policy()).is_err());
        assert_eq!(c, before);
    }
}

#[test]
fn renewal_fixes_deadline_and_stale_replay_cannot_extend_it() {
    let request = Renewal::new(uuid(4), ts(700), uuid(5)).unwrap();
    let mut early = claim();
    let mut delayed = early.clone();
    early.renew(&request, clock(200, 0), &policy()).unwrap();
    delayed.renew(&request, clock(300, 20), &policy()).unwrap();
    assert_eq!(early, delayed);
    assert_eq!(early.deadline(), ts(700));
    assert_eq!(request.proposed_deadline(), ts(700));
    assert_eq!(early.id(), &resource(1));
    assert_eq!(early.version(), &version());
    assert_eq!(
        early.renew(&request, clock(400, 0), &policy()),
        Err(ClaimError::RevisionMismatch)
    );
    assert_eq!(early.deadline(), ts(700));
    assert_eq!(
        early.revoke(uuid(4), clock(800, 0), &policy()),
        Err(ClaimError::RevisionMismatch)
    );
    assert_eq!(early.release(uuid(4)), Err(ClaimError::RevisionMismatch));
    assert_eq!(early.state(), ClaimState::Active);
    assert_eq!(
        early.revoke(uuid(5), clock(800, 0), &policy()),
        Ok(TerminalTransition::Changed)
    );
}

#[test]
fn failed_renewals_do_not_mutate_or_revive_claims() {
    let mut c = claim();
    let before = c.clone();
    for deadline in [499, 500] {
        assert_eq!(
            c.renew(
                &Renewal::new(uuid(4), ts(deadline), uuid(5)).unwrap(),
                clock(200, 0),
                &policy()
            ),
            Err(ClaimError::DeadlineNotForward)
        );
    }
    assert_eq!(
        c.renew(
            &Renewal::new(uuid(4), ts(1201), uuid(5)).unwrap(),
            clock(200, 0),
            &policy()
        ),
        Err(ClaimError::LifetimeExceeded)
    );
    let renewal = Renewal::new(uuid(4), ts(600), uuid(5)).unwrap();
    assert_eq!(
        c.renew(&renewal, clock(480, 10), &policy()),
        Err(ClaimError::RenewalMargin)
    );
    assert_eq!(
        c.renew(&renewal, clock(490, 10), &policy()),
        Err(ClaimError::Expired)
    );
    assert_eq!(
        c.renew(&renewal, clock(10_000, 0), &policy()),
        Err(ClaimError::Expired)
    );
    assert_eq!(c, before);
    assert!(Renewal::new(uuid(4), ts(600), uuid(4)).is_err());
    assert!(Renewal::new(uuid(4), ts(600), Uuid::nil()).is_err());
    c.revoke(uuid(4), clock(500, 0), &policy()).unwrap();
    assert_eq!(
        c.renew(&renewal, clock(100, 0), &policy()),
        Err(ClaimError::Terminal)
    );
}

#[test]
fn both_terminal_paths_change_accounting_once_and_never_reopen() {
    for release_first in [false, true] {
        let mut c = claim();
        let first = if release_first {
            c.release(uuid(4))
        } else {
            c.revoke(uuid(4), clock(500, 0), &policy())
        }
        .unwrap();
        assert!(first.accounting_must_change());
        let terminal_state = c.state();
        for _ in 0..10 {
            assert!(!c.release(uuid(4)).unwrap().accounting_must_change());
            assert!(
                !c.revoke(uuid(4), ClockObservation::untrusted(ts(0), 0), &policy())
                    .unwrap()
                    .accounting_must_change()
            );
        }
        assert_eq!(c.state(), terminal_state);
        assert_eq!(
            c.validate_use(clock(100, 0), &policy()),
            Err(ClaimError::Terminal)
        );
        assert_eq!(
            c.renew(
                &Renewal::new(uuid(4), ts(600), uuid(5)).unwrap(),
                clock(100, 0),
                &policy()
            ),
            Err(ClaimError::Terminal)
        );
        roundtrip(&c);
    }
}

proptest! {
    #[test]
    fn arbitrary_unicode_labels_follow_byte_contract(value in any::<String>()) {
        let valid = !value.is_empty() && value.len() <= 128;
        prop_assert_eq!(DomainId::new(&value).is_ok(),valid);
        prop_assert_eq!(StoreId::new(&value).is_ok(),valid);
        prop_assert_eq!(NamespaceId::new(&value).is_ok(),valid);
        prop_assert_eq!(serde_json::from_value::<DomainId>(json!(&value)).is_ok(),valid);
    }

    #[test]
    fn arbitrary_version_tuples_roundtrip_or_reject(key in any::<String>(), version in any::<String>()) {
        let valid = !key.is_empty() && key.len() <= 1024 && !version.is_empty() && version.len() <= 1024 && version != "null";
        let constructed = VersionRef::new(StoreId::new("s").unwrap(),&key,&version);
        let decoded = serde_json::from_value::<VersionRef>(json!({"store":"s","key":key,"version":version}));
        prop_assert_eq!(constructed.is_ok(),valid);
        prop_assert_eq!(decoded.is_ok(),valid);
        if let (Ok(a),Ok(b)) = (constructed,decoded) { prop_assert_eq!(a,b); }
    }

    #[test]
    fn timestamp_arithmetic_never_wraps(now in 0..=i64::MAX as u64, delta in any::<u64>()) {
        let expected = (now as u128) + (delta as u128);
        let result = ts(now).checked_add(delta);
        prop_assert_eq!(result.is_ok(),expected <= i64::MAX as u128);
        if let Ok(value) = result { prop_assert_eq!(value.as_millis() as u128,expected); }
    }

    #[test]
    fn fences_admit_exactly_open_generations(current in 1u64..1000, raw_closed in any::<u64>(), generation in 1u64..1100) {
        let closed = raw_closed % current;
        let f = fence(current,closed,closed);
        prop_assert_eq!(f.admit_command(&command(generation)).is_ok(),closed < generation && generation <= current);
        prop_assert_eq!(f.admit_creation(&command(current), &resource(generation)).is_ok(),generation == current);
        prop_assert_eq!(f.admit_existing(&command(current), &resource(generation)).is_ok(),generation <= current);
    }

    #[test]
    fn creation_binds_creator_uuid_even_within_an_open_generation(generation in 1u64..1000, creator_uuid in 1u128..u128::MAX, same_creator in any::<bool>()) {
        let f = fence(generation, 0, 0);
        let creator = CommandId::new(scope(generation), uuid(creator_uuid)).unwrap();
        let original = ResourceId::from_command(&creator);
        let candidate_uuid = if same_creator { creator_uuid } else { creator_uuid + 1 };
        let candidate = CommandId::new(scope(generation), uuid(candidate_uuid)).unwrap();
        prop_assert_eq!(f.admit_creation(&candidate, &original).is_ok(), same_creator);
        if !same_creator {
            prop_assert_eq!(f.admit_creation(&candidate, &original), Err(FenceError::CreatorMismatch));
        }
        prop_assert!(f.admit_existing(&candidate, &original).is_ok());
    }

    #[test]
    fn uncertainty_never_authorizes_use_and_revocation_together(now in 0u64..1000, uncertainty in 0u64..=50) {
        let mut c = claim();
        let observation = clock(now,uncertainty);
        let use_allowed = c.validate_use(observation,&policy()).is_ok();
        let revoked = c.revoke(uuid(4),observation,&policy()).is_ok();
        prop_assert!(!(use_allowed && revoked));
        prop_assert_eq!(use_allowed,now + uncertainty < 500);
        prop_assert_eq!(revoked,now.saturating_sub(uncertainty) >= 500);
    }

    #[test]
    fn terminal_sequences_decrement_exactly_once(releases in prop::collection::vec(any::<bool>(),1..100)) {
        let mut c = claim();
        let mut decrements = 0;
        for release in releases {
            let result = if release { c.release(uuid(4)) } else { c.revoke(uuid(4),clock(500,0),&policy()) }.unwrap();
            decrements += u32::from(result.accounting_must_change());
        }
        prop_assert_eq!(decrements,1);
        prop_assert_ne!(c.state(),ClaimState::Active);
    }

    #[test]
    fn valid_deserialization_cannot_bypass_nested_version_guards(bad in prop_oneof![Just(Value::Null), Just(json!("null")), Just(json!("")), Just(json!("🦀".repeat(257)))]) {
        let mut record = serde_json::to_value(claim()).unwrap();
        record["version"]["version"] = bad;
        prop_assert!(serde_json::from_value::<Claim>(record).is_err());
    }
}
