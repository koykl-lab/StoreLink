//! Synthetic codec/action fixtures, not a DynamoDB expression evaluator.
use super::*;
use crate::identity::{CommandId, StoreId};
use aws_sdk_dynamodb::primitives::Blob;
use uuid::Uuid;

fn domain() -> DomainId {
    DomainId::new("pruning-fixture").unwrap()
}
fn incarnation() -> Uuid {
    Uuid::from_u128(1)
}
fn id(generation: u64) -> ResourceId {
    ResourceId::new(
        Scope::new(domain(), incarnation(), generation).unwrap(),
        Uuid::from_u128(100 + generation as u128),
    )
    .unwrap()
}
fn fence(through: u64) -> DomainFence {
    DomainFence::new(domain(), incarnation(), through + 1, through, through).unwrap()
}
fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
fn n(value: u64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}
fn text_fields(row: &mut Item, fields: &[(&str, String)]) {
    for (key, value) in fields {
        row.insert((*key).into(), s(value));
    }
}
fn version() -> VersionRef {
    VersionRef::new(
        StoreId::new("store").unwrap(),
        "managed/object",
        "exact-version",
    )
    .unwrap()
}
fn tuple(row: &mut Item) {
    let v = version();
    text_fields(
        row,
        &[
            ("store", v.store().as_str().into()),
            ("object_key", v.key().into()),
            ("version_id", v.version().into()),
        ],
    );
}
fn provenance(row: &mut Item, creation: &ResourceId) {
    text_fields(
        row,
        &[
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("creation_domain", creation.scope().domain().as_str().into()),
            (
                "creation_incarnation",
                creation.scope().incarnation().to_string(),
            ),
            ("creation_uuid", creation.uuid().to_string()),
        ],
    );
    row.insert(
        "creation_generation".into(),
        n(creation.scope().generation()),
    );
}
fn control() -> Item {
    let mut row = control_key(&domain());
    text_fields(
        &mut row,
        &[
            ("domain", domain().as_str().into()),
            ("incarnation", incarnation().to_string()),
        ],
    );
    for (key, value) in [
        ("current_generation", 4),
        ("closed_through", 3),
        ("compacted_through", 3),
        ("revision", 1),
    ] {
        row.insert(key.into(), n(value));
    }
    row
}
fn pin_row(pin: &ResourceId, creation: &ResourceId, state: &str) -> Item {
    let mut row = resource_key("USE", pin);
    tuple(&mut row);
    provenance(&mut row, creation);
    text_fields(
        &mut row,
        &[
            ("record", "use".into()),
            ("kind", "USE".into()),
            ("domain", pin.scope().domain().as_str().into()),
            ("incarnation", pin.scope().incarnation().to_string()),
            ("resource_uuid", pin.uuid().to_string()),
            ("state", state.into()),
            ("revision", Uuid::from_u128(78).to_string()),
        ],
    );
    row.insert("generation".into(), n(pin.scope().generation()));
    row.insert("deadline".into(), n(100));
    row
}
fn stage_row(state: &str) -> Item {
    let (intent, attempt, claim) = (id(1), id(2), id(3));
    let mut row = resource_key("STAGED", &intent);
    tuple(&mut row);
    text_fields(
        &mut row,
        &[
            ("record", "staged-verified-v1".into()),
            ("kind", "STAGED".into()),
            ("domain", domain().as_str().into()),
            ("incarnation", incarnation().to_string()),
            ("intent_uuid", intent.uuid().to_string()),
            ("attempt_uuid", attempt.uuid().to_string()),
            ("claim_uuid", claim.uuid().to_string()),
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("revision", Uuid::from_u128(78).to_string()),
            ("state", state.into()),
        ],
    );
    for (key, value) in [
        ("intent_generation", 1),
        ("attempt_generation", 2),
        ("claim_generation", 3),
        ("deadline", 100),
        ("max_length", 100),
        ("verified_length", 10),
    ] {
        row.insert(key.into(), n(value));
    }
    row
}
fn admission(kind: &str, resource: &ResourceId, state: &str) -> Item {
    let mut row = resource_key(kind, resource);
    text_fields(
        &mut row,
        &[
            ("record", kind.to_ascii_lowercase()),
            ("domain", domain().as_str().into()),
            ("incarnation", incarnation().to_string()),
            ("resource_uuid", resource.uuid().to_string()),
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("store", "store".into()),
            ("object_key", "managed/object".into()),
            ("state", state.into()),
            ("revision", Uuid::from_u128(78).to_string()),
        ],
    );
    for (key, value) in [
        ("generation", resource.scope().generation()),
        ("deadline", 100),
        ("max_length", 100),
    ] {
        row.insert(key.into(), n(value));
    }
    row
}
fn attempt_row(state: &str) -> Item {
    let mut row = admission("ATTEMPT", &id(2), state);
    text_fields(
        &mut row,
        &[
            ("intent_domain", domain().as_str().into()),
            ("intent_incarnation", incarnation().to_string()),
            ("intent_uuid", id(1).uuid().to_string()),
        ],
    );
    for (key, value) in [
        ("intent_generation", 1),
        ("mpu_admitted_lower_bound", 1),
        ("max_mpu_use_until", 200),
    ] {
        row.insert(key.into(), n(value));
    }
    row
}
fn version_row(state: &str, references: u64, claims: u64) -> Item {
    let mut row = object_key(&domain(), &version().fingerprint());
    tuple(&mut row);
    provenance(&mut row, &id(1));
    text_fields(
        &mut row,
        &[
            ("record", "version".into()),
            ("kind", "VERSION".into()),
            ("state", state.into()),
        ],
    );
    row.insert("references".into(), n(references));
    row.insert("claims".into(), n(claims));
    row
}
fn receipt() -> (CommandToken, Item) {
    let command = CommandId::new(id(1).scope().clone(), id(1).uuid()).unwrap();
    let token: CommandToken =
        serde_json::from_value(serde_json::json!({"id": command, "digest": vec![9u8; 32]}))
            .unwrap();
    let mut row = receipt_key(token.id());
    text_fields(
        &mut row,
        &[
            ("domain", domain().as_str().into()),
            ("incarnation", incarnation().to_string()),
            ("command_uuid", command.uuid().to_string()),
            ("status", "COMMITTED".into()),
        ],
    );
    row.insert("generation".into(), n(1));
    row.insert(
        "request_digest".into(),
        AttributeValue::B(Blob::new([9; 32])),
    );
    row.insert("result".into(), AttributeValue::B(Blob::new(vec![7; 8192])));
    (token, row)
}

#[test]
fn receipt_retention_digest_and_large_result_are_preserved() {
    let (token, mut row) = receipt();
    assert!(matches!(
        Candidate::Receipt(&token).validate(&fence(0), &row),
        Err(PruningError::BeforeWatermark)
    ));
    Candidate::Receipt(&token)
        .validate(&fence(1), &row)
        .unwrap();
    row.insert(
        "request_digest".into(),
        AttributeValue::B(Blob::new([8; 32])),
    );
    assert!(matches!(
        Candidate::Receipt(&token).validate(&fence(1), &row),
        Err(PruningError::Journal(JournalError::DigestMismatch))
    ));
    row.insert(
        "request_digest".into(),
        AttributeValue::B(Blob::new([9; 32])),
    );
    row.insert("future_authority".into(), s("unknown"));
    assert!(
        Candidate::Receipt(&token)
            .validate(&fence(1), &row)
            .is_err()
    );
    row.remove("future_authority");
    row.remove("result");
    row.insert("status".into(), s("CANCELLED"));
    Candidate::Receipt(&token)
        .validate(&fence(1), &row)
        .unwrap();
}

#[test]
fn pins_require_terminal_state_and_published_watermark_not_clock_expiry() {
    let pin = id(2);
    for state in ["RELEASED", "REVOKED"] {
        let row = pin_row(&pin, &id(1), state);
        assert!(matches!(
            Candidate::Pin(&pin).validate(&fence(1), &row),
            Err(PruningError::BeforeWatermark)
        ));
        Candidate::Pin(&pin).validate(&fence(2), &row).unwrap();
    }
    assert!(matches!(
        Candidate::Pin(&pin).validate(&fence(3), &pin_row(&pin, &id(1), "ACTIVE")),
        Err(PruningError::Ineligible)
    ));
}

#[test]
fn staged_claim_creation_cannot_hide_behind_older_intent_address() {
    let intent = id(1);
    for state in ["RELEASED", "REVOKED"] {
        let row = stage_row(state);
        assert!(matches!(
            Candidate::Staged(&intent).validate(&fence(2), &row),
            Err(PruningError::BeforeWatermark)
        ));
        Candidate::Staged(&intent)
            .validate(&fence(3), &row)
            .unwrap();
    }
    assert!(matches!(
        Candidate::Staged(&intent).validate(&fence(3), &stage_row("ACTIVE")),
        Err(PruningError::Ineligible)
    ));
}

#[test]
fn intents_and_attempts_never_skip_their_terminal_transition() {
    for state in ["CONSUMED", "CANCELLED"] {
        Candidate::Intent(&id(1))
            .validate(&fence(3), &admission("INTENT", &id(1), state))
            .unwrap();
    }
    for state in ["OPEN", "SELECTED"] {
        assert!(matches!(
            Candidate::Intent(&id(1)).validate(&fence(3), &admission("INTENT", &id(1), state)),
            Err(PruningError::Ineligible)
        ));
    }
    Candidate::Attempt(&id(2))
        .validate(&fence(3), &attempt_row("SEALED"))
        .unwrap();
    assert!(matches!(
        Candidate::Attempt(&id(2)).validate(&fence(3), &attempt_row("ACTIVE")),
        Err(PruningError::Ineligible)
    ));
    assert!(matches!(
        Candidate::Attempt(&id(2)).validate(&fence(1), &attempt_row("SEALED")),
        Err(PruningError::BeforeWatermark)
    ));
}

#[test]
fn only_zero_count_deleted_version_tombstones_are_candidates() {
    let v = version();
    Candidate::Version(&v)
        .validate(&fence(1), &version_row("DELETED", 0, 0))
        .unwrap();
    for (state, refs, claims) in [
        ("READY", 0, 0),
        ("RETIRING", 0, 0),
        ("DELETED", 1, 0),
        ("DELETED", 0, 1),
    ] {
        assert!(matches!(
            Candidate::Version(&v).validate(&fence(3), &version_row(state, refs, claims)),
            Err(PruningError::Ineligible)
        ));
    }
    assert!(matches!(
        Candidate::Version(&v).validate(&fence(0), &version_row("DELETED", 0, 0)),
        Err(PruningError::BeforeWatermark)
    ));
}

#[test]
fn historical_incarnations_are_not_compared_numerically_but_domains_are() {
    let historical = pin_row(&id(2), &id(1), "RELEASED");
    let current = DomainFence::new(domain(), Uuid::from_u128(99), 1, 0, 0).unwrap();
    Candidate::Pin(&id(2))
        .validate(&current, &historical)
        .unwrap();
    let foreign =
        DomainFence::new(DomainId::new("foreign").unwrap(), incarnation(), 4, 3, 3).unwrap();
    assert!(
        Candidate::Pin(&id(2))
            .validate(&foreign, &historical)
            .is_err()
    );
    assert!(
        Candidate::Pin(&id(2))
            .key(&DomainId::new("foreign").unwrap())
            .is_err()
    );
}

#[test]
fn deletion_is_two_fully_guarded_actions_without_count_or_receipt_writes() {
    let (token, row) = receipt();
    for (candidate, item) in [
        (Candidate::Receipt(&token), row),
        (Candidate::Pin(&id(2)), pin_row(&id(2), &id(1), "RELEASED")),
    ] {
        let actions = deletion_actions("table", &control(), &item, &candidate).unwrap();
        assert_eq!(actions.len(), 2);
        assert!(actions[0].condition_check.is_some());
        let delete = actions[1].delete.as_ref().unwrap();
        assert!(
            actions
                .iter()
                .all(|a| a.put.is_none() && a.update.is_none())
        );
        let names = delete.expression_attribute_names.as_ref().unwrap();
        let values = delete.expression_attribute_values.as_ref().unwrap();
        for (field, value) in item
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "pk" | "sk"))
        {
            assert!(
                names.values().any(|name| name == field),
                "unguarded {field}"
            );
            assert!(
                values.values().any(|v| v == value),
                "unguarded value for {field}"
            );
        }
        assert_eq!(delete.key["pk"], item["pk"]);
        assert_eq!(delete.key["sk"], item["sk"]);
        let guard = actions[0].condition_check.as_ref().unwrap();
        assert!(guard.condition_expression.contains("attribute_not_exists"));
    }
}
