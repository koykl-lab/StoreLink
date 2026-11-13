//! Canned SDK HTTP responses and checked codec fixtures, not a database or
//! expression interpreter. No credentials or cloud endpoints are used.
use super::*;
use crate::dynamodb::{
    EnrolledTables,
    journal::receipt_key,
    references::object_key,
    uploads::{namespace_key, resource_key},
};
use aws_sdk_dynamodb::{
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
    primitives::Blob,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use uuid::Uuid;

const TABLE: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
fn domain() -> DomainId {
    DomainId::new("discovery-fixture").unwrap()
}
fn id(generation: u64) -> ResourceId {
    ResourceId::new(
        Scope::new(domain(), Uuid::from_u128(1), generation).unwrap(),
        Uuid::from_u128(100 + generation as u128),
    )
    .unwrap()
}
fn version() -> VersionRef {
    VersionRef::new(
        StoreId::new("store").unwrap(),
        "managed/object",
        "exact-version",
    )
    .unwrap()
}
fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
fn n(value: u64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}
fn strings(row: &mut Item, fields: &[(&str, String)]) {
    row.extend(fields.iter().map(|(k, v)| ((*k).into(), s(v))));
}
fn tuple(row: &mut Item) {
    let v = version();
    strings(
        row,
        &[
            ("store", v.store().as_str().into()),
            ("object_key", v.key().into()),
            ("version_id", v.version().into()),
        ],
    );
}
fn provenance(row: &mut Item) {
    strings(
        row,
        &[
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("creation_domain", domain().as_str().into()),
            ("creation_incarnation", Uuid::from_u128(1).to_string()),
            ("creation_uuid", id(1).uuid().to_string()),
        ],
    );
    row.insert("creation_generation".into(), n(1));
}
fn admission(kind: &str, generation: u64, state: &str) -> Item {
    let resource = id(generation);
    let mut row = resource_key(kind, &resource);
    strings(
        &mut row,
        &[
            ("record", kind.to_ascii_lowercase()),
            ("domain", domain().as_str().into()),
            ("incarnation", Uuid::from_u128(1).to_string()),
            ("resource_uuid", resource.uuid().to_string()),
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("store", "store".into()),
            ("object_key", "managed/object".into()),
            ("state", state.into()),
            ("revision", Uuid::from_u128(78).to_string()),
        ],
    );
    row.extend([
        ("generation".into(), n(generation)),
        ("deadline".into(), n(100)),
        ("max_length".into(), n(100)),
    ]);
    row
}
fn attempt() -> Item {
    let mut row = admission("ATTEMPT", 2, "ACTIVE");
    strings(
        &mut row,
        &[
            ("intent_domain", domain().as_str().into()),
            ("intent_incarnation", Uuid::from_u128(1).to_string()),
            ("intent_uuid", id(1).uuid().to_string()),
        ],
    );
    row.extend([
        ("intent_generation".into(), n(1)),
        ("mpu_admitted_lower_bound".into(), n(1)),
        ("max_mpu_use_until".into(), n(200)),
    ]);
    row
}
fn staged() -> Item {
    let mut row = resource_key("STAGED", &id(1));
    tuple(&mut row);
    strings(
        &mut row,
        &[
            ("record", "staged-verified-v1".into()),
            ("kind", "STAGED".into()),
            ("domain", domain().as_str().into()),
            ("incarnation", Uuid::from_u128(1).to_string()),
            ("intent_uuid", id(1).uuid().to_string()),
            ("attempt_uuid", id(2).uuid().to_string()),
            ("claim_uuid", id(3).uuid().to_string()),
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("revision", Uuid::from_u128(78).to_string()),
            ("state", "ACTIVE".into()),
        ],
    );
    row.extend([
        ("intent_generation".into(), n(1)),
        ("attempt_generation".into(), n(2)),
        ("claim_generation".into(), n(3)),
        ("deadline".into(), n(100)),
        ("max_length".into(), n(100)),
        ("verified_length".into(), n(10)),
    ]);
    row
}
fn pin() -> Item {
    let mut row = resource_key("USE", &id(2));
    tuple(&mut row);
    provenance(&mut row);
    strings(
        &mut row,
        &[
            ("record", "use".into()),
            ("kind", "USE".into()),
            ("domain", domain().as_str().into()),
            ("incarnation", Uuid::from_u128(1).to_string()),
            ("resource_uuid", id(2).uuid().to_string()),
            ("state", "ACTIVE".into()),
            ("revision", Uuid::from_u128(78).to_string()),
        ],
    );
    row.extend([("generation".into(), n(2)), ("deadline".into(), n(100))]);
    row
}
fn version_row() -> Item {
    let mut row = object_key(&domain(), &version().fingerprint());
    tuple(&mut row);
    provenance(&mut row);
    strings(
        &mut row,
        &[
            ("record", "version".into()),
            ("kind", "VERSION".into()),
            ("state", "READY".into()),
        ],
    );
    row.extend([("references".into(), n(1)), ("claims".into(), n(2))]);
    row
}
fn namespace() -> Item {
    let mut row = namespace_key(&domain(), &NamespaceId::new("namespace").unwrap());
    strings(
        &mut row,
        &[
            ("record", "namespace".into()),
            ("domain", domain().as_str().into()),
            ("incarnation", Uuid::from_u128(1).to_string()),
            ("namespace_id", "namespace".into()),
            ("configuration_revision", Uuid::from_u128(77).to_string()),
            ("state", "ACTIVE".into()),
            ("store", "store".into()),
            ("bucket", "unit-not-qualified".into()),
            ("expected_owner", "123456789012".into()),
            ("region", "us-west-2".into()),
            ("key_prefix", "fresh/".into()),
            ("provider_profile", "aws-s3-versioned-lifecycle-v0".into()),
        ],
    );
    row.extend([
        ("mpu_max_age_ms".into(), n(10000)),
        ("mpu_lifecycle_margin_ms".into(), n(1000)),
    ]);
    row
}
fn receipt() -> (CommandToken, Item) {
    let command = CommandId::new(id(1).scope().clone(), id(1).uuid()).unwrap();
    let token: CommandToken =
        serde_json::from_value(json!({"id": command, "digest": vec![0u8; 32]})).unwrap();
    let mut row = receipt_key(&command);
    strings(
        &mut row,
        &[
            ("domain", domain().as_str().into()),
            ("incarnation", Uuid::from_u128(1).to_string()),
            ("command_uuid", command.uuid().to_string()),
            ("status", "COMMITTED".into()),
        ],
    );
    row.extend([
        ("generation".into(), n(1)),
        (
            "request_digest".into(),
            AttributeValue::B(Blob::new([0; 32])),
        ),
        ("result".into(), AttributeValue::B(Blob::new(b"hello"))),
    ]);
    (token, row)
}
fn control() -> Item {
    let mut row = control_key(&domain());
    strings(
        &mut row,
        &[
            ("domain", domain().as_str().into()),
            ("incarnation", Uuid::from_u128(1).to_string()),
        ],
    );
    row.extend([
        ("current_generation".into(), n(4)),
        ("closed_through".into(), n(3)),
        ("compacted_through".into(), n(2)),
        ("revision".into(), n(1)),
    ]);
    row
}
fn reference() -> Item {
    let mut row = Item::from([
        (
            "pk".into(),
            s(format!("{}#REF#736c6f742dc3a923", prefix(&domain()))),
        ),
        ("sk".into(), s("M")),
        ("record".into(), s("reference")),
        ("reference_id".into(), s("slot-é#")),
        ("revision".into(), s(Uuid::from_u128(78).to_string())),
    ]);
    tuple(&mut row);
    row
}
fn store_binding() -> Item {
    Item::from([
        (
            "pk".into(),
            s(format!("{}#STORE#73746f7265", prefix(&domain()))),
        ),
        ("sk".into(), s("M")),
        ("record".into(), s("store-binding-v1")),
        ("domain".into(), s(domain().as_str())),
        ("store".into(), s("store")),
        ("bucket".into(), s("unit-not-qualified")),
        ("expected_owner".into(), s("123456789012")),
        ("region".into(), s("us-west-2")),
        (
            "provider_profile".into(),
            s("aws-s3-versioned-lifecycle-v0"),
        ),
    ])
}
fn fixtures() -> Vec<(Item, BoundedMaintenanceHint)> {
    use BoundedMaintenanceHint as Hint;
    let (token, receipt) = receipt();
    vec![
        (receipt, Hint::Receipt(token)),
        (admission("INTENT", 1, "OPEN"), Hint::Intent(id(1))),
        (attempt(), Hint::Attempt(id(2))),
        (staged(), Hint::Staged(id(1))),
        (pin(), Hint::UsePin(id(2))),
        (version_row(), Hint::Version(version())),
        (
            namespace(),
            Hint::Namespace(NamespaceId::new("namespace").unwrap()),
        ),
    ]
}
fn wire(row: &Item) -> Value {
    Value::Object(
        row.iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    match value {
                        AttributeValue::S(v) => json!({"S":v}),
                        AttributeValue::N(v) => json!({"N":v}),
                        AttributeValue::B(v) if v.as_ref() == [0; 32] => {
                            json!({"B":format!("{}=", "A".repeat(43))})
                        }
                        AttributeValue::B(v) if v.as_ref() == b"hello" => json!({"B":"aGVsbG8="}),
                        _ => panic!("unsupported fixture value"),
                    },
                )
            })
            .collect(),
    )
}
fn foreign_key() -> Item {
    HashMap::from([
        (
            "pk".into(),
            s(format!(
                "{}#foreign",
                prefix(&DomainId::new("another-domain").unwrap())
            )),
        ),
        ("sk".into(), s("not-M")),
    ])
}

struct NativeFixture {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    worker: Option<JoinHandle<()>>,
}
impl NativeFixture {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let replies = Arc::new(Mutex::new(VecDeque::<(u16, Value)>::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (stopping, responses, captured) = (stop.clone(), replies.clone(), requests.clone());
        let worker = std::thread::spawn(move || {
            loop {
                let (mut socket, _) = listener.accept().unwrap();
                if stopping.load(Ordering::SeqCst) {
                    break;
                }
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut headers = Vec::new();
                let mut byte = [0];
                while !headers.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).unwrap();
                    headers.push(byte[0]);
                    assert!(headers.len() < 32768);
                }
                let headers = String::from_utf8(headers).unwrap().to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                assert!(length < 1_000_000);
                let target = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("x-amz-target: "))
                    .unwrap()
                    .to_owned();
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                captured
                    .lock()
                    .unwrap()
                    .push((target, serde_json::from_slice(&body).unwrap()));
                let (status, body) = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected native operation");
                let body = body.to_string();
                write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        Self {
            address,
            stop,
            replies,
            requests,
            worker: Some(worker),
        }
    }
    fn reply(&self, value: Value) {
        self.replies.lock().unwrap().push_back((200, value));
    }
    async fn journal(&self) -> CommandJournal {
        self.reply(json!({"Table":{"TableName":"coord", "TableArn":TABLE, "TableStatus":"ACTIVE", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}], "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(Credentials::new(
                    "fixture", "fixture", None, None, "fixture",
                ))
                .endpoint_url(format!("http://{}", self.address))
                .retry_config(RetryConfig::disabled())
                .build(),
        );
        let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
        CommandJournal::new(client, tables, domain())
    }
    fn scan_requests(&self) -> Vec<Value> {
        let requests = self.requests.lock().unwrap();
        assert_eq!(requests[0].0, "dynamodb_20120810.describetable");
        requests[1..]
            .iter()
            .map(|(target, body)| {
                assert_eq!(
                    target, "dynamodb_20120810.scan",
                    "discovery must never mutate or query"
                );
                body.clone()
            })
            .collect()
    }
}
impl Drop for NativeFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn scan_wire_empty_filtered_page_keeps_foreign_continuation_and_evaluated_count() {
    let native = NativeFixture::new();
    let journal = native.journal().await;
    native.reply(
        json!({"Items":[], "Count":0, "ScannedCount":128, "LastEvaluatedKey":wire(&foreign_key())}),
    );
    let page = journal.discover_metadata_page(None, 128).await.unwrap();
    assert!(page.items().is_empty());
    assert_eq!(page.scanned_count(), 128);
    assert!(page.input_cursor().is_none());
    let cursor = page.next_cursor().unwrap();
    let encoded = serde_json::to_value(cursor).unwrap();
    let decoded: MetadataCursor = serde_json::from_value(encoded).unwrap();
    assert_eq!(&decoded, cursor);
    assert!(!format!("{cursor:?}").contains("foreign"));
    native.reply(json!({"Items":[], "Count":0, "ScannedCount":0, "LastEvaluatedKey":{}}));
    let terminal = journal
        .discover_metadata_page(Some(&decoded), 1)
        .await
        .unwrap();
    assert_eq!(terminal.input_cursor(), Some(&decoded));
    assert!(terminal.next_cursor().is_none());
    let requests = native.scan_requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(request["TableName"], TABLE);
        assert_eq!(request["ConsistentRead"], true);
        assert_eq!(request["FilterExpression"], "begins_with(#pk, :domain)");
        assert_eq!(request["ExpressionAttributeNames"], json!({"#pk":"pk"}));
        assert_eq!(
            request["ExpressionAttributeValues"],
            json!({":domain":{"S":prefix(&domain())}})
        );
        assert!(request.get("ProjectionExpression").is_none());
        assert!(request.get("Segment").is_none());
    }
    assert_eq!(requests[0]["Limit"], 128);
    assert!(requests[0].get("ExclusiveStartKey").is_none());
    assert_eq!(requests[1]["Limit"], 1);
    assert_eq!(requests[1]["ExclusiveStartKey"], wire(&foreign_key()));
}

#[tokio::test]
async fn limits_and_cursor_bindings_reject_before_io() {
    let native = NativeFixture::new();
    let journal = native.journal().await;
    for limit in [0, 129, u16::MAX] {
        assert_eq!(
            journal
                .discover_metadata_page(None, limit)
                .await
                .unwrap_err(),
            MetadataDiscoveryError::InvalidLimit
        );
    }
    let cursor = MetadataCursor::from_key(&journal, foreign_key()).unwrap();
    for (field, value) in [
        (
            "table",
            json!("arn:aws:dynamodb:us-west-2:123456789012:table/other"),
        ),
        ("domain", json!("other-domain")),
    ] {
        let mut wire = serde_json::to_value(&cursor).unwrap();
        wire[field] = value;
        let other: MetadataCursor = serde_json::from_value(wire).unwrap();
        assert_eq!(
            journal
                .discover_metadata_page(Some(&other), 1)
                .await
                .unwrap_err(),
            MetadataDiscoveryError::CursorBinding
        );
    }
    assert!(native.scan_requests().is_empty());
}

#[test]
fn cursor_deserialization_checks_structure_types_and_utf8_byte_lengths() {
    let valid = json!({"table":TABLE, "domain":"fixture", "pk":"p", "sk":"s"});
    for (field, value) in [
        ("table", json!("coord")),
        ("table", json!("arn:aws:dynamodb:r:a:table/coord/index/x")),
        ("domain", json!("")),
        ("pk", json!("")),
        ("sk", json!("")),
        ("pk", json!("é".repeat(1025))),
        ("sk", json!("é".repeat(513))),
        ("pk", json!({"S":"p"})),
        ("sk", json!(3)),
        ("extra", json!(true)),
    ] {
        let mut candidate = valid.clone();
        candidate[field] = value;
        assert!(
            serde_json::from_value::<MetadataCursor>(candidate).is_err(),
            "{field}"
        );
    }
    for field in ["table", "domain", "pk", "sk"] {
        let mut candidate = valid.clone();
        candidate.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<MetadataCursor>(candidate).is_err());
    }
    let mut boundary = valid;
    boundary["pk"] = json!("é".repeat(1024));
    boundary["sk"] = json!("é".repeat(512));
    assert!(serde_json::from_value::<MetadataCursor>(boundary).is_ok());
}

#[tokio::test]
async fn all_families_use_checked_decoders_and_preserve_ids_without_eligibility() {
    let native = NativeFixture::new();
    let journal = native.journal().await;
    for (row, expected) in fixtures() {
        assert_eq!(
            classify(&journal, &row),
            MetadataDiscoveryItem::Hint(expected)
        );
        for field in ["pk", "sk"] {
            let mut corrupt = row.clone();
            corrupt.insert(field.into(), s("not-canonical"));
            assert_eq!(classify(&journal, &corrupt), MetadataDiscoveryItem::Corrupt);
        }
        // Every expected schema attribute is validated, not just the suffix.
        for field in row.keys() {
            let mut corrupt = row.clone();
            corrupt.remove(field);
            assert_eq!(
                classify(&journal, &corrupt),
                MetadataDiscoveryItem::Corrupt,
                "missing {field}"
            );
        }
        let mut extra = row;
        extra.insert("unrecognized_authority".into(), s("not-supported"));
        assert_eq!(classify(&journal, &extra), MetadataDiscoveryItem::Corrupt);
    }
    assert_eq!(
        classify(&journal, &control()),
        MetadataDiscoveryItem::Permanent
    );
    let mut bad_control = control();
    bad_control.insert("revision".into(), n(0));
    assert_eq!(
        classify(&journal, &bad_control),
        MetadataDiscoveryItem::Corrupt
    );
    for (field, value) in [
        ("store", "other"),
        ("object_key", "other"),
        ("version_id", "other"),
        ("version_id", "null"),
    ] {
        let mut bad = version_row();
        bad.insert(field.into(), s(value));
        assert_eq!(classify(&journal, &bad), MetadataDiscoveryItem::Corrupt);
    }
    for discriminator in ["staged", "staged-verified-v2"] {
        let mut unsupported = staged();
        unsupported.insert("record".into(), s(discriminator));
        assert_eq!(
            classify(&journal, &unsupported),
            MetadataDiscoveryItem::Unsupported
        );
    }
    let mut missing_facts = staged();
    missing_facts.remove("verified_length");
    assert_eq!(
        classify(&journal, &missing_facts),
        MetadataDiscoveryItem::Corrupt
    );
    let mut bad_staged = staged();
    bad_staged.extend(resource_key("STAGED", &id(3)));
    assert_eq!(
        classify(&journal, &bad_staged),
        MetadataDiscoveryItem::Corrupt
    );
    let mut bad_receipt = receipt().1;
    bad_receipt.insert(
        "request_digest".into(),
        AttributeValue::B(Blob::new([0; 31])),
    );
    assert_eq!(
        classify(&journal, &bad_receipt),
        MetadataDiscoveryItem::Corrupt
    );
    for (field, value) in [
        ("generation", n(0)),
        ("incarnation", s(Uuid::nil().to_string())),
        ("resource_uuid", s(Uuid::nil().to_string())),
        ("domain", s("other-domain")),
    ] {
        let mut bad = admission("INTENT", 1, "OPEN");
        bad.insert(field.into(), value);
        assert_eq!(classify(&journal, &bad), MetadataDiscoveryItem::Corrupt);
    }
}

#[tokio::test]
async fn permanent_records_require_closed_schema_full_identity_and_canonical_keys() {
    let native = NativeFixture::new();
    let journal = native.journal().await;
    for (row, family, future) in [
        (reference(), "REF", "reference-v2"),
        (store_binding(), "STORE", "store-binding-v2"),
    ] {
        assert_eq!(classify(&journal, &row), MetadataDiscoveryItem::Permanent);
        for field in row.keys() {
            let mut missing = row.clone();
            missing.remove(field);
            assert_eq!(classify(&journal, &missing), MetadataDiscoveryItem::Corrupt);
            let mut wrong_type = row.clone();
            wrong_type.insert(field.clone(), n(1));
            assert_eq!(
                classify(&journal, &wrong_type),
                MetadataDiscoveryItem::Corrupt
            );
        }
        for (field, value) in [
            ("pk", format!("{}#{family}#00", prefix(&domain()))),
            ("pk", format!("{}#extra", text(&row, "pk").unwrap())),
            (
                "pk",
                text(&row, "pk").unwrap().replace(
                    &prefix(&domain()),
                    &prefix(&DomainId::new("foreign-domain").unwrap()),
                ),
            ),
            ("sk", "other".into()),
            ("unrecognized_authority", "secret-row-payload".into()),
        ] {
            let mut bad = row.clone();
            bad.insert(field.into(), s(value));
            assert_eq!(classify(&journal, &bad), MetadataDiscoveryItem::Corrupt);
        }
        let mut unknown = row;
        unknown.insert("record".into(), s(future));
        assert_eq!(
            classify(&journal, &unknown),
            MetadataDiscoveryItem::Unsupported
        );
    }
    for (field, value) in [
        ("reference_id", "other-slot"),
        ("reference_id", ""),
        ("revision", "00000000-0000-0000-0000-000000000000"),
        ("revision", "00000000-0000-0000-0000-00000000004E"),
        ("store", ""),
        ("object_key", ""),
        ("version_id", "null"),
    ] {
        let mut bad = reference();
        bad.insert(field.into(), s(value));
        assert_eq!(classify(&journal, &bad), MetadataDiscoveryItem::Corrupt);
    }
    for (field, value) in [
        ("domain", "foreign-domain".into()),
        ("store", "other-store".into()),
        ("bucket", "".into()),
        ("bucket", "b".repeat(64)),
        ("expected_owner", "12345678901".into()),
        ("expected_owner", "12345678901x".into()),
        ("region", "".into()),
        ("region", "r".repeat(65)),
        ("provider_profile", "aws-s3-versioned-lifecycle-v1".into()),
    ] {
        let mut bad = store_binding();
        bad.insert(field.into(), s(value));
        assert_eq!(classify(&journal, &bad), MetadataDiscoveryItem::Corrupt);
    }
    assert!(native.scan_requests().is_empty());
}

#[tokio::test]
async fn sdk_page_keeps_unknown_corrupt_and_all_valid_items_in_native_order() {
    let native = NativeFixture::new();
    let journal = native.journal().await;
    let mut unknown = control_key(&domain());
    unknown.insert(
        "pk".into(),
        s(format!("{}#FUTURE#opaque", prefix(&domain()))),
    );
    let mut corrupt = receipt().1;
    corrupt.remove("result");
    let mut rows = vec![unknown, corrupt, control(), reference(), store_binding()];
    rows.extend(fixtures().into_iter().map(|(row, _)| row));
    let marker = foreign_key();
    native.reply(json!({"Items":rows.iter().map(wire).collect::<Vec<_>>(), "Count":rows.len(), "ScannedCount":12, "LastEvaluatedKey":wire(&marker)}));
    let page = journal.discover_metadata_page(None, 16).await.unwrap();
    assert_eq!(
        &page.items()[..5],
        &[
            MetadataDiscoveryItem::Unsupported,
            MetadataDiscoveryItem::Corrupt,
            MetadataDiscoveryItem::Permanent,
            MetadataDiscoveryItem::Permanent,
            MetadataDiscoveryItem::Permanent
        ]
    );
    let hints: Vec<_> = fixtures()
        .into_iter()
        .map(|(_, hint)| MetadataDiscoveryItem::Hint(hint))
        .collect();
    assert_eq!(&page.items()[5..], hints);
    assert_eq!(page.scanned_count(), 12);
    assert_eq!(page.next_cursor().unwrap().key(&journal).unwrap(), marker);
    let debug = format!("{page:?}");
    for secret in [
        "hello",
        "result",
        "COMMITTED",
        "request_digest",
        "slot-é#",
        "unit-not-qualified",
        "123456789012",
        "store-binding-v1",
    ] {
        assert!(!debug.contains(secret));
    }
    assert_eq!(native.scan_requests().len(), 1);
}

#[tokio::test]
async fn malformed_native_cursor_or_page_and_native_errors_never_produce_progress() {
    let native = NativeFixture::new();
    let journal = native.journal().await;
    let cursor = MetadataCursor::from_key(&journal, foreign_key()).unwrap();
    let before = serde_json::to_value(&cursor).unwrap();
    let mut replies = vec![];
    for key in [
        json!({"pk":{"S":"p"}}),
        json!({"pk":{"N":"1"},"sk":{"S":"s"}}),
        json!({"pk":{"S":"p"},"sk":{"S":""}}),
        json!({"pk":{"S":"p"},"sk":{"S":"s"},"extra":{"S":"x"}}),
        json!({"pk":{"S":"p".repeat(2049)},"sk":{"S":"s"}}),
    ] {
        replies.push((
            json!({"Items":[],"Count":0,"ScannedCount":1,"LastEvaluatedKey":key}),
            MetadataDiscoveryError::InvalidCursor,
        ));
    }
    for body in [
        json!({"Items":[],"Count":1,"ScannedCount":1}),
        json!({"Items":[],"Count":0,"ScannedCount":-1}),
        json!({"Items":[],"Count":0,"ScannedCount":3}),
        json!({"Items":[wire(&control())],"Count":1,"ScannedCount":0}),
    ] {
        replies.push((body, MetadataDiscoveryError::InvalidPage));
    }
    replies.push((
        json!({"LastEvaluatedKey":{"pk":{"S":3}}}),
        MetadataDiscoveryError::Native,
    ));
    for (body, expected) in replies {
        native.reply(body);
        assert_eq!(
            journal
                .discover_metadata_page(Some(&cursor), 2)
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(serde_json::to_value(&cursor).unwrap(), before);
    }
    native.replies.lock().unwrap().push_back((
        400,
        json!({"__type":"AccessDeniedException","message":"secret-row-payload"}),
    ));
    let error = journal
        .discover_metadata_page(Some(&cursor), 2)
        .await
        .unwrap_err();
    assert_eq!(error, MetadataDiscoveryError::Native);
    assert!(!format!("{error:?} {error}").contains("secret-row-payload"));
    assert_eq!(serde_json::to_value(&cursor).unwrap(), before);
    assert_eq!(native.scan_requests().len(), 11);
}
