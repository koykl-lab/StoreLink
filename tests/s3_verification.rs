//! Native SDK loopback fixtures with canned responses, NOT a Dynamo expression
//! emulator, enrollment proof or AWS acceptance. Synthetic credentials only.
#![cfg(all(feature = "s3", feature = "dynamodb"))]

use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};
use storelink::{
    claim::ClaimPolicy,
    clock::{ClockObservation, TimestampMs},
    dynamodb::{CommandJournal, EnrolledTables, TransferAuthorization},
    identity::{DomainId, NamespaceId, ResourceId, Scope, StoreId, VersionRef},
    s3::{
        NamespaceConfiguration, NamespaceConfigurationSpec, QualifiedNamespace, VerificationError,
        attribution_metadata, attribution_metadata_with_extra, qualify_existing, verify_candidate,
    },
};
use uuid::Uuid;

const INC: &str = "11111111-1111-4111-8111-111111111111";
const CONFIG: &str = "22222222-2222-4222-8222-222222222222";
const INTENT: &str = "33333333-3333-4333-8333-333333333333";
const ATTEMPT: &str = "44444444-4444-4444-8444-444444444444";
const REV: &str = "55555555-5555-4555-8555-555555555555";
const KEY: &str = "managed/private-object";
const VERSION: &str = "version/+exact";
const PAYLOAD: &[u8] = b"abc";
const HASH_BASE64: &str = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=";

struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
    length: Option<i64>,
    gate: Option<mpsc::Receiver<()>>,
    before_headers: Option<(tokio::sync::oneshot::Sender<()>, mpsc::Receiver<()>)>,
    notify: Option<mpsc::Sender<()>>,
}
impl Reply {
    fn bytes(body: impl Into<Vec<u8>>) -> Self {
        let body = body.into();
        Self {
            status: 200,
            headers: String::new(),
            length: Some(body.len() as i64),
            body,
            gate: None,
            before_headers: None,
            notify: None,
        }
    }
    fn json(value: Value) -> Self {
        Self::bytes(value.to_string().into_bytes())
    }
}
struct Fixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    worker: Option<JoinHandle<()>>,
}
impl Fixture {
    fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let (stopping, captured, responses) = (stop.clone(), requests.clone(), replies.clone());
        let worker = std::thread::spawn(move || {
            loop {
                let (mut socket, _) = listener.accept().unwrap();
                if stopping.load(Ordering::SeqCst) {
                    break;
                }
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                    assert!(request.len() <= 32768);
                }
                let headers = String::from_utf8(request.clone()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(|n| n.parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                assert!(length < 32768);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                request.extend(body);
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(request).unwrap());
                let reply = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected native request");
                let length = reply
                    .length
                    .map(|n| format!("Content-Length: {n}\r\n"))
                    .unwrap_or_default();
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\n{length}Connection: close\r\n{}\r\n",
                    reply.status, reply.headers
                );
                if let Some((arrived, release)) = reply.before_headers {
                    let _ = arrived.send(());
                    release
                        .recv_timeout(Duration::from_secs(2))
                        .expect("bounded pre-header gate");
                }
                // Cancellation may close a request already issued. No mutation/abort follows.
                if socket.write_all(response.as_bytes()).is_err() {
                    continue;
                }
                if let Some(gate) = reply.gate {
                    let _ = gate.recv_timeout(Duration::from_secs(2));
                }
                if !headers.starts_with("HEAD ") {
                    let _ = socket.write_all(&reply.body);
                }
                if let Some(notify) = reply.notify {
                    let _ = notify.send(());
                }
            }
        });
        Self {
            address,
            stop,
            requests,
            replies,
            worker: Some(worker),
        }
    }
    fn push(&self, reply: Reply) {
        self.replies.lock().unwrap().push_back(reply);
    }
    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }
    fn s3(&self) -> aws_sdk_s3::Client {
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(credentials())
                .endpoint_url(self.endpoint())
                .force_path_style(true)
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeouts())
                .build(),
        )
    }
    fn dynamo(&self) -> aws_sdk_dynamodb::Client {
        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(credentials())
                .endpoint_url(self.endpoint())
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeouts())
                .build(),
        )
    }
    fn assert_done(&self, s3: bool) -> Vec<String> {
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "unconsumed canned responses"
        );
        let requests = self.requests.lock().unwrap().clone();
        assert!(requests.len() < 40);
        for r in &requests {
            assert!(r.contains("Credential=fixture-access/"));
            if s3 {
                assert!(
                    r.starts_with("HEAD ") || r.starts_with("GET "),
                    "mutating S3 request"
                );
                assert!(
                    r.to_ascii_lowercase()
                        .contains("\r\nx-amz-expected-bucket-owner: 123456789012\r\n")
                );
                if r.contains(KEY) {
                    assert!(
                        r.lines()
                            .next()
                            .unwrap()
                            .contains("versionId=version%2F%2Bexact")
                    );
                }
            } else {
                let target = r
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("x-amz-target:"))
                    .unwrap();
                assert!(
                    ["DescribeTable", "GetItem", "TransactGetItems"]
                        .iter()
                        .any(|v| target.ends_with(v))
                );
            }
        }
        requests
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        let result = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}
fn credentials() -> Credentials {
    Credentials::new(
        "fixture-access",
        "fixture-secret",
        None,
        None,
        "loopback-only",
    )
}
fn timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(2))
        .build()
}
fn configuration() -> NamespaceConfiguration {
    NamespaceConfiguration::new(NamespaceConfigurationSpec {
        id: NamespaceId::new("ns-é").unwrap(),
        store: StoreId::new("store").unwrap(),
        bucket: "storelink-fixture".into(),
        expected_owner: "123456789012".into(),
        region: "us-west-2".into(),
        key_prefix: "managed/".into(),
        mpu_max_age_ms: 3600000,
        mpu_lifecycle_margin_ms: 3600000,
    })
    .unwrap()
}
fn profile() -> Vec<Reply> {
    let mut head = Reply::bytes("");
    head.headers = "x-amz-bucket-region: us-west-2\r\n".into();
    let mut lock = Reply::bytes("<Error><Code>ObjectLockConfigurationNotFoundError</Code></Error>");
    lock.status = 404;
    vec![
        head,
        Reply::bytes("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
        Reply::bytes(
            "<LifecycleConfiguration><Rule><ID>mpu</ID><Status>Enabled</Status><Filter><Prefix>managed/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
        ),
        lock,
    ]
}
fn describe() -> Reply {
    Reply::json(
        json!({"Table": {"TableName":"coord", "TableArn":"arn:aws:dynamodb:us-west-2:123456789012:table/coord", "TableStatus":"ACTIVE",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}),
    )
}
fn domain() -> DomainId {
    DomainId::new("domain-é").unwrap()
}
fn prefix() -> String {
    format!("D#{:x}", Sha256::digest(domain().as_str().as_bytes()))
}
fn ts(n: u64) -> TimestampMs {
    TimestampMs::new(n).unwrap()
}
fn clock() -> Result<ClockObservation, storelink::clock::ClockError> {
    ClockObservation::trusted(ts(1000), 0)
}
fn policy() -> ClaimPolicy {
    ClaimPolicy::new(10000, 150, 10).unwrap()
}
fn attempt_id() -> ResourceId {
    ResourceId::new(
        Scope::new(domain(), Uuid::parse_str(INC).unwrap(), 1).unwrap(),
        Uuid::parse_str(ATTEMPT).unwrap(),
    )
    .unwrap()
}
fn candidate() -> VersionRef {
    VersionRef::new(StoreId::new("store").unwrap(), KEY, VERSION).unwrap()
}
fn put_text(item: &mut Value, key: &str, value: &str) {
    item[key] = json!({"S":value});
}
fn put_number(item: &mut Value, key: &str, value: u64) {
    item[key] = json!({"N":value.to_string()});
}
#[derive(Clone)]
struct Rows {
    control: Value,
    namespace: Value,
    intent: Value,
    attempt: Value,
}
impl Rows {
    fn new(hash: bool) -> Self {
        let mut control = json!({"pk":{"S":format!("{}#CONTROL", prefix())},"sk":{"S":"M"}});
        for (k, v) in [("domain", domain().as_str()), ("incarnation", INC)] {
            put_text(&mut control, k, v);
        }
        for (k, v) in [
            ("current_generation", 1),
            ("closed_through", 0),
            ("compacted_through", 0),
            ("revision", 1),
        ] {
            put_number(&mut control, k, v);
        }
        let mut namespace =
            json!({"pk":{"S":format!("{}#NS#6e732dc3a9", prefix())},"sk":{"S":"M"}});
        for (k, v) in [
            ("record", "namespace"),
            ("domain", domain().as_str()),
            ("incarnation", INC),
            ("namespace_id", "ns-é"),
            ("configuration_revision", CONFIG),
            ("state", "ACTIVE"),
            ("store", "store"),
            ("bucket", "storelink-fixture"),
            ("expected_owner", "123456789012"),
            ("region", "us-west-2"),
            ("key_prefix", "managed/"),
            ("provider_profile", "aws-s3-versioned-lifecycle-v0"),
        ] {
            put_text(&mut namespace, k, v);
        }
        for k in ["mpu_max_age_ms", "mpu_lifecycle_margin_ms"] {
            put_number(&mut namespace, k, 3600000);
        }
        let admission = |kind: &str, uuid: &str, state: &str| {
            let mut row =
                json!({"pk":{"S":format!("{}#{kind}#{INC}#1#{uuid}", prefix())},"sk":{"S":"M"}});
            for (k, v) in [
                ("record", kind.to_ascii_lowercase().as_str()),
                ("domain", domain().as_str()),
                ("incarnation", INC),
                ("resource_uuid", uuid),
                ("namespace_id", "ns-é"),
                ("configuration_revision", CONFIG),
                ("store", "store"),
                ("object_key", KEY),
                ("state", state),
                ("revision", REV),
            ] {
                put_text(&mut row, k, v);
            }
            for (k, v) in [
                ("generation", 1),
                ("deadline", 5000),
                ("max_length", 100),
                ("expected_length", 3),
            ] {
                put_number(&mut row, k, v);
            }
            if hash {
                row["full_sha256"] = json!({"B":HASH_BASE64});
            }
            row
        };
        let intent = admission("INTENT", INTENT, "OPEN");
        let mut attempt = admission("ATTEMPT", ATTEMPT, "ACTIVE");
        for (k, v) in [
            ("intent_domain", domain().as_str()),
            ("intent_incarnation", INC),
            ("intent_uuid", INTENT),
        ] {
            put_text(&mut attempt, k, v);
        }
        for (k, v) in [
            ("intent_generation", 1),
            ("mpu_admitted_lower_bound", 900),
            ("max_mpu_use_until", 9000),
        ] {
            put_number(&mut attempt, k, v);
        }
        Self {
            control,
            namespace,
            intent,
            attempt,
        }
    }
    fn enqueue(&self, fixture: &Fixture) {
        fixture.push(Reply::json(json!({"Item":self.attempt})));
        fixture.push(Reply::json(json!({"Responses":[{"Item":self.control},{"Item":self.namespace},{"Item":self.intent},{"Item":self.attempt}]})));
    }
}
struct Setup {
    s3: Fixture,
    db: Fixture,
    journal: CommandJournal,
    qualified: QualifiedNamespace,
    rows: Rows,
    metadata: HashMap<String, String>,
}
impl Setup {
    async fn new(hash: bool) -> Self {
        let s3 = Fixture::new(profile());
        let db = Fixture::new(vec![describe()]);
        let client = db.dynamo();
        let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
        let journal = CommandJournal::new(client, tables, domain());
        let qualified = qualify_existing(&s3.s3(), &configuration()).await.unwrap();
        let rows = Rows::new(hash);
        rows.enqueue(&db);
        let auth = journal
            .authorize_transfer(&attempt_id(), clock, &policy())
            .await
            .unwrap();
        let metadata = attribution_metadata(&auth).unwrap();
        Self {
            s3,
            db,
            journal,
            qualified,
            rows,
            metadata,
        }
    }
    fn head(&self) -> Reply {
        let mut reply = Reply::bytes("");
        reply.length = Some(3);
        reply.headers = format!("x-amz-version-id: {VERSION}\r\n");
        for (k, v) in &self.metadata {
            reply.headers += &format!("x-amz-meta-{k}: {v}\r\n");
        }
        reply
    }
    fn get(&self) -> Reply {
        let mut reply = Reply::bytes(PAYLOAD);
        reply.headers = format!("x-amz-version-id: {VERSION}\r\n");
        reply
    }
    async fn verify(&self) -> Result<storelink::s3::VerifiedUploadReceipt, VerificationError> {
        tokio::time::timeout(
            Duration::from_secs(3),
            verify_candidate(
                &self.journal,
                &self.qualified,
                &attempt_id(),
                &candidate(),
                &policy(),
                &clock,
            ),
        )
        .await
        .expect("bounded verification")
    }
    fn done(&self) -> Vec<String> {
        self.db.assert_done(false);
        self.s3.assert_done(true)
    }
    async fn auth(&self) -> TransferAuthorization {
        self.rows.enqueue(&self.db);
        self.journal
            .authorize_transfer(&attempt_id(), clock, &policy())
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn metadata_is_ascii_canonical_bounded_and_reserves_case_insensitive_prefix() {
    let setup = Setup::new(true).await;
    let auth = setup.auth().await;
    let m = attribution_metadata(&auth).unwrap();
    assert_eq!(m.len(), 11);
    assert_eq!(m["storelink-schema"], "1");
    assert_eq!(m["storelink-domain"], "646f6d61696e2dc3a9");
    assert_eq!(m["storelink-namespace"], "6e732dc3a9");
    assert_eq!(m["storelink-incarnation"], INC);
    assert_eq!(m["storelink-configuration-revision"], CONFIG);
    assert_eq!(m["storelink-intent-generation"], "1");
    assert_eq!(m["storelink-intent-uuid"], INTENT);
    assert_eq!(m["storelink-attempt-generation"], "1");
    assert_eq!(m["storelink-attempt-uuid"], ATTEMPT);
    let mut bytes = Vec::new();
    for part in ["store", KEY] {
        bytes.extend((part.len() as u64).to_be_bytes());
        bytes.extend(part.as_bytes());
    }
    assert_eq!(
        m["storelink-target-sha256"],
        format!("{:x}", Sha256::digest(bytes))
    );
    let mut bytes = 100u64.to_be_bytes().to_vec();
    bytes.push(1);
    bytes.extend(3u64.to_be_bytes());
    bytes.push(1);
    bytes.extend(Sha256::digest(PAYLOAD));
    assert_eq!(
        m["storelink-constraints-sha256"],
        format!("{:x}", Sha256::digest(bytes))
    );
    assert!(
        m.iter()
            .all(|(k, v)| k.is_ascii() && v.is_ascii() && k == &k.to_ascii_lowercase())
    );
    assert!(m.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() <= 2048);
    for key in ["storelink-schema", "StOrElInK-new"] {
        assert!(matches!(
            attribution_metadata_with_extra(&auth, &HashMap::from([(key.into(), "x".into())])),
            Err(VerificationError::ReservedMetadata)
        ));
    }
    assert!(
        attribution_metadata_with_extra(
            &auth,
            &HashMap::from([("unrelated".into(), "value".into())])
        )
        .is_ok()
    );
    assert!(matches!(
        attribution_metadata_with_extra(&auth, &HashMap::from([("big".into(), "x".repeat(2048))])),
        Err(VerificationError::MetadataSize)
    ));
    setup.done();
}

#[tokio::test]
async fn exact_head_only_receipt_has_latest_snapshots_without_claiming_hash() {
    let setup = Setup::new(false).await;
    setup.rows.enqueue(&setup.db);
    let mut latest = setup.rows.clone();
    put_text(&mut latest.intent, "revision", CONFIG);
    put_number(&mut latest.intent, "deadline", 6000);
    put_text(&mut latest.attempt, "revision", CONFIG);
    put_number(&mut latest.attempt, "deadline", 6000);
    latest.enqueue(&setup.db);
    let mut head = setup.head();
    head.headers += "x-amz-meta-unrelated: ignored\r\nETag: not-a-hash\r\n";
    setup.s3.push(head);
    let receipt = setup.verify().await.unwrap();
    assert_eq!(receipt.version(), &candidate());
    assert_eq!(receipt.length(), 3);
    assert_eq!(receipt.full_sha256(), None);
    assert_eq!(receipt.attempt().deadline(), ts(6000));
    assert_eq!(
        receipt.intent().revision(),
        Uuid::parse_str(CONFIG).unwrap()
    );
    assert_eq!(receipt.namespace().id(), configuration().id());
    for debug in [format!("{receipt:?}"), format!("{:?}", setup.qualified)] {
        for secret in [KEY, VERSION, "fixture-secret", "managed/"] {
            assert!(!debug.contains(secret));
        }
    }
    let requests = setup.done();
    assert_eq!(requests.iter().filter(|r| r.contains(KEY)).count(), 1);
}

#[tokio::test]
async fn exact_full_hash_success_never_uses_etag_or_asserted_checksum() {
    let setup = Setup::new(true).await;
    for _ in 0..4 {
        setup.rows.enqueue(&setup.db);
    }
    let mut head = setup.head();
    head.headers += "ETag: deliberately-wrong\r\nx-amz-checksum-sha256: unrelated-composite-2\r\n";
    setup.s3.push(head);
    setup.s3.push(setup.get());
    let receipt = setup.verify().await.unwrap();
    let expected: [u8; 32] = Sha256::digest(PAYLOAD).into();
    assert_eq!(receipt.full_sha256(), Some(&expected));
    assert_eq!(setup.done().iter().filter(|r| r.contains(KEY)).count(), 2);
}

#[tokio::test]
async fn wrong_missing_unknown_reserved_metadata_and_wrong_attribution_fail() {
    for mode in 0..14 {
        let mut setup = Setup::new(false).await;
        if mode < 11 {
            let mut keys: Vec<_> = setup.metadata.keys().cloned().collect();
            keys.sort();
            setup.metadata.insert(keys[mode].clone(), "wrong".into());
        } else if mode == 11 {
            setup.metadata.remove("storelink-schema");
        } else if mode == 12 {
            setup
                .metadata
                .insert("storelink-unknown".into(), "1".into());
        } else {
            setup.metadata.clear();
        }
        setup.rows.enqueue(&setup.db);
        setup.s3.push(setup.head());
        assert!(
            matches!(setup.verify().await, Err(VerificationError::Attribution)),
            "mode {mode}"
        );
        setup.done();
    }
}

#[tokio::test]
async fn head_rejects_missing_null_wrong_versions_and_invalid_lengths() {
    for mode in 0..7 {
        let setup = Setup::new(false).await;
        setup.rows.enqueue(&setup.db);
        let mut head = setup.head();
        match mode {
            0 => {
                head.headers = head
                    .headers
                    .replace(&format!("x-amz-version-id: {VERSION}\r\n"), "")
            }
            1 => head.headers = head.headers.replace(VERSION, "null"),
            2 => head.headers = head.headers.replace(VERSION, "another"),
            3 => head.length = None,
            4 => head.length = Some(-1),
            5 => head.length = Some(2),
            _ => head.length = Some(101),
        }
        setup.s3.push(head);
        assert!(setup.verify().await.is_err(), "mode {mode}");
        setup.done();
    }
    assert!(VersionRef::new(StoreId::new("store").unwrap(), KEY, "null").is_err());
}

#[tokio::test]
async fn get_rejects_wrong_versions_lengths_short_stream_and_hash_mismatch() {
    for mode in 0..8 {
        let setup = Setup::new(true).await;
        for _ in 0..2 {
            setup.rows.enqueue(&setup.db);
        }
        if matches!(mode, 5..=7) {
            setup.rows.enqueue(&setup.db); // Post-header admission before body validation.
        }
        setup.s3.push(setup.head());
        let mut get = setup.get();
        match mode {
            0 => get.headers.clear(),
            1 => get.headers = get.headers.replace(VERSION, "null"),
            2 => get.headers = get.headers.replace(VERSION, "other"),
            3 => get.length = Some(2),
            4 => get.length = Some(101),
            5 => get.body = b"ab".to_vec(), // Native premature EOF, never a receipt.
            6 => get.body = b"bad".to_vec(),
            _ => {
                // The native chunked body exceeds the advertised object length.
                get.headers += "Transfer-Encoding: chunked\r\n";
                get.body = b"4\r\nabcd\r\n0\r\n\r\n".to_vec();
            }
        }
        setup.s3.push(get);
        let error = setup.verify().await.expect_err("invalid candidate body");
        if mode == 7 {
            assert!(matches!(error, VerificationError::Length), "{error:?}");
        }
        setup.done();
    }
}

#[tokio::test]
async fn final_admission_rejects_cancel_seal_read_failure_and_configuration_changes() {
    for mode in 0..5 {
        let setup = Setup::new(false).await;
        setup.rows.enqueue(&setup.db);
        let mut final_rows = setup.rows.clone();
        match mode {
            0 => put_text(&mut final_rows.intent, "state", "CANCELLED"),
            1 => put_text(&mut final_rows.attempt, "state", "SEALED"),
            2 => put_text(&mut final_rows.namespace, "bucket", "other-bucket"),
            3 => {
                for row in [
                    &mut final_rows.namespace,
                    &mut final_rows.intent,
                    &mut final_rows.attempt,
                ] {
                    put_text(row, "configuration_revision", REV);
                }
            }
            _ => {
                final_rows.attempt = json!({});
            }
        }
        final_rows.enqueue(&setup.db);
        setup.s3.push(setup.head());
        // Corrupt discovery fails before TransactGet; provide only that response.
        if mode == 4 {
            setup.db.replies.lock().unwrap().pop_back();
        }
        assert!(setup.verify().await.is_err());
        setup.done();
    }
}

#[tokio::test]
async fn all_qualified_fields_and_exact_candidate_target_are_checked_before_s3() {
    for (field, value) in [
        ("namespace_id", "other"),
        ("store", "other"),
        ("bucket", "other-bucket"),
        ("expected_owner", "000000000000"),
        ("region", "us-east-1"),
        ("key_prefix", "managed/private"),
        ("mpu_max_age_ms", "3600001"),
        ("mpu_lifecycle_margin_ms", "3600001"),
    ] {
        let setup = Setup::new(false).await;
        let mut rows = setup.rows.clone();
        if field.starts_with("mpu_") {
            put_number(&mut rows.namespace, field, value.parse().unwrap());
        } else {
            put_text(&mut rows.namespace, field, value);
        }
        if field == "store" {
            for row in [&mut rows.intent, &mut rows.attempt] {
                put_text(row, field, value);
            }
        }
        if field == "namespace_id" {
            rows.namespace["pk"] = json!({"S":format!("{}#NS#6f74686572",prefix())});
            for row in [&mut rows.intent, &mut rows.attempt] {
                put_text(row, field, value);
            }
        }
        rows.enqueue(&setup.db);
        assert!(
            matches!(setup.verify().await, Err(VerificationError::Binding)),
            "{field}"
        );
        setup.done();
    }
    for (store, key) in [("other", KEY), ("store", "managed/other")] {
        let setup = Setup::new(false).await;
        setup.rows.enqueue(&setup.db);
        let candidate = VersionRef::new(StoreId::new(store).unwrap(), key, VERSION).unwrap();
        assert!(matches!(
            verify_candidate(
                &setup.journal,
                &setup.qualified,
                &attempt_id(),
                &candidate,
                &policy(),
                &clock
            )
            .await,
            Err(VerificationError::Binding)
        ));
        setup.done();
    }
}

#[tokio::test]
async fn stalled_body_timer_rechecks_native_authority_without_waiting_for_body() {
    for mode in 0..5 {
        let mut setup = Setup::new(true).await;
        // The first check runs after 50ms (200ms validity minus 150ms margin).
        for row in [&mut setup.rows.intent, &mut setup.rows.attempt] {
            put_number(row, "deadline", 1200);
        }
        for _ in 0..3 {
            setup.rows.enqueue(&setup.db); // Includes post-header admission before the timer.
        }
        let mut terminal = setup.rows.clone();
        match mode {
            0 => put_text(&mut terminal.intent, "state", "CANCELLED"),
            1 => put_text(&mut terminal.attempt, "state", "SEALED"),
            2 => put_text(&mut terminal.namespace, "bucket", "changed-bucket"),
            3 => {} // Post-RPC clock expiry below.
            _ => {} // Native read failure below.
        }
        if mode == 4 {
            let mut error = Reply::json(json!({"__type":"InternalServerError"}));
            error.status = 500;
            setup.db.push(error);
        } else {
            terminal.enqueue(&setup.db);
        }
        setup.s3.push(setup.head());
        let (release, gate) = mpsc::channel();
        let mut get = setup.get();
        get.gate = Some(gate);
        setup.s3.push(get);
        let now = AtomicU64::new(1000);
        let sampler = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
        let verifier = async {
            tokio::time::timeout(
                Duration::from_millis(800),
                verify_candidate(
                    &setup.journal,
                    &setup.qualified,
                    &attempt_id(),
                    &candidate(),
                    &policy(),
                    &sampler,
                ),
            )
            .await
            .expect("timer must interrupt pending read")
        };
        let change_clock = async {
            if mode == 3 {
                tokio::time::sleep(Duration::from_millis(25)).await;
                now.store(1200, Ordering::SeqCst);
            }
        };
        let (result, ()) = tokio::join!(verifier, change_clock);
        assert!(result.is_err(), "mode {mode}");
        drop(release); // Wake the fixture before joining its thread.
        setup.done();
    }
}

#[tokio::test]
async fn timer_accepts_external_renewal_and_preserves_pending_body() {
    let setup = Setup::new(true).await;
    let mut short = setup.rows.clone();
    for row in [&mut short.intent, &mut short.attempt] {
        put_number(row, "deadline", 1200);
    }
    for _ in 0..3 {
        short.enqueue(&setup.db); // Includes post-header admission before the timer.
    }
    let mut renewed = setup.rows.clone();
    for row in [&mut renewed.intent, &mut renewed.attempt] {
        put_text(row, "revision", CONFIG);
    }
    renewed.enqueue(&setup.db); // Timer's fresh admission.
    let (release, gate) = mpsc::channel();
    setup.db.replies.lock().unwrap().back_mut().unwrap().notify = Some(release);
    renewed.enqueue(&setup.db); // Final fresh admission.
    setup.s3.push(setup.head());
    let mut get = setup.get();
    get.gate = Some(gate);
    setup.s3.push(get);
    let receipt = setup.verify().await.unwrap();
    assert_eq!(receipt.attempt().deadline(), ts(5000));
    assert_eq!(
        receipt.attempt().revision(),
        Uuid::parse_str(CONFIG).unwrap()
    );
    let expected: [u8; 32] = Sha256::digest(PAYLOAD).into();
    assert_eq!(receipt.full_sha256(), Some(&expected));
    setup.done();
}

#[tokio::test]
async fn delayed_get_headers_refresh_admission_before_streaming() {
    for mode in 0..4 {
        let setup = Setup::new(true).await;
        let mut short = setup.rows.clone();
        for row in [&mut short.intent, &mut short.attempt] {
            put_number(row, "deadline", 1200);
        }
        for _ in 0..2 {
            short.enqueue(&setup.db);
        } // HEAD and GET admission at 1000.
        let mut refreshed = setup.rows.clone();
        for row in [&mut refreshed.intent, &mut refreshed.attempt] {
            put_text(row, "revision", CONFIG);
        }
        match mode {
            0 => {} // External renewal to 5000 must be accepted after headers at 1300.
            1 => put_text(&mut refreshed.intent, "state", "CANCELLED"),
            2 => refreshed = short, // No renewal: post-header admission is expired.
            _ => put_text(&mut refreshed.namespace, "bucket", "changed-bucket"),
        }
        refreshed.enqueue(&setup.db); // Required fresh post-header native read.
        if mode == 0 {
            refreshed.enqueue(&setup.db);
        } // Final admission.
        setup.s3.push(setup.head());
        let (arrived, request_arrived) = tokio::sync::oneshot::channel();
        let (release, gate) = mpsc::channel();
        let mut get = setup.get();
        get.before_headers = Some((arrived, gate));
        setup.s3.push(get);
        let now = AtomicU64::new(1000);
        let sampler = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
        let verification = async {
            verify_candidate(
                &setup.journal,
                &setup.qualified,
                &attempt_id(),
                &candidate(),
                &policy(),
                &sampler,
            )
            .await
        };
        let deliver_headers = async {
            request_arrived
                .await
                .expect("GET reached the pre-header gate");
            // Advance only after the pre-GET admission and actual native request.
            now.store(1300, Ordering::SeqCst);
            release.send(()).unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(verification, deliver_headers)
        })
        .await
        .expect("bounded delayed headers");
        match mode {
            0 => {
                let receipt = result.expect("external renewal survives delayed GET headers");
                let expected: [u8; 32] = Sha256::digest(PAYLOAD).into();
                assert_eq!(receipt.full_sha256(), Some(&expected));
                assert_eq!(receipt.length(), 3);
                assert_eq!(receipt.version(), &candidate());
                assert_eq!(receipt.intent().deadline(), ts(5000));
                assert_eq!(receipt.attempt().deadline(), ts(5000));
                assert_eq!(
                    receipt.attempt().revision(),
                    Uuid::parse_str(CONFIG).unwrap()
                );
            }
            1 => assert!(matches!(
                result,
                Err(VerificationError::Admission(
                    storelink::dynamodb::UploadError::Terminal
                ))
            )),
            2 => assert!(matches!(
                result,
                Err(VerificationError::Admission(
                    storelink::dynamodb::UploadError::Claim(storelink::claim::ClaimError::Expired)
                ))
            )),
            _ => assert!(matches!(result, Err(VerificationError::Binding))),
        }
        assert_eq!(setup.done().iter().filter(|r| r.contains(KEY)).count(), 2);
    }
}

#[tokio::test]
async fn final_post_read_clock_expiry_cannot_mint_stale_receipt() {
    let setup = Setup::new(false).await;
    for _ in 0..2 {
        setup.rows.enqueue(&setup.db);
    }
    setup.s3.push(setup.head());
    let calls = AtomicU64::new(0);
    let sampler = || {
        ClockObservation::trusted(
            ts(if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                1000
            } else {
                5000
            }),
            0,
        )
    };
    assert!(
        verify_candidate(
            &setup.journal,
            &setup.qualified,
            &attempt_id(),
            &candidate(),
            &policy(),
            &sampler
        )
        .await
        .is_err()
    );
    setup.done();
}

#[tokio::test]
async fn expired_or_untrusted_clock_cannot_issue_receipt_or_s3_request() {
    for trusted in [true, false] {
        let setup = Setup::new(false).await;
        setup.rows.enqueue(&setup.db);
        let sampler = || {
            if trusted {
                ClockObservation::trusted(ts(5000), 0)
            } else {
                Ok(ClockObservation::untrusted(ts(1000), 0))
            }
        };
        assert!(
            verify_candidate(
                &setup.journal,
                &setup.qualified,
                &attempt_id(),
                &candidate(),
                &policy(),
                &sampler
            )
            .await
            .is_err()
        );
        setup.done();
    }
}
