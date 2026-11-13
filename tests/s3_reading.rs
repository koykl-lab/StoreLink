//! Native SDK loopback and canned DynamoDB responses, not an expression engine
//! or AWS service acceptance. Only synthetic credentials; no cloud endpoints.
#![cfg(all(feature = "s3", feature = "dynamodb"))]

use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use storelink::{
    claim::ClaimPolicy,
    clock::{ClockError, ClockObservation, TimestampMs},
    dynamodb::{CommandJournal, EnrolledTables},
    identity::{DomainId, NamespaceId, ResourceId, Scope, StoreId, VersionRef},
    s3::{
        ByteRange, NamespaceConfiguration, NamespaceConfigurationSpec, QualifiedNamespace,
        ReadError, open_pinned_read, qualify_existing,
    },
};
use uuid::Uuid;

const INC: &str = "11111111-1111-4111-8111-111111111111";
const CONFIG: &str = "22222222-2222-4222-8222-222222222222";
const CREATION: &str = "33333333-3333-4333-8333-333333333333";
const PIN: &str = "44444444-4444-4444-8444-444444444444";
const REV: &str = "55555555-5555-4555-8555-555555555555";
const KEY: &str = "managed/private-object";
const VERSION: &str = "version/+exact";

struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
    length: Option<String>,
    chunked: bool,
    pending: bool,
    gate: Option<std::sync::mpsc::Receiver<Vec<u8>>>,
    action: Option<Box<dyn FnOnce() + Send>>,
}
impl Reply {
    fn bytes(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: body.into(),
            length: None,
            chunked: false,
            pending: false,
            gate: None,
            action: None,
        }
    }
    fn json(value: Value) -> Self {
        Self::bytes(value.to_string())
    }
    fn object(body: impl Into<Vec<u8>>) -> Self {
        let mut r = Self::bytes(body);
        r.headers = format!("x-amz-version-id: {VERSION}\r\n");
        r
    }
}
#[derive(Clone)]
struct Request {
    headers: String,
    body: Vec<u8>,
}
impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.lines().find_map(|l| {
            let (key, value) = l.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
    fn line(&self) -> &str {
        self.headers.lines().next().unwrap()
    }
}
struct Fixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<Request>>>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    closed: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Fixture {
    fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let (stopping, captured, responses, dropped) = (
            stop.clone(),
            requests.clone(),
            replies.clone(),
            closed.clone(),
        );
        let worker = std::thread::spawn(move || {
            loop {
                let (mut socket, _) = listener.accept().unwrap();
                if stopping.load(Ordering::SeqCst) {
                    break;
                }
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).unwrap();
                    header.push(byte[0]);
                    assert!(header.len() < 32768);
                }
                let mut request = Request {
                    headers: String::from_utf8(header).unwrap(),
                    body: Vec::new(),
                };
                let n = request
                    .header("content-length")
                    .map(|n| n.parse::<usize>().unwrap())
                    .unwrap_or(0);
                assert!(n < 1_000_000);
                request.body.resize(n, 0);
                socket.read_exact(&mut request.body).unwrap();
                let head = request.line().starts_with("HEAD ");
                captured.lock().unwrap().push(request);
                let reply = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected native request");
                if let Some(action) = reply.action {
                    action();
                }
                let length = reply.length.unwrap_or_else(|| reply.body.len().to_string());
                let framing = if reply.chunked {
                    "Transfer-Encoding: chunked\r\n".to_owned()
                } else {
                    format!("Content-Length: {length}\r\n")
                };
                write!(
                    socket,
                    "HTTP/1.1 {} Fixture\r\n{framing}Connection: close\r\n{}\r\n",
                    reply.status, reply.headers
                )
                .unwrap();
                if reply.pending {
                    // Loopback evidence that native transport processes body Drop
                    // once its runtime can progress. This is NOT a remote S3 I/O
                    // cancellation guarantee. Timeout is failure, not close proof.
                    let result = socket.read(&mut byte);
                    dropped.store(
                        matches!(result, Ok(0))
                            || result
                                .is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset),
                        Ordering::SeqCst,
                    );
                } else if let Some(gate) = reply.gate {
                    let bytes = gate
                        .recv_timeout(Duration::from_secs(10))
                        .expect("body gate sender");
                    let _ = socket.write_all(&bytes);
                } else if !head {
                    let _ = socket.write_all(&reply.body);
                }
            }
        });
        Self {
            address,
            stop,
            requests,
            replies,
            closed,
            worker: Some(worker),
        }
    }
    fn push(&self, reply: Reply) {
        self.replies.lock().unwrap().push_back(reply);
    }
    fn s3(&self) -> aws_sdk_s3::Client {
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(credentials())
                .endpoint_url(format!("http://{}", self.address))
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
                .endpoint_url(format!("http://{}", self.address))
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeouts())
                .build(),
        )
    }
    fn finish(&mut self, db: bool) -> Vec<Request> {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        self.worker.take().unwrap().join().unwrap();
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "unused canned replies"
        );
        let requests = self.requests.lock().unwrap().clone();
        for r in &requests {
            assert!(r.headers.contains("Credential=fixture-access/"));
            if db {
                assert!(
                    ["DescribeTable", "GetItem", "TransactGetItems"]
                        .iter()
                        .any(|s| r.header("x-amz-target").unwrap().ends_with(s)),
                    "no write/renew/release"
                );
                if r.header("x-amz-target").unwrap().ends_with("GetItem") {
                    assert_eq!(
                        serde_json::from_slice::<Value>(&r.body).unwrap()["ConsistentRead"],
                        true
                    );
                }
            } else {
                assert!(r.line().starts_with("GET ") || r.line().starts_with("HEAD "));
                assert_eq!(
                    r.header("x-amz-expected-bucket-owner"),
                    Some("123456789012")
                );
            }
        }
        requests
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            let result = worker.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}
fn credentials() -> Credentials {
    Credentials::new("fixture-access", "fixture-secret", None, None, "loopback")
}
fn timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(8))
        .build()
}
fn ts(n: u64) -> TimestampMs {
    TimestampMs::new(n).unwrap()
}
fn clock() -> Result<ClockObservation, ClockError> {
    ClockObservation::trusted(ts(1000), 0)
}
fn policy() -> ClaimPolicy {
    ClaimPolicy::new(10000, 150, 10).unwrap()
}
fn domain() -> DomainId {
    DomainId::new("domain-é").unwrap()
}
fn prefix() -> String {
    format!("D#{:x}", Sha256::digest(domain().as_str().as_bytes()))
}
fn pin_id() -> ResourceId {
    ResourceId::new(
        Scope::new(domain(), Uuid::parse_str(INC).unwrap(), 1).unwrap(),
        Uuid::parse_str(PIN).unwrap(),
    )
    .unwrap()
}
fn version() -> VersionRef {
    VersionRef::new(StoreId::new("store").unwrap(), KEY, VERSION).unwrap()
}
fn text(row: &mut Value, k: &str, v: &str) {
    row[k] = json!({"S":v});
}
fn number(row: &mut Value, k: &str, v: u64) {
    row[k] = json!({"N":v.to_string()});
}
fn configuration(id: &str) -> NamespaceConfiguration {
    NamespaceConfiguration::new(NamespaceConfigurationSpec {
        id: NamespaceId::new(id).unwrap(),
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
#[derive(Clone)]
struct Rows {
    control: Value,
    pin: Value,
    version: Value,
    namespace: Value,
}
impl Rows {
    fn new() -> Self {
        let mut control = json!({"pk":{"S":format!("{}#CONTROL",prefix())},"sk":{"S":"M"}});
        text(&mut control, "domain", domain().as_str());
        text(&mut control, "incarnation", INC);
        for (k, v) in [
            ("current_generation", 2),
            ("closed_through", 1),
            ("compacted_through", 1),
            ("revision", 1),
        ] {
            number(&mut control, k, v);
        }
        let mut namespace = json!({"pk":{"S":format!("{}#NS#6e732dc3a9",prefix())},"sk":{"S":"M"}});
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
            text(&mut namespace, k, v);
        }
        for k in ["mpu_max_age_ms", "mpu_lifecycle_margin_ms"] {
            number(&mut namespace, k, 3600000);
        }
        let mut pin = json!({"pk":{"S":format!("{}#USE#{INC}#1#{PIN}",prefix())},"sk":{"S":"M"}});
        let fingerprint: String = version()
            .fingerprint()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let mut version =
            json!({"pk":{"S":format!("{}#OBJ#{fingerprint}",prefix())},"sk":{"S":"M"}});
        for row in [&mut pin, &mut version] {
            for (k, v) in [
                ("store", "store"),
                ("object_key", KEY),
                ("version_id", VERSION),
                ("namespace_id", "ns-é"),
                ("configuration_revision", CONFIG),
                ("creation_domain", domain().as_str()),
                ("creation_incarnation", INC),
                ("creation_uuid", CREATION),
            ] {
                text(row, k, v);
            }
            number(row, "creation_generation", 1);
        }
        for (k, v) in [
            ("record", "use"),
            ("kind", "USE"),
            ("domain", domain().as_str()),
            ("incarnation", INC),
            ("resource_uuid", PIN),
            ("state", "ACTIVE"),
            ("revision", REV),
        ] {
            text(&mut pin, k, v);
        }
        number(&mut pin, "generation", 1);
        number(&mut pin, "deadline", 5000);
        for (k, v) in [
            ("record", "version"),
            ("kind", "VERSION"),
            ("state", "READY"),
        ] {
            text(&mut version, k, v);
        }
        number(&mut version, "claims", 1);
        number(&mut version, "references", 0);
        Self {
            control,
            pin,
            version,
            namespace,
        }
    }
    fn enqueue(&self, db: &Fixture) {
        self.enqueue_action(db, None);
    }
    fn enqueue_action(&self, db: &Fixture, action: Option<Box<dyn FnOnce() + Send>>) {
        db.push(Reply::json(json!({"Item":self.pin})));
        let mut reply = Reply::json(self.coherent());
        reply.action = action;
        db.push(reply);
    }
    fn coherent(&self) -> Value {
        json!({"Responses":[{"Item":self.control},{"Item":self.pin},{"Item":self.version},{"Item":self.namespace}]})
    }
}
struct Setup {
    s3: Fixture,
    db: Fixture,
    journal: CommandJournal,
    qualified: QualifiedNamespace,
    rows: Rows,
}
impl Setup {
    async fn new() -> Self {
        Self::with_namespace("ns-é").await
    }
    async fn with_namespace(id: &str) -> Self {
        let s3 = Fixture::new(profile());
        let qualified = qualify_existing(&s3.s3(), &configuration(id))
            .await
            .unwrap();
        let db = Fixture::new(vec![Reply::json(json!({"Table":{"TableName":"coord",
            "TableArn":"arn:aws:dynamodb:us-west-2:123456789012:table/coord","TableStatus":"ACTIVE",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}))]);
        let client = db.dynamo();
        let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
        let journal = CommandJournal::new(client, tables, domain());
        Self {
            s3,
            db,
            journal,
            qualified,
            rows: Rows::new(),
        }
    }
    fn open(&self, reply: Reply) {
        self.rows.enqueue(&self.db);
        self.s3.push(reply);
        self.rows.enqueue(&self.db);
    }
    fn finish(&mut self) -> (Vec<Request>, Vec<Request>) {
        (self.s3.finish(false), self.db.finish(true))
    }
}

#[tokio::test]
async fn full_range_empty_source_independence_and_exact_request() {
    for (payload, range) in [
        (b"abc".as_slice(), None),
        (b"bc".as_slice(), Some(ByteRange::new(1, 2).unwrap())),
        (b"".as_slice(), None),
    ] {
        let mut f = Setup::new().await;
        let mut reply = Reply::object(payload);
        if range.is_some() {
            reply.status = 206;
            reply.headers.push_str("Content-Range: bytes 1-2/3\r\n");
        }
        f.open(reply);
        let mut reader = open_pinned_read(
            &f.journal,
            &f.qualified,
            &pin_id(),
            range,
            &policy(),
            &clock,
        )
        .await
        .unwrap();
        assert_eq!(reader.length(), payload.len() as u64);
        assert_eq!(reader.version(), &version());
        assert_eq!(reader.read(&mut []).await.unwrap(), 0);
        let mut buffer = [0; 10];
        let mut actual = Vec::new();
        loop {
            let n = reader.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            actual.extend_from_slice(&buffer[..n]);
        }
        assert_eq!(actual, payload);
        drop(reader);
        let (s3, db) = f.finish();
        let get = s3.last().unwrap();
        assert_eq!(
            get.line(),
            "GET /storelink-fixture/managed/private-object?x-id=GetObject&versionId=version%2F%2Bexact HTTP/1.1"
        );
        assert_eq!(get.header("range"), range.map(|_| "bytes=1-2"));
        // No old reference/stage exists or is queried: only the independent pin.
        assert_eq!(db.len(), 5);
        let transaction: Value = serde_json::from_slice(&db[2].body).unwrap();
        let items = transaction["TransactItems"].as_array().unwrap();
        assert_eq!(items.len(), 4);
        for (item, row) in items.iter().zip([
            &f.rows.control,
            &f.rows.pin,
            &f.rows.version,
            &f.rows.namespace,
        ]) {
            assert_eq!(item["Get"]["Key"], json!({"pk":row["pk"],"sk":row["sk"]}));
        }
    }
}

#[tokio::test]
async fn bounded_chunks_and_count_changes_are_allowed() {
    let mut f = Setup::new().await;
    let body = vec![7; 200_000];
    f.rows.enqueue(&f.db);
    f.s3.push(Reply::object(body.clone()));
    number(&mut f.rows.version, "references", 99);
    number(&mut f.rows.version, "claims", 4);
    f.rows.enqueue(&f.db);
    let mut reader = open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock)
        .await
        .unwrap();
    let mut output = vec![0; 300_000];
    let mut result = Vec::new();
    loop {
        let n = reader.read(&mut output).await.unwrap();
        assert!(n <= 65536);
        if n == 0 {
            break;
        }
        result.extend_from_slice(&output[..n]);
    }
    assert_eq!(result, body);
    drop(reader);
    f.finish();
}

#[tokio::test]
async fn wrong_qualified_namespace_same_store_rejected_before_get() {
    let mut f = Setup::with_namespace("other").await;
    f.rows.enqueue(&f.db);
    assert!(matches!(
        open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock).await,
        Err(ReadError::Binding)
    ));
    assert_eq!(f.finish().0.len(), 4);
}

#[tokio::test]
async fn wrong_version_range_and_lengths_fail_closed() {
    for case in 0..8 {
        let mut f = Setup::new().await;
        f.rows.enqueue(&f.db);
        let mut r = Reply::object("abc");
        let range = if (2..=5).contains(&case) {
            Some(ByteRange::new(1, 3).unwrap())
        } else {
            None
        };
        match case {
            0 => r.headers = "x-amz-version-id: wrong\r\n".into(),
            1 => r.headers.clear(),
            2 => {} // Whole-object fallback must not pass a range read.
            3 => r.headers.push_str("Content-Range: bytes 0-2/4\r\n"),
            4 => r.headers.push_str("Content-Range: bytes 1-3/3\r\n"),
            5 => {
                r.body = b"ab".to_vec();
                r.headers.push_str("Content-Range: bytes 1-3/5\r\n");
            }
            6 => r.length = Some("-1".into()),
            7 => {
                r.chunked = true;
                r.body = b"3\r\nabc\r\n0\r\n\r\n".to_vec();
            }
            _ => unreachable!(),
        }
        f.s3.push(r);
        assert!(
            open_pinned_read(
                &f.journal,
                &f.qualified,
                &pin_id(),
                range,
                &policy(),
                &clock
            )
            .await
            .is_err(),
            "case {case}"
        );
        f.finish();
    }
}

#[tokio::test]
async fn early_eof_and_excess_transport_never_return_successful_complete_read() {
    for excess in [false, true] {
        let mut f = Setup::new().await;
        let mut r = Reply::object("ab");
        r.length = Some("3".into());
        if excess {
            // Conflicting chunk framing advertises three but supplies four. The
            // native HTTP/SDK parser may reject before our byte counter sees it.
            r.chunked = true;
            r.headers.push_str("Content-Length: 3\r\n");
            r.body = b"4\r\nabcd\r\n0\r\n\r\n".to_vec();
        }
        f.rows.enqueue(&f.db);
        f.s3.push(r);
        // Post-header admission is needed only if the SDK accepts the framing.
        f.rows.enqueue(&f.db);
        let opened =
            open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock).await;
        match opened {
            Ok(mut reader) => {
                let mut buf = [0; 10];
                let failure = loop {
                    match reader.read(&mut buf).await {
                        Ok(0) => panic!("accepted bad length"),
                        Ok(_) => {}
                        Err(e) => break e,
                    }
                };
                assert!(matches!(failure, ReadError::Length | ReadError::Body));
                assert!(matches!(
                    reader.read(&mut buf).await,
                    Err(ReadError::Closed)
                ));
            }
            Err(_) if excess => {
                f.db.replies.lock().unwrap().clear();
            }
            Err(e) => panic!("unexpected open error: {e}"),
        }
        f.finish();
    }
}

#[tokio::test]
async fn postheader_external_renewal_uses_fresh_coherent_authority() {
    let mut f = Setup::new().await;
    let now = Arc::new(AtomicU64::new(1000));
    let sampler = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
    number(&mut f.rows.pin, "deadline", 1300);
    f.rows.enqueue(&f.db);
    let mut r = Reply::object("abc");
    let time = now.clone();
    r.action = Some(Box::new(move || {
        time.store(2000, Ordering::SeqCst);
    }));
    f.s3.push(r);
    number(&mut f.rows.pin, "deadline", 6000);
    text(&mut f.rows.pin, "revision", CONFIG);
    f.rows.enqueue(&f.db);
    let mut reader = open_pinned_read(
        &f.journal,
        &f.qualified,
        &pin_id(),
        None,
        &policy(),
        &sampler,
    )
    .await
    .unwrap();
    assert_eq!(reader.read(&mut [0; 8]).await.unwrap(), 3);
    drop(reader);
    f.finish();
}

#[tokio::test]
async fn coherent_identity_control_and_creation_checks_fail_closed() {
    for case in 0..13 {
        let mut f = Setup::new().await;
        f.db.push(Reply::json(json!({"Item":f.rows.pin})));
        match case {
            0 => text(&mut f.rows.control, "incarnation", REV),
            1 => number(&mut f.rows.control, "closed_through", 9),
            2 => number(&mut f.rows.version, "claims", 0),
            3 => text(&mut f.rows.version, "state", "RETIRING"),
            4 => text(&mut f.rows.pin, "state", "REVOKED"),
            5 => text(&mut f.rows.pin, "creation_uuid", REV),
            6 => text(&mut f.rows.version, "creation_incarnation", REV),
            7 => text(&mut f.rows.namespace, "configuration_revision", REV),
            8 => text(&mut f.rows.namespace, "key_prefix", "elsewhere/"),
            9 => text(&mut f.rows.version, "version_id", "changed"),
            10 => number(&mut f.rows.pin, "deadline", 1000),
            11 => f.rows.pin = json!({}),
            12 => f.rows.control = json!({}),
            _ => unreachable!(),
        }
        f.db.push(Reply::json(f.rows.coherent()));
        assert!(
            f.journal
                .authorize_use(&pin_id(), &clock, &policy())
                .await
                .is_err(),
            "case {case}"
        );
        f.finish();
    }
}

#[tokio::test]
async fn historical_version_creator_survives_restore_and_closed_generation() {
    let mut f = Setup::new().await;
    for row in [&mut f.rows.pin, &mut f.rows.version] {
        text(row, "creation_incarnation", REV);
        number(row, "creation_generation", 999);
    }
    f.rows.enqueue(&f.db);
    let auth = f
        .journal
        .authorize_use(&pin_id(), &clock, &policy())
        .await
        .unwrap();
    assert_eq!(auth.valid_until(), ts(5000));
    f.finish();
}

#[tokio::test]
async fn changed_postheader_namespace_or_creation_is_not_renewal() {
    for namespace in [true, false] {
        let mut f = Setup::new().await;
        f.rows.enqueue(&f.db);
        f.s3.push(Reply::object("abc"));
        if namespace {
            text(&mut f.rows.namespace, "bucket", "different-bucket");
        } else {
            for row in [&mut f.rows.pin, &mut f.rows.version] {
                text(row, "creation_uuid", REV);
            }
        }
        f.rows.enqueue(&f.db);
        assert!(
            open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock)
                .await
                .is_err()
        );
        f.finish();
    }
}

#[tokio::test]
async fn clock_sample_follows_native_coherent_read() {
    let mut f = Setup::new().await;
    let trusted = Arc::new(AtomicBool::new(true));
    let change = trusted.clone();
    f.rows.enqueue_action(
        &f.db,
        Some(Box::new(move || {
            change.store(false, Ordering::SeqCst);
        })),
    );
    let sampler = || {
        if trusted.load(Ordering::SeqCst) {
            clock()
        } else {
            Err(ClockError::Untrusted)
        }
    };
    assert!(
        f.journal
            .authorize_use(&pin_id(), &sampler, &policy())
            .await
            .is_err()
    );
    f.finish();
}

// These fixtures join an OS server thread after dropping the native body. Keep
// a Tokio worker available to process the native HTTP connection's drop signal;
// a blocking join on a current-thread runtime would prevent that progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_body_expiry_revocation_and_untrusted_clock_are_bounded_and_drop_body() {
    for case in 0..3 {
        let mut f = Setup::new().await;
        let now = Arc::new(AtomicU64::new(1000));
        let trusted = Arc::new(AtomicBool::new(true));
        let sampler = || {
            if trusted.load(Ordering::SeqCst) {
                ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0)
            } else {
                Err(ClockError::Untrusted)
            }
        };
        number(&mut f.rows.pin, "deadline", 1300);
        let mut r = Reply::object("");
        r.pending = true;
        r.length = Some("3".into());
        f.open(r);
        let mut reader = open_pinned_read(
            &f.journal,
            &f.qualified,
            &pin_id(),
            None,
            &policy(),
            &sampler,
        )
        .await
        .unwrap();
        let mut output = [0; 8];
        let mut pending = Box::pin(reader.read(&mut output));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        match case {
            0 => {
                now.store(2000, Ordering::SeqCst);
                f.rows.enqueue(&f.db);
            }
            1 => {
                text(&mut f.rows.pin, "state", "REVOKED");
                f.rows.enqueue(&f.db);
            }
            2 => {
                trusted.store(false, Ordering::SeqCst);
            }
            _ => unreachable!(),
        }
        let result = tokio::time::timeout(Duration::from_secs(5), pending.as_mut())
            .await
            .expect("bounded validity timer");
        assert!(result.is_err());
        drop(pending);
        assert!(matches!(
            reader.read(&mut output).await,
            Err(ReadError::Closed)
        ));
        drop(reader);
        f.finish();
        assert!(f.s3.closed.load(Ordering::SeqCst));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_body_must_pass_local_deadline_before_returning_bytes() {
    let mut f = Setup::new().await;
    let now = Arc::new(AtomicU64::new(1000));
    let sampler = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
    let (send, receive) = std::sync::mpsc::channel();
    let mut r = Reply::object("");
    r.gate = Some(receive);
    r.length = Some("3".into());
    f.open(r);
    let mut reader = open_pinned_read(
        &f.journal,
        &f.qualified,
        &pin_id(),
        None,
        &policy(),
        &sampler,
    )
    .await
    .unwrap();
    let mut output = [99; 8];
    let mut pending = Box::pin(reader.read(&mut output));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    now.store(6000, Ordering::SeqCst);
    send.send(b"abc".to_vec()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pending.as_mut())
            .await
            .unwrap()
            .is_err()
    );
    drop(pending);
    assert_eq!(output, [99; 8]);
    assert!(matches!(
        reader.read(&mut output).await,
        Err(ReadError::Closed)
    ));
    drop(reader);
    f.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_body_reauthorization_allows_external_renewal_and_counts() {
    let mut f = Setup::new().await;
    number(&mut f.rows.pin, "deadline", 1300);
    let (send, receive) = std::sync::mpsc::channel();
    let mut r = Reply::object("");
    r.gate = Some(receive);
    r.length = Some("3".into());
    f.open(r);
    let mut reader = open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock)
        .await
        .unwrap();
    number(&mut f.rows.pin, "deadline", 9000);
    text(&mut f.rows.pin, "revision", CONFIG);
    number(&mut f.rows.version, "claims", 2);
    number(&mut f.rows.version, "references", 11);
    f.rows.enqueue_action(
        &f.db,
        Some(Box::new(move || {
            send.send(b"abc".to_vec()).unwrap();
        })),
    );
    let mut output = [0; 8];
    let n = tokio::time::timeout(Duration::from_secs(5), reader.read(&mut output))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&output[..n], b"abc");
    drop(reader);
    f.finish();
}

#[tokio::test]
async fn missing_or_failed_reads_never_authorize_or_sample_clock() {
    for case in 0..3 {
        let mut f = Setup::new().await;
        if case == 0 {
            f.db.push(Reply::json(json!({})));
        } else {
            f.db.push(Reply::json(json!({"Item":f.rows.pin})));
            let mut response = f.rows.coherent();
            if case == 1 {
                response["Responses"][1] = json!({});
                f.db.push(Reply::json(response));
            } else {
                let mut r =
                    Reply::json(json!({"__type":"InternalServerError","message":"fixture"}));
                r.status = 500;
                f.db.push(r);
            }
        }
        let sampler = || -> Result<ClockObservation, ClockError> {
            panic!("failed observation must not issue authority")
        };
        assert!(
            f.journal
                .authorize_use(&pin_id(), &sampler, &policy())
                .await
                .is_err()
        );
        f.finish();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_handle_drops_native_body_without_release() {
    let mut f = Setup::new().await;
    let mut r = Reply::object("");
    r.pending = true;
    r.length = Some("3".into());
    f.open(r);
    let reader = open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock)
        .await
        .unwrap();
    drop(reader);
    f.finish();
    assert!(f.s3.closed.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_pending_read_terminalizes_handle_without_renew_or_release() {
    let mut f = Setup::new().await;
    let mut r = Reply::object("");
    r.pending = true;
    r.length = Some("3".into());
    f.open(r);
    let mut reader = open_pinned_read(&f.journal, &f.qualified, &pin_id(), None, &policy(), &clock)
        .await
        .unwrap();
    let mut output = [0; 8];
    let mut future = Box::pin(reader.read(&mut output));
    // Poll once until pending, then cancel; no wall-clock timing assumption.
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(future);
    assert!(matches!(
        reader.read(&mut output).await,
        Err(ReadError::Closed)
    ));
    drop(reader);
    f.finish();
    assert!(f.s3.closed.load(Ordering::SeqCst));
}
