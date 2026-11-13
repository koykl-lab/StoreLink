//! Native SDK loopback + canned Dynamo JSON only, not an expression engine or
//! native AWS acceptance. Bounded generated input; synthetic credentials only.
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
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread::JoinHandle,
    time::Duration,
};
use storelink::{
    claim::ClaimPolicy,
    clock::{ClockError, ClockObservation, TimestampMs},
    dynamodb::{CommandJournal, EnrolledTables},
    identity::{DomainId, NamespaceId, ResourceId, Scope, StoreId},
    s3::{
        NamespaceConfiguration, NamespaceConfigurationSpec, QualifiedNamespace, TransferError,
        UploadCandidate, attribution_metadata, qualify_existing, transfer_upload,
    },
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use uuid::Uuid;

const PART: usize = 8 * 1024 * 1024;
const INC: &str = "11111111-1111-4111-8111-111111111111";
const CONFIG: &str = "22222222-2222-4222-8222-222222222222";
const INTENT: &str = "33333333-3333-4333-8333-333333333333";
const ATTEMPT: &str = "44444444-4444-4444-8444-444444444444";
const REV: &str = "55555555-5555-4555-8555-555555555555";
const KEY: &str = "managed/private-object";
const VERSION: &str = "version/+exact";
const HASH_BASE64: &str = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="; // SHA256(abc)

struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
    action: Option<Box<dyn FnOnce() + Send>>,
}
impl Reply {
    fn bytes(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: body.into(),
            action: None,
        }
    }
    fn json(v: Value) -> Self {
        Self::bytes(v.to_string().into_bytes())
    }
    fn version() -> Self {
        let mut r = Self::bytes("");
        r.headers = format!("x-amz-version-id: {VERSION}\r\n");
        r
    }
    fn create() -> Self {
        Self::bytes(
            "<InitiateMultipartUploadResult><Bucket>storelink-fixture</Bucket><Key>managed/private-object</Key><UploadId>private-upload-id</UploadId></InitiateMultipartUploadResult>",
        )
    }
    fn part(n: usize) -> Self {
        let mut r = Self::bytes("");
        // Canned native checksum token, not a fixture implementation of CRC.
        r.headers = format!("ETag: \"native-part-{n}\"\r\nx-amz-checksum-crc32: EjRWeA==\r\n");
        r
    }
    fn complete() -> Self {
        let mut r = Self::version();
        r.body = b"<CompleteMultipartUploadResult><Bucket>storelink-fixture</Bucket><Key>managed/private-object</Key><ETag>not-version-identity</ETag></CompleteMultipartUploadResult>".to_vec();
        r
    }
    fn error(code: &str) -> Self {
        let mut r = Self::bytes(format!(
            "<Error><Code>{code}</Code><Message>secret-server-payload</Message></Error>"
        ));
        r.status = 404;
        r
    }
}
struct Request {
    headers: String,
    body: Vec<u8>,
}
impl Request {
    fn line(&self) -> &str {
        self.headers.lines().next().unwrap()
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
    fn payload(&self) -> &[u8] {
        // Native S3 may wrap a replayable body in aws-chunked checksum framing.
        // Current SDK emits a single data chunk for these Bytes-backed bodies.
        if self.header("content-encoding") == Some("aws-chunked") {
            let end = self.body.windows(2).position(|s| s == b"\r\n").unwrap();
            let n =
                usize::from_str_radix(std::str::from_utf8(&self.body[..end]).unwrap(), 16).unwrap();
            assert!(n <= PART);
            &self.body[end + 2..end + 2 + n]
        } else {
            &self.body
        }
    }
}
struct Fixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<Request>>>,
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
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") {
                    assert_ne!(socket.read(&mut byte).unwrap(), 0);
                    header.push(byte[0]);
                    assert!(header.len() <= 32768);
                }
                let headers = String::from_utf8(header).unwrap();
                let mut request = Request {
                    headers,
                    body: Vec::new(),
                };
                let length = request
                    .header("content-length")
                    .map(|v| v.parse::<usize>().unwrap())
                    .unwrap_or(0);
                assert!(length <= PART + 32768);
                if request.header("expect") == Some("100-continue") {
                    socket.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").unwrap();
                }
                request.body.resize(length, 0);
                socket.read_exact(&mut request.body).unwrap();
                let head = request.line().starts_with("HEAD ");
                captured.lock().unwrap().push(request);
                let reply = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected native request (including abort)");
                if let Some(action) = reply.action {
                    action();
                }
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
                    reply.status,
                    reply.body.len(),
                    reply.headers
                );
                if socket.write_all(response.as_bytes()).is_ok() && !head {
                    let _ = socket.write_all(&reply.body);
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
    fn push(&self, r: Reply) {
        self.replies.lock().unwrap().push_back(r);
    }
    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }
    fn s3(
        &self,
        attempts: u32,
        checksum: Option<aws_sdk_s3::config::RequestChecksumCalculation>,
    ) -> aws_sdk_s3::Client {
        let mut builder = aws_sdk_s3::Config::builder();
        builder.set_request_checksum_calculation(checksum);
        aws_sdk_s3::Client::from_conf(
            builder
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(credentials())
                .endpoint_url(self.endpoint())
                .force_path_style(true)
                .retry_config(
                    RetryConfig::standard()
                        .with_max_attempts(attempts)
                        .with_initial_backoff(Duration::from_millis(1)),
                )
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
    fn done(&self, s3: bool) -> Vec<Request> {
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "unconsumed canned replies"
        );
        let requests = std::mem::take(&mut *self.requests.lock().unwrap());
        assert!(requests.len() < 60);
        for r in &requests {
            assert!(r.headers.contains("Credential=fixture-access/"));
            if s3 {
                assert_eq!(
                    r.header("x-amz-expected-bucket-owner"),
                    Some("123456789012")
                );
                assert!(!r.line().starts_with("DELETE "), "NO Abort on any path");
                if r.line().contains("private-object") {
                    assert!(
                        r.line()
                            .starts_with(&format!("PUT /storelink-fixture/{KEY}"))
                            || r.line()
                                .starts_with(&format!("POST /storelink-fixture/{KEY}"))
                    );
                    assert!(!r.line().contains("versionId="));
                }
            } else {
                assert!(
                    ["DescribeTable", "GetItem", "TransactGetItems"]
                        .iter()
                        .any(|t| r.header("x-amz-target").unwrap().ends_with(t))
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
        .operation_timeout(Duration::from_secs(5))
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
    vec![
        head,
        Reply::bytes("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
        Reply::bytes(
            "<LifecycleConfiguration><Rule><ID>mpu</ID><Status>Enabled</Status><Filter><Prefix>managed/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
        ),
        Reply::error("ObjectLockConfigurationNotFoundError"),
    ]
}
fn describe() -> Reply {
    Reply::json(
        json!({"Table":{"TableName":"coord","TableArn":"arn:aws:dynamodb:us-west-2:123456789012:table/coord","TableStatus":"ACTIVE",
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
fn clock() -> Result<ClockObservation, ClockError> {
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
fn text(row: &mut Value, k: &str, v: &str) {
    row[k] = json!({"S":v});
}
fn number(row: &mut Value, k: &str, v: u64) {
    row[k] = json!({"N":v.to_string()});
}
#[derive(Clone)]
struct Rows {
    control: Value,
    namespace: Value,
    intent: Value,
    attempt: Value,
}
impl Rows {
    fn new(max: u64, expected: Option<u64>, hash: bool) -> Self {
        let mut control = json!({"pk":{"S":format!("{}#CONTROL",prefix())},"sk":{"S":"M"}});
        for (k, v) in [("domain", domain().as_str()), ("incarnation", INC)] {
            text(&mut control, k, v);
        }
        for (k, v) in [
            ("current_generation", 1),
            ("closed_through", 0),
            ("compacted_through", 0),
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
        let admission = |kind: &str, id: &str, state: &str| {
            let mut row =
                json!({"pk":{"S":format!("{}#{kind}#{INC}#1#{id}",prefix())},"sk":{"S":"M"}});
            for (k, v) in [
                ("record", kind.to_ascii_lowercase().as_str()),
                ("domain", domain().as_str()),
                ("incarnation", INC),
                ("resource_uuid", id),
                ("namespace_id", "ns-é"),
                ("configuration_revision", CONFIG),
                ("store", "store"),
                ("object_key", KEY),
                ("state", state),
                ("revision", REV),
            ] {
                text(&mut row, k, v);
            }
            for (k, v) in [("generation", 1), ("deadline", 5000), ("max_length", max)] {
                number(&mut row, k, v);
            }
            if let Some(n) = expected {
                number(&mut row, "expected_length", n);
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
            text(&mut attempt, k, v);
        }
        for (k, v) in [
            ("intent_generation", 1),
            ("mpu_admitted_lower_bound", 900),
            ("max_mpu_use_until", 9000),
        ] {
            number(&mut attempt, k, v);
        }
        Self {
            control,
            namespace,
            intent,
            attempt,
        }
    }
    fn enqueue(&self, db: &Fixture) {
        self.enqueue_action(db, None);
    }
    fn enqueue_action(&self, db: &Fixture, action: Option<Box<dyn FnOnce() + Send>>) {
        db.push(Reply::json(json!({"Item":self.attempt})));
        let mut reply = Reply::json(
            json!({"Responses":[{"Item":self.control},{"Item":self.namespace},{"Item":self.intent},{"Item":self.attempt}]}),
        );
        reply.action = action;
        db.push(reply);
    }
    fn deadline(&mut self, n: u64) {
        number(&mut self.intent, "deadline", n);
        number(&mut self.attempt, "deadline", n);
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
    async fn new(max: u64, expected: Option<u64>, hash: bool) -> Self {
        Self::with_retries(max, expected, hash, 1).await
    }
    async fn with_retries(max: u64, expected: Option<u64>, hash: bool, attempts: u32) -> Self {
        Self::with_checksum(max, expected, hash, attempts, None).await
    }
    async fn with_checksum(
        max: u64,
        expected: Option<u64>,
        hash: bool,
        attempts: u32,
        checksum: Option<aws_sdk_s3::config::RequestChecksumCalculation>,
    ) -> Self {
        let s3 = Fixture::new(profile());
        let db = Fixture::new(vec![describe()]);
        let client = db.dynamo();
        let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
        let journal = CommandJournal::new(client, tables, domain());
        let qualified = qualify_existing(&s3.s3(attempts, checksum), &configuration())
            .await
            .unwrap();
        Self {
            s3,
            db,
            journal,
            qualified,
            rows: Rows::new(max, expected, hash),
        }
    }
    fn admissions(&self, n: usize) {
        for _ in 0..n {
            self.rows.enqueue(&self.db);
        }
    }
    async fn upload<R: AsyncRead + Unpin>(
        &self,
        reader: R,
    ) -> Result<UploadCandidate, TransferError> {
        tokio::time::timeout(
            Duration::from_secs(8),
            transfer_upload(
                &self.journal,
                &self.qualified,
                &attempt_id(),
                reader,
                &policy(),
                &clock,
            ),
        )
        .await
        .expect("bounded transfer")
    }
    fn done(&self) -> Vec<Request> {
        self.db.done(false);
        self.s3.done(true)
    }
    async fn expected_metadata(&self) -> std::collections::HashMap<String, String> {
        self.rows.enqueue(&self.db);
        attribution_metadata(
            &self
                .journal
                .authorize_transfer(&attempt_id(), clock, &policy())
                .await
                .unwrap(),
        )
        .unwrap()
    }
}
fn assert_candidate(c: &UploadCandidate, length: u64, hash: [u8; 32]) {
    assert_eq!(c.version().store().as_str(), "store");
    assert_eq!(c.version().key(), KEY);
    assert_eq!(c.version().version(), VERSION);
    assert_eq!(c.length(), length);
    assert_eq!(c.full_sha256(), &hash);
    for secret in [KEY, VERSION, "private-upload-id", "fixture-secret"] {
        assert!(!format!("{c:?}").contains(secret));
    }
}
fn mutations(requests: &[Request]) -> Vec<&Request> {
    requests
        .iter()
        .filter(|r| r.line().contains("private-object"))
        .collect()
}

#[tokio::test]
async fn empty_and_single_put_have_exact_bytes_metadata_and_local_hash() {
    for bytes in [b"".as_slice(), b"abc".as_slice()] {
        let setup = Setup::new(100, Some(bytes.len() as u64), !bytes.is_empty()).await;
        let metadata = setup.expected_metadata().await;
        setup.admissions(2);
        setup.s3.push(Reply::version());
        let c = setup.upload(bytes).await.unwrap();
        assert_candidate(&c, bytes.len() as u64, Sha256::digest(bytes).into());
        let requests = setup.done();
        let writes = mutations(&requests);
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].line(),
            format!("PUT /storelink-fixture/{KEY}?x-id=PutObject HTTP/1.1")
        );
        assert_eq!(writes[0].payload(), bytes);
        for (k, v) in metadata {
            assert_eq!(
                writes[0].header(&format!("x-amz-meta-{k}")),
                Some(v.as_str())
            );
        }
    }
}

#[tokio::test]
async fn multipart_enforces_native_crc32_for_when_required_client() {
    let length = PART as u64 + 1;
    let setup = Setup::with_checksum(
        length,
        Some(length),
        false,
        1,
        Some(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired),
    )
    .await;
    setup.admissions(6);
    for reply in [
        Reply::create(),
        Reply::part(1),
        Reply::part(2),
        Reply::complete(),
    ] {
        setup.s3.push(reply);
    }
    let candidate = setup
        .upload(tokio::io::repeat(b'x').take(length))
        .await
        .unwrap();
    assert_eq!(candidate.length(), length);
    let requests = setup.done();
    let writes = mutations(&requests);
    assert_eq!(writes.len(), 4);
    for part in &writes[1..=2] {
        assert_eq!(part.header("x-amz-sdk-checksum-algorithm"), Some("CRC32"));
        assert!(
            part.header("x-amz-checksum-crc32").is_some()
                || part.header("x-amz-trailer") == Some("x-amz-checksum-crc32")
        );
    }
    assert_eq!(
        std::str::from_utf8(writes[3].payload())
            .unwrap()
            .matches("<ChecksumCRC32>EjRWeA==</ChecksumCRC32>")
            .count(),
        2
    );
}

#[tokio::test]
async fn multipart_two_parts_exact_body_numbers_metadata_and_complete_tokens() {
    let length = PART as u64 + 3;
    for expected in [None, Some(length)] {
        let setup = Setup::new(length, expected, false).await;
        let metadata = setup.expected_metadata().await;
        setup.admissions(6); // initial/create/part1/source-resume/part2/complete
        for r in [
            Reply::create(),
            Reply::part(1),
            Reply::part(2),
            Reply::complete(),
        ] {
            setup.s3.push(r);
        }
        let c = setup
            .upload(tokio::io::repeat(b'x').take(length))
            .await
            .unwrap();
        let mut hash = Sha256::new();
        for _ in 0..PART / 8192 {
            hash.update([b'x'; 8192]);
        }
        hash.update(b"xxx");
        assert_candidate(&c, length, hash.finalize().into());
        let requests = setup.done();
        let writes = mutations(&requests);
        assert_eq!(writes.len(), 4);
        assert!(writes[0].line().contains("?uploads"));
        assert_eq!(writes[0].header("x-amz-checksum-algorithm"), Some("CRC32"));
        assert_eq!(writes[0].header("x-amz-checksum-type"), Some("COMPOSITE"));
        for (k, v) in metadata {
            assert_eq!(
                writes[0].header(&format!("x-amz-meta-{k}")),
                Some(v.as_str())
            );
        }
        for (index, n) in [(1, PART), (2, 3)] {
            assert!(
                writes[index]
                    .line()
                    .contains(&format!("partNumber={index}"))
            );
            assert!(writes[index].line().contains("uploadId=private-upload-id"));
            assert_eq!(
                writes[index].header("x-amz-sdk-checksum-algorithm"),
                Some("CRC32")
            );
            assert!(
                writes[index].header("x-amz-checksum-crc32").is_some()
                    || writes[index].header("x-amz-trailer") == Some("x-amz-checksum-crc32")
            );
            assert_eq!(writes[index].payload().len(), n);
            assert!(writes[index].payload().iter().all(|b| *b == b'x'));
        }
        let complete = std::str::from_utf8(writes[3].payload()).unwrap();
        for n in [1, 2] {
            assert!(complete.contains(&format!("<PartNumber>{n}</PartNumber>")));
            assert!(complete.contains(&format!("native-part-{n}")));
        }
        assert!(writes[3].line().starts_with("POST "));
        assert_eq!(
            complete
                .matches("<ChecksumCRC32>EjRWeA==</ChecksumCRC32>")
                .count(),
            2
        );
        assert_eq!(writes[3].header("x-amz-checksum-type"), Some("COMPOSITE"));
    }
}

#[tokio::test]
async fn nonuniform_lookahead_is_hashed_and_sent_exactly_once() {
    let length = PART as u64 + 4;
    let setup = Setup::new(length, Some(length), false).await;
    setup.admissions(6);
    for r in [
        Reply::create(),
        Reply::part(1),
        Reply::part(2),
        Reply::complete(),
    ] {
        setup.s3.push(r);
    }
    let marker = b"\xfe\x00\x01\x02\x03";
    let source = tokio::io::repeat(b'a')
        .take(PART as u64 - 1)
        .chain(marker.as_slice());
    let candidate = setup.upload(source).await.unwrap();
    let chunk = [b'a'; 8192];
    let mut hash = Sha256::new();
    for _ in 0..(PART - 1) / chunk.len() {
        hash.update(chunk);
    }
    hash.update(&chunk[..(PART - 1) % chunk.len()]);
    hash.update(marker);
    assert_candidate(&candidate, length, hash.finalize().into());
    let requests = setup.done();
    let writes = mutations(&requests);
    assert_eq!(writes[1].payload().len(), PART);
    assert!(writes[1].payload()[..PART - 1].iter().all(|b| *b == b'a'));
    assert_eq!(writes[1].payload()[PART - 1], 0xfe);
    assert_eq!(writes[2].payload(), b"\x00\x01\x02\x03");
}

#[tokio::test]
async fn exactly_eight_mib_is_single_put_not_an_empty_last_part() {
    let setup = Setup::new(PART as u64, None, false).await;
    setup.admissions(2);
    setup.s3.push(Reply::version());
    assert_eq!(
        setup
            .upload(tokio::io::repeat(7).take(PART as u64))
            .await
            .unwrap()
            .length(),
        PART as u64
    );
    let requests = setup.done();
    let writes = mutations(&requests);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload().len(), PART);
}

#[tokio::test]
async fn missing_null_empty_and_oversized_version_ids_never_use_etag() {
    for multipart in [false, true] {
        for version in [
            None,
            Some("null".to_owned()),
            Some(String::new()),
            Some("v".repeat(1025)),
        ] {
            let length = if multipart { PART as u64 + 1 } else { 0 };
            let setup = Setup::new(length, None, false).await;
            setup.admissions(if multipart { 6 } else { 2 });
            if multipart {
                for r in [Reply::create(), Reply::part(1), Reply::part(2)] {
                    setup.s3.push(r);
                }
            }
            let mut r = if multipart {
                Reply::complete()
            } else {
                Reply::version()
            };
            r.headers = "ETag: not-a-version\r\n".into();
            if let Some(v) = version {
                r.headers += &format!("x-amz-version-id: {v}\r\n");
            }
            setup.s3.push(r);
            assert!(matches!(
                setup.upload(tokio::io::repeat(0).take(length)).await,
                Err(TransferError::Version)
            ));
            setup.done();
        }
    }
}

struct Counted {
    count: Arc<AtomicUsize>,
}
impl AsyncRead for Counted {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let n = buf.remaining();
        buf.initialize_unfilled().fill(b'x');
        buf.advance(n);
        self.count.fetch_add(n, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn max_length_reads_only_one_excess_byte_and_constraints_precede_put() {
    for max in [0, 2, 100] {
        let setup = Setup::new(max, None, false).await;
        setup.admissions(1);
        let count = Arc::new(AtomicUsize::new(0));
        assert!(matches!(
            setup
                .upload(Counted {
                    count: count.clone()
                })
                .await,
            Err(TransferError::Length)
        ));
        assert_eq!(count.load(Ordering::SeqCst), max as usize + 1);
        assert!(mutations(&setup.done()).is_empty());
    }
    for (expected, hash) in [(Some(2), false), (Some(4), false), (Some(3), true)] {
        let setup = Setup::new(100, expected, hash).await;
        setup.admissions(1);
        let result = setup.upload(b"xyz".as_slice()).await;
        if hash {
            assert!(matches!(result, Err(TransferError::Hash)));
        } else {
            assert!(matches!(result, Err(TransferError::Length)));
        }
        assert!(mutations(&setup.done()).is_empty());
    }
}

#[tokio::test]
async fn mpu_length_and_hash_failures_never_complete_or_abort() {
    for mode in 0..3 {
        let length = PART as u64 + 1;
        let setup = Setup::new(
            if mode == 0 { PART as u64 } else { length },
            (mode == 1).then_some(length - 1),
            mode == 2,
        )
        .await;
        setup.admissions(if mode == 0 { 1 } else { 5 });
        if mode != 0 {
            for r in [Reply::create(), Reply::part(1), Reply::part(2)] {
                setup.s3.push(r);
            }
        }
        let result = setup.upload(tokio::io::repeat(b'x').take(length)).await;
        if mode == 2 {
            assert!(matches!(result, Err(TransferError::Hash)));
        } else {
            assert!(matches!(result, Err(TransferError::Length)));
        }
        let requests = setup.done();
        let writes = mutations(&requests);
        assert_eq!(writes.len(), if mode == 0 { 0 } else { 3 });
    }
}

#[tokio::test]
async fn native_errors_unknown_complete_and_missing_tokens_never_abort() {
    for mode in 0..8 {
        let setup = Setup::new(PART as u64 + 1, None, false).await;
        match mode {
            0 => {
                setup.admissions(2);
                setup.s3.push(Reply::error("AccessDenied"));
            }
            1 => {
                setup.admissions(2);
                setup
                    .s3
                    .push(Reply::bytes("<InitiateMultipartUploadResult/>"));
            }
            2 | 3 | 6 | 7 => {
                setup.admissions(3);
                setup.s3.push(Reply::create());
                let mut reply = if mode == 2 {
                    Reply::error("ServiceUnavailable")
                } else {
                    Reply::bytes("")
                };
                if mode >= 6 {
                    reply.headers = "ETag: token-without-checksum\r\n".into();
                    if mode == 7 {
                        reply.headers += "x-amz-checksum-crc32: \r\n";
                    }
                }
                setup.s3.push(reply);
            }
            _ => {
                setup.admissions(6);
                for r in [Reply::create(), Reply::part(1), Reply::part(2)] {
                    setup.s3.push(r);
                }
                let mut r = Reply::error(if mode == 4 {
                    "NoSuchUpload"
                } else {
                    "InternalError"
                });
                if mode == 5 {
                    r.status = 200;
                } // Embedded Complete failure.
                setup.s3.push(r);
            }
        }
        let result = setup
            .upload(tokio::io::repeat(b'x').take(PART as u64 + 1))
            .await;
        if matches!(mode, 1 | 3 | 6 | 7) {
            assert!(matches!(result, Err(TransferError::MultipartToken)));
        } else {
            assert!(matches!(result, Err(TransferError::OutcomeUnknown)));
        }
        let e = result.unwrap_err();
        for secret in [KEY, "private-upload-id", "secret-server-payload"] {
            assert!(!format!("{e:?}: {e}").contains(secret));
        }
        setup.done();
    }
}

struct Failing {
    remaining: usize,
}
impl AsyncRead for Failing {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Err(std::io::Error::other("secret-source-payload")));
        }
        let n = buf.remaining().min(self.remaining);
        buf.initialize_unfilled()[..n].fill(1);
        buf.advance(n);
        self.remaining -= n;
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn midstream_source_error_leaves_incomplete_mpu_for_lifecycle() {
    let setup = Setup::new(PART as u64 + 10, None, false).await;
    setup.admissions(4);
    setup.s3.push(Reply::create());
    setup.s3.push(Reply::part(1));
    let e = setup
        .upload(Failing {
            remaining: PART + 2,
        })
        .await
        .unwrap_err();
    assert!(matches!(e, TransferError::Source));
    assert!(!format!("{e:?}").contains("secret-source-payload"));
    assert_eq!(mutations(&setup.done()).len(), 2);
}

struct PendingSource {
    dropped: Arc<AtomicBool>,
    polled: Arc<AtomicUsize>,
    notify: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AsyncRead for PendingSource {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.polled.fetch_add(1, Ordering::SeqCst);
        if let Some(tx) = self.notify.take() {
            let _ = tx.send(());
        }
        Poll::Pending
    }
}
impl Drop for PendingSource {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn pending_input_revalidates_on_bounded_timer_and_drops_reader_on_loss() {
    for mode in 0..3 {
        let mut setup = Setup::new(100, None, false).await;
        setup.rows.deadline(1100);
        setup.rows.enqueue(&setup.db);
        let now = Arc::new(AtomicU64::new(1000));
        let mut lost = setup.rows.clone();
        match mode {
            0 => text(&mut lost.intent, "state", "CANCELLED"),
            1 => text(&mut lost.attempt, "state", "SEALED"),
            _ => {}
        }
        let n = now.clone();
        lost.enqueue_action(
            &setup.db,
            Some(Box::new(move || {
                if mode == 2 {
                    n.store(1100, Ordering::SeqCst);
                }
            })),
        );
        let dropped = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicUsize::new(0));
        let reader = PendingSource {
            dropped: dropped.clone(),
            polled: polled.clone(),
            notify: None,
        };
        let sample = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            transfer_upload(
                &setup.journal,
                &setup.qualified,
                &attempt_id(),
                reader,
                &policy(),
                &sample,
            ),
        )
        .await
        .expect("timer must revoke pending source");
        assert!(matches!(result, Err(TransferError::Admission(_))));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(polled.load(Ordering::SeqCst) > 0);
        assert!(mutations(&setup.done()).is_empty());
    }
}

#[tokio::test]
async fn renewal_preserves_partial_buffer_and_pending_source_without_local_ttl_refresh() {
    let mut setup = Setup::new(3, Some(3), true).await;
    setup.rows.deadline(1100);
    setup.rows.enqueue(&setup.db);
    let mut renewed = setup.rows.clone();
    renewed.deadline(8000);
    text(&mut renewed.intent, "revision", CONFIG);
    text(&mut renewed.attempt, "revision", CONFIG);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let now = Arc::new(AtomicU64::new(1000));
    let n = now.clone();
    renewed.enqueue_action(
        &setup.db,
        Some(Box::new(move || {
            n.store(1100, Ordering::SeqCst);
            let _ = tx.send(());
        })),
    );
    renewed.enqueue(&setup.db); // Fresh Put authority after all bytes/hash checks.
    setup.s3.push(Reply::version());
    let (mut writer, reader) = tokio::io::duplex(16);
    writer.write_all(b"a").await.unwrap();
    let producer = async move {
        rx.await.unwrap();
        writer.write_all(b"bc").await.unwrap();
        writer.shutdown().await.unwrap();
    };
    let sample = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
    let id = attempt_id();
    let p = policy();
    let upload = transfer_upload(&setup.journal, &setup.qualified, &id, reader, &p, &sample);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(upload, producer)
    })
    .await
    .unwrap();
    assert_candidate(&result.unwrap(), 3, Sha256::digest(b"abc").into());
    let requests = setup.done();
    assert_eq!(mutations(&requests)[0].payload(), b"abc");
}

#[tokio::test]
async fn cancellation_and_expiry_between_mpu_operations_stop_admission_not_abort() {
    for expire in [false, true] {
        let setup = Setup::new(PART as u64 + 1, None, false).await;
        setup.admissions(3); // initial, create, first part
        let mut lost = setup.rows.clone();
        if !expire {
            text(&mut lost.intent, "state", "CANCELLED");
        }
        lost.enqueue(&setup.db);
        let now = Arc::new(AtomicU64::new(1000));
        setup.s3.push(Reply::create());
        let mut part = Reply::part(1);
        let n = now.clone();
        part.action = Some(Box::new(move || {
            if expire {
                n.store(5000, Ordering::SeqCst);
            }
        }));
        setup.s3.push(part);
        let sample = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
        let result = transfer_upload(
            &setup.journal,
            &setup.qualified,
            &attempt_id(),
            tokio::io::repeat(1).take(PART as u64 + 1),
            &policy(),
            &sample,
        )
        .await;
        assert!(matches!(result, Err(TransferError::Admission(_))));
        assert_eq!(mutations(&setup.done()).len(), 2);
    }
}

#[tokio::test]
async fn dropping_transfer_future_with_pending_mpu_source_never_aborts_or_detaches() {
    let setup = Setup::new(PART as u64 + 2, None, false).await;
    setup.admissions(4);
    setup.s3.push(Reply::create());
    setup.s3.push(Reply::part(1));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let reader = tokio::io::repeat(1)
        .take(PART as u64 + 1)
        .chain(PendingSource {
            dropped: dropped.clone(),
            polled: Arc::new(AtomicUsize::new(0)),
            notify: Some(tx),
        });
    let id = attempt_id();
    let p = policy();
    let mut transfer = Box::pin(transfer_upload(
        &setup.journal,
        &setup.qualified,
        &id,
        reader,
        &p,
        &clock,
    ));
    tokio::select! {
        biased;
        _ = rx => {},
        r = &mut transfer => panic!("unexpected completion: {r:?}"),
        _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("source did not become pending"),
    }
    drop(transfer);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(mutations(&setup.done()).len(), 2);
}

#[tokio::test]
async fn full_qualified_configuration_and_immutable_initial_binding_are_rechecked() {
    for mode in 0..6 {
        let setup = Setup::new(3, Some(3), false).await;
        let mut changed = setup.rows.clone();
        match mode {
            0 => text(&mut changed.namespace, "bucket", "another-bucket"),
            1 => text(&mut changed.namespace, "expected_owner", "999999999999"),
            2 => text(&mut changed.namespace, "region", "us-east-1"),
            3 => number(&mut changed.namespace, "mpu_lifecycle_margin_ms", 7200000),
            4 => {
                number(&mut changed.intent, "max_length", 4);
                number(&mut changed.attempt, "max_length", 4);
            }
            _ => number(&mut changed.attempt, "mpu_admitted_lower_bound", 901),
        }
        if mode >= 4 {
            setup.rows.enqueue(&setup.db);
        }
        changed.enqueue(&setup.db);
        assert!(matches!(
            setup.upload(b"abc".as_slice()).await,
            Err(TransferError::Binding)
        ));
        assert!(mutations(&setup.done()).is_empty());
    }
}

#[tokio::test]
async fn dispatch_resamples_local_clock_after_successful_native_admission() {
    let setup = Setup::new(0, None, false).await;
    setup.rows.enqueue(&setup.db);
    let phase = Arc::new(AtomicUsize::new(0));
    let p = phase.clone();
    setup.rows.enqueue_action(
        &setup.db,
        Some(Box::new(move || {
            p.store(1, Ordering::SeqCst);
        })),
    );
    let sample = || {
        let stage = phase.load(Ordering::SeqCst);
        let n = if stage == 0 {
            1000
        } else {
            // Native admission + timer scheduling succeed; the distinct actual
            // S3-dispatch sample must reject, not reuse either old observation.
            if phase.fetch_add(1, Ordering::SeqCst) >= 3 {
                5000
            } else {
                1000
            }
        };
        ClockObservation::trusted(ts(n), 0)
    };
    let result = transfer_upload(
        &setup.journal,
        &setup.qualified,
        &attempt_id(),
        b"".as_slice(),
        &policy(),
        &sample,
    )
    .await;
    assert!(matches!(result, Err(TransferError::Expired)));
    assert!(mutations(&setup.done()).is_empty());
}

#[tokio::test]
async fn response_after_deadline_is_only_candidate_not_live_authority() {
    let setup = Setup::new(0, None, false).await;
    setup.admissions(2);
    let now = Arc::new(AtomicU64::new(1000));
    let n = now.clone();
    let mut reply = Reply::version();
    reply.action = Some(Box::new(move || {
        n.store(6000, Ordering::SeqCst);
    }));
    setup.s3.push(reply);
    let sample = || ClockObservation::trusted(ts(now.load(Ordering::SeqCst)), 0);
    let c = transfer_upload(
        &setup.journal,
        &setup.qualified,
        &attempt_id(),
        b"".as_slice(),
        &policy(),
        &sample,
    )
    .await
    .unwrap();
    assert_eq!(c.version().version(), VERSION);
    assert_eq!(now.load(Ordering::SeqCst), 6000);
    setup.done();
}

#[tokio::test]
async fn native_retries_replay_put_and_part_bytes_and_ambiguous_complete_stays_unknown() {
    let setup = Setup::with_retries(3, Some(3), true, 2).await;
    setup.admissions(2);
    let mut retry = Reply::error("InternalError");
    retry.status = 500;
    setup.s3.push(retry);
    setup.s3.push(Reply::version());
    assert_eq!(setup.upload(b"abc".as_slice()).await.unwrap().length(), 3);
    let requests = setup.done();
    let writes = mutations(&requests);
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].payload(), b"abc");
    assert_eq!(writes[1].payload(), b"abc");

    let setup = Setup::with_retries(PART as u64 + 1, None, false, 2).await;
    setup.admissions(6);
    setup.s3.push(Reply::create());
    let mut retry = Reply::error("InternalError");
    retry.status = 500;
    setup.s3.push(retry);
    setup.s3.push(Reply::part(1));
    setup.s3.push(Reply::part(2));
    // First Complete may have committed despite a provider error. A native
    // retry's NoSuchUpload does not prove absence or justify a latest-version GET.
    let mut ambiguous = Reply::error("InternalError");
    ambiguous.status = 500;
    setup.s3.push(ambiguous);
    setup.s3.push(Reply::error("NoSuchUpload"));
    assert!(matches!(
        setup
            .upload(tokio::io::repeat(b'x').take(PART as u64 + 1))
            .await,
        Err(TransferError::OutcomeUnknown)
    ));
    let requests = setup.done();
    let writes = mutations(&requests);
    assert_eq!(writes.len(), 6);
    assert_eq!(writes[1].payload().len(), PART);
    assert_eq!(writes[1].payload(), writes[2].payload());
    assert_eq!(writes[1].line(), writes[2].line());
    assert_eq!(writes[4].payload(), writes[5].payload());
    assert_eq!(writes[4].line(), writes[5].line());
}

#[tokio::test]
async fn max_excess_after_first_part_and_late_part_failure_do_not_complete() {
    for excess in [false, true] {
        let setup = Setup::new(PART as u64 + 1, None, false).await;
        setup.admissions(if excess { 4 } else { 5 });
        setup.s3.push(Reply::create());
        setup.s3.push(Reply::part(1));
        if !excess {
            setup.s3.push(Reply::error("AccessDenied"));
        }
        let result = setup
            .upload(tokio::io::repeat(b'x').take(PART as u64 + if excess { 2 } else { 1 }))
            .await;
        if excess {
            assert!(matches!(result, Err(TransferError::Length)));
        } else {
            assert!(matches!(result, Err(TransferError::OutcomeUnknown)));
        }
        assert_eq!(mutations(&setup.done()).len(), if excess { 2 } else { 3 });
    }
}
