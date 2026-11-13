//! Native SDK loopback/canned responses only. No cloud, Dynamo expression
//! evaluator, default credentials, writes, or assertion of retirement authority.
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
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use storelink::{
    dynamodb::{CommandJournal, EnrolledTables},
    identity::{DomainId, NamespaceId, StoreId, VersionRef},
    s3::{
        NamespaceConfiguration, NamespaceConfigurationSpec, OrphanAttribution,
        OrphanObservationError, QualifiedNamespace, list_version_page, observe_orphan_object,
        qualify_existing,
    },
};

const INC: &str = "11111111-1111-4111-8111-111111111111";
const CONFIG: &str = "22222222-2222-4222-8222-222222222222";
const INTENT: &str = "33333333-3333-4333-8333-333333333333";
const ATTEMPT: &str = "44444444-4444-4444-8444-444444444444";
const KEY: &str = "managed/private-object";
const VERSION: &str = "version/+exact";

struct Reply {
    status: u16,
    headers: String,
    body: String,
    length: Option<i64>,
}
impl Reply {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: body.into(),
            length: None,
        }
    }
    fn json(value: Value) -> Self {
        Self::ok(value.to_string())
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
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                    assert!(request.len() < 32768);
                }
                let headers = String::from_utf8(request.clone()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(|v| v.parse::<usize>().unwrap())
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
                let head = headers.starts_with("HEAD ");
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                    reply.status,
                    reply
                        .length
                        .unwrap_or(if head { 0 } else { reply.body.len() as i64 }),
                    reply.headers,
                    if head { "" } else { &reply.body }
                );
                let _ = socket.write_all(response.as_bytes());
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
    fn db(&self) -> aws_sdk_dynamodb::Client {
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
    fn done(&self, s3: bool) -> Vec<String> {
        assert!(self.replies.lock().unwrap().is_empty());
        let requests = self.requests.lock().unwrap().clone();
        for r in &requests {
            assert!(r.contains("Credential=fixture-access/"));
            if s3 {
                assert!(r.starts_with("HEAD ") || r.starts_with("GET "));
                assert!(
                    r.to_ascii_lowercase()
                        .contains("\r\nx-amz-expected-bucket-owner: 123456789012\r\n")
                );
                assert!(!r.contains("intelligent-tiering"));
            } else {
                let target = r
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("x-amz-target:"))
                    .unwrap();
                assert!(target.ends_with("DescribeTable") || target.ends_with("GetItem"));
                if target.ends_with("GetItem") {
                    assert!(r.contains("\"ConsistentRead\":true"));
                }
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
        .operation_timeout(Duration::from_secs(3))
        .build()
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
    let mut head = Reply::ok("");
    head.headers = "x-amz-bucket-region: us-west-2\r\n".into();
    let mut lock = Reply::ok("<Error><Code>ObjectLockConfigurationNotFoundError</Code></Error>");
    lock.status = 404;
    vec![
        head,
        Reply::ok("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
        Reply::ok(
            "<LifecycleConfiguration><Rule><ID>mpu</ID><Status>Enabled</Status><Filter><Prefix>managed/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
        ),
        lock,
    ]
}
fn namespace() -> Value {
    let domain = "domain-é";
    let mut row = json!({"pk":{"S":format!("D#{:x}#NS#6e732dc3a9", Sha256::digest(domain.as_bytes()))},"sk":{"S":"M"}});
    for (k, v) in [
        ("record", "namespace"),
        ("domain", domain),
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
        row[k] = json!({"S":v});
    }
    for k in ["mpu_max_age_ms", "mpu_lifecycle_margin_ms"] {
        row[k] = json!({"N":"3600000"});
    }
    row
}
fn metadata() -> HashMap<String, String> {
    let mut hash = Sha256::new();
    for p in ["store", KEY] {
        hash.update((p.len() as u64).to_be_bytes());
        hash.update(p.as_bytes());
    }
    [
        ("schema", "1".into()),
        ("domain", "646f6d61696e2dc3a9".into()),
        ("namespace", "6e732dc3a9".into()),
        ("incarnation", INC.into()),
        ("configuration-revision", CONFIG.into()),
        ("intent-generation", "1".into()),
        ("intent-uuid", INTENT.into()),
        ("attempt-generation", "2".into()),
        ("attempt-uuid", ATTEMPT.into()),
        ("target-sha256", format!("{:x}", hash.finalize())),
        ("constraints-sha256", "a".repeat(64)),
    ]
    .into_iter()
    .map(|(k, v)| (format!("storelink-{k}"), v))
    .collect()
}
fn head(m: &HashMap<String, String>) -> Reply {
    let mut r = Reply::ok("");
    r.length = Some(3);
    r.headers = format!(
        "x-amz-version-id: {VERSION}\r\nLast-Modified: Tue, 14 Nov 2023 22:13:20 GMT\r\nx-amz-storage-class: INTELLIGENT_TIERING\r\n"
    );
    for (k, v) in m {
        r.headers += &format!("x-amz-meta-{k}: {v}\r\n");
    }
    r
}
fn candidate() -> VersionRef {
    VersionRef::new(StoreId::new("store").unwrap(), KEY, VERSION).unwrap()
}
struct Setup {
    s3: Fixture,
    db: Fixture,
    qualified: QualifiedNamespace,
    journal: CommandJournal,
}
impl Setup {
    async fn new() -> Self {
        let s3 = Fixture::new(profile());
        let qualified = qualify_existing(&s3.s3(), &configuration("ns-é"))
            .await
            .unwrap();
        let db = Fixture::new(vec![Reply::json(
            json!({"Table":{"TableName":"coord","TableArn":"arn:aws:dynamodb:us-west-2:123456789012:table/coord","TableStatus":"ACTIVE","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}),
        )]);
        let client = db.db();
        let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
        let journal = CommandJournal::new(client, tables, DomainId::new("domain-é").unwrap());
        Self {
            s3,
            db,
            qualified,
            journal,
        }
    }
    async fn observe(
        &self,
        r: Reply,
    ) -> Result<storelink::s3::ObservedOrphanObject, OrphanObservationError> {
        self.db.push(Reply::json(json!({"Item":namespace()})));
        self.s3.push(r);
        observe_orphan_object(&self.journal, &self.qualified, &candidate()).await
    }
    fn done(&self) {
        self.db.done(false);
        self.s3.done(true);
    }
}

#[tokio::test]
async fn exact_observation_is_read_only_and_exposes_checked_provenance() {
    let s = Setup::new().await;
    let o = s.observe(head(&metadata())).await.unwrap();
    assert_eq!(o.namespace().id().as_str(), "ns-é");
    assert_eq!(o.version(), &candidate());
    assert_eq!(o.length(), 3);
    assert_eq!(o.last_modified().as_millis(), 1700000000000);
    let OrphanAttribution::Known(k) = o.attribution() else {
        panic!("known schema")
    };
    assert_eq!(k.intent_id().uuid().to_string(), INTENT);
    assert_eq!(k.attempt_id().uuid().to_string(), ATTEMPT);
    assert_eq!(k.intent_id().scope().generation(), 1);
    assert_eq!(k.attempt_id().scope().generation(), 2);
    assert_eq!(k.constraints_sha256(), &[0xaa; 32]);
    let requests = s.s3.done(true);
    assert!(requests.last().unwrap().starts_with(
        "HEAD /storelink-fixture/managed/private-object?versionId=version%2F%2Bexact "
    ));
    s.done();
}

#[tokio::test]
async fn missing_and_malformed_known_metadata_are_not_authority() {
    let s = Setup::new().await;
    assert!(matches!(
        s.observe(head(&HashMap::new()))
            .await
            .unwrap()
            .attribution(),
        OrphanAttribution::Unattributed(_)
    ));
    for (field, value) in [
        ("intent-generation", "01"),
        ("attempt-generation", "0"),
        ("attempt-generation", "18446744073709551616"),
        ("intent-uuid", "33333333333343338333333333333333"),
        ("attempt-uuid", "00000000-0000-0000-0000-000000000000"),
        ("constraints-sha256", "ABC"),
        ("domain", "ff"),
        ("namespace", "6e732dC3a9"),
    ] {
        let mut m = metadata();
        m.insert(format!("storelink-{field}"), value.into());
        assert!(
            matches!(
                s.observe(head(&m)).await.unwrap().attribution(),
                OrphanAttribution::Unattributed(_)
            ),
            "{field}"
        );
    }
    let mut m = metadata();
    m.insert("storelink-intent-generation".into(), "3".into());
    assert!(matches!(
        s.observe(head(&m)).await.unwrap().attribution(),
        OrphanAttribution::Unattributed(_)
    ));
    for field in [
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
    ] {
        let mut m = metadata();
        m.remove(&format!("storelink-{field}"));
        assert!(matches!(
            s.observe(head(&m)).await.unwrap().attribution(),
            OrphanAttribution::Unattributed(_)
        ));
    }
    s.done();
}

#[tokio::test]
async fn future_foreign_and_wrong_target_are_conflicts_even_with_missing_fields() {
    let s = Setup::new().await;
    for (field, value) in [
        ("schema", "2"),
        ("schema", "garbage"),
        ("future-field", "x"),
        ("domain", "666f726569676e"),
        ("namespace", "666f726569676e"),
        ("configuration-revision", INC),
        ("target-sha256", &"b".repeat(64)),
    ] {
        for incomplete in [false, true] {
            let mut m = metadata();
            m.insert(format!("storelink-{field}"), value.into());
            if incomplete {
                m.remove("storelink-attempt-uuid");
            }
            assert!(
                matches!(
                    s.observe(head(&m)).await,
                    Err(OrphanObservationError::AttributionConflict)
                ),
                "{field}"
            );
        }
    }
    let mut m = metadata();
    m.insert("unrelated".into(), "x".repeat(2048));
    assert!(matches!(
        s.observe(head(&m)).await,
        Err(OrphanObservationError::Metadata)
    ));
    s.done();
}

#[tokio::test]
async fn native_head_errors_never_imply_absence_or_valid_object() {
    let s = Setup::new().await;
    for change in [0, 1, 2, 3, 4, 5, 6, 7, 8] {
        let mut r = head(&metadata());
        match change {
            0 => r.headers = r.headers.replace(VERSION, "null"),
            1 => r.headers = r.headers.replace(VERSION, "other"),
            2 => {
                r.headers = r
                    .headers
                    .replace(&format!("x-amz-version-id: {VERSION}\r\n"), "")
            }
            3 => r.headers += "x-amz-delete-marker: true\r\n",
            4 => {
                r.headers = r
                    .headers
                    .replace("Last-Modified: Tue, 14 Nov 2023 22:13:20 GMT\r\n", "")
            }
            5 => {
                r.headers = r.headers.replace(
                    "Tue, 14 Nov 2023 22:13:20 GMT",
                    "Wed, 31 Dec 1969 23:59:59 GMT",
                )
            }
            6 => r.headers += "x-amz-missing-meta: 1\r\n",
            7 => r.status = 404,
            8 => r.status = 403,
            _ => unreachable!(),
        }
        assert!(s.observe(r).await.is_err(), "case {change}");
    }
    s.done();
}

#[tokio::test]
async fn checked_namespace_and_full_config_bind_before_head() {
    let s = Setup::new().await;
    for field in [
        "bucket",
        "expected_owner",
        "region",
        "key_prefix",
        "store",
        "mpu_max_age_ms",
        "mpu_lifecycle_margin_ms",
    ] {
        let mut ns = namespace();
        if field.starts_with("mpu_") {
            ns[field] = json!({"N":"3600001"});
        } else {
            ns[field] = json!({"S": match field { "bucket"=>"different-bucket", "expected_owner"=>"999999999999", "region"=>"us-east-1", "key_prefix"=>"other/", _=>"different-store" }});
        }
        s.db.push(Reply::json(json!({"Item":ns})));
        assert!(
            observe_orphan_object(&s.journal, &s.qualified, &candidate())
                .await
                .is_err()
        );
    }
    s.db.push(Reply::json(json!({})));
    assert!(
        observe_orphan_object(&s.journal, &s.qualified, &candidate())
            .await
            .is_err()
    );
    for (store, key) in [("store", "unmanaged/key"), ("other", "managed/key")] {
        s.db.push(Reply::json(json!({"Item":namespace()})));
        let v = VersionRef::new(StoreId::new(store).unwrap(), key, VERSION).unwrap();
        assert!(
            observe_orphan_object(&s.journal, &s.qualified, &v)
                .await
                .is_err()
        );
    }
    assert_eq!(s.s3.done(true).len(), 4);
    s.done();
}

fn entry(marker: bool, key: &str, version: &str) -> String {
    let tag = if marker { "DeleteMarker" } else { "Version" };
    format!(
        "<{tag}><Key>{key}</Key><VersionId>{version}</VersionId><IsLatest>false</IsLatest><LastModified>2023-11-14T22:13:20.000Z</LastModified>{}</{tag}>",
        if marker {
            ""
        } else {
            "<Size>3</Size><StorageClass>INTELLIGENT_TIERING</StorageClass>"
        }
    )
}
fn page(contents: &str, truncated: &str, cursor: &str) -> String {
    format!(
        "<ListVersionsResult><Name>storelink-fixture</Name><Prefix>managed%2F</Prefix><EncodingType>url</EncodingType><MaxKeys>2</MaxKeys><IsTruncated>{truncated}</IsTruncated>{cursor}{contents}</ListVersionsResult>"
    )
}
#[tokio::test]
async fn historical_attribution_and_native_terminal_markers_are_preserved() {
    let setup = Setup::new().await;
    let historical = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    let mut m = metadata();
    m.insert("storelink-incarnation".into(), historical.into());
    let observed = setup.observe(head(&m)).await.unwrap();
    let OrphanAttribution::Known(known) = observed.attribution() else {
        panic!("historical attribution discarded")
    };
    assert_eq!(
        known.intent_id().scope().incarnation().to_string(),
        historical
    );
    assert_eq!(
        known.attempt_id().scope().incarnation().to_string(),
        historical
    );
    setup.done();
    let f = Fixture::new(profile());
    let q = qualify_existing(&f.s3(), &configuration("ns-é"))
        .await
        .unwrap();
    for markers in [
        "",
        "<NextKeyMarker></NextKeyMarker><NextVersionIdMarker></NextVersionIdMarker>",
        "<NextKeyMarker>managed%2Fnext</NextKeyMarker><NextVersionIdMarker>not-returned</NextVersionIdMarker>",
    ] {
        f.push(Reply::ok(page("", "false", markers)));
        assert!(
            list_version_page(&q, None, 2)
                .await
                .unwrap()
                .next_cursor()
                .is_none()
        );
    }
    f.push(Reply::ok(page(&entry(false, "managed%2Fa", "returned"), "true",
        "<NextKeyMarker>managed%2Fb</NextKeyMarker><NextVersionIdMarker>not-returned</NextVersionIdMarker>")));
    let p = list_version_page(&q, None, 2).await.unwrap();
    assert_eq!(p.next_cursor().unwrap().key_marker(), "managed/b");
    let debug = format!("{p:?}");
    for private_value in [
        q.configuration().bucket(),
        q.configuration().expected_owner(),
        "managed/b",
        "not-returned",
    ] {
        assert!(!debug.contains(private_value));
    }
    f.done(true);
}

#[tokio::test]
async fn one_native_page_keeps_markers_separate_and_checks_bound_progress() {
    let f = Fixture::new(profile());
    let q = qualify_existing(&f.s3(), &configuration("ns-é"))
        .await
        .unwrap();
    let content = entry(false, "managed%2Fa+%20%C3%A9", "v1");
    let marker = entry(true, "managed%2Fz", "m1");
    let next =
        "<NextKeyMarker>managed%2Fz</NextKeyMarker><NextVersionIdMarker>m1</NextVersionIdMarker>";
    f.push(Reply::ok(page(&(content + &marker), "true", next)));
    let first = list_version_page(&q, None, 2).await.unwrap();
    assert_eq!(first.versions()[0].key(), "managed/a+ é");
    assert_eq!(first.delete_markers()[0].version(), "m1");
    let cursor = first.next_cursor().unwrap();
    assert_eq!(cursor.key_marker(), "managed/z");
    for reply in [
        page(
            "",
            "true",
            "<KeyMarker>managed%2Fz</KeyMarker><VersionIdMarker>m1</VersionIdMarker><NextKeyMarker>managed%2Fz</NextKeyMarker><NextVersionIdMarker>m1</NextVersionIdMarker>",
        ),
        page("", "false", ""),
    ] {
        f.push(Reply::ok(reply));
        assert!(list_version_page(&q, Some(cursor), 2).await.is_err());
    }
    f.push(Reply::ok(page(
        "",
        "false",
        "<KeyMarker>managed%2Fz</KeyMarker><VersionIdMarker>m1</VersionIdMarker>",
    )));
    assert!(
        list_version_page(&q, Some(cursor), 2)
            .await
            .unwrap()
            .next_cursor()
            .is_none()
    );
    let other = Fixture::new(profile());
    let other_q = qualify_existing(&other.s3(), &configuration("other-ns"))
        .await
        .unwrap();
    assert!(matches!(
        list_version_page(&other_q, Some(cursor), 2).await,
        Err(OrphanObservationError::Binding)
    ));
    assert_eq!(other.done(true).len(), 4);
    for limit in [0, 1001] {
        assert!(matches!(
            list_version_page(&q, None, limit).await,
            Err(OrphanObservationError::Limit)
        ));
    }
    let requests = f.done(true);
    assert_eq!(requests.len(), 8);
    for r in &requests[4..] {
        assert!(r.starts_with("GET "));
        assert!(r.contains("versions"));
        assert!(r.contains("prefix=managed%2F"));
        assert!(r.contains("max-keys=2"));
    }
    assert!(requests[5].contains("key-marker=managed%2Fz"));
    assert!(requests[5].contains("version-id-marker=m1"));
}

#[tokio::test]
async fn malformed_listing_fields_are_not_an_empty_success() {
    let f = Fixture::new(profile());
    let q = qualify_existing(&f.s3(), &configuration("ns-é"))
        .await
        .unwrap();
    let valid = page(&entry(false, "managed%2Fa", "v1"), "false", "");
    let cases = vec![
        valid.replace("<IsTruncated>false</IsTruncated>", ""),
        valid.replace(
            "<IsTruncated>false</IsTruncated>",
            "<IsTruncated>true</IsTruncated>",
        ),
        valid.replace(
            "<Name>storelink-fixture</Name>",
            "<Name>other-bucket</Name>",
        ),
        valid.replace("<Prefix>managed%2F</Prefix>", "<Prefix>other%2F</Prefix>"),
        valid.replace("<EncodingType>url</EncodingType>", ""),
        valid.replace("<MaxKeys>2</MaxKeys>", "<MaxKeys>1000</MaxKeys>"),
        valid.replace("managed%2Fa", "outside%2Fa"),
        valid.replace("managed%2Fa", "managed%GG"),
        valid.replace("<VersionId>v1</VersionId>", "<VersionId>null</VersionId>"),
        valid.replace("<VersionId>v1</VersionId>", ""),
        valid.replace("<Size>3</Size>", "<Size>-1</Size>"),
        valid.replace("<Size>3</Size>", ""),
        valid.replace("<LastModified>2023-11-14T22:13:20.000Z</LastModified>", ""),
        valid.replace("<IsLatest>false</IsLatest>", ""),
        page(
            &(entry(false, "managed%2Fa", "v1") + &entry(true, "managed%2Fa", "v1")),
            "false",
            "",
        ),
        page(
            &(entry(false, "managed%2Fa", "v1")
                + &entry(false, "managed%2Fa", "v2")
                + &entry(false, "managed%2Fa", "v3")),
            "false",
            "",
        ),
        page(
            "",
            "true",
            "<NextKeyMarker>outside%2Fa</NextKeyMarker><NextVersionIdMarker>missing</NextVersionIdMarker>",
        ),
    ];
    for (i, body) in cases.into_iter().enumerate() {
        f.push(Reply::ok(body));
        assert!(list_version_page(&q, None, 2).await.is_err(), "case {i}");
    }
    f.done(true);
}
