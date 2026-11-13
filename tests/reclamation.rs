//! Official SDK loopback / native action-shape tests with canned service replies.
//! NOT a DynamoDB expression engine, cloud acceptance or a complete GC proof.
#![cfg(feature = "dynamodb")]

use aws_sdk_dynamodb::config::{
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
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use storelink::{
    dynamodb::{CommandJournal, EnrolledTables, ReclamationError, VersionState},
    identity::{DomainId, StoreId, VersionRef},
};

const INC: &str = "11111111-1111-4111-8111-111111111111";
const OTHER_INC: &str = "99999999-9999-4999-8999-999999999999";
const CONFIG: &str = "22222222-2222-4222-8222-222222222222";
const CREATOR: &str = "33333333-3333-4333-8333-333333333333";
const TABLE: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
const KEY: &str = "managed/private-object";
const VERSION: &str = "version/+exact";

struct Reply {
    status: u16,
    headers: String,
    body: String,
    disconnect: bool,
}
impl Reply {
    fn body(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: body.into(),
            disconnect: false,
        }
    }
    fn json(value: Value) -> Self {
        Self::body(value.to_string())
    }
    fn item(value: Value) -> Self {
        Self::json(json!({"Item": value}))
    }
    fn lost() -> Self {
        Self {
            disconnect: true,
            ..Self::body("")
        }
    }
    fn conflict() -> Self {
        Self {
            status: 400,
            ..Self::json(
                json!({"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                "CancellationReasons":[{"Code":"None"},{"Code":"ConditionalCheckFailed"}]}),
            )
        }
    }
}
struct Fixture {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<String>>>,
    worker: Option<JoinHandle<()>>,
}
impl Fixture {
    fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let queue = Arc::new(Mutex::new(VecDeque::from(replies)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (stop, responses, captured) = (stopping.clone(), queue.clone(), requests.clone());
        let worker = std::thread::spawn(move || {
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    assert_eq!(stream.read(&mut byte).unwrap(), 1);
                    bytes.push(byte[0]);
                    assert!(bytes.len() < 32768);
                }
                let header = String::from_utf8(bytes.clone()).unwrap();
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(|n| n.parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                assert!(length < 65536);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).unwrap();
                bytes.extend(body);
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(bytes).unwrap());
                let reply = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected SDK request");
                if reply.disconnect {
                    continue;
                }
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
                    reply.status,
                    reply.body.len(),
                    reply.headers
                );
                stream.write_all(response.as_bytes()).unwrap();
                if !header.starts_with("HEAD ") {
                    stream.write_all(reply.body.as_bytes()).unwrap();
                }
            }
        });
        Self {
            address,
            stopping,
            replies: queue,
            requests,
            worker: Some(worker),
        }
    }
    fn push(&self, reply: Reply) {
        self.replies.lock().unwrap().push_back(reply);
    }
    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }
    fn db(&self) -> aws_sdk_dynamodb::Client {
        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(credentials())
                .endpoint_url(self.endpoint())
                // The terminal transaction MUST override these host retries.
                .retry_config(RetryConfig::standard().with_max_attempts(3))
                .timeout_config(timeouts())
                .build(),
        )
    }
    fn done(&self) -> Vec<String> {
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "unused canned responses"
        );
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        let result = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}
fn credentials() -> Credentials {
    Credentials::new("fixture-access", "fixture-secret", None, None, "loopback")
}
fn timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(3))
        .build()
}
fn domain() -> DomainId {
    DomainId::new("reclamation-domain").unwrap()
}
fn version() -> VersionRef {
    VersionRef::new(StoreId::new("store").unwrap(), KEY, VERSION).unwrap()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn prefix() -> String {
    format!("D#{}", hex(&Sha256::digest(domain().as_str().as_bytes())))
}
fn text(row: &mut Value, key: &str, value: &str) {
    row[key] = json!({"S": value});
}
fn number(row: &mut Value, key: &str, value: u64) {
    row[key] = json!({"N": value.to_string()});
}
fn version_row(state: &str, references: u64, claims: u64) -> Value {
    let mut row = json!({"pk":{"S":format!("{}#OBJ#{}", prefix(), hex(&version().fingerprint()))},"sk":{"S":"M"}});
    for (key, value) in [
        ("record", "version"),
        ("kind", "VERSION"),
        ("store", "store"),
        ("object_key", KEY),
        ("version_id", VERSION),
        ("state", state),
        ("namespace_id", "ns"),
        ("configuration_revision", CONFIG),
        ("creation_domain", domain().as_str()),
        ("creation_incarnation", INC),
        ("creation_uuid", CREATOR),
    ] {
        text(&mut row, key, value);
    }
    for (key, value) in [
        ("creation_generation", 1),
        ("references", references),
        ("claims", claims),
    ] {
        number(&mut row, key, value);
    }
    row
}
fn control() -> Value {
    let mut row = json!({"pk":{"S":format!("{}#CONTROL", prefix())},"sk":{"S":"M"}});
    text(&mut row, "domain", domain().as_str());
    text(&mut row, "incarnation", INC);
    // Fully closed/compacted creation epoch still permits terminal cleanup.
    for (key, value) in [
        ("current_generation", 3),
        ("closed_through", 2),
        ("compacted_through", 2),
        ("revision", 5),
    ] {
        number(&mut row, key, value);
    }
    row
}
async fn journal(db: &Fixture) -> CommandJournal {
    db.push(Reply::json(json!({"Table":{
        "TableName":"coord", "TableArn":TABLE, "TableStatus":"ACTIVE",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]
    }})));
    let client = db.db();
    let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
    CommandJournal::new(client, tables, domain())
}
fn payload(request: &str) -> Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}
fn transactions(requests: &[String]) -> Vec<Value> {
    requests
        .iter()
        .filter(|r| r.contains("DynamoDB_20120810.TransactWriteItems"))
        .map(|r| payload(r))
        .collect()
}
fn assert_strong_reads(requests: &[String]) {
    for request in requests
        .iter()
        .filter(|r| r.contains("DynamoDB_20120810.GetItem"))
    {
        assert_eq!(payload(request)["ConsistentRead"], true);
    }
}
fn assert_full_guard(action: &Value, old: &Value) {
    let expression = action["ConditionExpression"].as_str().unwrap();
    assert!(!expression.contains(" OR "));
    let names = action["ExpressionAttributeNames"].as_object().unwrap();
    let values = action["ExpressionAttributeValues"].as_object().unwrap();
    assert_eq!(values.len(), old.as_object().unwrap().len());
    for (field, expected) in old.as_object().unwrap() {
        let alias = names
            .iter()
            .find(|(_, name)| name.as_str() == Some(field))
            .unwrap()
            .0;
        let parameter = format!(":v{}", alias.strip_prefix("#f").unwrap());
        assert_eq!(&values[&parameter], expected);
        assert!(
            expression
                .split(" AND ")
                .any(|term| term == format!("{alias} = {parameter}"))
        );
    }
}
fn assert_transition(tx: &Value, fence: &Value, old: &Value, state: &str) {
    // The official SDK supplies its own native idempotency token. It is not a
    // StoreLink operation ID or receipt; do not replace SDK middleware to omit it.
    assert!(tx["ClientRequestToken"].as_str().is_some());
    let actions = tx["TransactItems"].as_array().unwrap();
    assert_eq!(
        actions.len(),
        2,
        "no new receipts/resources/counter changes"
    );
    assert_full_guard(&actions[0]["ConditionCheck"], fence);
    assert_full_guard(&actions[1]["Put"], old);
    assert_eq!(actions[1]["Put"]["TableName"], TABLE);
    let mut next = old.clone();
    text(&mut next, "state", state);
    assert_eq!(actions[1]["Put"]["Item"], next);
    for action in actions {
        assert!(action.get("Update").is_none());
    }
}

#[tokio::test]
async fn retirement_guards_full_control_and_old_version_without_receipts() {
    for rotated in [false, true] {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let mut row = version_row("READY", 0, 0);
        let mut fence = control();
        if rotated {
            number(&mut row, "creation_generation", 9);
            text(&mut fence, "incarnation", OTHER_INC);
            // Cross-incarnation generation order is NOT authority.
            number(&mut fence, "current_generation", 1);
            number(&mut fence, "closed_through", 0);
            number(&mut fence, "compacted_through", 0);
        }
        db.push(Reply::item(row.clone()));
        let observed = journal.read_version(&version()).await.unwrap().unwrap();
        db.push(Reply::item(fence.clone()));
        db.push(Reply::json(json!({})));
        journal.retire_unprotected_version(&observed).await.unwrap();
        let requests = db.done();
        assert_strong_reads(&requests);
        let tx = transactions(&requests);
        assert_eq!(tx.len(), 1);
        assert_transition(&tx[0], &fence, &row, "RETIRING");
    }
}

#[tokio::test]
async fn references_claims_terminal_states_missing_and_bad_control_fail_closed() {
    for (state, refs, claims) in [
        ("READY", 1, 0),
        ("READY", 0, 1),
        ("RETIRING", 0, 0),
        ("DELETED", 0, 0),
    ] {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        db.push(Reply::item(version_row(state, refs, claims)));
        let observed = journal.read_version(&version()).await.unwrap().unwrap();
        assert!(matches!(
            journal.retire_unprotected_version(&observed).await,
            Err(ReclamationError::Ineligible)
        ));
        assert!(transactions(&db.done()).is_empty());
    }
    let db = Fixture::new(vec![]);
    let missing_journal = journal(&db).await;
    db.push(Reply::json(json!({})));
    assert!(
        missing_journal
            .read_version(&version())
            .await
            .unwrap()
            .is_none()
    );
    assert!(transactions(&db.done()).is_empty());

    for bad in [
        "future",
        "nil-incarnation",
        "bad-incarnation",
        "watermark",
        "revision",
        "domain",
        "missing",
    ] {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let mut row = version_row("READY", 0, 0);
        let mut fence = control();
        match bad {
            "future" => number(&mut row, "creation_generation", 4),
            "nil-incarnation" => text(
                &mut fence,
                "incarnation",
                "00000000-0000-0000-0000-000000000000",
            ),
            "bad-incarnation" => text(&mut fence, "incarnation", "not-a-uuid"),
            "watermark" => number(&mut fence, "compacted_through", 4),
            "revision" => number(&mut fence, "revision", 0),
            "domain" => text(&mut fence, "domain", "other"),
            _ => {}
        }
        db.push(Reply::item(row));
        let observed = journal.read_version(&version()).await.unwrap().unwrap();
        db.push(if bad == "missing" {
            Reply::json(json!({}))
        } else {
            Reply::item(fence)
        });
        assert!(
            journal.retire_unprotected_version(&observed).await.is_err(),
            "{bad}"
        );
        assert!(transactions(&db.done()).is_empty());
    }
}

#[tokio::test]
async fn retirement_conflict_and_lost_reply_are_one_attempt_and_can_be_reread() {
    for lost in [false, true] {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let row = version_row("READY", 0, 0);
        db.push(Reply::item(row.clone()));
        let observed = journal.read_version(&version()).await.unwrap().unwrap();
        db.push(Reply::item(control()));
        db.push(if lost {
            Reply::lost()
        } else {
            Reply::conflict()
        });
        let error = journal
            .retire_unprotected_version(&observed)
            .await
            .unwrap_err();
        if lost {
            assert!(matches!(error, ReclamationError::OutcomeUnknown));
        } else {
            assert!(matches!(error, ReclamationError::Conflict));
        }
        // A stale snapshot never authorizes an unconditional upsert, including
        // when the native conditional failure was caused by a now-missing row.
        let requests = db.done();
        let tx = transactions(&requests);
        assert_eq!(tx.len(), 1, "SDK retries must be disabled for terminal CAS");
        assert_transition(&tx[0], &control(), &row, "RETIRING");
        db.push(Reply::item(version_row("RETIRING", 0, 0)));
        assert_eq!(
            journal
                .read_version(&version())
                .await
                .unwrap()
                .unwrap()
                .state(),
            VersionState::Retiring
        );
        db.done();
    }
}

#[cfg(feature = "s3")]
mod object {
    use super::*;
    use storelink::{
        identity::NamespaceId,
        s3::{
            DeletionError, DeletionOutcome, NamespaceConfiguration, NamespaceConfigurationSpec,
            QualifiedNamespace, delete_retiring_version, qualify_existing,
        },
    };

    fn configuration() -> NamespaceConfiguration {
        NamespaceConfiguration::new(NamespaceConfigurationSpec {
            id: NamespaceId::new("ns").unwrap(),
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
    fn namespace() -> Value {
        let mut row = json!({"pk":{"S":format!("{}#NS#6e73", prefix())},"sk":{"S":"M"}});
        for (key, value) in [
            ("record", "namespace"),
            ("domain", domain().as_str()),
            ("incarnation", INC),
            ("namespace_id", "ns"),
            ("configuration_revision", CONFIG),
            ("state", "ACTIVE"),
            ("store", "store"),
            ("bucket", "storelink-fixture"),
            ("expected_owner", "123456789012"),
            ("region", "us-west-2"),
            ("key_prefix", "managed/"),
            ("provider_profile", "aws-s3-versioned-lifecycle-v0"),
        ] {
            text(&mut row, key, value);
        }
        number(&mut row, "mpu_max_age_ms", 3600000);
        number(&mut row, "mpu_lifecycle_margin_ms", 3600000);
        row
    }
    async fn qualify(s3: &Fixture, attempts: u32) -> QualifiedNamespace {
        s3.push(Reply {
            headers: "x-amz-bucket-region: us-west-2\r\n".into(),
            ..Reply::body("")
        });
        s3.push(Reply::body(
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
        ));
        s3.push(Reply::body("<LifecycleConfiguration><Rule><ID>mpu</ID><Status>Enabled</Status><Filter><Prefix>managed/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"));
        s3.push(Reply {
            status: 404,
            ..Reply::body("<Error><Code>ObjectLockConfigurationNotFoundError</Code></Error>")
        });
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(credentials())
                .endpoint_url(s3.endpoint())
                .force_path_style(true)
                .retry_config(RetryConfig::standard().with_max_attempts(attempts))
                .timeout_config(timeouts())
                .build(),
        );
        qualify_existing(&client, &configuration()).await.unwrap()
    }
    fn delete_ack() -> Reply {
        Reply {
            status: 204,
            headers: format!("x-amz-version-id: {VERSION}\r\nx-amz-delete-marker: false\r\n"),
            ..Reply::body("")
        }
    }
    fn deletes(s3: &Fixture) -> Vec<String> {
        let requests = s3.done();
        let requests: Vec<_> = requests.into_iter().skip(4).collect();
        for request in &requests {
            let line = request.lines().next().unwrap();
            let parts: Vec<_> = line.split_whitespace().collect();
            assert_eq!(parts[0], "DELETE");
            let (path, query) = parts[1].split_once('?').unwrap();
            assert_eq!(path, "/storelink-fixture/managed/private-object");
            let parameters: Vec<_> = query.split('&').collect();
            assert!(
                parameters.contains(&"versionId=version%2F%2Bexact"),
                "{line}"
            );
            assert!(
                parameters
                    .iter()
                    .all(|p| matches!(*p, "versionId=version%2F%2Bexact" | "x-id=DeleteObject")),
                "{line}"
            );
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("\r\nx-amz-expected-bucket-owner: 123456789012\r\n")
            );
            assert!(!request.contains("uploadId="));
        }
        requests
    }
    fn enqueue_read(db: &Fixture, state: &str, ns: Value) {
        db.push(Reply::item(version_row(state, 0, 0)));
        db.push(Reply::item(ns));
    }
    fn enqueue_finish(db: &Fixture, fence: Value, reply: Reply) {
        db.push(Reply::item(fence));
        db.push(reply);
    }

    #[tokio::test]
    async fn exact_ack_marks_deleted_even_with_historical_creator_and_rotated_namespace() {
        for rotated in [false, true] {
            let db = Fixture::new(vec![]);
            let journal = journal(&db).await;
            let s3 = Fixture::new(vec![]);
            let qualified = qualify(&s3, 1).await;
            let mut ns = namespace();
            let mut fence = control();
            if rotated {
                text(&mut ns, "incarnation", OTHER_INC);
                text(&mut fence, "incarnation", OTHER_INC);
                number(&mut fence, "current_generation", 1);
                number(&mut fence, "closed_through", 0);
                number(&mut fence, "compacted_through", 0);
            }
            enqueue_read(&db, "RETIRING", ns);
            s3.push(delete_ack());
            enqueue_finish(&db, fence.clone(), Reply::json(json!({})));
            assert_eq!(
                delete_retiring_version(&journal, &qualified, &version())
                    .await
                    .unwrap(),
                DeletionOutcome::Deleted
            );
            assert_eq!(deletes(&s3).len(), 1);
            let requests = db.done();
            assert_strong_reads(&requests);
            let tx = transactions(&requests);
            assert_eq!(tx.len(), 1);
            assert_transition(&tx[0], &fence, &version_row("RETIRING", 0, 0), "DELETED");
        }
    }

    #[tokio::test]
    async fn missing_ready_protected_and_deleted_never_issue_s3_delete() {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let s3 = Fixture::new(vec![]);
        let qualified = qualify(&s3, 1).await;
        db.push(Reply::json(json!({})));
        assert!(matches!(
            delete_retiring_version(&journal, &qualified, &version()).await,
            Err(DeletionError::MissingVersion)
        ));
        for (state, refs, claims) in [("READY", 0, 0), ("RETIRING", 1, 0), ("RETIRING", 0, 1)] {
            db.push(Reply::item(version_row(state, refs, claims)));
            assert!(matches!(
                delete_retiring_version(&journal, &qualified, &version()).await,
                Err(DeletionError::Ineligible)
            ));
        }
        db.push(Reply::item(version_row("RETIRING", 0, 0)));
        db.push(Reply::json(json!({})));
        assert!(matches!(
            delete_retiring_version(&journal, &qualified, &version()).await,
            Err(DeletionError::MissingNamespace)
        ));
        enqueue_read(&db, "DELETED", namespace());
        assert_eq!(
            delete_retiring_version(&journal, &qualified, &version())
                .await
                .unwrap(),
            DeletionOutcome::AlreadyDeleted
        );
        assert!(deletes(&s3).is_empty());
        assert!(transactions(&db.done()).is_empty());
    }

    #[tokio::test]
    async fn same_store_does_not_authorize_wrong_namespace_or_any_rebinding() {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let s3 = Fixture::new(vec![]);
        let qualified = qualify(&s3, 1).await;
        for field in [
            "namespace_id",
            "configuration_revision",
            "bucket",
            "expected_owner",
            "region",
            "key_prefix",
            "mpu_max_age_ms",
            "mpu_lifecycle_margin_ms",
            "store",
            "provider_profile",
        ] {
            let mut ns = namespace();
            let mut row = version_row("RETIRING", 0, 0);
            match field {
                "namespace_id" => {
                    text(&mut row, field, "other");
                    text(&mut ns, field, "other");
                    text(&mut ns, "pk", &format!("{}#NS#6f74686572", prefix()));
                }
                "configuration_revision" => text(&mut ns, field, OTHER_INC),
                "bucket" => text(&mut ns, field, "other-fixture"),
                "expected_owner" => text(&mut ns, field, "987654321012"),
                "region" => text(&mut ns, field, "us-east-1"),
                "key_prefix" => text(&mut ns, field, "managed/private-"),
                "mpu_max_age_ms" | "mpu_lifecycle_margin_ms" => number(&mut ns, field, 3600001),
                _ => text(&mut ns, field, "other"),
            }
            db.push(Reply::item(row));
            db.push(Reply::item(ns));
            assert!(
                delete_retiring_version(&journal, &qualified, &version())
                    .await
                    .is_err(),
                "{field}"
            );
        }
        assert!(deletes(&s3).is_empty());
        assert!(transactions(&db.done()).is_empty());
    }

    #[tokio::test]
    async fn s3_denial_missing_bucket_ambiguous_or_inconsistent_ack_never_finish() {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let s3 = Fixture::new(vec![]);
        let qualified = qualify(&s3, 1).await;
        let replies = vec![
            Reply {
                status: 403,
                ..Reply::body("<Error><Code>AccessDenied</Code></Error>")
            },
            Reply {
                status: 404,
                ..Reply::body("<Error><Code>NoSuchBucket</Code></Error>")
            },
            Reply {
                status: 500,
                ..Reply::body("<Error><Code>InternalError</Code></Error>")
            },
            Reply::lost(),
            Reply {
                headers: "x-amz-version-id: wrong\r\n".into(),
                ..delete_ack()
            },
            Reply {
                headers: format!("x-amz-version-id: {VERSION}\r\nx-amz-delete-marker: true\r\n"),
                ..delete_ack()
            },
        ];
        let count = replies.len();
        for reply in replies {
            enqueue_read(&db, "RETIRING", namespace());
            s3.push(reply);
            assert!(matches!(
                delete_retiring_version(&journal, &qualified, &version()).await,
                Err(DeletionError::OutcomeUnknown)
            ));
        }
        assert_eq!(deletes(&s3).len(), count);
        assert!(transactions(&db.done()).is_empty());
    }

    #[tokio::test]
    async fn native_sdk_ambiguous_delete_retries_the_same_exact_identity() {
        let db = Fixture::new(vec![]);
        let journal = journal(&db).await;
        let s3 = Fixture::new(vec![]);
        let qualified = qualify(&s3, 2).await;
        enqueue_read(&db, "RETIRING", namespace());
        s3.push(Reply::lost());
        s3.push(delete_ack());
        enqueue_finish(&db, control(), Reply::json(json!({})));
        assert_eq!(
            delete_retiring_version(&journal, &qualified, &version())
                .await
                .unwrap(),
            DeletionOutcome::Deleted
        );
        let requests = deletes(&s3);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].lines().next(), requests[1].lines().next());
        assert_eq!(transactions(&db.done()).len(), 1);
    }

    #[tokio::test]
    async fn conflict_or_unknown_after_ack_is_safe_to_reread_and_exactly_retry() {
        for lost in [false, true] {
            let db = Fixture::new(vec![]);
            let journal = journal(&db).await;
            let s3 = Fixture::new(vec![]);
            let qualified = qualify(&s3, 1).await;
            enqueue_read(&db, "RETIRING", namespace());
            s3.push(delete_ack());
            enqueue_finish(
                &db,
                control(),
                if lost {
                    Reply::lost()
                } else {
                    Reply::conflict()
                },
            );
            let error = delete_retiring_version(&journal, &qualified, &version())
                .await
                .unwrap_err();
            if lost {
                assert!(matches!(
                    error,
                    DeletionError::MetadataAfterDelete(ReclamationError::OutcomeUnknown)
                ));
            } else {
                assert!(matches!(
                    error,
                    DeletionError::MetadataAfterDelete(ReclamationError::Conflict)
                ));
            }
            assert_eq!(transactions(&db.done()).len(), 1);
            // A retry against an already physically deleted exact ID is harmless;
            // an empty native success acknowledgement still permits terminal CAS.
            enqueue_read(&db, "RETIRING", namespace());
            s3.push(Reply {
                status: 204,
                ..Reply::body("")
            });
            enqueue_finish(&db, control(), Reply::json(json!({})));
            assert_eq!(
                delete_retiring_version(&journal, &qualified, &version())
                    .await
                    .unwrap(),
                DeletionOutcome::Deleted
            );
            enqueue_read(&db, "DELETED", namespace());
            assert_eq!(
                delete_retiring_version(&journal, &qualified, &version())
                    .await
                    .unwrap(),
                DeletionOutcome::AlreadyDeleted
            );
            assert_eq!(deletes(&s3).len(), 2);
            assert_eq!(transactions(&db.done()).len(), 2);
        }
    }
}
