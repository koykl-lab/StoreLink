//! Real SDK + scripted loopback HTTP, not an expression VM or native-cloud proof.
use super::*;
use crate::dynamodb::{EnrolledTables, MetadataDiscoveryItem};
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
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

const TABLE: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
fn domain() -> DomainId {
    DomainId::new("progress-fixture").unwrap()
}
fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
fn control() -> Item {
    let mut item = control_key(&domain());
    item.extend([
        ("domain".into(), s(domain().as_str())),
        ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
        ("current_generation".into(), n(4)),
        ("closed_through".into(), n(3)),
        ("compacted_through".into(), n(1)),
        ("revision".into(), n(7)),
        ("resolution_retention_ms".into(), n(100)),
        ("compaction_candidate".into(), n(2)),
        ("compaction_not_before_ms".into(), n(500)),
    ]);
    item
}
fn marker() -> Item {
    Item::from([
        ("pk".into(), s("another-domain-native-pk")),
        ("sk".into(), s("not-M")),
    ])
}
fn wire(item: &Item) -> Value {
    Value::Object(
        item.iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    match v {
                        AttributeValue::S(v) => json!({"S":v}),
                        AttributeValue::N(v) => json!({"N":v}),
                        _ => panic!("fixture type"),
                    },
                )
            })
            .collect(),
    )
}
fn unmarshal(value: &Value) -> Item {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                if let Some(v) = v.get("S") {
                    s(v.as_str().unwrap())
                } else {
                    AttributeValue::N(v["N"].as_str().unwrap().into())
                },
            )
        })
        .collect()
}
fn row(journal: &CommandJournal, cursor: Option<MetadataCursor>) -> Item {
    encode(
        journal,
        &Progress {
            incarnation: Uuid::from_u128(1),
            cursor,
            completed_passes: 3,
            last_completion: Some(TimestampMs::new(200).unwrap()),
        },
    )
    .unwrap()
}

struct Native {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    worker: Option<JoinHandle<()>>,
}
impl Native {
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
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let target = headers
                    .lines()
                    .find_map(|l| l.strip_prefix("x-amz-target: "))
                    .unwrap()
                    .to_owned();
                assert!(length < 1_000_000);
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
                    .unwrap_or((500, json!({"__type":"InternalServerError"})));
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
        self.journal_for(TABLE, domain()).await
    }
    async fn journal_for(&self, table: &str, domain: DomainId) -> CommandJournal {
        self.reply(json!({"Table":{"TableName":table.rsplit('/').next().unwrap(),"TableArn":table,"TableStatus":"ACTIVE","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(Credentials::new(
                    "fixture", "fixture", None, None, "fixture",
                ))
                .endpoint_url(format!("http://{}", self.address))
                // Prove operation-scoped retry disabling even for a retrying client.
                .retry_config(RetryConfig::standard().with_max_attempts(3))
                .build(),
        );
        let tables = EnrolledTables::enroll(&client, table, &[]).await.unwrap();
        CommandJournal::new(client, tables, domain)
    }
    async fn load(
        &self,
        journal: &CommandJournal,
        control: &Item,
        progress: Option<&Item>,
    ) -> MetadataProgressObservation {
        self.reply(json!({"Responses":[{"Item":wire(control)}, progress.map(|p| json!({"Item":wire(p)})).unwrap_or(json!({}))]}));
        journal.load_metadata_progress().await.unwrap()
    }
    async fn page(
        &self,
        journal: &CommandJournal,
        cursor: Option<&MetadataCursor>,
        next: Option<Item>,
    ) -> MetadataDiscoveryPage {
        let mut output = json!({"Items":[], "Count":0, "ScannedCount":1});
        if let Some(next) = next {
            output["LastEvaluatedKey"] = wire(&next);
        }
        self.reply(output);
        journal.discover_metadata_page(cursor, 1).await.unwrap()
    }
    fn requests(&self, op: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(target, _)| target.ends_with(op))
            .map(|(_, body)| body.clone())
            .collect()
    }
    fn last_put(&self) -> Item {
        unmarshal(
            &self.requests("transactwriteitems").last().unwrap()["TransactItems"][1]["Put"]["Item"],
        )
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        self.worker.take().unwrap().join().unwrap();
    }
}

// Inspect native request construction only; never evaluate DynamoDB expressions.
fn assert_guard(action: &Value, old: &Item, fields: &[&str]) {
    let names = action["ExpressionAttributeNames"].as_object().unwrap();
    let values = action["ExpressionAttributeValues"].as_object().unwrap();
    let expression = action["ConditionExpression"].as_str().unwrap();
    for field in fields.iter().filter(|f| !["pk", "sk"].contains(f)) {
        let alias = names
            .iter()
            .find(|(_, v)| v.as_str() == Some(field))
            .unwrap()
            .0;
        if let Some(value) = old.get(*field) {
            let encoded = wire(&Item::from([("v".into(), value.clone())]));
            assert!(
                values.iter().any(|(v_alias, v)| v == &encoded["v"]
                    && expression.contains(&format!("{alias} = {v_alias}"))),
                "missing equality for {field}"
            );
        } else {
            assert!(
                expression.contains(&format!("attribute_not_exists({alias})")),
                "missing absence for {field}"
            );
        }
    }
}

#[tokio::test]
async fn initial_creation_empty_filtered_continuation_and_terminal_reset() {
    let native = Native::new();
    let journal = native.journal().await;
    let observation = native.load(&journal, &control(), None).await;
    assert_eq!(observation.cursor().unwrap(), None);
    assert_eq!(observation.completed_passes(), 0);
    assert_eq!(observation.last_completion(), None);
    let page = native.page(&journal, None, Some(marker())).await;
    assert!(page.items().is_empty());
    native.reply(json!({}));
    journal
        .checkpoint_considered_metadata_page(
            observation,
            page,
            Some(TimestampMs::new(100).unwrap()),
        )
        .await
        .unwrap();
    let first = native.last_put();
    assert_eq!(first["cursor_pk"], marker()["pk"]);
    assert_eq!(first["cursor_sk"], marker()["sk"]);
    assert_eq!(first["completed_passes"], n(0));
    assert!(!first.contains_key("last_completion_ms"));
    assert!(!first.contains_key("table"));
    assert!(!first.contains_key("cursor"));
    let requests = native.requests("transactwriteitems");
    let actions = requests[0]["TransactItems"].as_array().unwrap();
    assert_eq!(actions.len(), 2);
    assert!(actions[0].get("ConditionCheck").is_some());
    assert!(actions[1].get("Put").is_some());
    assert_eq!(
        actions[1]["Put"]["ConditionExpression"],
        "attribute_not_exists(#pk)"
    );
    assert_eq!(
        actions[0]["ConditionCheck"]["Key"],
        wire(&control_key(&domain()))
    );
    assert_guard(&actions[0]["ConditionCheck"], &control(), &CONTROL_FIELDS);
    assert!(requests[0].to_string().find("#CMD#").is_none());
    let gets = native.requests("transactgetitems");
    assert_eq!(gets[0]["TransactItems"].as_array().unwrap().len(), 2);
    for (index, expected) in [control_key(&domain()), key(&domain())].iter().enumerate() {
        let get = &gets[0]["TransactItems"][index]["Get"];
        assert_eq!(get["Key"], wire(expected));
        assert_eq!(get["TableName"], TABLE);
        assert!(get.get("ProjectionExpression").is_none());
    }
    let observation = native.load(&journal, &control(), Some(&first)).await;
    let decoded = observation.cursor().unwrap().unwrap();
    assert_eq!(decoded.key(&journal).unwrap(), marker());
    let page = native.page(&journal, Some(decoded), None).await;
    native.reply(json!({}));
    journal
        .checkpoint_considered_metadata_page(
            observation,
            page,
            Some(TimestampMs::new(300).unwrap()),
        )
        .await
        .unwrap();
    let terminal = native.last_put();
    assert_eq!(terminal["completed_passes"], n(1));
    assert_eq!(terminal["last_completion_ms"], n(300));
    assert!(!terminal.contains_key("cursor_pk"));
    assert!(!terminal.contains_key("cursor_sk"));
    assert_ne!(terminal["revision"], first["revision"]);
    assert_eq!(terminal["pk"], first["pk"]);
    assert_guard(
        &native.requests("transactwriteitems")[1]["TransactItems"][1]["Put"],
        &first,
        &FIELDS,
    );
    // A new completed pass without a supplied diagnostic must not retain old time.
    let observation = native.load(&journal, &control(), Some(&terminal)).await;
    let page = native.page(&journal, None, None).await;
    native.reply(json!({}));
    journal
        .checkpoint_considered_metadata_page(observation, page, None)
        .await
        .unwrap();
    assert_eq!(native.last_put()["completed_passes"], n(2));
    assert!(!native.last_put().contains_key("last_completion_ms"));
}

#[tokio::test]
async fn rejects_foreign_empty_pages_observations_and_wrong_input_without_writes() {
    let native = Native::new();
    let journal = native.journal().await;
    for (table, domain) in [
        (TABLE, DomainId::new("other").unwrap()),
        (
            "arn:aws:dynamodb:us-west-2:123456789012:table/other",
            domain(),
        ),
    ] {
        let foreign = native.journal_for(table, domain).await;
        let observation = native.load(&journal, &control(), None).await;
        let page = native.page(&foreign, None, None).await;
        assert_eq!(
            journal
                .checkpoint_considered_metadata_page(observation, page, None)
                .await
                .unwrap_err(),
            MetadataProgressError::Binding
        );
        let observation = native.load(&journal, &control(), None).await;
        assert_eq!(
            foreign
                .reset_metadata_progress(observation)
                .await
                .unwrap_err(),
            MetadataProgressError::Binding
        );
    }
    let cursor = MetadataCursor::from_key(&journal, marker()).unwrap();
    let saved = row(&journal, Some(cursor.clone()));
    let observation = native.load(&journal, &control(), Some(&saved)).await;
    let wrong = native.page(&journal, None, None).await;
    assert_eq!(
        journal
            .checkpoint_considered_metadata_page(observation, wrong, None)
            .await
            .unwrap_err(),
        MetadataProgressError::InputCursorMismatch
    );
    let observation = native.load(&journal, &control(), None).await;
    let wrong = native.page(&journal, Some(&cursor), None).await;
    assert_eq!(
        journal
            .checkpoint_considered_metadata_page(observation, wrong, None)
            .await
            .unwrap_err(),
        MetadataProgressError::InputCursorMismatch
    );
    assert!(native.requests("transactwriteitems").is_empty());
}

#[tokio::test]
async fn historical_incarnation_requires_explicit_fully_guarded_scheduling_reset() {
    let native = Native::new();
    let journal = native.journal().await;
    let mut saved = row(
        &journal,
        Some(MetadataCursor::from_key(&journal, marker()).unwrap()),
    );
    saved.insert("incarnation".into(), s(Uuid::from_u128(99).to_string()));
    let observation = native.load(&journal, &control(), Some(&saved)).await;
    assert!(observation.requires_reset());
    assert_eq!(
        observation.cursor().unwrap_err(),
        MetadataProgressError::ResetRequired
    );
    let page = native.page(&journal, None, None).await;
    assert_eq!(
        journal
            .checkpoint_considered_metadata_page(observation, page, None)
            .await
            .unwrap_err(),
        MetadataProgressError::ResetRequired
    );
    assert!(native.requests("transactwriteitems").is_empty());
    let observation = native.load(&journal, &control(), Some(&saved)).await;
    native.reply(json!({}));
    journal.reset_metadata_progress(observation).await.unwrap();
    let reset = native.last_put();
    assert_eq!(reset["incarnation"], control()["incarnation"]);
    assert_eq!(reset["completed_passes"], n(0));
    assert!(!reset.contains_key("cursor_pk"));
    assert!(!reset.contains_key("last_completion_ms"));
    assert_ne!(reset["revision"], saved["revision"]);
    let write = &native.requests("transactwriteitems")[0];
    assert_eq!(write["TransactItems"].as_array().unwrap().len(), 2);
    assert_guard(
        &write["TransactItems"][0]["ConditionCheck"],
        &control(),
        &CONTROL_FIELDS,
    );
    assert_guard(&write["TransactItems"][1]["Put"], &saved, &FIELDS);
    let observation = native.load(&journal, &control(), Some(&reset)).await;
    assert!(!observation.requires_reset());
    assert!(observation.cursor().unwrap().is_none());
}

#[tokio::test]
async fn native_cas_conflicts_and_unknowns_are_one_attempt_and_require_reobservation() {
    let native = Native::new();
    let journal = native.journal().await;
    let saved = row(&journal, None);
    for (status, response, expected) in [
        (
            400,
            json!({"__type":"TransactionCanceledException","CancellationReasons":[{"Code":"None"},{"Code":"ConditionalCheckFailed"}]}),
            MetadataProgressError::Conflict,
        ),
        (
            400,
            json!({"__type":"TransactionCanceledException","CancellationReasons":[{"Code":"ConditionalCheckFailed"},{"Code":"None"}]}),
            MetadataProgressError::Conflict,
        ),
        (
            500,
            json!({"__type":"InternalServerError","message":"response lost or service failure"}),
            MetadataProgressError::OutcomeUnknown,
        ),
        (
            400,
            json!({"__type":"TransactionCanceledException","CancellationReasons":[{"Code":"TransactionConflict"}]}),
            MetadataProgressError::OutcomeUnknown,
        ),
    ] {
        let observation = native.load(&journal, &control(), Some(&saved)).await;
        let page = native.page(&journal, None, None).await;
        let before = native.requests("transactwriteitems").len();
        native.replies.lock().unwrap().push_back((status, response));
        assert_eq!(
            journal
                .checkpoint_considered_metadata_page(observation, page, None)
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(native.requests("transactwriteitems").len(), before + 1);
    }
    // Read failures also cannot be upgraded to missing progress/start-of-pass.
    native
        .replies
        .lock()
        .unwrap()
        .push_back((500, json!({"__type":"InternalServerError"})));
    let before = native.requests("transactgetitems").len();
    assert_eq!(
        journal.load_metadata_progress().await.unwrap_err(),
        MetadataProgressError::NativeRead
    );
    assert_eq!(native.requests("transactgetitems").len(), before + 1);
}

#[tokio::test]
async fn closed_decoder_and_coherent_control_fail_closed() {
    let native = Native::new();
    let journal = native.journal().await;
    let valid = row(&journal, None);
    let mut corruptions = Vec::new();
    for (name, value) in [
        ("domain", s("other")),
        ("pk", s("wrong")),
        ("sk", s("wrong")),
        ("incarnation", s(Uuid::nil().to_string())),
        ("revision", s("invalid")),
        ("completed_passes", n(0)),
        ("completed_passes", s("3")),
        ("completed_passes", AttributeValue::N("03".into())),
        ("last_completion_ms", n(u64::MAX)),
        ("extra", s("unknown")),
        ("cursor_pk", s("only-half")),
        ("cursor_sk", s("only-half")),
    ] {
        let mut item = valid.clone();
        item.insert(name.into(), value);
        corruptions.push(item);
    }
    for field in [
        "record",
        "domain",
        "incarnation",
        "revision",
        "completed_passes",
    ] {
        let mut item = valid.clone();
        item.remove(field);
        corruptions.push(item);
    }
    for item in corruptions {
        assert!(matches!(
            decode(&journal, &item),
            Err(MetadataProgressError::Corrupt)
        ));
        native.reply(json!({"Responses":[{"Item":wire(&control())},{"Item":wire(&item)}]}));
        assert_eq!(
            journal.load_metadata_progress().await.unwrap_err(),
            MetadataProgressError::Corrupt
        );
    }
    for bad_control in [
        None,
        Some({
            let mut c = control();
            c.insert("hidden_authority".into(), n(1));
            c
        }),
        Some({
            let mut c = control();
            c.remove("compaction_candidate");
            c
        }),
        Some({
            let mut c = control();
            c.insert("domain".into(), s("wrong"));
            c
        }),
    ] {
        native.reply(json!({"Responses":[bad_control.as_ref().map(|c| json!({"Item":wire(c)})).unwrap_or(json!({})), {}]}));
        assert_eq!(
            journal.load_metadata_progress().await.unwrap_err(),
            MetadataProgressError::Corrupt
        );
    }
    for responses in [json!([]), json!([{}]), json!([{}, {}, {}])] {
        native.reply(json!({"Responses":responses}));
        assert_eq!(
            journal.load_metadata_progress().await.unwrap_err(),
            MetadataProgressError::Corrupt
        );
    }
    assert!(native.requests("transactwriteitems").is_empty());
}

#[tokio::test]
async fn compact_bound_native_key_limits_and_counter_overflow() {
    let native = Native::new();
    let journal = native.journal().await;
    let boundary = Item::from([
        ("pk".into(), s("é".repeat(1024))),
        ("sk".into(), s("é".repeat(512))),
    ]);
    let cursor = MetadataCursor::from_key(&journal, boundary.clone()).unwrap();
    let saved = row(&journal, Some(cursor));
    assert_eq!(
        decode(&journal, &saved)
            .unwrap()
            .cursor
            .unwrap()
            .key(&journal)
            .unwrap(),
        boundary
    );
    for (field, value) in [
        ("cursor_pk", s("é".repeat(1025))),
        ("cursor_sk", s("é".repeat(513))),
        ("cursor_pk", n(1)),
        ("cursor_sk", s("")),
    ] {
        let mut item = saved.clone();
        item.insert(field.into(), value);
        assert!(matches!(
            decode(&journal, &item),
            Err(MetadataProgressError::Corrupt)
        ));
    }
    let large_journal = native
        .journal_for(TABLE, DomainId::new("d".repeat(128)).unwrap())
        .await;
    // Every legal native marker fits even with maximal domain/count/time. Do
    // not truncate markers or relax their independent 2048/1024-byte limits.
    let maximum = Progress {
        incarnation: Uuid::from_u128(1),
        cursor: Some(MetadataCursor::from_key(&large_journal, boundary).unwrap()),
        completed_passes: u64::MAX,
        last_completion: Some(TimestampMs::MAX),
    };
    let mut maximum_row = encode(&large_journal, &maximum).unwrap();
    assert!(decode(&large_journal, &maximum_row).is_ok());
    maximum_row.insert("cursor_pk".into(), s("p".repeat(4000)));
    assert!(matches!(
        decode(&large_journal, &maximum_row),
        Err(MetadataProgressError::RecordSize)
    ));
    // Exact existing compact checker includes the same growth reserve.
    assert!(bounded(&Item::from([("x".into(), s("x".repeat(3935)))])).is_ok());
    assert_eq!(
        bounded(&Item::from([("x".into(), s("x".repeat(3936)))])),
        Err(MetadataProgressError::RecordSize)
    );
    let mut max = row(&journal, None);
    max.insert("completed_passes".into(), n(u64::MAX));
    let observation = native.load(&journal, &control(), Some(&max)).await;
    let page = native.page(&journal, None, None).await;
    assert_eq!(
        journal
            .checkpoint_considered_metadata_page(observation, page, None)
            .await
            .unwrap_err(),
        MetadataProgressError::CounterOverflow
    );
    assert!(native.requests("transactwriteitems").is_empty());
}

#[tokio::test]
async fn uncheckpointed_page_repeats_and_nonterminal_preserves_completion_metrics() {
    let native = Native::new();
    let journal = native.journal().await;
    let saved = row(
        &journal,
        Some(MetadataCursor::from_key(&journal, marker()).unwrap()),
    );
    let mut no_window = control();
    for field in [
        "compaction_candidate",
        "compaction_not_before_ms",
        "resolution_retention_ms",
    ] {
        no_window.remove(field);
    }
    let abandoned = native.load(&journal, &no_window, Some(&saved)).await;
    let page = native
        .page(&journal, abandoned.cursor().unwrap(), None)
        .await;
    // Simulate a caller deadline before complete consideration: no checkpoint.
    drop(page);
    drop(abandoned);
    assert!(native.requests("transactwriteitems").is_empty());
    let observation = native.load(&journal, &no_window, Some(&saved)).await;
    let next = Item::from([
        ("pk".into(), s("next-native-pk")),
        ("sk".into(), s("next-native-sk")),
    ]);
    let page = native
        .page(&journal, observation.cursor().unwrap(), Some(next))
        .await;
    let scans = native.requests("scan");
    assert_eq!(scans[0]["ExclusiveStartKey"], scans[1]["ExclusiveStartKey"]);
    native.reply(json!({}));
    journal
        .checkpoint_considered_metadata_page(observation, page, Some(TimestampMs::MAX))
        .await
        .unwrap();
    let updated = native.last_put();
    assert_eq!(updated["completed_passes"], saved["completed_passes"]);
    assert_eq!(updated["last_completion_ms"], saved["last_completion_ms"]);
    assert_ne!(updated["revision"], saved["revision"]);
    assert_guard(
        &native.requests("transactwriteitems")[0]["TransactItems"][0]["ConditionCheck"],
        &no_window,
        &CONTROL_FIELDS,
    );
    // Same-incarnation reset remains explicit and resets scheduling metrics only.
    let observation = native.load(&journal, &no_window, Some(&updated)).await;
    native.reply(json!({}));
    journal.reset_metadata_progress(observation).await.unwrap();
    assert_eq!(native.last_put()["completed_passes"], n(0));
    assert!(!native.last_put().contains_key("cursor_pk"));
}

#[tokio::test]
async fn discovery_classifies_valid_progress_permanent_corrupt_known_and_unsupported_future() {
    let native = Native::new();
    let journal = native.journal().await;
    let valid = row(
        &journal,
        Some(MetadataCursor::from_key(&journal, marker()).unwrap()),
    );
    let mut malformed = valid.clone();
    malformed.insert("cursor_pk".into(), n(3));
    let mut unknown_field = valid.clone();
    unknown_field.insert("new_authority".into(), s("no"));
    let mut future = valid.clone();
    future.insert("record".into(), s("metadata-progress-v2"));
    native.reply(json!({"Items":[wire(&valid),wire(&malformed),wire(&unknown_field),wire(&future)],"Count":4,"ScannedCount":4}));
    let page = journal.discover_metadata_page(None, 4).await.unwrap();
    assert_eq!(
        page.items(),
        &[
            MetadataDiscoveryItem::Permanent,
            MetadataDiscoveryItem::Corrupt,
            MetadataDiscoveryItem::Corrupt,
            MetadataDiscoveryItem::Unsupported
        ]
    );
    native.reply(json!({"Responses":[{"Item":wire(&control())},{"Item":wire(&future)}]}));
    assert_eq!(
        journal.load_metadata_progress().await.unwrap_err(),
        MetadataProgressError::Unsupported
    );
}
