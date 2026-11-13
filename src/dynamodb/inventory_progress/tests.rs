//! Native SDK scripted loopback, not cloud acceptance or an expression emulator.
use super::super::uploads::n;
#[cfg(feature = "s3")]
use super::super::{journal::control_key, uploads::namespace_key};
use super::*;
use crate::dynamodb::{EnrolledTables, MetadataDiscoveryItem};
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region, retry::RetryConfig};
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
    DomainId::new("inventory-fixture").unwrap()
}
fn nsid() -> NamespaceId {
    NamespaceId::new("namespace").unwrap()
}
fn s(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}
#[cfg(feature = "s3")]
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
#[cfg(feature = "s3")]
fn namespace() -> Item {
    let mut item = namespace_key(&domain(), &nsid());
    for (name, value) in [
        ("record", "namespace".into()),
        ("domain", domain().as_str().into()),
        ("namespace_id", nsid().as_str().into()),
        ("incarnation", Uuid::from_u128(1).to_string()),
        ("configuration_revision", Uuid::from_u128(2).to_string()),
        ("state", "ACTIVE".into()),
        ("store", "store".into()),
        ("bucket", "storelink-fixture".into()),
        ("expected_owner", "123456789012".into()),
        ("region", "us-west-2".into()),
        ("key_prefix", "managed/".into()),
        ("provider_profile", "aws-s3-versioned-lifecycle-v0".into()),
    ] {
        item.insert(name.into(), s(value));
    }
    item.insert("mpu_max_age_ms".into(), n(3600000));
    item.insert("mpu_lifecycle_margin_ms".into(), n(3600000));
    item
}
fn row() -> Item {
    let mut item = key(&domain(), &nsid());
    item.extend([
        ("record".into(), s(RECORD)),
        ("domain".into(), s(domain().as_str())),
        ("namespace_id".into(), s(nsid().as_str())),
        (
            "configuration_revision".into(),
            s(Uuid::from_u128(2).to_string()),
        ),
        ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
        ("revision".into(), s(Uuid::from_u128(3).to_string())),
        ("completed_passes".into(), n(3)),
        ("last_completion_ms".into(), n(200)),
    ]);
    item
}
fn marked_row() -> Item {
    let mut item = row();
    item.insert("key_marker".into(), s("managed/native +é"));
    item.insert("version_id_marker".into(), s("native/+version"));
    item
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
#[cfg(feature = "s3")]
fn unmarshal(v: &Value) -> Item {
    v.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                if let Some(sv) = v.get("S") {
                    s(sv.as_str().unwrap())
                } else {
                    AttributeValue::N(v["N"].as_str().unwrap().into())
                },
            )
        })
        .collect()
}

struct Native {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    replies: Arc<Mutex<VecDeque<(u16, String, String)>>>,
    #[cfg(feature = "s3")]
    requests: Arc<Mutex<Vec<(String, String)>>>,
    worker: Option<JoinHandle<()>>,
}
impl Native {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let replies = Arc::new(Mutex::new(VecDeque::<(u16, String, String)>::new()));
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
                let headers = String::from_utf8(headers).unwrap();
                let lower = headers.to_ascii_lowercase();
                let length = lower
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .map(|s| s.parse::<usize>().unwrap())
                    .unwrap_or(0);
                assert!(length < 1_000_000);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                captured
                    .lock()
                    .unwrap()
                    .push((headers.clone(), String::from_utf8(body).unwrap()));
                let (status, extra, body) = responses.lock().unwrap().pop_front().unwrap_or((
                    500,
                    String::new(),
                    "{\"__type\":\"InternalServerError\"}".into(),
                ));
                let body = if headers.starts_with("HEAD ") {
                    String::new()
                } else {
                    body
                };
                let _ = write!(
                    socket,
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}",
                    body.len()
                );
            }
        });
        Self {
            address,
            stop,
            replies,
            #[cfg(feature = "s3")]
            requests,
            worker: Some(worker),
        }
    }
    fn json(&self, v: Value) {
        self.error(200, v);
    }
    fn error(&self, status: u16, v: Value) {
        self.replies.lock().unwrap().push_back((
            status,
            "Content-Type: application/x-amz-json-1.0\r\n".into(),
            v.to_string(),
        ));
    }
    async fn journal(&self) -> CommandJournal {
        self.journal_for(TABLE, domain()).await
    }
    async fn journal_for(&self, table: &str, domain: DomainId) -> CommandJournal {
        self.json(json!({"Table":{"TableName":table.rsplit('/').next().unwrap(),"TableArn":table,"TableStatus":"ACTIVE","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(Credentials::new(
                    "fixture", "fixture", None, None, "fixture",
                ))
                .endpoint_url(format!("http://{}", self.address))
                .retry_config(RetryConfig::standard().with_max_attempts(3))
                .build(),
        );
        let tables = EnrolledTables::enroll(&client, table, &[]).await.unwrap();
        CommandJournal::new(client, tables, domain)
    }
    #[cfg(feature = "s3")]
    fn requests(&self, op: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(h, _)| h.to_ascii_lowercase().contains(&format!(".{op}\r\n")))
            .map(|(_, b)| serde_json::from_str(b).unwrap())
            .collect()
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        let result = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn inventory_progress_classification_is_checked_in_dynamodb_only_builds() {
    let native = Native::new();
    let journal = native.journal().await;
    let valid = marked_row();
    let mut corrupt = valid.clone();
    corrupt.insert("key_marker".into(), n(1));
    let mut unknown = valid.clone();
    unknown.insert("TTL".into(), n(3));
    let mut future = valid.clone();
    future.insert("record".into(), s("inventory-progress-v2"));
    native.json(json!({"Items":[wire(&valid),wire(&corrupt),wire(&unknown),wire(&future)],"Count":4,"ScannedCount":4}));
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
}

#[tokio::test]
async fn inventory_closed_codec_canonical_key_native_bounds_and_compact_size() {
    let native = Native::new();
    let journal = native.journal().await;
    let valid = marked_row();
    for (field, v) in [
        ("pk", s("noncanonical")),
        ("sk", s("other")),
        ("domain", s("foreign")),
        ("namespace_id", s("foreign")),
        ("configuration_revision", s(Uuid::nil().to_string())),
        ("incarnation", s("bad")),
        ("revision", s("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA")),
        ("completed_passes", AttributeValue::N("03".into())),
        ("completed_passes", n(0)),
        ("last_completion_ms", n(u64::MAX)),
        ("key_marker", s("")),
        ("key_marker", s("é".repeat(513))),
        ("version_id_marker", s("v".repeat(1025))),
        ("version_id_marker", s("null")),
        ("key_marker", n(1)),
        ("unexpected", s("field")),
    ] {
        let mut item = valid.clone();
        item.insert(field.into(), v);
        assert!(
            matches!(
                decode(&journal, &item),
                Err(InventoryProgressError::Corrupt)
            ),
            "{field}"
        );
    }
    for field in FIELDS
        .iter()
        .filter(|f| !["last_completion_ms"].contains(f))
    {
        let mut item = valid.clone();
        item.remove(*field);
        assert!(
            matches!(
                decode(&journal, &item),
                Err(InventoryProgressError::Corrupt)
            ),
            "{field}"
        );
    }
    let mut future = valid.clone();
    future.insert("record".into(), s("inventory-progress-v9"));
    assert!(matches!(
        decode(&journal, &future),
        Err(InventoryProgressError::Unsupported)
    ));
    let d = DomainId::new("d".repeat(128)).unwrap();
    let ns = NamespaceId::new("é".repeat(64)).unwrap();
    let journal = native.journal_for(TABLE, d.clone()).await;
    let mut maximum = valid;
    maximum.extend(key(&d, &ns));
    maximum.insert("domain".into(), s(d.as_str()));
    maximum.insert("namespace_id".into(), s(ns.as_str()));
    maximum.insert("key_marker".into(), s("é".repeat(512)));
    maximum.insert("version_id_marker".into(), s("é".repeat(512)));
    maximum.insert("completed_passes".into(), n(u64::MAX));
    maximum.insert("last_completion_ms".into(), n(TimestampMs::MAX.as_millis()));
    assert!(decode(&journal, &maximum).is_ok());
    maximum.insert("key_marker".into(), s("x".repeat(4000)));
    assert!(matches!(
        decode(&journal, &maximum),
        Err(InventoryProgressError::RecordSize)
    ));
    assert!(bounded(&Item::from([("x".into(), s("x".repeat(3935)))])).is_ok());
    assert_eq!(
        bounded(&Item::from([("x".into(), s("x".repeat(3936)))])),
        Err(InventoryProgressError::RecordSize)
    );
}

#[cfg(feature = "s3")]
mod s3_tests {
    use super::super::super::maintenance::FIELDS as CONTROL_FIELDS;
    use super::*;
    use crate::s3::{
        NamespaceConfiguration, NamespaceConfigurationSpec, QualifiedNamespace, VersionPage,
        VersionPageCursor, list_version_page, qualify_existing,
    };

    fn spec() -> NamespaceConfigurationSpec {
        NamespaceConfigurationSpec {
            id: nsid(),
            store: StoreId::new("store").unwrap(),
            bucket: "storelink-fixture".into(),
            expected_owner: "123456789012".into(),
            region: "us-west-2".into(),
            key_prefix: "managed/".into(),
            mpu_max_age_ms: 3600000,
            mpu_lifecycle_margin_ms: 3600000,
        }
    }
    fn escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }
    fn url(s: &str) -> String {
        s.as_bytes().iter().map(|b| format!("%{b:02X}")).collect()
    }
    impl Native {
        fn xml(&self, body: impl Into<String>) {
            self.replies.lock().unwrap().push_back((
                200,
                "Content-Type: application/xml\r\n".into(),
                body.into(),
            ));
        }
        async fn qualified(&self, spec: NamespaceConfigurationSpec) -> QualifiedNamespace {
            let region = spec.region.clone();
            self.replies.lock().unwrap().push_back((
                200,
                format!("x-amz-bucket-region: {region}\r\n"),
                String::new(),
            ));
            self.xml("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>");
            self.xml(format!("<LifecycleConfiguration><Rule><ID>mpu</ID><Status>Enabled</Status><Filter><Prefix>{}</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>", escape(&spec.key_prefix)));
            self.replies.lock().unwrap().push_back((
                404,
                "Content-Type: application/xml\r\n".into(),
                "<Error><Code>ObjectLockConfigurationNotFoundError</Code></Error>".into(),
            ));
            let client = aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new(region))
                    .credentials_provider(Credentials::new(
                        "fixture", "fixture", None, None, "fixture",
                    ))
                    .endpoint_url(format!("http://{}", self.address))
                    .force_path_style(true)
                    .retry_config(RetryConfig::disabled())
                    .build(),
            );
            qualify_existing(&client, &NamespaceConfiguration::new(spec).unwrap())
                .await
                .unwrap()
        }
        fn response(
            &self,
            control: Option<&Item>,
            namespace: Option<&Item>,
            progress: Option<&Item>,
        ) {
            let responses = [control, namespace, progress]
                .map(|i| i.map(|v| json!({"Item":wire(v)})).unwrap_or(json!({})));
            self.json(json!({"Responses": responses}));
        }
        async fn load(
            &self,
            journal: &CommandJournal,
            q: &QualifiedNamespace,
            p: Option<&Item>,
        ) -> InventoryProgressObservation {
            self.response(Some(&control()), Some(&namespace()), p);
            journal.load_inventory_progress(q).await.unwrap()
        }
        async fn page(
            &self,
            q: &QualifiedNamespace,
            cursor: Option<&VersionPageCursor>,
            next: Option<(&str, &str)>,
            terminal_markers: bool,
            entries: bool,
        ) -> VersionPage {
            let c = q.configuration();
            let next_xml = next.or(if terminal_markers { Some(("outside-ignored", "null")) } else { None })
                .map(|(k,v)| format!("<NextKeyMarker>{}</NextKeyMarker><NextVersionIdMarker>{}</NextVersionIdMarker>", url(k), escape(v))).unwrap_or_default();
            let input = cursor
                .map(|p| {
                    format!(
                        "<KeyMarker>{}</KeyMarker><VersionIdMarker>{}</VersionIdMarker>",
                        url(p.key_marker()),
                        escape(p.version_id_marker())
                    )
                })
                .unwrap_or_default();
            let entries = if entries {
                "<Version><Key>managed%2Fa</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-01T00:00:00Z</LastModified><Size>1</Size></Version><DeleteMarker><Key>managed%2Fa</Key><VersionId>delete1</VersionId><IsLatest>false</IsLatest><LastModified>2026-01-01T00:00:00Z</LastModified></DeleteMarker>"
            } else {
                ""
            };
            self.xml(format!("<ListVersionsResult><Name>{}</Name><Prefix>{}</Prefix><EncodingType>url</EncodingType><MaxKeys>2</MaxKeys><IsTruncated>{}</IsTruncated>{input}{next_xml}{entries}</ListVersionsResult>", c.bucket(), url(c.key_prefix()), next.is_some()));
            list_version_page(q, cursor, 2).await.unwrap()
        }
        fn last_put(&self) -> Item {
            unmarshal(
                &self.requests("transactwriteitems").last().unwrap()["TransactItems"][2]["Put"]["Item"],
            )
        }
    }
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
            if let Some(v) = old.get(*field) {
                let encoded = wire(&Item::from([("v".into(), v.clone())]));
                assert!(
                    values.iter().any(|(va, v)| v == &encoded["v"]
                        && expression.contains(&format!("{alias} = {va}"))),
                    "missing equality for {field}"
                );
            } else {
                assert!(expression.contains(&format!("attribute_not_exists({alias})")));
            }
        }
    }
    fn guards(native: &Native, index: usize, old: Option<&Item>, ns: &Item, ctl: &Item) {
        let requests = native.requests("transactwriteitems");
        let write = &requests[index];
        let actions = write["TransactItems"].as_array().unwrap();
        assert_eq!(actions.len(), 3);
        assert_guard(&actions[0]["ConditionCheck"], ctl, &CONTROL_FIELDS);
        let fields: Vec<_> = ns.keys().map(String::as_str).collect();
        assert_guard(&actions[1]["ConditionCheck"], ns, &fields);
        assert_eq!(
            actions[0]["ConditionCheck"]["Key"],
            wire(&control_key(&domain()))
        );
        assert_eq!(
            actions[1]["ConditionCheck"]["Key"],
            wire(&namespace_key(&domain(), &nsid()))
        );
        if let Some(old) = old {
            assert_guard(&actions[2]["Put"], old, &FIELDS);
        } else {
            assert_eq!(
                actions[2]["Put"]["ConditionExpression"],
                "attribute_not_exists(#pk)"
            );
        }
        assert!(!write.to_string().contains("#CMD#"));
        assert!(!write.to_string().contains("\"Delete\""));
        assert!(!write.to_string().contains("\"Update\""));
    }

    #[tokio::test]
    async fn native_three_row_read_three_action_cas_restart_and_empty_terminal_binding() {
        let native = Native::new();
        let journal = native.journal().await;
        let q = native.qualified(spec()).await;
        let observation = native.load(&journal, &q, None).await;
        assert!(observation.cursor().unwrap().is_none());
        assert_eq!(observation.completed_passes(), 0);
        assert_eq!(observation.namespace().id(), &nsid());
        let page = native
            .page(
                &q,
                None,
                Some(("managed/native +é", "native/+version")),
                false,
                true,
            )
            .await;
        assert_eq!(page.versions().len(), 1);
        assert_eq!(page.delete_markers().len(), 1);
        // Native marker deliberately names neither returned version nor delete marker.
        native.json(json!({}));
        journal
            .checkpoint_considered_inventory_page(observation, page, Some(TimestampMs::MAX))
            .await
            .unwrap();
        let saved = native.last_put();
        assert_eq!(saved["key_marker"], s("managed/native +é"));
        assert_eq!(saved["version_id_marker"], s("native/+version"));
        assert_eq!(saved["completed_passes"], n(0));
        assert!(!saved.contains_key("last_completion_ms"));
        for f in [
            "bucket",
            "store",
            "key_prefix",
            "provider_profile",
            "TTL",
            "configuration",
        ] {
            assert!(!saved.contains_key(f));
        }
        guards(&native, 0, None, &namespace(), &control());
        let gets = native.requests("transactgetitems");
        assert_eq!(gets[0]["TransactItems"].as_array().unwrap().len(), 3);
        for (i, k) in [
            control_key(&domain()),
            namespace_key(&domain(), &nsid()),
            key(&domain(), &nsid()),
        ]
        .iter()
        .enumerate()
        {
            let get = &gets[0]["TransactItems"][i]["Get"];
            assert_eq!(get["Key"], wire(k));
            assert_eq!(get["TableName"], TABLE);
            assert!(get.get("ProjectionExpression").is_none());
        }
        // Restart with a newly qualified SDK token, not forged configuration evidence.
        let q = native.qualified(spec()).await;
        let observation = native.load(&journal, &q, Some(&saved)).await;
        let cursor = observation.cursor().unwrap().unwrap();
        assert_eq!(cursor.key_marker(), "managed/native +é");
        let page = native.page(&q, Some(cursor), None, true, false).await;
        assert!(page.versions().is_empty());
        assert!(page.next_cursor().is_none());
        native.json(json!({}));
        journal
            .checkpoint_considered_inventory_page(
                observation,
                page,
                Some(TimestampMs::new(300).unwrap()),
            )
            .await
            .unwrap();
        let terminal = native.last_put();
        assert_eq!(terminal["completed_passes"], n(1));
        assert_eq!(terminal["last_completion_ms"], n(300));
        assert!(!terminal.contains_key("key_marker"));
        assert!(!terminal.contains_key("version_id_marker"));
        assert_eq!(terminal["pk"], saved["pk"]);
        assert_ne!(terminal["revision"], saved["revision"]);
        guards(&native, 1, Some(&saved), &namespace(), &control());
        let observation = native.load(&journal, &q, Some(&terminal)).await;
        let page = native.page(&q, None, None, false, false).await;
        native.json(json!({}));
        journal
            .checkpoint_considered_inventory_page(observation, page, None)
            .await
            .unwrap();
        assert!(!native.last_put().contains_key("last_completion_ms"));
        // Normal SDK restoration actually emits both raw markers on the request.
        let requests = native.requests.lock().unwrap();
        let listing = requests
            .iter()
            .find(|(h, _)| h.starts_with("GET ") && h.contains("version-id-marker="))
            .unwrap();
        assert!(
            listing
                .0
                .contains("key-marker=managed%2Fnative%20%2B%C3%A9")
        );
        assert!(listing.0.contains("version-id-marker=native%2F%2Bversion"));
    }

    #[tokio::test]
    async fn foreign_configuration_namespace_and_input_rejected_even_empty_terminal() {
        let native = Native::new();
        let journal = native.journal().await;
        let q = native.qualified(spec()).await;
        for field in [
            "id", "store", "bucket", "owner", "region", "prefix", "age", "margin",
        ] {
            let mut changed = spec();
            match field {
                "id" => changed.id = NamespaceId::new("other").unwrap(),
                "store" => changed.store = StoreId::new("other").unwrap(),
                "bucket" => changed.bucket = "other-bucket".into(),
                "owner" => changed.expected_owner = "123456789013".into(),
                "region" => changed.region = "us-east-1".into(),
                "prefix" => changed.key_prefix = "other/".into(),
                "age" => changed.mpu_max_age_ms += 1,
                "margin" => changed.mpu_lifecycle_margin_ms += 1,
                _ => unreachable!(),
            }
            let foreign = native.qualified(changed).await;
            let observation = native.load(&journal, &q, None).await;
            let page = native.page(&foreign, None, None, false, false).await;
            assert_eq!(
                journal
                    .checkpoint_considered_inventory_page(observation, page, None)
                    .await
                    .unwrap_err(),
                InventoryProgressError::Binding,
                "{field}"
            );
        }
        let saved = marked_row();
        let observation = native.load(&journal, &q, Some(&saved)).await;
        let page = native.page(&q, None, None, false, false).await;
        assert_eq!(
            journal
                .checkpoint_considered_inventory_page(observation, page, None)
                .await
                .unwrap_err(),
            InventoryProgressError::InputCursorMismatch
        );
        let source = native.load(&journal, &q, Some(&saved)).await;
        let observation = native.load(&journal, &q, None).await;
        let page = native
            .page(&q, source.cursor().unwrap(), None, false, false)
            .await;
        assert_eq!(
            journal
                .checkpoint_considered_inventory_page(observation, page, None)
                .await
                .unwrap_err(),
            InventoryProgressError::InputCursorMismatch
        );
        let mut other_input = saved.clone();
        other_input.insert("version_id_marker".into(), s("other-native-version"));
        let observation = native.load(&journal, &q, Some(&other_input)).await;
        let page = native
            .page(&q, source.cursor().unwrap(), None, false, false)
            .await;
        assert_eq!(
            journal
                .checkpoint_considered_inventory_page(observation, page, None)
                .await
                .unwrap_err(),
            InventoryProgressError::InputCursorMismatch
        );
        for (table, d) in [
            (TABLE, DomainId::new("foreign").unwrap()),
            (
                "arn:aws:dynamodb:us-west-2:123456789012:table/other",
                domain(),
            ),
        ] {
            let foreign = native.journal_for(table, d).await;
            let observation = native.load(&journal, &q, None).await;
            assert_eq!(
                foreign
                    .reset_inventory_progress(observation)
                    .await
                    .unwrap_err(),
                InventoryProgressError::Binding
            );
        }
        assert!(native.requests("transactwriteitems").is_empty());
    }

    #[tokio::test]
    async fn configuration_and_incarnation_changes_need_explicit_reset_not_fresh_start() {
        let native = Native::new();
        let journal = native.journal().await;
        let q = native.qualified(spec()).await;
        for field in ["incarnation", "configuration_revision"] {
            let mut saved = marked_row();
            saved.insert(field.into(), s(Uuid::from_u128(99).to_string()));
            // Old namespace prefixes need not fit current namespace configuration.
            saved.insert("key_marker".into(), s("previous-prefix/native"));
            let observation = native.load(&journal, &q, Some(&saved)).await;
            assert!(observation.requires_reset());
            assert_eq!(
                observation.cursor().unwrap_err(),
                InventoryProgressError::ResetRequired
            );
            assert_eq!(observation.completed_passes(), 3);
            let page = native.page(&q, None, None, false, false).await;
            assert_eq!(
                journal
                    .checkpoint_considered_inventory_page(observation, page, None)
                    .await
                    .unwrap_err(),
                InventoryProgressError::ResetRequired
            );
            let observation = native.load(&journal, &q, Some(&saved)).await;
            native.json(json!({}));
            journal.reset_inventory_progress(observation).await.unwrap();
            let reset = native.last_put();
            assert_eq!(reset["incarnation"], control()["incarnation"]);
            assert_eq!(
                reset["configuration_revision"],
                namespace()["configuration_revision"]
            );
            assert_eq!(reset["completed_passes"], n(0));
            assert!(!reset.contains_key("key_marker"));
            assert!(!reset.contains_key("last_completion_ms"));
            assert_ne!(reset["revision"], saved["revision"]);
            guards(
                &native,
                native.requests("transactwriteitems").len() - 1,
                Some(&saved),
                &namespace(),
                &control(),
            );
            let observation = native.load(&journal, &q, Some(&reset)).await;
            assert!(!observation.requires_reset());
            assert!(observation.cursor().unwrap().is_none());
        }
        // Permanent namespace mappings can outlive a control incarnation. A reset
        // updates scheduling only and leaves that older configuration row intact.
        let mut ctl = control();
        ctl.insert("incarnation".into(), s(Uuid::from_u128(100).to_string()));
        native.response(Some(&ctl), Some(&namespace()), Some(&row()));
        let observation = journal.load_inventory_progress(&q).await.unwrap();
        assert!(observation.requires_reset());
        native.json(json!({}));
        journal.reset_inventory_progress(observation).await.unwrap();
        guards(&native, 2, Some(&row()), &namespace(), &ctl);
    }

    #[tokio::test]
    async fn native_conflict_unknown_and_read_failure_disable_retries() {
        let native = Native::new();
        let journal = native.journal().await;
        let q = native.qualified(spec()).await;
        for index in 0..3 {
            let observation = native.load(&journal, &q, Some(&row())).await;
            let page = native.page(&q, None, None, false, false).await;
            let mut reasons = vec![json!({"Code":"None"}); 3];
            reasons[index] = json!({"Code":"ConditionalCheckFailed"});
            native.error(
                400,
                json!({"__type":"TransactionCanceledException", "CancellationReasons":reasons}),
            );
            assert_eq!(
                journal
                    .checkpoint_considered_inventory_page(observation, page, None)
                    .await
                    .unwrap_err(),
                InventoryProgressError::Conflict
            );
            assert_eq!(native.requests("transactwriteitems").len(), index + 1);
        }
        for (status, body) in [
            (500, json!({"__type":"InternalServerError"})),
            (
                400,
                json!({"__type":"TransactionCanceledException", "CancellationReasons":[{"Code":"TransactionConflict"}]}),
            ),
        ] {
            let observation = native.load(&journal, &q, Some(&row())).await;
            let before = native.requests("transactwriteitems").len();
            native.error(status, body);
            assert_eq!(
                journal
                    .reset_inventory_progress(observation)
                    .await
                    .unwrap_err(),
                InventoryProgressError::OutcomeUnknown
            );
            assert_eq!(native.requests("transactwriteitems").len(), before + 1);
        }
        let before = native.requests("transactgetitems").len();
        native.error(500, json!({"__type":"InternalServerError"}));
        assert_eq!(
            journal.load_inventory_progress(&q).await.unwrap_err(),
            InventoryProgressError::NativeRead
        );
        assert_eq!(native.requests("transactgetitems").len(), before + 1);
    }

    #[tokio::test]
    async fn coherent_namespace_exact_tuple_and_control_fail_closed() {
        let native = Native::new();
        let journal = native.journal().await;
        let q = native.qualified(spec()).await;
        for (field, v, expected) in [
            ("bucket", s("other-bucket"), InventoryProgressError::Binding),
            (
                "expected_owner",
                s("123456789013"),
                InventoryProgressError::Binding,
            ),
            ("region", s("us-east-1"), InventoryProgressError::Binding),
            ("store", s("other"), InventoryProgressError::Binding),
            ("key_prefix", s("other/"), InventoryProgressError::Binding),
            (
                "mpu_max_age_ms",
                n(3600001),
                InventoryProgressError::Binding,
            ),
            (
                "mpu_lifecycle_margin_ms",
                n(3600001),
                InventoryProgressError::Binding,
            ),
            ("namespace_id", s("other"), InventoryProgressError::Corrupt),
            (
                "configuration_revision",
                s("bad"),
                InventoryProgressError::Corrupt,
            ),
            ("domain", s("foreign"), InventoryProgressError::Corrupt),
            (
                "provider_profile",
                s("future"),
                InventoryProgressError::Corrupt,
            ),
            ("unknown", s("field"), InventoryProgressError::Corrupt),
        ] {
            let mut ns = namespace();
            ns.insert(field.into(), v);
            native.response(Some(&control()), Some(&ns), None);
            assert_eq!(
                journal.load_inventory_progress(&q).await.unwrap_err(),
                expected,
                "{field}"
            );
        }
        for which in 0..3 {
            let mut ctl = control();
            if which == 2 {
                ctl.insert("unknown".into(), n(1));
            }
            let ns = namespace();
            native.response(
                if which == 0 { None } else { Some(&ctl) },
                if which == 1 { None } else { Some(&ns) },
                None,
            );
            assert_eq!(
                journal.load_inventory_progress(&q).await.unwrap_err(),
                InventoryProgressError::Corrupt
            );
        }
        for responses in [json!([]), json!([{}, {}]), json!([{}, {}, {}, {}])] {
            native.json(json!({"Responses":responses}));
            assert_eq!(
                journal.load_inventory_progress(&q).await.unwrap_err(),
                InventoryProgressError::Corrupt
            );
        }
        let mut foreign = row();
        let id = NamespaceId::new("other").unwrap();
        foreign.extend(key(&domain(), &id));
        foreign.insert("namespace_id".into(), s(id.as_str()));
        native.response(Some(&control()), Some(&namespace()), Some(&foreign));
        assert_eq!(
            journal.load_inventory_progress(&q).await.unwrap_err(),
            InventoryProgressError::Binding
        );
        let mut bad_prefix = marked_row();
        bad_prefix.insert("key_marker".into(), s("outside/native"));
        native.response(Some(&control()), Some(&namespace()), Some(&bad_prefix));
        assert_eq!(
            journal.load_inventory_progress(&q).await.unwrap_err(),
            InventoryProgressError::Corrupt
        );
        let mut future = row();
        future.insert("record".into(), s("inventory-progress-v2"));
        native.response(Some(&control()), Some(&namespace()), Some(&future));
        assert_eq!(
            journal.load_inventory_progress(&q).await.unwrap_err(),
            InventoryProgressError::Unsupported
        );
        assert!(native.requests("transactwriteitems").is_empty());
    }

    #[tokio::test]
    async fn partial_page_never_autoacks_nonterminal_preserves_metrics_overflow_fails() {
        let native = Native::new();
        let journal = native.journal().await;
        let q = native.qualified(spec()).await;
        let saved = marked_row();
        let observation = native.load(&journal, &q, Some(&saved)).await;
        let page = native
            .page(&q, observation.cursor().unwrap(), None, false, false)
            .await;
        drop(page);
        drop(observation); // crash/deadline before ALL items considered
        assert!(native.requests("transactwriteitems").is_empty());
        let observation = native.load(&journal, &q, Some(&saved)).await;
        assert_eq!(
            observation.cursor().unwrap().unwrap().key_marker(),
            "managed/native +é"
        );
        let page = native
            .page(
                &q,
                observation.cursor().unwrap(),
                Some(("managed/z", "next")),
                false,
                false,
            )
            .await;
        native.json(json!({}));
        journal
            .checkpoint_considered_inventory_page(observation, page, Some(TimestampMs::MAX))
            .await
            .unwrap();
        let next = native.last_put();
        assert_eq!(next["completed_passes"], saved["completed_passes"]);
        assert_eq!(next["last_completion_ms"], saved["last_completion_ms"]);
        assert_eq!(next["key_marker"], s("managed/z"));
        let mut max = row();
        max.insert("completed_passes".into(), n(u64::MAX));
        let observation = native.load(&journal, &q, Some(&max)).await;
        let page = native.page(&q, None, None, false, false).await;
        assert_eq!(
            journal
                .checkpoint_considered_inventory_page(observation, page, None)
                .await
                .unwrap_err(),
            InventoryProgressError::CounterOverflow
        );
        assert_eq!(native.requests("transactwriteitems").len(), 1);
        // Explicit same-incarnation restart is also a CAS, never an implicit start.
        let observation = native.load(&journal, &q, Some(&next)).await;
        native.json(json!({}));
        journal.reset_inventory_progress(observation).await.unwrap();
        assert_eq!(native.last_put()["completed_passes"], n(0));
    }

    #[tokio::test]
    async fn restoration_checks_native_bounds_prefix_and_exact_qualified_binding() {
        let native = Native::new();
        let q = native.qualified(spec()).await;
        for (k, v) in [
            ("", "v"),
            ("outside/k", "v"),
            ("managed/k", "null"),
            ("managed/k", ""),
        ] {
            assert!(VersionPageCursor::restore(&q, k, v).is_err());
        }
        assert!(VersionPageCursor::restore(&q, &"x".repeat(1025), "v").is_err());
        assert!(VersionPageCursor::restore(&q, "managed/k", &"v".repeat(1025)).is_err());
        let key = format!("managed/{}", "k".repeat(1016));
        let cursor = VersionPageCursor::restore(&q, &key, &"v".repeat(1024)).unwrap();
        let page = native.page(&q, Some(&cursor), None, false, false).await;
        assert!(page.next_cursor().is_none());
        let mut changed = spec();
        changed.id = NamespaceId::new("other").unwrap();
        let other = native.qualified(changed).await;
        let before = native.requests.lock().unwrap().len();
        assert!(list_version_page(&other, Some(&cursor), 2).await.is_err());
        assert_eq!(native.requests.lock().unwrap().len(), before);
    }
}
