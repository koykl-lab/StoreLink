//! Native SDK serialization/transport with scripted service responses, NOT a
//! DynamoDB expression evaluator or a claim of native-service acceptance.
use super::super::uploads::{n, s};
use super::super::{ActionError, EnrolledTables, EnrollmentError};
use super::*;
use crate::identity::DomainId;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure;
use serde_json::{Value, json};
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
use uuid::Uuid;

const ARN: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
const APP: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/application";
fn domain() -> DomainId {
    DomainId::new("application-pruning-test").unwrap()
}
fn scope(generation: u64) -> Scope {
    Scope::new(domain(), Uuid::from_u128(1), generation).unwrap()
}
fn control() -> Item {
    let mut item = control_key(&domain());
    item.extend([
        ("domain".into(), s(domain().as_str())),
        ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
        ("current_generation".into(), n(4)),
        ("closed_through".into(), n(2)),
        ("compacted_through".into(), n(1)),
        ("revision".into(), n(7)),
        ("resolution_retention_ms".into(), n(100)),
    ]);
    item
}
fn key() -> Item {
    HashMap::from([("logical_id".into(), s("candidate"))])
}
fn item() -> Item {
    let mut row = key();
    row.extend([
        ("state".into(), s("COMMITTED")),
        ("revision".into(), s("unique-revision-1")),
    ]);
    row
}
fn get() -> Get {
    Get::builder()
        .table_name("application")
        .set_key(Some(key()))
        .build()
        .unwrap()
}
fn delete() -> Delete {
    Delete::builder()
        .table_name("application")
        .set_key(Some(key()))
        .condition_expression("#state = :terminal AND #revision = :revision")
        .expression_attribute_names("#state", "state")
        .expression_attribute_names("#revision", "revision")
        .expression_attribute_values(":terminal", s("COMMITTED"))
        .expression_attribute_values(":revision", s("unique-revision-1"))
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .build()
        .unwrap()
}
fn wire(item: &Item) -> Value {
    Value::Object(
        item.iter()
            .map(|(name, value)| {
                let value = match value {
                    AttributeValue::S(v) => json!({"S": v}),
                    AttributeValue::N(v) => json!({"N": v}),
                    _ => panic!("scalar fixture only"),
                };
                (name.clone(), value)
            })
            .collect(),
    )
}
fn observation(present: bool) -> ApplicationPruningObservation {
    ApplicationPruningObservation {
        target: Get::builder()
            .table_name(APP)
            .set_key(Some(key()))
            .build()
            .unwrap(),
        control: Control::parse(Some(control()), &domain(), ARN)
            .unwrap()
            .observation,
        control_item: control(),
        item: present.then(item),
    }
}

// Every HTTP request uses the official SDK and a synthetic endpoint/credentials.
// None consumes a request and loses its response. No AWS credential discovery.
type Replies = Arc<Mutex<VecDeque<Option<(u16, Value)>>>>;
struct Fixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    replies: Replies,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    worker: Option<JoinHandle<()>>,
}
impl Fixture {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let replies: Replies = Arc::new(Mutex::new(VecDeque::new()));
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
                assert!(length < 4_500_000);
                let target = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("x-amz-target: "))
                    .unwrap();
                let mut bytes = vec![0; length];
                socket.read_exact(&mut bytes).unwrap();
                captured
                    .lock()
                    .unwrap()
                    .push((target.into(), serde_json::from_slice(&bytes).unwrap()));
                let response = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected SDK request/retry");
                if let Some((status, body)) = response {
                    let body = body.to_string();
                    write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
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
    fn reply(&self, status: u16, value: Value) {
        self.replies
            .lock()
            .unwrap()
            .push_back(Some((status, value)));
    }
    fn rows(&self, control: &Item, item: Option<&Item>) {
        self.reply(
            200,
            json!({"Responses": [
                {"Item": wire(control)}, item.map(|v| json!({"Item": wire(v)})).unwrap_or(json!({}))
            ]}),
        );
    }
    fn conflict(&self) {
        self.reply(400, json!({"__type":"TransactionCanceledException", "Message":"fresh guard rejected",
            "CancellationReasons":[{"Code":"None"},{"Code":"ConditionalCheckFailed","Message":"revision changed"}]}));
    }
    fn requests(&self) -> Vec<(String, Value)> {
        self.requests.lock().unwrap().clone()
    }
    fn client(&self) -> aws_sdk_dynamodb::Client {
        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(Credentials::new(
                    "fixture", "fixture", None, None, "fixture",
                ))
                .endpoint_url(format!("http://{}", self.address))
                .retry_config(RetryConfig::standard().with_max_attempts(5))
                .build(),
        )
    }
    async fn journal(&self) -> CommandJournal {
        for (name, arn) in [
            ("coord", ARN),
            ("application", APP),
            (
                "second",
                "arn:aws:dynamodb:us-west-2:123456789012:table/second",
            ),
        ] {
            self.reply(200, json!({"Table": {"TableName":name,"TableArn":arn,"TableStatus":"ACTIVE",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
        }
        let client = self.client();
        let tables =
            EnrolledTables::enroll(&client, "coord", &["application".into(), "second".into()])
                .await
                .unwrap();
        CommandJournal::new(client, tables, domain())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        self.worker.take().unwrap().join().unwrap();
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "unused fixture responses"
        );
    }
}

#[tokio::test]
async fn full_coherent_observation_and_exact_two_action_wire_without_receipt() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    for pending in [false, true] {
        let mut control = control();
        if pending {
            control.insert("compaction_candidate".into(), n(2));
            control.insert("compaction_not_before_ms".into(), n(1100));
        }
        let mut full_item = wire(&item());
        full_item["future_app_field"] = json!({"M":{"nested":{"L":[{"BOOL":true}]}}});
        fixture.reply(
            200,
            json!({"Responses":[{"Item":wire(&control)}, {"Item":full_item}]}),
        );
        let observed = journal
            .observe_application_pruning_candidate(get())
            .await
            .unwrap();
        assert_eq!(observed.fence().compacted_through(), 1);
        assert!(observed.item().unwrap().contains_key("future_app_field"));
        fixture.reply(200, json!({}));
        let mut native = delete();
        native.table_name = APP.into(); // aliases bind to identical canonical target
        assert_eq!(
            journal
                .prune_application_item(observed, native, &[scope(1)])
                .await
                .unwrap(),
            PruningOutcome::Removed
        );
        let requests = fixture.requests();
        let (kind, read) = &requests[requests.len() - 2];
        assert!(kind.ends_with("transactgetitems"));
        let gets = read["TransactItems"].as_array().unwrap();
        assert_eq!(gets.len(), 2);
        assert_eq!(
            gets[0],
            json!({"Get":{"TableName":ARN,"Key":wire(&control_key(&domain()))}})
        );
        assert_eq!(gets[1], json!({"Get":{"TableName":APP,"Key":wire(&key())}}));
        let (kind, write) = requests.last().unwrap();
        assert!(kind.ends_with("transactwriteitems"));
        // Preserve the native SDK's ephemeral idempotency facility. It is not
        // a StoreLink command ID or a durable receipt that needs later cleanup.
        assert!(
            write["ClientRequestToken"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
        );
        assert_eq!(write.as_object().unwrap().len(), 2);
        let actions = write["TransactItems"].as_array().unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].as_object().unwrap().len(), 1);
        assert_eq!(actions[1].as_object().unwrap().len(), 1);
        assert_full_control_guard(&actions[0]["ConditionCheck"], &control);
        assert_eq!(
            actions[1]["Delete"],
            json!({
                "TableName":APP,"Key":wire(&key()),
                "ConditionExpression":delete().condition_expression().unwrap(),
                "ExpressionAttributeNames":{"#state":"state","#revision":"revision"},
                "ExpressionAttributeValues":{":terminal":{"S":"COMMITTED"},":revision":{"S":"unique-revision-1"}},
                "ReturnValuesOnConditionCheckFailure":"ALL_OLD"
            })
        );
    }
    assert_eq!(fixture.requests().len(), 7); // 3 enrollment + 2*(one read + one write)
}

fn assert_full_control_guard(check: &Value, old: &Item) {
    assert_eq!(check["TableName"], ARN);
    assert_eq!(check["Key"], wire(&control_key(&domain())));
    let names = check["ExpressionAttributeNames"].as_object().unwrap();
    let values = check["ExpressionAttributeValues"].as_object().unwrap();
    let expression = check["ConditionExpression"].as_str().unwrap();
    let terms: Vec<_> = expression.split(" AND ").collect();
    // Shared pruning guard also retains uploads' two absent optional guards.
    assert_eq!(terms.len(), FIELDS.len() + 2);
    for field in FIELDS {
        let (alias, _) = names
            .iter()
            .find(|(_, value)| value.as_str() == Some(field))
            .unwrap();
        if let Some(value) = wire(old).get(field) {
            let term = terms
                .iter()
                .find(|term| term.starts_with(&format!("{alias} = ")))
                .unwrap();
            let parameter = term.split_once(" = ").unwrap().1;
            assert_eq!(&values[parameter], value);
        } else {
            assert!(terms.contains(&format!("attribute_not_exists({alias})").as_str()));
        }
    }
}

#[tokio::test]
async fn projection_aliases_reserved_and_unenrolled_targets_fail_without_reads() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    for projection in ["", "logical_id"] {
        let mut get = get();
        get.projection_expression = Some(projection.into());
        assert!(matches!(
            journal.observe_application_pruning_candidate(get).await,
            Err(ApplicationPruningError::ProjectedRead)
        ));
    }
    let mut aliased = get();
    aliased.expression_attribute_names = Some(HashMap::new());
    assert!(matches!(
        journal.observe_application_pruning_candidate(aliased).await,
        Err(ApplicationPruningError::ProjectedRead)
    ));
    for table in ["coord", ARN, "unknown"] {
        let mut get = get();
        get.table_name = table.into();
        assert!(matches!(
            journal.observe_application_pruning_candidate(get).await,
            Err(ApplicationPruningError::Journal(JournalError::Enrollment(
                EnrollmentError::ForbiddenRead
            )))
        ));
        let mut delete = delete();
        delete.table_name = table.into();
        assert!(matches!(
            journal
                .prune_application_item(observation(false), delete, &[scope(1)])
                .await,
            Err(ApplicationPruningError::Journal(JournalError::Enrollment(
                EnrollmentError::ForbiddenAction { .. }
            )))
        ));
    }
    assert_eq!(fixture.requests().len(), 3);
}

#[tokio::test]
async fn incomplete_or_malformed_control_is_never_absence() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    for rows in [json!([]), json!([{}]), json!([{}, {}]), json!([{}, {}, {}])] {
        fixture.reply(200, json!({"Responses":rows}));
        assert!(
            journal
                .observe_application_pruning_candidate(get())
                .await
                .is_err()
        );
    }
    for (field, value) in [
        ("revision", n(0)),
        ("compacted_through", n(3)),
        ("resolution_retention_ms", n(0)),
        ("resolution_retention_ms", s("100")),
        ("resolution_retention_ms", n(u64::MAX)),
        ("compaction_candidate", n(2)),
        ("compaction_not_before_ms", n(1100)),
        ("domain", s("foreign")),
        ("incarnation", s(Uuid::nil().to_string())),
        ("unrecognized_authority", s("danger")),
        ("pk", s("wrong-key")),
    ] {
        let mut row = control();
        row.insert(field.into(), value);
        fixture.rows(&row, None);
        assert!(
            journal
                .observe_application_pruning_candidate(get())
                .await
                .is_err(),
            "{field}"
        );
    }
    for field in [
        "pk",
        "sk",
        "domain",
        "incarnation",
        "current_generation",
        "closed_through",
        "compacted_through",
        "revision",
    ] {
        let mut row = control();
        row.remove(field);
        fixture.rows(&row, None);
        assert!(
            journal
                .observe_application_pruning_candidate(get())
                .await
                .is_err(),
            "missing {field}"
        );
    }
    fixture.rows(&control(), None);
    let absent = journal
        .observe_application_pruning_candidate(get())
        .await
        .unwrap();
    assert!(absent.item().is_none());
    assert_eq!(
        journal
            .prune_application_item(absent, delete(), &[scope(1)])
            .await
            .unwrap(),
        PruningOutcome::ObservationAbsent
    );
    assert!(
        fixture
            .requests()
            .iter()
            .all(|(kind, _)| !kind.ends_with("transactwriteitems"))
    );
}

#[tokio::test]
async fn target_and_control_binding_precede_absence() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    for present in [false, true] {
        for wrong_key in [false, true] {
            let mut delete = delete();
            if wrong_key {
                delete.key.insert("logical_id".into(), s("other"));
            } else {
                delete.table_name = "second".into();
            }
            assert!(matches!(
                journal
                    .prune_application_item(observation(present), delete, &[scope(1)])
                    .await,
                Err(ApplicationPruningError::TargetMismatch)
            ));
        }
        let foreign_domain = CommandJournal::new(
            fixture.client(),
            journal.enrolled_tables().clone(),
            DomainId::new("other").unwrap(),
        );
        assert!(matches!(
            foreign_domain
                .prune_application_item(observation(present), delete(), &[scope(1)])
                .await,
            Err(ApplicationPruningError::TargetMismatch)
        ));
        fixture.reply(200, json!({"Table":{"TableName":"second",
            "TableArn":"arn:aws:dynamodb:us-west-2:123456789012:table/second", "TableStatus":"ACTIVE",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
        let foreign_coord = CommandJournal::new(
            fixture.client(),
            EnrolledTables::enroll(&fixture.client(), "second", &[])
                .await
                .unwrap(),
            domain(),
        );
        assert!(matches!(
            foreign_coord
                .prune_application_item(observation(present), delete(), &[scope(1)])
                .await,
            Err(ApplicationPruningError::TargetMismatch)
        ));
    }
    assert_eq!(fixture.requests().len(), 5); // enrollment only
}

#[tokio::test]
async fn declarations_conditions_and_published_frontier_are_mandatory() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    for present in [false, true] {
        for condition in [None, Some(""), Some("   ")] {
            let mut delete = delete();
            delete.condition_expression = condition.map(str::to_owned);
            assert!(matches!(
                journal
                    .prune_application_item(observation(present), delete, &[scope(1)])
                    .await,
                Err(ApplicationPruningError::MissingCondition)
            ));
        }
        assert!(matches!(
            journal
                .prune_application_item(observation(present), delete(), &[])
                .await,
            Err(ApplicationPruningError::MissingScopes)
        ));
        for generation in [2, 3, 4, 5] {
            // closed-but-not-compacted, open, current, future
            assert!(matches!(
                journal
                    .prune_application_item(
                        observation(present),
                        delete(),
                        &[scope(1), scope(generation)]
                    )
                    .await,
                Err(ApplicationPruningError::Pruning(
                    PruningError::BeforeWatermark
                ))
            ));
        }
        let foreign = Scope::new(DomainId::new("foreign").unwrap(), Uuid::from_u128(2), 1).unwrap();
        assert!(matches!(
            journal
                .prune_application_item(observation(present), delete(), &[foreign])
                .await,
            Err(ApplicationPruningError::Pruning(PruningError::WrongDomain))
        ));
    }
    let historical = Scope::new(domain(), Uuid::from_u128(2), 999).unwrap();
    fixture.reply(200, json!({}));
    assert_eq!(
        journal
            .prune_application_item(observation(true), delete(), &[scope(1), historical])
            .await
            .unwrap(),
        PruningOutcome::Removed
    );
    assert_eq!(fixture.requests().len(), 4);
}

#[tokio::test]
async fn native_validation_and_one_attempt_ambiguity_conflict_preserve_diagnostics() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    for lost in [false, true] {
        let before = fixture.requests().len();
        if lost {
            fixture.replies.lock().unwrap().push_back(None);
        } else {
            fixture.reply(
                500,
                json!({"__type":"InternalServerError", "Message":"ambiguous write"}),
            );
        }
        let error = journal
            .prune_application_item(observation(true), delete(), &[scope(1)])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApplicationPruningError::NativeWrite {
                outcome: PruningError::OutcomeUnknown,
                ..
            }
        ));
        if !lost {
            assert!(error.to_string().contains("ambiguous write"));
        }
        assert_eq!(fixture.requests().len(), before + 1);
    }
    fixture.conflict();
    let error = journal
        .prune_application_item(observation(true), delete(), &[scope(1)])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ApplicationPruningError::NativeWrite {
            outcome: PruningError::Conflict,
            ..
        }
    ));
    assert!(error.to_string().contains("fresh guard rejected"));
    fixture.reply(
        400,
        json!({"__type":"ValidationException", "Message":"invalid condition syntax"}),
    );
    let mut invalid = delete();
    invalid.condition_expression = Some("THIS IS NOT DYNAMODB".into());
    let error = journal
        .prune_application_item(observation(true), invalid, &[scope(1)])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ApplicationPruningError::NativeWrite {
            outcome: PruningError::OutcomeUnknown,
            ..
        }
    ));
    assert!(
        error
            .to_string()
            .contains("ValidationException: invalid condition syntax")
    );
    for (status, code) in [(400, "ValidationException"), (500, "InternalServerError")] {
        let before = fixture.requests().len();
        fixture.reply(
            status,
            json!({"__type":code, "Message":"native read diagnostic"}),
        );
        let mut malformed = get();
        malformed.key.clear();
        let error = journal
            .observe_application_pruning_candidate(malformed)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApplicationPruningError::Journal(JournalError::Native(_))
        ));
        assert!(error.to_string().contains(code));
        assert!(error.to_string().contains("native read diagnostic"));
        assert_eq!(fixture.requests().len(), before + 1);
    }
}

#[tokio::test]
async fn application_action_nesting_bounds_are_reused() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    let mut nested = s("value");
    for _ in 0..34 {
        nested = AttributeValue::L(vec![nested]);
    }
    let mut delete = delete();
    delete
        .expression_attribute_values
        .as_mut()
        .unwrap()
        .insert(":deep".into(), nested);
    assert!(matches!(
        journal
            .prune_application_item(observation(true), delete, &[scope(1)])
            .await,
        Err(ApplicationPruningError::Journal(JournalError::Actions(
            ActionError::NestingLimit
        )))
    ));
    assert_eq!(fixture.requests().len(), 3);
}

#[tokio::test]
async fn current_state_aba_and_control_rotation_keep_actual_native_request_guards() {
    let fixture = Fixture::new();
    let journal = fixture.journal().await;
    // Script application state A -> B -> A with a different revision, including
    // a newly protected input. No bespoke evaluator: inspect actual native CAS
    // operands and return the service's conditional-failure response explicitly.
    fixture.rows(&control(), Some(&item()));
    let stale = journal
        .observe_application_pruning_candidate(get())
        .await
        .unwrap();
    let mut current = item();
    current.insert("state".into(), s("OPEN"));
    current.insert("revision".into(), s("unique-revision-2"));
    current.insert("protected_input".into(), s("new-input"));
    current.insert("state".into(), s("COMMITTED"));
    current.insert("revision".into(), s("unique-revision-3"));
    fixture.conflict();
    assert!(matches!(
        journal
            .prune_application_item(stale, delete(), &[scope(1)])
            .await,
        Err(ApplicationPruningError::NativeWrite {
            outcome: PruningError::Conflict,
            ..
        })
    ));
    let request = fixture.requests().last().unwrap().1.clone();
    assert_ne!(
        request["TransactItems"][1]["Delete"]["ExpressionAttributeValues"][":revision"],
        wire(&current)["revision"]
    );
    assert_eq!(
        request["TransactItems"][1]["Delete"]["ConditionExpression"],
        delete().condition_expression().unwrap()
    );
    // Reobservation does not release the new protected input for the application.
    fixture.rows(&control(), Some(&current));
    let fresh = journal
        .observe_application_pruning_candidate(get())
        .await
        .unwrap();
    assert!(fresh.item().unwrap().contains_key("protected_input"));
    drop(fresh); // caller must perform counted releases, not blindly prune

    fixture.rows(&control(), Some(&item()));
    let stale = journal
        .observe_application_pruning_candidate(get())
        .await
        .unwrap();
    let mut rotated = control();
    rotated.insert("incarnation".into(), s(Uuid::from_u128(3).to_string()));
    rotated.insert("revision".into(), n(8));
    fixture.reply(
        400,
        json!({"__type":"TransactionCanceledException", "Message":"control rotated",
        "CancellationReasons":[{"Code":"ConditionalCheckFailed"},{"Code":"None"}]}),
    );
    assert!(matches!(
        journal
            .prune_application_item(stale, delete(), &[scope(1)])
            .await,
        Err(ApplicationPruningError::NativeWrite {
            outcome: PruningError::Conflict,
            ..
        })
    ));
    let request = fixture.requests().last().unwrap().1.clone();
    assert_full_control_guard(&request["TransactItems"][0]["ConditionCheck"], &control());
    assert_ne!(control()["incarnation"], rotated["incarnation"]);
    // An acknowledged write can lose its reply; subsequent absence remains just
    // ObservationAbsent, not success/cancellation attribution to that write.
    fixture.replies.lock().unwrap().push_back(None);
    assert!(matches!(
        journal
            .prune_application_item(observation(true), delete(), &[scope(1)])
            .await,
        Err(ApplicationPruningError::NativeWrite {
            outcome: PruningError::OutcomeUnknown,
            ..
        })
    ));
    fixture.rows(&control(), None);
    let absent = journal
        .observe_application_pruning_candidate(get())
        .await
        .unwrap();
    assert_eq!(
        journal
            .prune_application_item(absent, delete(), &[scope(1)])
            .await
            .unwrap(),
        PruningOutcome::ObservationAbsent
    );
}
