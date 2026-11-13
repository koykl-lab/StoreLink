use super::*;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region, retry::RetryConfig};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const A: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/a";
fn domain() -> DomainId {
    DomainId::new("observation-test").unwrap()
}
fn control() -> Value {
    json!({"pk":{"S":format!("{}#CONTROL", prefix(&domain()))},"sk":{"S":"M"},
        "domain":{"S":domain().as_str()},"incarnation":{"S":Uuid::from_u128(1).to_string()},
        "current_generation":{"N":"5"},"closed_through":{"N":"3"},
        "compacted_through":{"N":"2"},"revision":{"N":"1"}})
}
fn get(table: &str, key: &str) -> Get {
    Get::builder()
        .table_name(table)
        .key("pk", s(key))
        .key("sk", s("M"))
        .build()
        .unwrap()
}
struct Fixture {
    journal: CommandJournal,
    requests: Arc<Mutex<Vec<Value>>>,
    server: tokio::task::JoinHandle<()>,
}
async fn fixture(replies: Vec<(u16, Value)>) -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let server = tokio::spawn(async move {
        let mut responses = Vec::new();
        for name in ["coord", "a", "b"] {
            responses.push((200,json!({"Table":{"TableName":name,
                "TableArn":format!("arn:aws:dynamodb:us-west-2:123456789012:table/{name}"),
                "TableStatus":"ACTIVE", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}})));
        }
        responses.extend(replies);
        for (status, reply) in responses {
            let (mut socket, _) =
                tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let request = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    assert!(bytes.len() < 16384);
                    bytes.push(socket.read_u8().await.unwrap());
                }
                let headers = String::from_utf8(bytes).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(length < 128 * 1024);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                serde_json::from_slice::<Value>(&body).unwrap()
            })
            .await
            .unwrap();
            seen.lock().unwrap().push(request);
            let body = serde_json::to_vec(&reply).unwrap();
            let headers = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    let client = Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-west-2"))
            .credentials_provider(Credentials::new(
                "synthetic",
                "synthetic",
                None,
                None,
                "fixture",
            ))
            .endpoint_url(endpoint)
            .retry_config(RetryConfig::disabled())
            .build(),
    );
    let tables = EnrolledTables::enroll(&client, "coord", &["a".into(), "b".into()])
        .await
        .unwrap();
    Fixture {
        journal: CommandJournal::new(client, tables, domain()),
        requests,
        server,
    }
}
#[tokio::test]
async fn ordered_coherent_items_preserve_native_projection_and_absence() {
    let f=fixture(vec![(200,json!({"Responses":[{"Item":control()},{"Item":{"value":{"S":"first"}}},{},{"Item":{"value":{"S":"third"}}}]}))]).await;
    let projected = Get::builder()
        .table_name("a")
        .key("pk", s("one"))
        .key("sk", s("M"))
        .projection_expression("#v")
        .expression_attribute_names("#v", "value")
        .build()
        .unwrap();
    let observed = f
        .journal
        .observe_application_items(vec![projected, get("b", "two"), get(A, "three")])
        .await
        .unwrap();
    assert_eq!(observed.items().len(), 3);
    assert_eq!(observed.items()[0].as_ref().unwrap()["value"], s("first"));
    assert!(observed.items()[1].is_none());
    assert_eq!(observed.items()[2].as_ref().unwrap()["value"], s("third"));
    assert_eq!(observed.fence().compacted_through(), 2);
    f.server.await.unwrap();
    let requests = f.requests.lock().unwrap();
    let items = requests[3]["TransactItems"].as_array().unwrap();
    assert_eq!(items.len(), 4);
    assert_eq!(items[1]["Get"]["TableName"], A);
    assert_eq!(items[3]["Get"]["TableName"], A);
    assert_eq!(items[1]["Get"]["ProjectionExpression"], "#v");
    assert_eq!(items[1]["Get"]["ExpressionAttributeNames"]["#v"], "value");
    assert_eq!(items[2]["Get"]["Key"]["pk"]["S"], "two");
}
#[tokio::test]
async fn bounds_and_every_target_are_validated_before_io() {
    // Keep a live endpoint until a final valid witness request. An invalid
    // call that reaches I/O consumes this response and cannot pass accidentally
    // because a closed listener returned a transport error.
    let f = fixture(vec![(200, json!({"Responses":[{"Item":control()},{}]}))]).await;
    assert!(matches!(f.journal.observe_application_items(vec![]).await,
        Err(JournalError::Corrupt(message)) if message == "application observation requires 1..=99 Gets"));
    assert!(matches!(
        f.journal.observe_application_items((0..100).map(|i| get("a", &i.to_string())).collect()).await,
        Err(JournalError::Corrupt(message)) if message == "application observation requires 1..=99 Gets"
    ));
    for bad in ["coord", "unknown"] {
        assert!(matches!(
            f.journal
                .observe_application_items(vec![get("a", "good"), get(bad, "bad")])
                .await,
            Err(JournalError::Enrollment(EnrollmentError::ForbiddenRead))
        ));
    }
    assert!(
        f.journal
            .observe_application_item(get("a", "witness"))
            .await
            .unwrap()
            .item()
            .is_none()
    );
    f.server.await.unwrap();
    let requests = f.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests[3]["TransactItems"][1]["Get"]["Key"]["pk"]["S"],
        "witness"
    );
}
#[tokio::test]
async fn native_hundred_item_boundary_and_single_item_delegation() {
    let mut rows = vec![json!({"Item":control()})];
    rows.extend((0..99).map(|_| json!({})));
    let f = fixture(vec![
        (200, json!({"Responses":rows})),
        (200, json!({"Responses":[{"Item":control()},{}]})),
    ])
    .await;
    let observed = f
        .journal
        .observe_application_items((0..99).map(|i| get("a", &i.to_string())).collect())
        .await
        .unwrap();
    assert_eq!(observed.items().len(), 99);
    assert!(observed.items().iter().all(Option::is_none));
    let one = f
        .journal
        .observe_application_item(get("a", "single"))
        .await
        .unwrap();
    assert!(one.item().is_none());
    assert_eq!(one.fence(), observed.fence());
    f.server.await.unwrap();
    let requests = f.requests.lock().unwrap();
    assert_eq!(requests[3]["TransactItems"].as_array().unwrap().len(), 100);
    assert_eq!(requests[4]["TransactItems"].as_array().unwrap().len(), 2);
}
#[tokio::test]
async fn incomplete_invalid_control_and_native_validation_never_become_absence() {
    let f=fixture(vec![(200,json!({"Responses":[{"Item":control()}]})),
        (200,json!({"Responses":[{},{}]})),
        (400,json!({"__type":"com.amazonaws.dynamodb.v20120810#ValidationException","message":"invalid projection"}))]).await;
    assert!(
        f.journal
            .observe_application_items(vec![get("a", "x")])
            .await
            .is_err()
    );
    assert!(matches!(
        f.journal
            .observe_application_items(vec![get("a", "x")])
            .await,
        Err(JournalError::MissingControl)
    ));
    let Err(JournalError::Native(message)) = f
        .journal
        .observe_application_items(vec![get("a", "x")])
        .await
    else {
        panic!("native diagnostic required")
    };
    assert!(message.contains("ValidationException") && message.contains("invalid projection"));
    f.server.await.unwrap();
}
