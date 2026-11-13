//! Native SDK + fixed loopback HTTP responses, not an S3 emulator or cloud test.
#![cfg(feature = "s3")]

use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
};
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
    identity::{NamespaceId, StoreId},
    s3::{
        LifecycleError, NamespaceConfiguration, NamespaceConfigurationSpec, QualificationError,
        qualify_existing, qualify_fresh,
    },
};

fn spec() -> NamespaceConfigurationSpec {
    NamespaceConfigurationSpec {
        id: NamespaceId::new("test-namespace").unwrap(),
        store: StoreId::new("test-store").unwrap(),
        bucket: "storelink-fixture".into(),
        expected_owner: "123456789012".into(),
        region: "us-west-2".into(),
        key_prefix: "managed/".into(),
        mpu_max_age_ms: 3600000,
        mpu_lifecycle_margin_ms: 3600000,
    }
}
fn configuration() -> NamespaceConfiguration {
    NamespaceConfiguration::new(spec()).unwrap()
}
struct Reply {
    status: u16,
    headers: String,
    body: String,
}
impl Reply {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: body.into(),
        }
    }
    fn error(status: u16, code: &str) -> Self {
        Self {
            status,
            headers: String::new(),
            body: format!("<Error><Code>{code}</Code><Message>fixture</Message></Error>"),
        }
    }
}
struct Fixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    worker: Option<JoinHandle<()>>,
    expected_count: usize,
}
impl Fixture {
    fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let expected_count = replies.len();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (responses, captured, stopping) = (replies.clone(), requests.clone(), stop.clone());
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
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                    assert!(request.len() <= 32768);
                }
                let request = String::from_utf8(request).unwrap();
                let head = request.starts_with("HEAD ");
                captured.lock().unwrap().push(request);
                let reply = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| Reply::error(500, "UnexpectedRequest"));
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n{}\r\n{}",
                    reply.status,
                    if head { 0 } else { reply.body.len() },
                    reply.headers,
                    if head { "" } else { &reply.body }
                );
                socket.write_all(response.as_bytes()).unwrap();
            }
        });
        Self {
            address,
            stop,
            requests,
            replies,
            worker: Some(worker),
            expected_count,
        }
    }
    fn client(&self, region: &str) -> Client {
        Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new(region.to_owned()))
                .credentials_provider(Credentials::new(
                    "fixture-access",
                    "fixture-secret",
                    None,
                    None,
                    "loopback-only",
                ))
                .endpoint_url(format!("http://{}", self.address))
                .force_path_style(true)
                .retry_config(RetryConfig::disabled())
                .timeout_config(
                    TimeoutConfig::builder()
                        .operation_timeout(Duration::from_secs(5))
                        .build(),
                )
                .build(),
        )
    }
    fn assert_finished(&self) -> Vec<String> {
        assert!(
            self.replies.lock().unwrap().is_empty(),
            "not all fixed replies consumed"
        );
        let requests = self.requests.lock().unwrap().clone();
        assert_eq!(
            requests.len(),
            self.expected_count,
            "unexpected additional request"
        );
        for request in &requests {
            assert!(
                request.starts_with("GET /storelink-fixture")
                    || request.starts_with("HEAD /storelink-fixture"),
                "mutating/unexpected request: {request}"
            );
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("\r\nx-amz-expected-bucket-owner: 123456789012\r\n"),
                "owner missing: {request}"
            );
            assert!(
                request.contains("Credential=fixture-access/"),
                "not explicitly supplied fixture credentials"
            );
        }
        requests
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address); // Wake blocking accept; never poll/sleep.
        let result = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}
fn profile() -> Vec<Reply> {
    vec![
        Reply {
            status: 200,
            headers: "x-amz-bucket-region: us-west-2\r\n".into(),
            body: String::new(),
        },
        Reply::ok("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
        Reply::ok(
            "<LifecycleConfiguration><Rule><ID>mpu</ID><Status>Enabled</Status><Filter><Prefix>managed/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
        ),
        Reply::error(404, "ObjectLockConfigurationNotFoundError"),
    ]
}
fn versions(extra: &str) -> Reply {
    Reply::ok(format!(
        "<ListVersionsResult><Name>storelink-fixture</Name><Prefix>managed%2F</Prefix><EncodingType>url</EncodingType><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>{extra}</ListVersionsResult>"
    ))
}
fn uploads(extra: &str) -> Reply {
    Reply::ok(format!(
        "<ListMultipartUploadsResult><Bucket>storelink-fixture</Bucket><Prefix>managed%2F</Prefix><EncodingType>url</EncodingType><MaxUploads>1</MaxUploads><IsTruncated>false</IsTruncated>{extra}</ListMultipartUploadsResult>"
    ))
}

#[test]
fn checked_configuration_bounds_and_no_key_normalization() {
    for bucket in [
        "ab",
        "BadBucket",
        "a_b",
        "a..b",
        "-abc",
        "abc-",
        "127.0.0.1",
        "999.999.999.999",
        "xn--abc",
        "sthree-test",
        "amzn-s3-demo-test",
        "abc-s3alias",
        "abc--ol-s3",
        "abc.mrap",
        "abc--x-s3",
        "abc--table-s3",
        "arn:aws:s3:::bucket",
    ] {
        let mut s = spec();
        s.bucket = bucket.into();
        assert!(NamespaceConfiguration::new(s).is_err(), "{bucket}");
    }
    for length in [3, 63] {
        let mut s = spec();
        s.bucket = "a".repeat(length);
        assert!(NamespaceConfiguration::new(s).is_ok());
    }
    let mut s = spec();
    s.bucket = "a".repeat(64);
    assert!(NamespaceConfiguration::new(s).is_err());
    for owner in [
        "",
        "12345678901",
        "1234567890123",
        "12345678901x",
        "１２３４５６７８９０１２",
    ] {
        let mut s = spec();
        s.expected_owner = owner.into();
        assert!(NamespaceConfiguration::new(s).is_err());
    }
    for region in [
        "",
        "US-WEST-2",
        "us west 2",
        "us-west-",
        "us--west-2",
        "http://localhost",
    ] {
        let mut s = spec();
        s.region = region.into();
        assert!(NamespaceConfiguration::new(s).is_err());
    }
    for (age, margin) in [(0, 1), (1, 0), (u64::MAX, 1), (1, u64::MAX)] {
        let mut s = spec();
        s.mpu_max_age_ms = age;
        s.mpu_lifecycle_margin_ms = margin;
        assert!(matches!(
            NamespaceConfiguration::new(s),
            Err(QualificationError::Lifecycle(
                LifecycleError::InvalidAgePolicy
            ))
        ));
    }
    for prefix in [
        "".to_owned(),
        "é".repeat(512),
        "//../+ %\u{1}\nimage".into(),
    ] {
        let mut s = spec();
        s.key_prefix = prefix.clone();
        assert_eq!(NamespaceConfiguration::new(s).unwrap().key_prefix(), prefix);
    }
    let mut s = spec();
    s.key_prefix = "é".repeat(513);
    assert!(NamespaceConfiguration::new(s).is_err());
    let mut s = spec();
    s.mpu_max_age_ms = u64::MAX - 1;
    s.mpu_lifecycle_margin_ms = 1;
    assert!(NamespaceConfiguration::new(s).is_ok());
}

#[tokio::test]
async fn native_fresh_checks_all_read_only_requests_and_exact_prefix_limits() {
    let mut replies = profile();
    replies.extend([versions(""), uploads("")]);
    let fixture = Fixture::new(replies);
    let fresh = qualify_fresh(&fixture.client("us-west-2"), &configuration())
        .await
        .unwrap();
    assert_eq!(fresh.configuration().id(), &spec().id);
    assert_eq!(fresh.lifecycle().covering_abort_days, 3);
    let requests = fixture.assert_finished();
    for (index, operation) in [(1, "versioning"), (2, "lifecycle"), (3, "object-lock")] {
        assert!(requests[index].lines().next().unwrap().contains(operation));
    }
    for (index, limit) in [(4, "max-keys=1"), (5, "max-uploads=1")] {
        let line = requests[index].lines().next().unwrap();
        assert!(line.contains("prefix=managed%2F"), "{line}");
        assert!(line.contains("encoding-type=url"));
        assert!(line.contains(limit));
        assert!(!line.contains("delimiter") && !line.contains("marker"));
    }
    assert!(requests[4].lines().next().unwrap().contains("versions"));
    assert!(requests[5].lines().next().unwrap().contains("uploads"));
}

#[tokio::test]
async fn native_fresh_preserves_non_path_utf8_prefix_bytes() {
    let mut input = spec();
    input.key_prefix = "é/+ %\u{1}../".into();
    let config = NamespaceConfiguration::new(input).unwrap();
    let encoded = "%C3%A9%2F%2B%20%25%01..%2F";
    let mut replies = profile();
    replies[2].body = replies[2]
        .body
        .replace("<Prefix>managed/</Prefix>", "<Prefix></Prefix>");
    let mut version_reply = versions("");
    version_reply.body = version_reply.body.replace("managed%2F", encoded);
    let mut upload_reply = uploads("");
    upload_reply.body = upload_reply.body.replace("managed%2F", encoded);
    replies.extend([version_reply, upload_reply]);
    let fixture = Fixture::new(replies);
    let token = qualify_fresh(&fixture.client("us-west-2"), &config)
        .await
        .unwrap();
    assert_eq!(token.configuration().key_prefix(), "é/+ %\u{1}../");
    let requests = fixture.assert_finished();
    for request in &requests[4..] {
        assert!(
            request
                .lines()
                .next()
                .unwrap()
                .contains(&format!("prefix={encoded}"))
        );
    }
}

#[tokio::test]
async fn existing_health_does_not_require_empty_scope_or_database() {
    let fixture = Fixture::new(profile());
    let qualified = qualify_existing(&fixture.client("us-west-2"), &configuration())
        .await
        .unwrap();
    assert_eq!(qualified.configuration().store(), &spec().store);
    assert_eq!(fixture.assert_finished().len(), 4); // No history/MPU probe.
}

#[tokio::test]
async fn qualification_does_not_request_optional_intelligent_tiering_configuration() {
    for fresh in [false, true] {
        // No Intelligent-Tiering response is supplied. Success does not establish
        // that restore-required optional archive tiers are absent.
        let mut replies = profile();
        if fresh {
            replies.extend([versions(""), uploads("")]);
        }
        let fixture = Fixture::new(replies);
        let client = fixture.client("us-west-2");
        if fresh {
            qualify_fresh(&client, &configuration()).await.unwrap();
        } else {
            qualify_existing(&client, &configuration()).await.unwrap();
        }
        for request in fixture.assert_finished() {
            assert!(
                !request
                    .lines()
                    .next()
                    .unwrap()
                    .contains("intelligent-tiering"),
                "unexpected optional tiering inspection: {request}"
            );
        }
    }
}

#[tokio::test]
async fn client_and_actual_bucket_region_must_match_and_redirects_fail() {
    let fixture = Fixture::new(vec![]);
    assert!(matches!(
        qualify_existing(&fixture.client("eu-west-1"), &configuration()).await,
        Err(QualificationError::RegionMismatch)
    ));
    fixture.assert_finished();
    for header in ["x-amz-bucket-region: eu-west-1\r\n", ""] {
        let fixture = Fixture::new(vec![Reply {
            status: 200,
            headers: header.into(),
            body: String::new(),
        }]);
        assert!(matches!(
            qualify_existing(&fixture.client("us-west-2"), &configuration()).await,
            Err(QualificationError::RegionMismatch)
        ));
        fixture.assert_finished();
    }
    let fixture = Fixture::new(vec![Reply {
        status: 301,
        headers: "x-amz-bucket-region: eu-west-1\r\n".into(),
        body: String::new(),
    }]);
    assert!(
        qualify_existing(&fixture.client("us-west-2"), &configuration())
            .await
            .is_err()
    );
    fixture.assert_finished();
}

#[tokio::test]
async fn owner_denial_is_not_bucket_qualification() {
    let fixture = Fixture::new(vec![Reply::error(403, "AccessDenied")]);
    assert!(matches!(
        qualify_existing(&fixture.client("us-west-2"), &configuration()).await,
        Err(QualificationError::Request {
            operation: "HeadBucket",
            ..
        })
    ));
    fixture.assert_finished(); // Every request actually carried the configured owner.
}

#[tokio::test]
async fn actual_versioning_and_lifecycle_are_required_before_fresh_probes() {
    let mut replies = profile();
    replies.truncate(3);
    replies[1] =
        Reply::ok("<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>");
    let fixture = Fixture::new(replies);
    assert!(matches!(
        qualify_fresh(&fixture.client("us-west-2"), &configuration()).await,
        Err(QualificationError::Lifecycle(
            LifecycleError::VersioningRequired
        ))
    ));
    fixture.assert_finished();
    let mut replies = profile();
    replies.truncate(3);
    replies[2] = Reply::ok("<LifecycleConfiguration/>");
    let fixture = Fixture::new(replies);
    assert!(matches!(
        qualify_fresh(&fixture.client("us-west-2"), &configuration()).await,
        Err(QualificationError::Lifecycle(
            LifecycleError::MissingMultipartCoverage
        ))
    ));
    fixture.assert_finished();
}

#[tokio::test]
async fn native_lifecycle_uses_the_same_explicit_mpu_age_and_margin() {
    let mut input = spec();
    input.mpu_max_age_ms = 3 * 86400000;
    input.mpu_lifecycle_margin_ms = 1;
    let config = NamespaceConfiguration::new(input).unwrap();
    let mut replies = profile();
    replies.truncate(3);
    let fixture = Fixture::new(replies);
    assert!(matches!(
        qualify_existing(&fixture.client("us-west-2"), &config).await,
        Err(QualificationError::Lifecycle(
            LifecycleError::AbortBeforeSupportedAge
        ))
    ));
    fixture.assert_finished();
}

#[tokio::test]
async fn only_exact_object_lock_not_configured_is_absence() {
    for (status, code) in [
        (404, "NoSuchBucket"),
        (404, "NotFound"),
        (403, "AccessDenied"),
        (403, "ObjectLockConfigurationNotFoundError"),
        (404, "ObjectLockConfigurationNotFound"),
    ] {
        let mut replies = profile();
        replies.truncate(4);
        replies[3] = Reply::error(status, code);
        let fixture = Fixture::new(replies);
        assert!(
            matches!(
                qualify_existing(&fixture.client("us-west-2"), &configuration()).await,
                Err(QualificationError::Request {
                    operation: "GetObjectLockConfiguration",
                    ..
                })
            ),
            "{status} {code}"
        );
        fixture.assert_finished();
    }
    for body in [
        "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
        "<ObjectLockConfiguration/>",
        "<ObjectLockConfiguration><ObjectLockEnabled>Future</ObjectLockEnabled></ObjectLockConfiguration>",
    ] {
        let mut replies = profile();
        replies.truncate(4);
        replies[3] = Reply::ok(body);
        let fixture = Fixture::new(replies);
        assert!(
            qualify_existing(&fixture.client("us-west-2"), &configuration())
                .await
                .is_err()
        );
        fixture.assert_finished();
    }
}

#[tokio::test]
async fn native_lifecycle_allows_ordinary_intelligent_tiering() {
    let mut replies = profile();
    replies[2] = Reply::ok(
        "<LifecycleConfiguration><Rule><ID>instant</ID><Status>Enabled</Status><Filter><Prefix>managed/</Prefix></Filter><Transition><Days>0</Days><StorageClass>INTELLIGENT_TIERING</StorageClass></Transition><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
    );
    let fixture = Fixture::new(replies);
    qualify_existing(&fixture.client("us-west-2"), &configuration())
        .await
        .unwrap();
    fixture.assert_finished();
}

#[tokio::test]
async fn fresh_rejects_versions_delete_markers_and_uploads_without_cleanup() {
    for entry in [
        "<Version><Key>managed/a</Key><VersionId>v1</VersionId></Version>",
        "<DeleteMarker><Key>managed/a</Key><VersionId>d1</VersionId></DeleteMarker>",
    ] {
        let mut replies = profile();
        replies.push(versions(entry));
        let fixture = Fixture::new(replies);
        assert!(matches!(
            qualify_fresh(&fixture.client("us-west-2"), &configuration()).await,
            Err(QualificationError::ScopeNotEmpty)
        ));
        fixture.assert_finished();
    }
    let mut replies = profile();
    replies.extend([
        versions(""),
        uploads("<Upload><Key>managed/a</Key><UploadId>mpu1</UploadId></Upload>"),
    ]);
    let fixture = Fixture::new(replies);
    assert!(matches!(
        qualify_fresh(&fixture.client("us-west-2"), &configuration()).await,
        Err(QualificationError::ScopeNotEmpty)
    ));
    fixture.assert_finished();
}

#[tokio::test]
async fn fresh_rejects_incomplete_ambiguous_listing_without_continuing_or_mutating() {
    for multipart in [false, true] {
        for change in [
            "truncated",
            "missing-truncated",
            "wrong-prefix",
            "next-marker",
            "common-prefix",
            "wrong-bucket",
            "wrong-limit",
        ] {
            let mut reply = if multipart { uploads("") } else { versions("") };
            reply.body = match change {
                "truncated" => reply.body.replace(
                    "<IsTruncated>false</IsTruncated>",
                    "<IsTruncated>true</IsTruncated>",
                ),
                "missing-truncated" => reply.body.replace("<IsTruncated>false</IsTruncated>", ""),
                "wrong-prefix" => reply.body.replace("managed%2F", "other%2F"),
                "next-marker" => reply.body.replace(
                    "<IsTruncated>",
                    "<NextKeyMarker>later</NextKeyMarker><IsTruncated>",
                ),
                "common-prefix" => reply.body.replace(
                    "<IsTruncated>",
                    "<CommonPrefixes><Prefix>hidden</Prefix></CommonPrefixes><IsTruncated>",
                ),
                "wrong-bucket" => reply.body.replace("storelink-fixture", "other-bucket"),
                "wrong-limit" => reply.body.replace(">1<", ">0<"),
                _ => unreachable!(),
            };
            let mut replies = profile();
            if multipart {
                replies.push(versions(""));
            }
            replies.push(reply);
            let fixture = Fixture::new(replies);
            assert!(
                qualify_fresh(&fixture.client("us-west-2"), &configuration())
                    .await
                    .is_err(),
                "{multipart} {change}"
            );
            fixture.assert_finished();
        }
    }
}
