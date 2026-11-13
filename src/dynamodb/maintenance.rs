//! Bounded generation closure and resolution-retention control (protocol §10).
//!
//! This is NOT a scheduler, receipt pruner, orphan collector, resource compactor,
//! or restore API. Publishing a watermark does not delete anything. The host
//! supplies fresh trusted clock samples and schedules progress without traffic.
//! Errors never establish historical rollback; reread current control instead.

use std::collections::HashMap;

use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::types::{AttributeValue, Put};
use thiserror::Error;

use super::journal::{control_key, number, parse_control};
use super::uploads::{check_size, n};
use super::{CommandJournal, JournalError, UploadError};
use crate::clock::{ClockError, ClockObservation, TimestampMs};
use crate::identity::{DomainFence, DomainId, FenceError};

type Item = HashMap<String, AttributeValue>;
const RETENTION: &str = "resolution_retention_ms";
const CANDIDATE: &str = "compaction_candidate";
const NOT_BEFORE: &str = "compaction_not_before_ms";
pub(super) const FIELDS: [&str; 11] = [
    "pk",
    "sk",
    "domain",
    "incarnation",
    "current_generation",
    "closed_through",
    "compacted_through",
    "revision",
    RETENTION,
    CANDIDATE,
    NOT_BEFORE,
];

/// Explicit host policy, with no defaults or deserialization bypass. Retention
/// is immutable within an incarnation; uncertainty is the local clock trust cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenancePolicy {
    resolution_retention_ms: u64,
    maximum_clock_uncertainty_ms: u64,
}
impl MaintenancePolicy {
    pub fn new(
        resolution_retention_ms: u64,
        maximum_clock_uncertainty_ms: u64,
    ) -> Result<Self, MaintenanceError> {
        if resolution_retention_ms == 0 {
            return Err(MaintenanceError::InvalidPolicy);
        }
        TimestampMs::new(resolution_retention_ms)?;
        TimestampMs::new(maximum_clock_uncertainty_ms)?;
        Ok(Self {
            resolution_retention_ms,
            maximum_clock_uncertainty_ms,
        })
    }
    pub fn resolution_retention_ms(&self) -> u64 {
        self.resolution_retention_ms
    }
    pub fn maximum_clock_uncertainty_ms(&self) -> u64 {
        self.maximum_clock_uncertainty_ms
    }
}

/// Current evidence bound to the canonical table ARN and domain, never command
/// authority or proof of which caller performed a historical write. Not
/// deserializable; no maintenance operation accepts a caller-supplied snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceObservation {
    coordination_arn: String,
    fence: DomainFence,
    revision: u64,
    resolution_retention_ms: Option<u64>,
    window: Option<(u64, TimestampMs)>,
}
impl MaintenanceObservation {
    pub fn coordination_arn(&self) -> &str {
        &self.coordination_arn
    }
    pub fn fence(&self) -> &DomainFence {
        &self.fence
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn resolution_retention_ms(&self) -> Option<u64> {
        self.resolution_retention_ms
    }
    /// Fixed candidate and conservative publication deadline, if armed.
    pub fn compaction_window(&self) -> Option<(u64, TimestampMs)> {
        self.window
    }
}

/// Acknowledged writes and current no-write observations are deliberately distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceOutcome {
    Configured,
    AdvancedAndClosed,
    Armed,
    Published,
    /// Observed identical configuration, not proof this caller installed it.
    PolicyAlreadyConfigured,
    NoEligibleBoundary,
    WindowPending,
    NoPendingWindow,
    NotDue,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub outcome: MaintenanceOutcome,
    pub observation: MaintenanceObservation,
}
#[derive(Debug, Error)]
pub enum MaintenanceError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error("retention must be positive")]
    InvalidPolicy,
    #[error("maintenance requires installed retention policy")]
    MissingPolicy,
    #[error("retention policy differs from the immutable installed value")]
    PolicyMismatch,
    #[error("malformed or unsupported maintenance control schema")]
    Corrupt,
    #[error("control record exceeds the 4000-byte bound (including growth reserve)")]
    RecordSize,
    #[error("control revision overflow")]
    RevisionOverflow,
    #[error(
        "native maintenance conditional conflict; reread current control, not historical rollback"
    )]
    Conflict,
    #[error(
        "native maintenance write outcome UNKNOWN; reread current control, never infer rollback"
    )]
    OutcomeUnknown,
}

pub(super) struct Control {
    item: Item,
    pub(super) observation: MaintenanceObservation,
}
impl Control {
    pub(super) fn parse(
        item: Option<Item>,
        domain: &DomainId,
        arn: &str,
    ) -> Result<Self, MaintenanceError> {
        let fence = parse_control(item.as_ref(), domain)?;
        let item = item.ok_or(JournalError::MissingControl)?;
        bounded(&item)?;
        // Closed schema. Unknown authority is not forward-compatible evidence.
        // Future schemas must fence old writers; all legitimate control writes
        // increment revision. Never append authority behind an old revision.
        if item.keys().any(|name| !FIELDS.contains(&name.as_str())) {
            return Err(MaintenanceError::Corrupt);
        }
        let retention = optional_number(&item, RETENTION)?;
        if let Some(retention) = retention {
            if retention == 0 {
                return Err(MaintenanceError::Corrupt);
            }
            TimestampMs::new(retention).map_err(|_| MaintenanceError::Corrupt)?;
        }
        let window = match (
            optional_number(&item, CANDIDATE)?,
            optional_number(&item, NOT_BEFORE)?,
        ) {
            (None, None) => None,
            (Some(candidate), Some(deadline))
                if retention.is_some()
                    && candidate > fence.compacted_through()
                    && candidate <= fence.closed_through() =>
            {
                let deadline = TimestampMs::new(deadline).map_err(|_| MaintenanceError::Corrupt)?;
                // A canonical deadline must be representable as upper + retention.
                if deadline.as_millis() < retention.unwrap() {
                    return Err(MaintenanceError::Corrupt);
                }
                Some((candidate, deadline))
            }
            _ => return Err(MaintenanceError::Corrupt),
        };
        let revision = number(&item, "revision")?;
        Ok(Self {
            item,
            observation: MaintenanceObservation {
                coordination_arn: arn.into(),
                fence,
                revision,
                resolution_retention_ms: retention,
                window,
            },
        })
    }
    fn require_policy(&self, policy: &MaintenancePolicy) -> Result<(), MaintenanceError> {
        match self.observation.resolution_retention_ms {
            None => Err(MaintenanceError::MissingPolicy),
            Some(value) if value != policy.resolution_retention_ms => {
                Err(MaintenanceError::PolicyMismatch)
            }
            Some(_) => Ok(()),
        }
    }
    fn report(self, outcome: MaintenanceOutcome) -> MaintenanceReport {
        MaintenanceReport {
            outcome,
            observation: self.observation,
        }
    }
}
fn optional_number(item: &Item, field: &str) -> Result<Option<u64>, MaintenanceError> {
    item.contains_key(field)
        .then(|| number(item, field))
        .transpose()
        .map_err(Into::into)
}
fn bounded(item: &Item) -> Result<(), MaintenanceError> {
    check_size(item).map_err(|e| match e {
        UploadError::RecordSize => MaintenanceError::RecordSize,
        _ => MaintenanceError::Corrupt,
    })
}

impl CommandJournal {
    /// Strong current observation only. Use after Conflict/OutcomeUnknown; no
    /// observation proves that a particular earlier attempt committed or aborted.
    pub async fn read_maintenance_control(
        &self,
    ) -> Result<MaintenanceObservation, MaintenanceError> {
        Ok(self.maintenance_control().await?.observation)
    }

    /// Install explicit retention only if absent. Identical existing retention is
    /// an observed no-op; a mismatch cannot migrate policy within an incarnation.
    pub async fn configure_compaction_policy(
        &self,
        policy: &MaintenancePolicy,
    ) -> Result<MaintenanceReport, MaintenanceError> {
        let old = self.maintenance_control().await?;
        if old.observation.resolution_retention_ms.is_some() {
            old.require_policy(policy)?;
            return Ok(old.report(MaintenanceOutcome::PolicyAlreadyConfigured));
        }
        let mut next = old.item.clone();
        next.insert(RETENTION.into(), n(policy.resolution_retention_ms));
        self.maintenance_replace(old, next, MaintenanceOutcome::Configured)
            .await
    }

    /// One native CAS advances current and closes through old current. No clock
    /// is sampled: closure may commit arbitrarily late. Pending windows stay fixed.
    pub async fn advance_and_close_generation(
        &self,
        policy: &MaintenancePolicy,
    ) -> Result<MaintenanceReport, MaintenanceError> {
        let old = self.maintenance_control().await?;
        old.require_policy(policy)?;
        let mut fence = old.observation.fence.clone();
        let closing = fence.current();
        fence.advance_generation()?;
        fence.close_through(closing)?;
        let mut next = old.item.clone();
        next.insert("current_generation".into(), n(fence.current()));
        next.insert("closed_through".into(), n(fence.closed_through()));
        self.maintenance_replace(old, next, MaintenanceOutcome::AdvancedAndClosed)
            .await
    }

    /// Sample fresh trusted time ONLY AFTER strongly observing an already closed
    /// eligible boundary. The sampler must read the host clock when invoked, not
    /// return an observation captured earlier. Store upper + retention and freeze
    /// this candidate even if closure subsequently advances.
    pub async fn arm_compaction_window(
        &self,
        policy: &MaintenancePolicy,
        fresh_sampler: &impl Fn() -> Result<ClockObservation, ClockError>,
    ) -> Result<MaintenanceReport, MaintenanceError> {
        let old = self.maintenance_control().await?;
        old.require_policy(policy)?;
        if old.observation.window.is_some() {
            return Ok(old.report(MaintenanceOutcome::WindowPending));
        }
        let fence = &old.observation.fence;
        if fence.closed_through() == fence.compacted_through() {
            return Ok(old.report(MaintenanceOutcome::NoEligibleBoundary));
        }
        let clock = fresh_sampler()?;
        clock.validate(policy.maximum_clock_uncertainty_ms)?;
        let deadline = clock
            .upper_bound()?
            .checked_add(policy.resolution_retention_ms)?;
        let mut next = old.item.clone();
        next.insert(CANDIDATE.into(), n(fence.closed_through()));
        next.insert(NOT_BEFORE.into(), n(deadline.as_millis()));
        self.maintenance_replace(old, next, MaintenanceOutcome::Armed)
            .await
    }

    /// Publish EXACTLY the stored candidate only when a newly sampled trusted
    /// lower bound reaches its fixed deadline. Clears the pair, not retention.
    pub async fn publish_compaction_window(
        &self,
        policy: &MaintenancePolicy,
        fresh_sampler: &impl Fn() -> Result<ClockObservation, ClockError>,
    ) -> Result<MaintenanceReport, MaintenanceError> {
        let old = self.maintenance_control().await?;
        old.require_policy(policy)?;
        let Some((candidate, deadline)) = old.observation.window else {
            return Ok(old.report(MaintenanceOutcome::NoPendingWindow));
        };
        let clock = fresh_sampler()?;
        clock.validate(policy.maximum_clock_uncertainty_ms)?;
        if clock.lower_bound()? < deadline {
            return Ok(old.report(MaintenanceOutcome::NotDue));
        }
        let mut fence = old.observation.fence.clone();
        fence.compact_through(candidate)?;
        let mut next = old.item.clone();
        next.insert("compacted_through".into(), n(fence.compacted_through()));
        next.remove(CANDIDATE);
        next.remove(NOT_BEFORE);
        self.maintenance_replace(old, next, MaintenanceOutcome::Published)
            .await
    }

    async fn maintenance_control(&self) -> Result<Control, MaintenanceError> {
        let response = self
            .native_client()
            .get_item()
            .table_name(self.coordination_arn())
            .set_key(Some(control_key(self.domain_id())))
            .consistent_read(true)
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| JournalError::Native(e.to_string()))?;
        Control::parse(response.item, self.domain_id(), self.coordination_arn())
    }

    async fn maintenance_replace(
        &self,
        old: Control,
        mut next: Item,
        outcome: MaintenanceOutcome,
    ) -> Result<MaintenanceReport, MaintenanceError> {
        let revision = old
            .observation
            .revision
            .checked_add(1)
            .ok_or(MaintenanceError::RevisionOverflow)?;
        next.insert("revision".into(), n(revision));
        let next = Control::parse(Some(next), self.domain_id(), self.coordination_arn())?;
        let put = replacement(self.coordination_arn(), &old.item, next.item.clone())?;
        // One SDK attempt, no maintenance command token, receipt, or retry loop.
        self.native_client()
            .put_item()
            .table_name(put.table_name())
            .set_item(Some(put.item().clone()))
            .set_condition_expression(put.condition_expression)
            .set_expression_attribute_names(put.expression_attribute_names)
            .set_expression_attribute_values(put.expression_attribute_values)
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| {
                if e.as_service_error()
                    .is_some_and(|e| e.is_conditional_check_failed_exception())
                {
                    MaintenanceError::Conflict
                } else {
                    MaintenanceError::OutcomeUnknown
                }
            })?;
        Ok(next.report(outcome))
    }
}

// Native DynamoDB predicates, never evaluated locally. Full equality including
// keys, revision and all present optional fields; explicit absence for each
// missing optional field. Unknown fields were rejected by Control::parse.
fn replacement(table: &str, old: &Item, next: Item) -> Result<Put, MaintenanceError> {
    bounded(&next)?;
    let mut names = HashMap::new();
    let mut values = Item::new();
    let mut terms = Vec::new();
    for (i, field) in FIELDS.iter().enumerate() {
        let alias = format!("#f{i}");
        names.insert(alias.clone(), (*field).into());
        if let Some(value) = old.get(*field) {
            let parameter = format!(":v{i}");
            values.insert(parameter.clone(), value.clone());
            terms.push(format!("{alias} = {parameter}"));
        } else {
            terms.push(format!("attribute_not_exists({alias})"));
        }
    }
    Put::builder()
        .table_name(table)
        .set_item(Some(next))
        .condition_expression(terms.join(" AND "))
        .set_expression_attribute_names(Some(names))
        .set_expression_attribute_values(Some(values))
        .build()
        .map_err(|_| MaintenanceError::Corrupt)
}

#[cfg(test)]
mod tests {
    use super::super::uploads::{guard, s};
    use super::super::{ApplicationActions, EnrolledTables, Outcome};
    use super::*;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use serde_json::{Value, json};
    use std::{
        collections::VecDeque,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread::JoinHandle,
        time::Duration,
    };
    use uuid::Uuid;

    const ARN: &str = "arn:aws:dynamodb:us-west-2:123456789012:table/coord";
    fn domain() -> DomainId {
        DomainId::new("maintenance-unit").unwrap()
    }
    fn policy() -> MaintenancePolicy {
        MaintenancePolicy::new(100, 5).unwrap()
    }
    fn clock(now: u64) -> Result<ClockObservation, ClockError> {
        ClockObservation::trusted(TimestampMs::new(now)?, 5)
    }
    fn row(current: u64, closed: u64, compacted: u64) -> Item {
        let mut row = control_key(&domain());
        row.extend([
            ("domain".into(), s(domain().as_str())),
            ("incarnation".into(), s(Uuid::from_u128(1).to_string())),
            ("current_generation".into(), n(current)),
            ("closed_through".into(), n(closed)),
            ("compacted_through".into(), n(compacted)),
            ("revision".into(), n(1)),
        ]);
        row
    }
    fn configured(current: u64, closed: u64, compacted: u64) -> Item {
        let mut row = row(current, closed, compacted);
        row.insert(RETENTION.into(), n(100));
        row
    }
    fn pending() -> Item {
        let mut row = configured(3, 2, 0);
        row.insert(CANDIDATE.into(), n(1));
        row.insert(NOT_BEFORE.into(), n(1105));
        row
    }
    fn wire(row: &Item) -> Value {
        Value::Object(
            row.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        match v {
                            AttributeValue::S(s) => json!({"S":s}),
                            AttributeValue::N(n) => json!({"N":n}),
                            _ => panic!("flat scalar fixture only"),
                        },
                    )
                })
                .collect(),
        )
    }
    fn from_wire(row: &Value) -> Item {
        row.as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                let value = if let Some(sv) = v.get("S") {
                    s(sv.as_str().unwrap())
                } else {
                    AttributeValue::N(v["N"].as_str().unwrap().into())
                };
                (k.clone(), value)
            })
            .collect()
    }

    // Real SDK transport/serialization with canned service observations. No
    // expression evaluation, database emulation, credentials or cloud calls.
    // None drops the reply after consuming the request (lost response fixture).
    type Replies = Arc<Mutex<VecDeque<Option<(u16, Value)>>>>;
    struct NativeFixture {
        address: std::net::SocketAddr,
        stop: Arc<AtomicBool>,
        replies: Replies,
        requests: Arc<Mutex<Vec<Value>>>,
        worker: Option<JoinHandle<()>>,
    }
    impl NativeFixture {
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
                    let headers = String::from_utf8(headers).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|n| n.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    assert!(length < 1_000_000);
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).unwrap();
                    captured
                        .lock()
                        .unwrap()
                        .push(serde_json::from_slice(&body).unwrap());
                    let response = responses
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("unexpected SDK request");
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
        fn reply(&self, value: Value) {
            self.replies.lock().unwrap().push_back(Some((200, value)));
        }
        fn item(&self, row: &Item) {
            self.reply(json!({"Item":wire(row)}));
        }
        fn error(&self, status: u16, kind: &str) {
            self.replies
                .lock()
                .unwrap()
                .push_back(Some((status, json!({"__type":kind,"message":"fixture"}))));
        }
        fn lost_reply(&self) {
            self.replies.lock().unwrap().push_back(None);
        }
        fn requests(&self) -> Vec<Value> {
            self.requests.lock().unwrap().clone()
        }
        fn last_item(&self) -> Item {
            from_wire(&self.requests().last().unwrap()["Item"])
        }
        async fn journal(&self) -> CommandJournal {
            self.reply(json!({"Table":{"TableName":"coord","TableArn":ARN,"TableStatus":"ACTIVE",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}));
            let client = aws_sdk_dynamodb::Client::from_conf(
                aws_sdk_dynamodb::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new("us-west-2"))
                    .credentials_provider(Credentials::new(
                        "fixture", "fixture", None, None, "fixture",
                    ))
                    .endpoint_url(format!("http://{}", self.address))
                    // Maintenance must override even a retry-enabled supplied client.
                    .retry_config(RetryConfig::standard().with_max_attempts(3))
                    .build(),
            );
            let tables = EnrolledTables::enroll(&client, "coord", &[]).await.unwrap();
            CommandJournal::new(client, tables, domain())
        }
    }
    impl Drop for NativeFixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            self.worker.take().unwrap().join().unwrap();
            assert!(
                self.replies.lock().unwrap().is_empty(),
                "unused canned replies"
            );
        }
    }

    fn assert_full_guard(request: &Value, old: &Item) {
        assert_eq!(request["TableName"], ARN);
        assert!(request.get("ClientRequestToken").is_none());
        let names = request["ExpressionAttributeNames"].as_object().unwrap();
        let values = request["ExpressionAttributeValues"].as_object().unwrap();
        let expression = request["ConditionExpression"].as_str().unwrap();
        let terms: Vec<_> = expression.split(" AND ").collect();
        assert_eq!(terms.len(), FIELDS.len());
        assert_eq!(names.len(), FIELDS.len());
        assert_eq!(values.len(), old.len());
        for (i, field) in FIELDS.iter().enumerate() {
            let alias = format!("#f{i}");
            assert_eq!(names[&alias], *field);
            if old.contains_key(*field) {
                assert!(terms.contains(&format!("{alias} = :v{i}").as_str()));
                assert_eq!(values[&format!(":v{i}")], wire(old)[*field]);
            } else {
                assert!(terms.contains(&format!("attribute_not_exists({alias})").as_str()));
            }
        }
    }

    #[test]
    fn explicit_policy_has_no_unchecked_duration_or_default() {
        assert!(matches!(
            MaintenancePolicy::new(0, 1),
            Err(MaintenanceError::InvalidPolicy)
        ));
        assert!(MaintenancePolicy::new(u64::MAX, 0).is_err());
        assert!(MaintenancePolicy::new(1, u64::MAX).is_err());
        let policy = MaintenancePolicy::new(TimestampMs::MAX.as_millis(), 0).unwrap();
        assert_eq!(
            policy.resolution_retention_ms(),
            TimestampMs::MAX.as_millis()
        );
        assert_eq!(policy.maximum_clock_uncertainty_ms(), 0);
    }

    #[test]
    fn strict_schema_rejects_corrupt_partial_noncanonical_and_unknown_authority() {
        let base = configured(3, 2, 0);
        for (field, bad) in [
            (RETENTION, n(0)),
            (RETENTION, n(u64::MAX)),
            (RETENTION, s("100")),
            (RETENTION, AttributeValue::N("0100".into())),
            ("revision", n(0)),
            ("revision", AttributeValue::N("1e0".into())),
            ("current_generation", n(0)),
            ("closed_through", n(3)),
            ("compacted_through", n(3)),
            ("incarnation", s(Uuid::nil().to_string())),
            ("domain", s("other")),
            ("sk", s("other")),
            ("new_authority", n(1)),
            ("schema_version", n(2)),
            (CANDIDATE, n(1)),
            (NOT_BEFORE, n(1105)),
        ] {
            let mut invalid = base.clone();
            invalid.insert(field.into(), bad);
            assert!(
                Control::parse(Some(invalid), &domain(), ARN).is_err(),
                "{field}"
            );
        }
        for candidate in [0, 3, u64::MAX] {
            let mut invalid = pending();
            invalid.insert(CANDIDATE.into(), n(candidate));
            assert!(Control::parse(Some(invalid), &domain(), ARN).is_err());
        }
        for deadline in [0, 99, u64::MAX] {
            let mut invalid = pending();
            invalid.insert(NOT_BEFORE.into(), n(deadline));
            assert!(Control::parse(Some(invalid), &domain(), ARN).is_err());
        }
        let mut invalid = pending();
        invalid.remove(RETENTION);
        assert!(Control::parse(Some(invalid), &domain(), ARN).is_err());
        let mut invalid = pending();
        invalid.insert("compacted_through".into(), n(1));
        assert!(Control::parse(Some(invalid), &domain(), ARN).is_err());
        for field in FIELDS.iter().take(8) {
            let mut invalid = base.clone();
            invalid.remove(*field);
            assert!(Control::parse(Some(invalid), &domain(), ARN).is_err());
        }
    }

    #[test]
    fn maximum_control_fields_fit_and_whole_row_size_gate_is_enforced() {
        let domain = DomainId::new("x".repeat(128)).unwrap();
        let mut item = configured(u64::MAX, u64::MAX - 1, u64::MAX - 2);
        item.extend(control_key(&domain));
        item.insert("domain".into(), s(domain.as_str()));
        item.insert("revision".into(), n(u64::MAX));
        item.insert(RETENTION.into(), n(TimestampMs::MAX.as_millis()));
        item.insert(CANDIDATE.into(), n(u64::MAX - 1));
        item.insert(NOT_BEFORE.into(), n(TimestampMs::MAX.as_millis()));
        assert_eq!(item.len(), 11);
        let parsed = Control::parse(Some(item.clone()), &domain, ARN).unwrap();
        assert_eq!(parsed.observation.revision(), u64::MAX);
        let size: usize = item
            .iter()
            .map(|(k, v)| {
                k.len()
                    + match v {
                        AttributeValue::S(s) | AttributeValue::N(s) => s.len(),
                        _ => unreachable!(),
                    }
            })
            .sum();
        assert!(size + 64 <= 4000);
        assert!(
            replacement(ARN, &item, item.clone())
                .unwrap()
                .condition_expression()
                .unwrap()
                .len()
                < 4096
        );
        // Deliberately bypass typed identity construction to prove whole-row gate.
        item.insert("domain".into(), s("x".repeat(4000)));
        assert!(matches!(bounded(&item), Err(MaintenanceError::RecordSize)));
        assert!(matches!(
            replacement(ARN, &item, item.clone()),
            Err(MaintenanceError::RecordSize)
        ));
    }

    #[tokio::test]
    async fn configure_install_idempotence_mismatch_and_full_absence_guards() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        let old = row(1, 0, 0);
        fixture.item(&old);
        fixture.reply(json!({}));
        let report = journal
            .configure_compaction_policy(&policy())
            .await
            .unwrap();
        assert_eq!(report.outcome, MaintenanceOutcome::Configured);
        assert_eq!(report.observation.revision(), 2);
        assert_eq!(report.observation.coordination_arn(), ARN);
        assert_eq!(report.observation.fence().domain(), &domain());
        let requests = fixture.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1]["ConsistentRead"], true);
        assert_eq!(requests[1]["TableName"], ARN);
        assert_full_guard(&requests[2], &old);
        let next = fixture.last_item();
        for (k, v) in &old {
            if k != "revision" {
                assert_eq!(&next[k], v);
            }
        }
        fixture.item(&next);
        let observed = journal
            .configure_compaction_policy(&policy())
            .await
            .unwrap();
        assert_eq!(
            observed.outcome,
            MaintenanceOutcome::PolicyAlreadyConfigured
        );
        assert_eq!(observed.observation, report.observation);
        fixture.item(&next);
        assert!(matches!(
            journal
                .configure_compaction_policy(&MaintenancePolicy::new(101, 5).unwrap())
                .await,
            Err(MaintenanceError::PolicyMismatch)
        ));
        assert_eq!(fixture.requests().len(), 5); // no extra writes
    }

    #[tokio::test]
    async fn closed_boundary_sampled_after_read_and_pending_candidate_survives_closure() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        let old = configured(2, 1, 0);
        let calls = AtomicUsize::new(0);
        fixture.item(&old);
        fixture.reply(json!({}));
        let sampler = || {
            // Sampling before the read would see only enrollment and fail.
            let requests = fixture.requests();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1]["ConsistentRead"], true);
            calls.fetch_add(1, Ordering::SeqCst);
            clock(1000)
        };
        let armed = journal
            .arm_compaction_window(&policy(), &sampler)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(armed.outcome, MaintenanceOutcome::Armed);
        assert_eq!(
            armed.observation.compaction_window(),
            Some((1, TimestampMs::new(1105).unwrap()))
        );
        assert_full_guard(fixture.requests().last().unwrap(), &old);
        let pending = fixture.last_item();
        fixture.item(&pending);
        fixture.reply(json!({}));
        let closed = journal
            .advance_and_close_generation(&policy())
            .await
            .unwrap();
        assert_eq!(closed.outcome, MaintenanceOutcome::AdvancedAndClosed);
        assert_eq!(closed.observation.fence().current(), 3);
        assert_eq!(closed.observation.fence().closed_through(), 2);
        assert_eq!(
            closed.observation.compaction_window(),
            armed.observation.compaction_window()
        );
        assert_full_guard(fixture.requests().last().unwrap(), &pending);
        let later = fixture.last_item();
        fixture.item(&later);
        let no_sample =
            || -> Result<ClockObservation, ClockError> { panic!("pending must not reset clock") };
        assert_eq!(
            journal
                .arm_compaction_window(&policy(), &no_sample)
                .await
                .unwrap()
                .outcome,
            MaintenanceOutcome::WindowPending
        );
        assert_eq!(fixture.requests().len(), 6);
    }

    #[tokio::test]
    async fn publish_lower_bound_edges_exact_candidate_and_subsequent_arm() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        let old = pending();
        fixture.item(&old);
        let report = journal
            .publish_compaction_window(&policy(), &|| clock(1109))
            .await
            .unwrap();
        assert_eq!(report.outcome, MaintenanceOutcome::NotDue); // lower=1104 < 1105
        assert_eq!(report.observation.revision(), 1);
        fixture.item(&old);
        fixture.reply(json!({}));
        let published = journal
            .publish_compaction_window(&policy(), &|| clock(1110))
            .await
            .unwrap();
        assert_eq!(published.outcome, MaintenanceOutcome::Published); // lower=deadline
        assert_eq!(published.observation.fence().compacted_through(), 1); // NOT closed=2
        assert_eq!(published.observation.compaction_window(), None);
        assert_eq!(published.observation.resolution_retention_ms(), Some(100));
        assert_full_guard(fixture.requests().last().unwrap(), &old);
        let next = fixture.last_item();
        assert!(!next.contains_key(CANDIDATE) && !next.contains_key(NOT_BEFORE));
        fixture.item(&next);
        fixture.reply(json!({}));
        let next_window = journal
            .arm_compaction_window(&policy(), &|| clock(1200))
            .await
            .unwrap();
        assert_eq!(
            next_window.observation.compaction_window(),
            Some((2, TimestampMs::new(1305).unwrap()))
        );
        assert_full_guard(fixture.requests().last().unwrap(), &next);
    }

    #[tokio::test]
    async fn no_work_missing_policy_missing_corrupt_and_failed_reads_never_sample() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        let no_sample = || -> Result<ClockObservation, ClockError> { panic!("must not sample") };
        fixture.item(&configured(1, 0, 0));
        assert_eq!(
            journal
                .arm_compaction_window(&policy(), &no_sample)
                .await
                .unwrap()
                .outcome,
            MaintenanceOutcome::NoEligibleBoundary
        );
        fixture.item(&configured(2, 1, 0));
        assert_eq!(
            journal
                .publish_compaction_window(&policy(), &no_sample)
                .await
                .unwrap()
                .outcome,
            MaintenanceOutcome::NoPendingWindow
        );
        fixture.item(&row(2, 1, 0));
        assert!(matches!(
            journal.arm_compaction_window(&policy(), &no_sample).await,
            Err(MaintenanceError::MissingPolicy)
        ));
        fixture.item(&row(2, 1, 0));
        assert!(matches!(
            journal.advance_and_close_generation(&policy()).await,
            Err(MaintenanceError::MissingPolicy)
        ));
        fixture.item(&row(2, 1, 0));
        assert!(matches!(
            journal
                .publish_compaction_window(&policy(), &no_sample)
                .await,
            Err(MaintenanceError::MissingPolicy)
        ));
        fixture.reply(json!({}));
        assert!(matches!(
            journal.arm_compaction_window(&policy(), &no_sample).await,
            Err(MaintenanceError::Journal(JournalError::MissingControl))
        ));
        fixture.reply(json!({}));
        assert!(matches!(
            journal.configure_compaction_policy(&policy()).await,
            Err(MaintenanceError::Journal(JournalError::MissingControl))
        ));
        let mut corrupt = pending();
        corrupt.remove(NOT_BEFORE);
        fixture.item(&corrupt);
        assert!(
            journal
                .publish_compaction_window(&policy(), &no_sample)
                .await
                .is_err()
        );
        fixture.error(500, "InternalServerError");
        assert!(matches!(
            journal.arm_compaction_window(&policy(), &no_sample).await,
            Err(MaintenanceError::Journal(JournalError::Native(_)))
        ));
        fixture.error(500, "InternalServerError");
        assert!(
            journal
                .publish_compaction_window(&policy(), &no_sample)
                .await
                .is_err()
        );
        assert_eq!(fixture.requests().len(), 11); // read override prevents retries
    }

    #[tokio::test]
    async fn untrusted_uncertain_sampler_errors_and_time_overflow_stop_age_writes() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        for clock in [
            ClockObservation::untrusted(TimestampMs::new(2000).unwrap(), 0),
            ClockObservation::trusted(TimestampMs::new(2000).unwrap(), 6).unwrap(),
        ] {
            fixture.item(&configured(2, 1, 0));
            assert!(matches!(
                journal
                    .arm_compaction_window(&policy(), &|| Ok(clock))
                    .await,
                Err(MaintenanceError::Clock(_))
            ));
            fixture.item(&pending());
            assert!(matches!(
                journal
                    .publish_compaction_window(&policy(), &|| Ok(clock))
                    .await,
                Err(MaintenanceError::Clock(_))
            ));
        }
        fixture.item(&configured(2, 1, 0));
        assert!(matches!(
            journal
                .arm_compaction_window(&policy(), &|| Err(ClockError::Untrusted))
                .await,
            Err(MaintenanceError::Clock(ClockError::Untrusted))
        ));
        fixture.item(&configured(2, 1, 0));
        assert!(matches!(
            journal
                .arm_compaction_window(&policy(), &|| ClockObservation::trusted(
                    TimestampMs::MAX,
                    0
                ))
                .await,
            Err(MaintenanceError::Clock(ClockError::Overflow))
        ));
        // Closure needs no clock, so remains available independently of clock health.
        fixture.item(&configured(2, 1, 0));
        fixture.reply(json!({}));
        assert_eq!(
            journal
                .advance_and_close_generation(&policy())
                .await
                .unwrap()
                .outcome,
            MaintenanceOutcome::AdvancedAndClosed
        );
        assert_eq!(fixture.requests().len(), 9);
    }

    #[tokio::test]
    async fn overflow_and_policy_mismatch_never_write() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        fixture.item(&configured(u64::MAX, u64::MAX - 1, 0));
        assert!(matches!(
            journal.advance_and_close_generation(&policy()).await,
            Err(MaintenanceError::Fence(FenceError::GenerationOverflow))
        ));
        let mut old = configured(2, 1, 0);
        old.insert("revision".into(), n(u64::MAX));
        fixture.item(&old);
        assert!(matches!(
            journal.advance_and_close_generation(&policy()).await,
            Err(MaintenanceError::RevisionOverflow)
        ));
        fixture.item(&old);
        assert!(matches!(
            journal
                .arm_compaction_window(&policy(), &|| clock(1000))
                .await,
            Err(MaintenanceError::RevisionOverflow)
        ));
        let mut pending = pending();
        pending.insert("revision".into(), n(u64::MAX));
        fixture.item(&pending);
        assert!(matches!(
            journal
                .publish_compaction_window(&policy(), &|| clock(2000))
                .await,
            Err(MaintenanceError::RevisionOverflow)
        ));
        old.remove(RETENTION);
        fixture.item(&old);
        assert!(matches!(
            journal.configure_compaction_policy(&policy()).await,
            Err(MaintenanceError::RevisionOverflow)
        ));
        let mismatch = MaintenancePolicy::new(101, 5).unwrap();
        let no_sample = || -> Result<ClockObservation, ClockError> { panic!("policy mismatch") };
        fixture.item(&configured(2, 1, 0));
        assert!(matches!(
            journal.advance_and_close_generation(&mismatch).await,
            Err(MaintenanceError::PolicyMismatch)
        ));
        fixture.item(&configured(2, 1, 0));
        assert!(matches!(
            journal.arm_compaction_window(&mismatch, &no_sample).await,
            Err(MaintenanceError::PolicyMismatch)
        ));
        fixture.item(&pending);
        assert!(matches!(
            journal
                .publish_compaction_window(&mismatch, &no_sample)
                .await,
            Err(MaintenanceError::PolicyMismatch)
        ));
        assert_eq!(fixture.requests().len(), 9);
    }

    #[tokio::test]
    async fn conflicts_and_lost_replies_require_current_observation_not_historical_inference() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        fixture.item(&row(1, 0, 0));
        fixture.lost_reply();
        assert!(matches!(
            journal.configure_compaction_policy(&policy()).await,
            Err(MaintenanceError::OutcomeUnknown)
        ));
        assert_eq!(fixture.requests().len(), 3); // one write attempt despite retry-enabled client
        let configured = fixture.last_item();
        fixture.item(&configured); // canned current postcondition, NOT local evaluation
        let current = journal.read_maintenance_control().await.unwrap();
        assert_eq!(current.resolution_retention_ms(), Some(100));
        fixture.item(&configured);
        assert_eq!(
            journal
                .configure_compaction_policy(&policy())
                .await
                .unwrap()
                .outcome,
            MaintenanceOutcome::PolicyAlreadyConfigured
        );
        fixture.item(&configured);
        fixture.error(400, "ConditionalCheckFailedException");
        assert!(matches!(
            journal.advance_and_close_generation(&policy()).await,
            Err(MaintenanceError::Conflict)
        ));
        assert_full_guard(fixture.requests().last().unwrap(), &configured);
        let closed = fixture.last_item();
        fixture.item(&closed); // another writer may have closed it
        let current = journal.read_maintenance_control().await.unwrap();
        assert_eq!(current.fence().closed_through(), 1);
        fixture.item(&closed);
        fixture.error(500, "InternalServerError");
        assert!(matches!(
            journal
                .arm_compaction_window(&policy(), &|| clock(1000))
                .await,
            Err(MaintenanceError::OutcomeUnknown)
        ));
        assert_eq!(fixture.requests().len(), 10);
    }

    #[tokio::test]
    async fn journal_receipt_precedence_and_missing_compacted_expired_with_new_fields() {
        let fixture = NativeFixture::new();
        let journal = fixture.journal().await;
        fixture.item(&configured(1, 0, 0));
        let context = journal.command_context().await.unwrap();
        let command = journal
            .prepare_metadata_only(
                context,
                ApplicationActions::new(vec![]).unwrap(),
                b"result".to_vec(),
            )
            .unwrap();
        fixture.reply(json!({}));
        assert_eq!(
            journal.execute(&command).await.unwrap().outcome,
            Outcome::Committed(b"result".to_vec())
        );
        let submitted = fixture.requests().last().unwrap().clone();
        let receipt = submitted["TransactItems"][1]["Put"]["Item"].clone();
        assert!(!receipt.is_null());
        let closed = pending();
        fixture.reply(json!({"Responses":[{"Item":wire(&closed)},{}]}));
        assert_eq!(
            journal.resolve(command.token()).await.unwrap(),
            Outcome::NotCommitted
        );
        let mut compacted = configured(4, 3, 1);
        compacted.insert(CANDIDATE.into(), n(2));
        compacted.insert(NOT_BEFORE.into(), n(1105));
        fixture.reply(json!({"Responses":[{"Item":wire(&compacted)},{}]}));
        assert_eq!(
            journal.resolve(command.token()).await.unwrap(),
            Outcome::Expired
        );
        fixture.reply(json!({"Responses":[{"Item":wire(&compacted)},{"Item":receipt}]}));
        assert_eq!(
            journal.resolve(command.token()).await.unwrap(),
            Outcome::Committed(b"result".to_vec())
        );
        // Existing terminal cleanup guards must retain every new observed field.
        let action = guard(ARN, compacted.clone()).unwrap();
        let check = action.condition_check().unwrap();
        for field in [RETENTION, CANDIDATE, NOT_BEFORE] {
            let alias = check
                .expression_attribute_names()
                .unwrap()
                .iter()
                .find(|(_, v)| v.as_str() == field)
                .unwrap()
                .0;
            assert!(
                check
                    .condition_expression()
                    .contains(&format!("{alias} = "))
            );
        }
        assert_eq!(
            check.expression_attribute_values().unwrap().len(),
            compacted.len()
        );
    }
}
