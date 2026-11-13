//! Receipt-bearing regional metadata commands, not an S3 coordinator.
//!
//! No method rotates fences, compacts receipts, restores metadata, or enrolls an
//! S3 namespace. Administrators must not roll back the control record. There is
//! no TTL dependency. A historical result is evidence, not a current capability.

use std::collections::HashMap;
use std::fmt;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Get, Put, TransactGetItem, TransactWriteItem,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{ActionError, ApplicationActions, EnrolledTables, EnrollmentError};
use crate::identity::{CommandId, DomainFence, DomainId, Scope};

type Item = HashMap<String, AttributeValue>;

#[cfg(test)]
#[path = "journal_control_tests.rs"]
mod control_tests;

/// One serializable observation of an application item and this domain's fence.
/// Neither the item nor its absence establishes an operation outcome, permission,
/// or resource identity. The application must validate its own schema, namespace,
/// principal and resource token; only command receipts prove command history.
#[derive(Clone)]
pub struct ApplicationItemObservation {
    item: Option<Item>,
    fence: DomainFence,
}
impl ApplicationItemObservation {
    pub fn item(&self) -> Option<&Item> {
        self.item.as_ref()
    }
    pub fn fence(&self) -> &DomainFence {
        &self.fence
    }
}

/// A fresh command authority obtained by reading the existing control record.
/// Not Clone or Deserialize: preparing consumes this authority exactly once.
#[derive(Debug)]
pub struct CommandContext {
    id: CommandId,
    owner: Uuid,
}
impl CommandContext {
    /// Also usable with `ResourceId::from_command` during library plan building.
    pub fn id(&self) -> &CommandId {
        &self.id
    }
}

/// Durable resolution/cancellation identity, never an executable plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandToken {
    id: CommandId,
    digest: [u8; 32],
}
impl CommandToken {
    pub fn id(&self) -> &CommandId {
        &self.id
    }
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

/// An immutable, complete native submission. Clone only for EXACT retry.
/// Native request bodies are intentionally not exported or deserializable.
#[derive(Clone)]
pub struct PreparedCommand {
    owner: Uuid,
    token: CommandToken,
    client_request_token: String,
    actions: Vec<TransactWriteItem>,
    origins: Vec<ActionOrigin>,
    result: Vec<u8>,
}
impl fmt::Debug for PreparedCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedCommand")
            .field("token", &self.token)
            .field("action_count", &self.actions.len())
            .finish_non_exhaustive()
    }
}
impl PreparedCommand {
    pub fn token(&self) -> &CommandToken {
        &self.token
    }
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Committed(Vec<u8>),
    Cancelled,
    /// An absent receipt in an open generation is NOT rollback.
    Unknown,
    /// Closed generation with guaranteed receipt retention and no receipt.
    NotCommitted,
    /// Historical outcome unavailable. Never permission to transparently replay.
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionOrigin {
    Admission,
    Application { index: usize },
    Internal { index: usize },
    Receipt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationReason {
    pub origin: ActionOrigin,
    pub code: String,
    pub message: Option<String>,
}

/// Diagnostic for ONE submission, not a historical non-commit assertion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptFailure {
    /// Reasons are mapped only when the service supplies the full indexed list.
    TransactionCancelled {
        reasons: Vec<CancellationReason>,
    },
    NativeValidation {
        code: String,
        message: String,
    },
    Native {
        code: Option<String>,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReport {
    pub outcome: Outcome,
    pub attempt_failure: Option<AttemptFailure>,
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error(transparent)]
    Actions(#[from] ActionError),
    #[error(transparent)]
    Enrollment(#[from] EnrollmentError),
    #[error("complete transaction has {count} actions, exceeding 100")]
    Budget { count: usize },
    #[error("command context/prepared command belongs to another journal instance")]
    WrongJournal,
    #[error("command domain does not match the journal")]
    WrongDomain,
    #[error("command generation is in the future")]
    FutureGeneration,
    #[error("domain has no control record; explicit greenfield initialization is required")]
    MissingControl,
    #[error("invalid journal record: {0}")]
    Corrupt(String),
    #[error("receipt request digest does not match the command token")]
    DigestMismatch,
    #[error("native DynamoDB operation failed (outcome not inferred): {0}")]
    Native(String),
}

/// Uses only supplied native clients and enrolled table identities. Construction
/// performs no I/O and never initializes, repairs or restores state. Clone retains
/// the same local preparation authority; a different instance can resolve tokens.
#[derive(Clone)]
pub struct CommandJournal {
    client: Client,
    tables: EnrolledTables,
    domain: DomainId,
    owner: Uuid,
}

impl CommandJournal {
    pub fn new(client: Client, tables: EnrolledTables, domain: DomainId) -> Self {
        Self {
            client,
            tables,
            domain,
            owner: Uuid::new_v4(),
        }
    }

    /// Clone the exact native client captured by this journal for application
    /// observations, keeping endpoint/credentials/transport aligned with execution.
    /// This is a cooperating-host read integration seam, not coordination write
    /// authority or an IAM sandbox. Callers already own the supplied SDK client;
    /// writes must still use the prescribed journal composition contract.
    pub fn application_observation_client(&self) -> Client {
        self.client.clone()
    }

    pub(super) fn native_client(&self) -> &Client {
        &self.client
    }

    pub(super) fn coordination_arn(&self) -> &str {
        self.tables.coordination_table_arn()
    }

    pub(crate) fn domain_id(&self) -> &DomainId {
        &self.domain
    }

    /// Read an enrolled application item and the coordination fence in ONE
    /// native TransactGetItems. This avoids a consumer reconstructing private
    /// control keys/decoders or combining observations from different instants.
    /// Keys/projections remain native. A projected/missing item is not authority
    /// or proof of non-commit; validate application identity and use resolve()
    /// for command history. This performs no writes or default initialization.
    pub async fn observe_application_item(
        &self,
        get: Get,
    ) -> Result<ApplicationItemObservation, JournalError> {
        let get = self.tables.qualify_application_read(get)?;
        let control = Get::builder()
            .table_name(self.coordination_arn())
            .set_key(Some(control_key(&self.domain)))
            .build()
            .map_err(|e| JournalError::Corrupt(e.to_string()))?;
        let output = self
            .client
            .transact_get_items()
            .set_transact_items(Some(vec![
                TransactGetItem::builder().get(control).build(),
                TransactGetItem::builder().get(get).build(),
            ]))
            .send()
            .await
            .map_err(|e| {
                JournalError::Native(native_message(e.as_service_error(), e.to_string()))
            })?;
        let rows = output.responses();
        if rows.len() != 2 {
            return Err(JournalError::Corrupt(
                "incomplete application/fence observation".into(),
            ));
        }
        let fence = parse_control(rows[0].item.as_ref(), &self.domain)?;
        Ok(ApplicationItemObservation {
            item: rows[1].item.clone(),
            fence,
        })
    }

    /// ADMINISTRATOR/GREENFIELD ONLY. Conditional creation of generation 1;
    /// never replaces an existing incarnation, even after an ambiguous response.
    /// This is NOT S3 namespace enrollment or a metadata restoration procedure.
    /// Returns the fresh incarnation on acknowledged success.
    pub async fn initialize_fresh_domain(&self) -> Result<Uuid, JournalError> {
        let incarnation = Uuid::new_v4();
        let mut item = control_key(&self.domain);
        item.insert("domain".into(), s(self.domain.as_str()));
        item.insert("incarnation".into(), s(incarnation.to_string()));
        for (name, value) in [
            ("current_generation", 1),
            ("closed_through", 0),
            ("compacted_through", 0),
            ("revision", 1),
        ] {
            item.insert(name.into(), n(value));
        }
        self.client
            .put_item()
            .table_name(self.tables.coordination_table_arn())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await
            .map_err(|e| {
                JournalError::Native(native_message(e.as_service_error(), e.to_string()))
            })?;
        Ok(incarnation)
    }

    pub async fn command_context(&self) -> Result<CommandContext, JournalError> {
        let response = self
            .client
            .get_item()
            .table_name(self.tables.coordination_table_arn())
            .set_key(Some(control_key(&self.domain)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| {
                JournalError::Native(native_message(e.as_service_error(), e.to_string()))
            })?;
        let fence = parse_control(response.item.as_ref(), &self.domain)?;
        let scope = Scope::new(self.domain.clone(), fence.incarnation(), fence.current())
            .map_err(|e| JournalError::Corrupt(e.to_string()))?;
        let id = CommandId::new(scope, Uuid::new_v4())
            .map_err(|e| JournalError::Corrupt(e.to_string()))?;
        Ok(CommandContext {
            id,
            owner: self.owner,
        })
    }

    /// Pure application metadata ONLY: no managed-reference, claim or object
    /// lifetime changes may be hidden in these native actions. Such operations
    /// require future library builders that compose their accounting/guards.
    pub fn prepare_metadata_only(
        &self,
        context: CommandContext,
        application: ApplicationActions,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, JournalError> {
        self.prepare_internal(context, application, Vec::new(), result)
    }

    /// Internal effects must already include their full identity/state guards.
    /// This seam is not a public arbitrary accounting API. Receipt-only cancel
    /// must remain sufficient to fence these effects; resource cleanup/cancellation
    /// of already-existing intents belongs to a separate coordinated command.
    pub(crate) fn prepare_internal(
        &self,
        context: CommandContext,
        application: ApplicationActions,
        internal: Vec<TransactWriteItem>,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, JournalError> {
        self.prepare_internal_guarded(context, application, internal, result, None)
    }

    /// Library-only extra observation predicate on the SAME control row as
    /// command admission. Never append a second native action for that item.
    pub(crate) fn prepare_internal_guarded(
        &self,
        context: CommandContext,
        application: ApplicationActions,
        internal: Vec<TransactWriteItem>,
        result: Vec<u8>,
        control_guard: Option<ConditionCheck>,
    ) -> Result<PreparedCommand, JournalError> {
        if context.owner != self.owner {
            return Err(JournalError::WrongJournal);
        }
        self.check_domain(&context.id)?;
        let application = self.tables.qualify_application(application)?;
        let count = application
            .inspect()
            .action_count
            .checked_add(internal.len())
            .and_then(|n| n.checked_add(2))
            .unwrap_or(usize::MAX);
        if count > 100 {
            return Err(JournalError::Budget { count });
        }
        let mut admission = self.admission(&context.id)?;
        if let Some(extra) = control_guard {
            let check = admission
                .condition_check
                .take()
                .ok_or_else(|| JournalError::Corrupt("missing admission condition".into()))?;
            admission.condition_check = Some(merge_control_guard(check, extra)?);
        }
        let mut actions = vec![admission];
        let mut origins = vec![ActionOrigin::Admission];
        for (index, action) in application.into_native().into_iter().enumerate() {
            actions.push(action);
            origins.push(ActionOrigin::Application { index });
        }
        for (index, action) in internal.into_iter().enumerate() {
            actions.push(action);
            origins.push(ActionOrigin::Internal { index });
        }
        // Reuse the complete pinned-native-field fingerprint, never Debug text.
        let effects = ApplicationActions::new(actions)?;
        let mut hash = Sha256::new();
        hash.update(b"StoreLink/Command/v0\0");
        let identity =
            serde_json::to_vec(&context.id).map_err(|e| JournalError::Corrupt(e.to_string()))?;
        hash.update((identity.len() as u64).to_be_bytes());
        hash.update(identity);
        hash.update(effects.inspect().digest);
        hash.update((result.len() as u64).to_be_bytes());
        hash.update(&result);
        let token = CommandToken {
            id: context.id,
            digest: hash.finalize().into(),
        };
        let mut actions = effects.into_native();
        actions.push(self.receipt_put(&token, Some(&result))?);
        origins.push(ActionOrigin::Receipt);
        Ok(PreparedCommand {
            owner: self.owner,
            token,
            client_request_token: Uuid::new_v4().to_string(),
            actions,
            origins,
            result,
        })
    }

    /// Sends the frozen body/token. Any submission error is followed by a coherent
    /// native transaction read before exposing its per-attempt diagnostic.
    pub async fn execute(
        &self,
        command: &PreparedCommand,
    ) -> Result<ExecutionReport, JournalError> {
        if command.owner != self.owner {
            return Err(JournalError::WrongJournal);
        }
        let response = self
            .client
            .transact_write_items()
            .set_transact_items(Some(command.actions.clone()))
            .client_request_token(&command.client_request_token)
            .send()
            .await;
        match response {
            Ok(_) => Ok(ExecutionReport {
                outcome: Outcome::Committed(command.result.clone()),
                attempt_failure: None,
            }),
            Err(error) => {
                let failure = classify_failure(
                    error.as_service_error(),
                    &command.origins,
                    error.to_string(),
                );
                let outcome = self.resolve(&command.token).await?;
                Ok(ExecutionReport {
                    outcome,
                    attempt_failure: Some(failure),
                })
            }
        }
    }

    /// Atomically fences this command's original effects using the SAME receipt
    /// key. This does not release any independently acquired resource or cancel
    /// S3 I/O. There is no timeout-as-rollback or indefinite retry loop.
    pub async fn cancel(&self, token: &CommandToken) -> Result<ExecutionReport, JournalError> {
        self.check_domain(&token.id)?;
        // Validate stored state before trying admission (also detects malformed
        // controls and future generations even when no receipt is yet present).
        let prior = self.resolve(token).await?;
        if prior != Outcome::Unknown {
            return Ok(ExecutionReport {
                outcome: prior,
                attempt_failure: None,
            });
        }
        let actions = vec![self.admission(&token.id)?, self.receipt_put(token, None)?];
        let response = self
            .client
            .transact_write_items()
            .set_transact_items(Some(actions))
            .client_request_token(Uuid::new_v4().to_string())
            .send()
            .await;
        match response {
            Ok(_) => Ok(ExecutionReport {
                outcome: Outcome::Cancelled,
                attempt_failure: None,
            }),
            Err(error) => {
                let failure = classify_failure(
                    error.as_service_error(),
                    &[ActionOrigin::Admission, ActionOrigin::Receipt],
                    error.to_string(),
                );
                Ok(ExecutionReport {
                    outcome: self.resolve(token).await?,
                    attempt_failure: Some(failure),
                })
            }
        }
    }

    /// TransactGetItems supplies one coherent serializable control/receipt view.
    /// Matching receipt wins even after compaction or incarnation rotation.
    pub async fn resolve(&self, token: &CommandToken) -> Result<Outcome, JournalError> {
        self.check_domain(&token.id)?;
        let gets = [control_key(&self.domain), receipt_key(&token.id)]
            .into_iter()
            .map(|key| {
                Get::builder()
                    .table_name(self.tables.coordination_table_arn())
                    .set_key(Some(key))
                    .build()
                    .map(|get| TransactGetItem::builder().get(get).build())
                    .map_err(|e| JournalError::Corrupt(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let response = self
            .client
            .transact_get_items()
            .set_transact_items(Some(gets))
            .send()
            .await
            .map_err(|e| {
                JournalError::Native(native_message(e.as_service_error(), e.to_string()))
            })?;
        let records = response.responses();
        if records.len() != 2 {
            return Err(JournalError::Corrupt(
                "transaction read response count".into(),
            ));
        }
        let fence = parse_control(records[0].item.as_ref(), &self.domain)?;
        if fence.incarnation() == token.id.scope().incarnation()
            && token.id.scope().generation() > fence.current()
        {
            return Err(JournalError::FutureGeneration);
        }
        if let Some(receipt) = records[1].item.as_ref() {
            return parse_receipt(receipt, token);
        }
        if token.id.scope().incarnation() != fence.incarnation()
            || token.id.scope().generation() <= fence.compacted_through()
        {
            Ok(Outcome::Expired)
        } else if token.id.scope().generation() <= fence.closed_through() {
            Ok(Outcome::NotCommitted)
        } else {
            Ok(Outcome::Unknown)
        }
    }

    fn check_domain(&self, id: &CommandId) -> Result<(), JournalError> {
        if id.scope().domain() != &self.domain {
            Err(JournalError::WrongDomain)
        } else {
            Ok(())
        }
    }
    fn admission(&self, id: &CommandId) -> Result<TransactWriteItem, JournalError> {
        let check = ConditionCheck::builder()
            .table_name(self.tables.coordination_table_arn())
            .set_key(Some(control_key(&self.domain)))
            .condition_expression("#d = :d AND #i = :i AND #closed < :g AND #current >= :g")
            .expression_attribute_names("#d", "domain")
            .expression_attribute_names("#i", "incarnation")
            .expression_attribute_names("#closed", "closed_through")
            .expression_attribute_names("#current", "current_generation")
            .expression_attribute_values(":d", s(self.domain.as_str()))
            .expression_attribute_values(":i", s(id.scope().incarnation().to_string()))
            .expression_attribute_values(":g", n(id.scope().generation()))
            .build()
            .map_err(|e| JournalError::Corrupt(e.to_string()))?;
        Ok(TransactWriteItem::builder().condition_check(check).build())
    }
    fn receipt_put(
        &self,
        token: &CommandToken,
        result: Option<&[u8]>,
    ) -> Result<TransactWriteItem, JournalError> {
        let mut item = receipt_key(&token.id);
        item.insert("domain".into(), s(token.id.scope().domain().as_str()));
        item.insert(
            "incarnation".into(),
            s(token.id.scope().incarnation().to_string()),
        );
        item.insert("generation".into(), n(token.id.scope().generation()));
        item.insert("command_uuid".into(), s(token.id.uuid().to_string()));
        item.insert(
            "request_digest".into(),
            AttributeValue::B(Blob::new(token.digest)),
        );
        item.insert(
            "status".into(),
            s(if result.is_some() {
                "COMMITTED"
            } else {
                "CANCELLED"
            }),
        );
        if let Some(result) = result {
            item.insert("result".into(), AttributeValue::B(Blob::new(result)));
        }
        let put = Put::builder()
            .table_name(self.tables.coordination_table_arn())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .map_err(|e| JournalError::Corrupt(e.to_string()))?;
        Ok(TransactWriteItem::builder().put(put).build())
    }
}

fn native_message<E: ProvideErrorMetadata>(error: Option<&E>, fallback: String) -> String {
    error
        .map(|e| {
            format!(
                "{}: {}",
                e.code().unwrap_or("service error"),
                e.message().unwrap_or("no message")
            )
        })
        .unwrap_or(fallback)
}

fn classify_failure(
    error: Option<&TransactWriteItemsError>,
    origins: &[ActionOrigin],
    message: String,
) -> AttemptFailure {
    if let Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) = error {
        let native = cancelled.cancellation_reasons();
        let reasons = if native.len() == origins.len() {
            native
                .iter()
                .zip(origins)
                .filter_map(|(reason, origin)| {
                    reason
                        .code()
                        .filter(|code| *code != "None")
                        .map(|code| CancellationReason {
                            origin: origin.clone(),
                            code: code.to_owned(),
                            message: reason.message().map(str::to_owned),
                        })
                })
                .collect()
        } else {
            Vec::new()
        };
        return AttemptFailure::TransactionCancelled { reasons };
    }
    let code = error.and_then(|e| e.code()).map(str::to_owned);
    let message = error
        .and_then(|e| e.message())
        .map(str::to_owned)
        .unwrap_or(message);
    if matches!(
        code.as_deref(),
        Some("ValidationException" | "IdempotentParameterMismatchException")
    ) {
        AttemptFailure::NativeValidation {
            code: code.unwrap(),
            message,
        }
    } else {
        AttemptFailure::Native { code, message }
    }
}

fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
fn n(value: u64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}
pub(super) fn prefix(domain: &DomainId) -> String {
    format!("D#{:x}", Sha256::digest(domain.as_str().as_bytes()))
}
fn key(pk: String) -> Item {
    HashMap::from([("pk".into(), s(pk)), ("sk".into(), s("M"))])
}
pub(super) fn control_key(domain: &DomainId) -> Item {
    key(format!("{}#CONTROL", prefix(domain)))
}
pub(super) fn receipt_key(id: &CommandId) -> Item {
    key(format!(
        "{}#CMD#{}#{}#{}",
        prefix(id.scope().domain()),
        id.scope().incarnation(),
        id.scope().generation(),
        id.uuid()
    ))
}
fn text<'a>(item: &'a Item, name: &str) -> Result<&'a str, JournalError> {
    match item.get(name) {
        Some(AttributeValue::S(v)) => Ok(v),
        _ => Err(JournalError::Corrupt(format!("missing/non-string {name}"))),
    }
}
pub(super) fn number(item: &Item, name: &str) -> Result<u64, JournalError> {
    match item.get(name) {
        Some(AttributeValue::N(v)) => v
            .parse()
            .ok()
            .filter(|n: &u64| n.to_string() == *v)
            .ok_or_else(|| JournalError::Corrupt(format!("invalid unsigned integer {name}"))),
        _ => Err(JournalError::Corrupt(format!("missing/non-number {name}"))),
    }
}
fn check_key(item: &Item, expected: Item) -> Result<(), JournalError> {
    for (name, value) in expected {
        if item.get(&name) != Some(&value) {
            return Err(JournalError::Corrupt("record key mismatch".into()));
        }
    }
    Ok(())
}
pub(super) fn parse_control(
    item: Option<&Item>,
    domain: &DomainId,
) -> Result<DomainFence, JournalError> {
    let item = item.ok_or(JournalError::MissingControl)?;
    check_key(item, control_key(domain))?;
    if text(item, "domain")? != domain.as_str() {
        return Err(JournalError::Corrupt("control domain mismatch".into()));
    }
    let incarnation = Uuid::parse_str(text(item, "incarnation")?)
        .map_err(|e| JournalError::Corrupt(e.to_string()))?;
    if incarnation.to_string() != text(item, "incarnation")? || number(item, "revision")? == 0 {
        return Err(JournalError::Corrupt("invalid incarnation/revision".into()));
    }
    DomainFence::new(
        domain.clone(),
        incarnation,
        number(item, "current_generation")?,
        number(item, "closed_through")?,
        number(item, "compacted_through")?,
    )
    .map_err(|e| JournalError::Corrupt(e.to_string()))
}
fn merge_control_guard(
    mut admission: ConditionCheck,
    extra: ConditionCheck,
) -> Result<ConditionCheck, JournalError> {
    if admission.table_name != extra.table_name
        || admission.key != extra.key
        || extra.condition_expression.is_empty()
        || extra.return_values_on_condition_check_failure.is_some()
    {
        return Err(JournalError::Corrupt(
            "extra control guard must target the exact admission item".into(),
        ));
    }
    let names = admission.expression_attribute_names.get_or_insert_default();
    for (key, value) in extra.expression_attribute_names.unwrap_or_default() {
        if names.get(&key).is_some_and(|prior| prior != &value) {
            return Err(JournalError::Corrupt(
                "conflicting control guard name alias".into(),
            ));
        }
        names.insert(key, value);
    }
    let values = admission
        .expression_attribute_values
        .get_or_insert_default();
    for (key, value) in extra.expression_attribute_values.unwrap_or_default() {
        if values.get(&key).is_some_and(|prior| prior != &value) {
            return Err(JournalError::Corrupt(
                "conflicting control guard value alias".into(),
            ));
        }
        values.insert(key, value);
    }
    admission.condition_expression = format!(
        "({}) AND ({})",
        admission.condition_expression, extra.condition_expression
    );
    Ok(admission)
}

pub(super) fn parse_receipt(item: &Item, token: &CommandToken) -> Result<Outcome, JournalError> {
    check_key(item, receipt_key(&token.id))?;
    if text(item, "domain")? != token.id.scope().domain().as_str()
        || text(item, "incarnation")? != token.id.scope().incarnation().to_string()
        || number(item, "generation")? != token.id.scope().generation()
        || text(item, "command_uuid")? != token.id.uuid().to_string()
    {
        return Err(JournalError::Corrupt("receipt identity mismatch".into()));
    }
    match item.get("request_digest") {
        Some(AttributeValue::B(v)) if v.as_ref().len() == 32 => {
            if v.as_ref() != token.digest {
                return Err(JournalError::DigestMismatch);
            }
        }
        _ => return Err(JournalError::Corrupt("invalid request digest".into())),
    }
    match text(item, "status")? {
        "COMMITTED" => match item.get("result") {
            Some(AttributeValue::B(v)) => Ok(Outcome::Committed(v.as_ref().to_vec())),
            _ => Err(JournalError::Corrupt(
                "committed receipt lacks binary result".into(),
            )),
        },
        "CANCELLED" if !item.contains_key("result") => Ok(Outcome::Cancelled),
        _ => Err(JournalError::Corrupt(
            "invalid receipt status/result".into(),
        )),
    }
}
