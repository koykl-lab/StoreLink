use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::{self, Write};

use aws_sdk_dynamodb::types::{AttributeValue, TransactWriteItem};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// An owned native application fragment. It is not a standalone executable plan.
/// Library guards/accounting/outcomes must still be composed by the coordinator.
#[derive(Clone)]
pub struct ApplicationActions {
    actions: Vec<TransactWriteItem>,
    digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationInspection {
    pub action_count: usize,
    pub digest: [u8; 32],
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ActionError {
    #[error("an application fragment alone exceeds the 100-action native limit")]
    TooManyActions,
    #[error("transaction item {index} must contain exactly one native action")]
    InvalidAction { index: usize },
    #[error("attribute nesting exceeds the supported 32-level native document bound")]
    NestingLimit,
    #[error("unsupported native AttributeValue variant; adapter update required")]
    UnsupportedAttribute,
    #[error("native action fingerprint serialization failed: {0}")]
    Serialization(String),
}

impl ApplicationActions {
    /// Takes ownership; subsequent mutation of a caller's clone cannot change it.
    /// Empty fragments are valid for library-only operations. The eventual complete
    /// plan must reserve space for library actions and enforce the native budget.
    pub fn new(actions: Vec<TransactWriteItem>) -> Result<Self, ActionError> {
        if actions.len() > 100 {
            return Err(ActionError::TooManyActions);
        }
        let canonical = actions
            .iter()
            .enumerate()
            .map(|(index, action)| CanonicalAction::new(action, index))
            .collect::<Result<Vec<_>, _>>()?;
        let mut sink = HashSink(Sha256::new());
        sink.0.update(b"storelink-native-actions-v1\0");
        serde_json::to_writer(&mut sink, &canonical)
            .map_err(|e| ActionError::Serialization(e.to_string()))?;
        let digest = sink.0.finalize().into();
        Ok(Self { actions, digest })
    }

    pub(super) fn into_native(self) -> Vec<TransactWriteItem> {
        self.actions
    }

    pub(super) fn table_names(&self) -> impl Iterator<Item = &str> {
        self.actions.iter().map(|action| {
            action
                .put
                .as_ref()
                .map(|v| v.table_name())
                .or_else(|| action.update.as_ref().map(|v| v.table_name()))
                .or_else(|| action.delete.as_ref().map(|v| v.table_name()))
                .or_else(|| action.condition_check.as_ref().map(|v| v.table_name()))
                .expect("ApplicationActions constructor validated exactly one action")
        })
    }

    pub fn inspect(&self) -> ApplicationInspection {
        ApplicationInspection {
            action_count: self.actions.len(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for ApplicationActions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplicationActions")
            .field("action_count", &self.actions.len())
            .finish_non_exhaustive()
    }
}

struct HashSink(Sha256);
impl Write for HashSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// This is a fingerprint encoding for the pinned SDK's request fields, NOT a
// DynamoDB wire codec, expression parser or database-semantics implementation.
// Keep every native request field in the digest when upgrading the SDK.
#[derive(Serialize)]
struct CanonicalAction<'a> {
    kind: &'static str,
    table: &'a str,
    attributes: BTreeMap<&'a str, CanonicalAttribute<'a>>,
    update: Option<&'a str>,
    condition: Option<&'a str>,
    names: Option<BTreeMap<&'a str, &'a str>>,
    values: Option<BTreeMap<&'a str, CanonicalAttribute<'a>>>,
    failure_return_values: Option<&'a str>,
}

impl<'a> CanonicalAction<'a> {
    fn new(action: &'a TransactWriteItem, index: usize) -> Result<Self, ActionError> {
        let count = usize::from(action.put.is_some())
            + usize::from(action.update.is_some())
            + usize::from(action.delete.is_some())
            + usize::from(action.condition_check.is_some());
        if count != 1 {
            return Err(ActionError::InvalidAction { index });
        }
        let (kind, table, attributes, update, condition, names, values, failure) =
            if let Some(v) = &action.put {
                (
                    "put",
                    v.table_name(),
                    v.item(),
                    None,
                    v.condition_expression(),
                    v.expression_attribute_names(),
                    v.expression_attribute_values(),
                    v.return_values_on_condition_check_failure(),
                )
            } else if let Some(v) = &action.update {
                (
                    "update",
                    v.table_name(),
                    v.key(),
                    Some(v.update_expression()),
                    v.condition_expression(),
                    v.expression_attribute_names(),
                    v.expression_attribute_values(),
                    v.return_values_on_condition_check_failure(),
                )
            } else if let Some(v) = &action.delete {
                (
                    "delete",
                    v.table_name(),
                    v.key(),
                    None,
                    v.condition_expression(),
                    v.expression_attribute_names(),
                    v.expression_attribute_values(),
                    v.return_values_on_condition_check_failure(),
                )
            } else if let Some(v) = &action.condition_check {
                (
                    "check",
                    v.table_name(),
                    v.key(),
                    None,
                    Some(v.condition_expression()),
                    v.expression_attribute_names(),
                    v.expression_attribute_values(),
                    v.return_values_on_condition_check_failure(),
                )
            } else {
                return Err(ActionError::InvalidAction { index });
            };
        Ok(Self {
            kind,
            table,
            attributes: attributes_map(attributes, 0)?,
            update,
            condition,
            names: names.map(|m| m.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()),
            values: values.map(|m| attributes_map(m, 0)).transpose()?,
            failure_return_values: failure.map(|v| v.as_str()),
        })
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "value")]
enum CanonicalAttribute<'a> {
    String(&'a str),
    Number(&'a str),
    Binary(&'a [u8]),
    Bool(bool),
    Null(bool),
    StringSet(&'a [String]),
    NumberSet(&'a [String]),
    BinarySet(Vec<&'a [u8]>),
    List(Vec<Self>),
    Map(BTreeMap<&'a str, Self>),
}

fn attributes_map(
    values: &HashMap<String, AttributeValue>,
    depth: usize,
) -> Result<BTreeMap<&str, CanonicalAttribute<'_>>, ActionError> {
    values
        .iter()
        .map(|(k, v)| Ok((k.as_str(), attribute(v, depth)?)))
        .collect()
}

fn attribute(value: &AttributeValue, depth: usize) -> Result<CanonicalAttribute<'_>, ActionError> {
    if depth > 32 {
        return Err(ActionError::NestingLimit);
    }
    Ok(match value {
        AttributeValue::S(v) => CanonicalAttribute::String(v),
        AttributeValue::N(v) => CanonicalAttribute::Number(v),
        AttributeValue::B(v) => CanonicalAttribute::Binary(v.as_ref()),
        AttributeValue::Bool(v) => CanonicalAttribute::Bool(*v),
        AttributeValue::Null(v) => CanonicalAttribute::Null(*v),
        AttributeValue::Ss(v) => CanonicalAttribute::StringSet(v),
        AttributeValue::Ns(v) => CanonicalAttribute::NumberSet(v),
        AttributeValue::Bs(v) => {
            CanonicalAttribute::BinarySet(v.iter().map(|b| b.as_ref()).collect())
        }
        AttributeValue::L(v) => CanonicalAttribute::List(
            v.iter()
                .map(|v| attribute(v, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        AttributeValue::M(v) => CanonicalAttribute::Map(attributes_map(v, depth + 1)?),
        _ => return Err(ActionError::UnsupportedAttribute),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::{Put, ReturnValuesOnConditionCheckFailure, Update};

    fn put(item: HashMap<String, AttributeValue>) -> TransactWriteItem {
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name("application")
                    .set_item(Some(item))
                    .build()
                    .unwrap(),
            )
            .build()
    }
    fn fingerprint(action: TransactWriteItem) -> [u8; 32] {
        ApplicationActions::new(vec![action])
            .unwrap()
            .inspect()
            .digest
    }

    #[test]
    fn map_order_does_not_change_fingerprint() {
        let a = HashMap::from([
            ("a".into(), AttributeValue::S("one".into())),
            ("b".into(), AttributeValue::N("2".into())),
        ]);
        let mut b = HashMap::new();
        b.insert("b".into(), AttributeValue::N("2".into()));
        b.insert("a".into(), AttributeValue::S("one".into()));
        assert_eq!(fingerprint(put(a)), fingerprint(put(b)));
    }

    #[test]
    fn all_native_attribute_kinds_are_distinguished() {
        let values = [
            AttributeValue::S("1".into()),
            AttributeValue::N("1".into()),
            AttributeValue::B(Blob::new(b"1")),
            AttributeValue::Bool(true),
            AttributeValue::Null(true),
            AttributeValue::Ss(vec!["1".into()]),
            AttributeValue::Ns(vec!["1".into()]),
            AttributeValue::Bs(vec![Blob::new(b"1")]),
            AttributeValue::L(vec![AttributeValue::S("1".into())]),
            AttributeValue::M(HashMap::from([(
                "nested".into(),
                AttributeValue::S("1".into()),
            )])),
        ];
        let hashes: std::collections::HashSet<_> = values
            .into_iter()
            .map(|value| fingerprint(put(HashMap::from([("value".into(), value)]))))
            .collect();
        assert_eq!(hashes.len(), 10);
    }

    #[test]
    fn condition_names_values_and_failure_options_bind_digest() {
        let base = Update::builder()
            .table_name("application")
            .key("id", AttributeValue::S("a".into()))
            .update_expression("SET #value = :value")
            .condition_expression("revision = :revision")
            .expression_attribute_names("#value", "media")
            .expression_attribute_values(":value", AttributeValue::S("version".into()))
            .expression_attribute_values(":revision", AttributeValue::N("1".into()))
            .build()
            .unwrap();
        let digest = |v| fingerprint(TransactWriteItem::builder().update(v).build());
        let original = digest(base.clone());
        let mut v = base.clone();
        v.table_name = "different".into();
        assert_ne!(original, digest(v));
        let mut v = base.clone();
        v.update_expression = "REMOVE #value".into();
        assert_ne!(original, digest(v));
        let mut v = base.clone();
        v.condition_expression = None;
        assert_ne!(original, digest(v));
        let mut v = base.clone();
        v.expression_attribute_names = None;
        assert_ne!(original, digest(v));
        let mut v = base.clone();
        v.expression_attribute_values = None;
        assert_ne!(original, digest(v));
        let mut v = base;
        v.return_values_on_condition_check_failure =
            Some(ReturnValuesOnConditionCheckFailure::AllOld);
        assert_ne!(original, digest(v));
    }

    #[test]
    fn rejects_empty_or_multiple_actions_and_excess_action_count() {
        assert!(matches!(
            ApplicationActions::new(vec![TransactWriteItem::builder().build()]),
            Err(ActionError::InvalidAction { index: 0 })
        ));
        let one = put(HashMap::from([(
            "id".into(),
            AttributeValue::S("a".into()),
        )]));
        let mut multiple = one.clone();
        multiple.update = Some(
            Update::builder()
                .table_name("application")
                .key("id", AttributeValue::S("a".into()))
                .update_expression("SET a = :v")
                .build()
                .unwrap(),
        );
        assert!(matches!(
            ApplicationActions::new(vec![multiple]),
            Err(ActionError::InvalidAction { .. })
        ));
        assert!(matches!(
            ApplicationActions::new(vec![one; 101]),
            Err(ActionError::TooManyActions)
        ));
    }

    #[test]
    fn immutable_fragment_does_not_alias_input_and_debug_omits_data() {
        let mut input = put(HashMap::from([(
            "private".into(),
            AttributeValue::S("secret-marker".into()),
        )]));
        let fragment = ApplicationActions::new(vec![input.clone()]).unwrap();
        let before = fragment.inspect();
        input.put.as_mut().unwrap().table_name = "different".into();
        assert_eq!(before, fragment.inspect());
        assert!(!format!("{fragment:?}").contains("secret-marker"));
        assert_ne!(before.digest, fingerprint(input));
    }

    #[test]
    fn excessive_nesting_fails_before_recursive_serialization() {
        let mut value = AttributeValue::Bool(true);
        for _ in 0..34 {
            value = AttributeValue::L(vec![value]);
        }
        assert!(matches!(
            ApplicationActions::new(vec![put(HashMap::from([("v".into(), value)]))]),
            Err(ActionError::NestingLimit)
        ));
    }
}
