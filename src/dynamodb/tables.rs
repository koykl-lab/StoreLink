use std::collections::{BTreeMap, BTreeSet};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{KeyType, ScalarAttributeType, TableDescription, TableStatus};
use thiserror::Error;

use super::ApplicationActions;

/// Table enrollment is explicit. It is not an IAM sandbox or permission to infer
/// application references from arbitrary item values.
#[derive(Clone, Debug)]
pub struct EnrolledTables {
    coordination: TableIdentity,
    applications: BTreeMap<String, TableIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableIdentity {
    name: String,
    arn: String,
    transaction_domain: String,
}

#[derive(Debug, Error)]
pub enum EnrollmentError {
    #[error("DescribeTable failed: {0}")]
    Describe(String),
    #[error("table description is incomplete or not ACTIVE")]
    InvalidDescription,
    #[error("coordination table must have string partition key pk and string sort key sk")]
    CoordinationSchema,
    #[error("application table is outside the coordination account/Region transaction domain")]
    DifferentTransactionDomain,
    #[error("application table aliases overlap or include the reserved coordination table")]
    AliasCollision,
    #[error("application action {index} targets an unenrolled/reserved table")]
    ForbiddenAction { index: usize },
    #[error("application read targets an unenrolled/reserved table")]
    ForbiddenRead,
    #[error(transparent)]
    Actions(#[from] super::ActionError),
}

impl TableIdentity {
    fn from_description(table: &TableDescription) -> Result<Self, EnrollmentError> {
        if table.table_status() != Some(&TableStatus::Active) {
            return Err(EnrollmentError::InvalidDescription);
        }
        let name = table
            .table_name()
            .filter(|v| !v.is_empty())
            .ok_or(EnrollmentError::InvalidDescription)?;
        let arn = table
            .table_arn()
            .ok_or(EnrollmentError::InvalidDescription)?;
        let (domain, resource) = arn
            .split_once(":table/")
            .ok_or(EnrollmentError::InvalidDescription)?;
        let parts: Vec<_> = domain.split(':').collect();
        if parts.len() != 5
            || parts[0] != "arn"
            || parts[1].is_empty()
            || parts[2] != "dynamodb"
            || parts[3].is_empty()
            || parts[4].is_empty()
            || resource != name
        {
            return Err(EnrollmentError::InvalidDescription);
        }
        Ok(Self {
            name: name.to_owned(),
            arn: arn.to_owned(),
            transaction_domain: domain.to_owned(),
        })
    }
}

impl EnrolledTables {
    /// Reads canonical identities through the supplied native client. Normal
    /// table provisioning and IAM authorization remain outside this operation.
    pub async fn enroll(
        client: &Client,
        coordination: &str,
        applications: &[String],
    ) -> Result<Self, EnrollmentError> {
        let describe = client
            .describe_table()
            .table_name(coordination)
            .send()
            .await
            .map_err(|e| EnrollmentError::Describe(e.to_string()))?;
        let coordination_description = describe.table.ok_or(EnrollmentError::InvalidDescription)?;
        let coordination_identity = TableIdentity::from_description(&coordination_description)?;
        if coordination != coordination_identity.name && coordination != coordination_identity.arn {
            return Err(EnrollmentError::InvalidDescription);
        }
        let mut app_descriptions = Vec::with_capacity(applications.len());
        let mut verified_bare_aliases = BTreeSet::new();
        for name in applications {
            let response = client
                .describe_table()
                .table_name(name)
                .send()
                .await
                .map_err(|e| EnrollmentError::Describe(e.to_string()))?;
            let description = response.table.ok_or(EnrollmentError::InvalidDescription)?;
            let identity = TableIdentity::from_description(&description)?;
            if name == &identity.name {
                // Only the actual bare-name lookup establishes this alias. A
                // cross-account ARN lookup does not establish its bare name.
                verified_bare_aliases.insert(identity.name.clone());
            } else if name != &identity.arn {
                return Err(EnrollmentError::InvalidDescription);
            }
            app_descriptions.push(description);
        }
        Self::from_verified_descriptions(
            &coordination_description,
            &app_descriptions,
            &verified_bare_aliases,
        )
    }

    fn from_verified_descriptions(
        coordination: &TableDescription,
        applications: &[TableDescription],
        verified_bare_aliases: &BTreeSet<String>,
    ) -> Result<Self, EnrollmentError> {
        let identity = TableIdentity::from_description(coordination)?;
        let keys: BTreeSet<_> = coordination
            .key_schema()
            .iter()
            .map(|v| (v.attribute_name(), v.key_type().as_str()))
            .collect();
        let expected = BTreeSet::from([
            ("pk", KeyType::Hash.as_str()),
            ("sk", KeyType::Range.as_str()),
        ]);
        if keys != expected || coordination.key_schema().len() != 2 {
            return Err(EnrollmentError::CoordinationSchema);
        }
        for name in ["pk", "sk"] {
            let definitions: Vec<_> = coordination
                .attribute_definitions()
                .iter()
                .filter(|v| v.attribute_name() == name)
                .collect();
            if definitions.len() != 1 || definitions[0].attribute_type() != &ScalarAttributeType::S
            {
                return Err(EnrollmentError::CoordinationSchema);
            }
        }
        let mut aliases = BTreeMap::new();
        for table in applications {
            let app = TableIdentity::from_description(table)?;
            if app.transaction_domain != identity.transaction_domain {
                return Err(EnrollmentError::DifferentTransactionDomain);
            }
            if app.arn == identity.arn || app.name == identity.name {
                return Err(EnrollmentError::AliasCollision);
            }
            let mut allowed = vec![&app.arn];
            if verified_bare_aliases.contains(&app.name) {
                allowed.push(&app.name);
            }
            for alias in allowed {
                if aliases.insert(alias.clone(), app.clone()).is_some() {
                    return Err(EnrollmentError::AliasCollision);
                }
            }
        }
        Ok(Self {
            coordination: identity,
            applications: aliases,
        })
    }

    #[cfg(test)]
    fn from_descriptions(
        coordination: &TableDescription,
        applications: &[TableDescription],
    ) -> Result<Self, EnrollmentError> {
        // Unit fixtures model successful bare-name DescribeTable requests.
        let verified = applications
            .iter()
            .filter_map(|v| v.table_name().map(str::to_owned))
            .collect();
        Self::from_verified_descriptions(coordination, applications, &verified)
    }

    /// Always use this canonical identity for coordination I/O. A bare name can
    /// resolve in a different account than an explicitly enrolled table ARN.
    pub fn coordination_table_arn(&self) -> &str {
        &self.coordination.arn
    }

    /// Bind a native application read to an independently verified canonical ARN.
    /// Projection/key semantics remain native and application-owned.
    pub(crate) fn qualify_application_read(
        &self,
        mut get: aws_sdk_dynamodb::types::Get,
    ) -> Result<aws_sdk_dynamodb::types::Get, EnrollmentError> {
        get.table_name = self
            .applications
            .get(&get.table_name)
            .ok_or(EnrollmentError::ForbiddenRead)?
            .arn
            .clone();
        Ok(get)
    }

    /// Bind verified aliases to their canonical ARNs before freezing a command.
    /// Later credential refresh cannot silently retarget a bare table name.
    pub fn qualify_application(
        &self,
        fragment: ApplicationActions,
    ) -> Result<ApplicationActions, EnrollmentError> {
        self.validate_application(&fragment)?;
        let mut actions = fragment.into_native();
        for action in &mut actions {
            let target = if let Some(v) = &mut action.put {
                &mut v.table_name
            } else if let Some(v) = &mut action.update {
                &mut v.table_name
            } else if let Some(v) = &mut action.delete {
                &mut v.table_name
            } else {
                &mut action
                    .condition_check
                    .as_mut()
                    .expect("validated single action")
                    .table_name
            };
            *target = self
                .applications
                .get(target)
                .expect("validated enrolled target")
                .arn
                .clone();
        }
        Ok(ApplicationActions::new(actions)?)
    }

    /// Guards canonical ARNs and independently proven bare-name aliases. It does not parse
    /// application expressions or guarantee completeness of reference declarations.
    pub fn validate_application(
        &self,
        fragment: &ApplicationActions,
    ) -> Result<(), EnrollmentError> {
        for (index, name) in fragment.table_names().enumerate() {
            if !self.applications.contains_key(name) {
                return Err(EnrollmentError::ForbiddenAction { index });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, Put, TransactWriteItem,
    };

    fn table(name: &str, region: &str) -> TableDescription {
        TableDescription::builder()
            .table_name(name)
            .table_arn(format!(
                "arn:aws:dynamodb:{region}:000000000000:table/{name}"
            ))
            .table_status(TableStatus::Active)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .build()
    }
    fn fragment(table: &str) -> ApplicationActions {
        ApplicationActions::new(vec![
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .item("id", AttributeValue::S("synthetic".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        ])
        .unwrap()
    }

    #[test]
    fn application_reads_preserve_native_fields_and_bind_verified_aliases() {
        let coord = table("coord", "us-west-2");
        let app = table("application", "us-west-2");
        let enrolled =
            EnrolledTables::from_descriptions(&coord, std::slice::from_ref(&app)).unwrap();
        let get = aws_sdk_dynamodb::types::Get::builder()
            .table_name("application")
            .key("tenant_id", AttributeValue::S("synthetic".into()))
            .projection_expression("#id")
            .expression_attribute_names("#id", "tenant_id")
            .build()
            .unwrap();
        let actual = enrolled.qualify_application_read(get.clone()).unwrap();
        assert_eq!(actual.table_name(), app.table_arn().unwrap());
        assert_eq!(actual.key(), get.key());
        assert_eq!(actual.projection_expression(), get.projection_expression());
        assert_eq!(
            actual.expression_attribute_names(),
            get.expression_attribute_names()
        );
        for forbidden in ["coord", coord.table_arn().unwrap(), "unknown"] {
            let mut candidate = get.clone();
            candidate.table_name = forbidden.into();
            assert!(matches!(
                enrolled.qualify_application_read(candidate),
                Err(EnrollmentError::ForbiddenRead)
            ));
        }
        let explicit = EnrolledTables::from_verified_descriptions(
            &coord,
            std::slice::from_ref(&app),
            &BTreeSet::new(),
        )
        .unwrap();
        assert!(matches!(
            explicit.qualify_application_read(get),
            Err(EnrollmentError::ForbiddenRead)
        ));
    }

    #[test]
    fn protects_reserved_table_in_name_and_arn_forms() {
        let coord = table("coord", "us-west-2");
        let app = table("arbitrary-application", "us-west-2");
        let enrolled =
            EnrolledTables::from_descriptions(&coord, std::slice::from_ref(&app)).unwrap();
        for alias in [
            coord.table_name().unwrap(),
            coord.table_arn().unwrap(),
            "unknown",
        ] {
            assert!(matches!(
                enrolled.validate_application(&fragment(alias)),
                Err(EnrollmentError::ForbiddenAction { index: 0 })
            ));
        }
        for alias in [app.table_name().unwrap(), app.table_arn().unwrap()] {
            enrolled.validate_application(&fragment(alias)).unwrap();
        }
    }

    #[test]
    fn arn_enrollment_never_assumes_client_account_bare_aliases() {
        let mut coord = table("coord", "us-west-2");
        coord.table_arn = Some("arn:aws:dynamodb:us-west-2:111111111111:table/coord".into());
        let mut app = table("application", "us-west-2");
        app.table_arn = Some("arn:aws:dynamodb:us-west-2:111111111111:table/application".into());
        // These descriptions came from explicit account-B ARN lookups, while
        // the native client's current credentials may belong to account A.
        let enrolled = EnrolledTables::from_verified_descriptions(
            &coord,
            std::slice::from_ref(&app),
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(
            enrolled.coordination_table_arn(),
            coord.table_arn().unwrap()
        );
        assert!(
            enrolled
                .validate_application(&fragment("application"))
                .is_err()
        );
        assert!(enrolled.validate_application(&fragment("coord")).is_err());
        enrolled
            .validate_application(&fragment(app.table_arn().unwrap()))
            .unwrap();
    }

    #[test]
    fn verified_bare_aliases_are_frozen_as_canonical_arns() {
        let coord = table("coord", "us-west-2");
        let app = table("application", "us-west-2");
        let enrolled =
            EnrolledTables::from_descriptions(&coord, std::slice::from_ref(&app)).unwrap();
        let qualified = enrolled
            .qualify_application(fragment("application"))
            .unwrap();
        assert_eq!(
            qualified.inspect(),
            fragment(app.table_arn().unwrap()).inspect()
        );
        assert_eq!(
            qualified.table_names().collect::<Vec<_>>(),
            vec![app.table_arn().unwrap()]
        );
    }

    #[test]
    fn preserves_arbitrary_application_key_schema() {
        let mut app = table("application", "us-west-2");
        app.key_schema = Some(vec![
            KeySchemaElement::builder()
                .attribute_name("asset_number")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        ]);
        app.attribute_definitions = Some(vec![
            AttributeDefinition::builder()
                .attribute_name("asset_number")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        ]);
        EnrolledTables::from_descriptions(&table("coord", "us-west-2"), &[app]).unwrap();
    }

    #[test]
    fn rejects_cross_domain_alias_collisions_and_invalid_coordination_schema() {
        let coord = table("coord", "us-west-2");
        assert!(matches!(
            EnrolledTables::from_descriptions(&coord, &[table("app", "us-east-1")]),
            Err(EnrollmentError::DifferentTransactionDomain)
        ));
        assert!(matches!(
            EnrolledTables::from_descriptions(&coord, std::slice::from_ref(&coord)),
            Err(EnrollmentError::AliasCollision)
        ));
        let app = table("app", "us-west-2");
        assert!(matches!(
            EnrolledTables::from_descriptions(&coord, &[app.clone(), app]),
            Err(EnrollmentError::AliasCollision)
        ));
        let mut wrong = coord;
        wrong.key_schema = None;
        assert!(matches!(
            EnrolledTables::from_descriptions(&wrong, &[]),
            Err(EnrollmentError::CoordinationSchema)
        ));
    }
}
