//! Explicit, ignored loopback-only native fixture. See docs/native-acceptance.md.
//! Current DynamoDB Local canonical-ARN TransactGetItems incompatibility is NOT
//! worked around: this test fails honestly rather than weakening enrollment.
#![cfg(feature = "dynamodb")]

#[path = "common/journal_contract.rs"]
mod journal_contract;

use aws_sdk_dynamodb::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
};
use uuid::Uuid;

async fn create(client: &Client, table: &str) -> Result<(), String> {
    client
        .create_table()
        .table_name(table)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .map_err(|e| e.to_string())?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .map_err(|e| e.to_string())?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .map_err(|e| e.to_string())?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .map_err(|e| e.to_string())?,
        )
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn cleanup(client: &Client, table: &str) -> Result<(), String> {
    match client.delete_table().table_name(table).send().await {
        Ok(_) => Ok(()),
        Err(e)
            if e.as_service_error()
                .is_some_and(|e| e.is_resource_not_found_exception()) =>
        {
            Ok(())
        }
        Err(e) => Err(format!("{table}: {e}")),
    }
}

#[tokio::test]
#[ignore = "requires explicit loopback DynamoDB Local; canonical-ARN limitation currently fails this test"]
async fn native_journal_contract() {
    let endpoint = std::env::var("STORELINK_DYNAMODB_LOCAL_ENDPOINT")
        .expect("requires STORELINK_DYNAMODB_LOCAL_ENDPOINT");
    let port = endpoint
        .strip_prefix("http://127.0.0.1:")
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|p| *p > 0)
        .expect("endpoint must be http://127.0.0.1:<nonzero port>");
    assert_eq!(endpoint, format!("http://127.0.0.1:{port}"));
    // No default credential/config chain, DNS aliases, paths or remote endpoints.
    let client = Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("ddblocal"))
            .credentials_provider(Credentials::new(
                "storelinklocal",
                "synthetic-only",
                None,
                None,
                "local-fixture",
            ))
            .endpoint_url(endpoint)
            .build(),
    );
    let suffix = Uuid::new_v4().simple().to_string();
    let coord = format!("storelink-journal-coord-{suffix}");
    let app = format!("storelink-journal-app-{suffix}");
    let c = client.clone();
    let coord_worker = coord.clone();
    let app_worker = app.clone();
    // Provisioning/enrollment as well as assertions are inside the panic boundary.
    let result = tokio::spawn(async move {
        let (coord_result, app_result) =
            tokio::join!(create(&c, &coord_worker), create(&c, &app_worker));
        assert!(
            coord_result.is_ok() && app_result.is_ok(),
            "local table creation: {coord_result:?}; {app_result:?}"
        );
        let fixture = journal_contract::Fixture::enroll(c, coord_worker, app_worker).await;
        journal_contract::exercise(fixture).await;
    })
    .await;
    // Both outcomes settle even when one delete fails.
    let (coord_cleanup, app_cleanup) =
        tokio::join!(cleanup(&client, &coord), cleanup(&client, &app));
    assert!(
        coord_cleanup.is_ok() && app_cleanup.is_ok(),
        "local cleanup: {coord_cleanup:?}; {app_cleanup:?}; contract: {result:?}"
    );
    result.expect("native contract panicked (cleanup attempted)");
}
