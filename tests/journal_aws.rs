//! Opt-in, ignored REAL AWS tests. Compiling this file is not AWS acceptance.
//! No AWS client/config/credential loading occurs outside the ignored entrypoints.
//! See docs/native-acceptance.md for authorization, IAM and recovery requirements.
#![cfg(feature = "aws-acceptance")]

#[path = "common/journal_contract.rs"]
mod journal_contract;

use aws_config::{profile::ProfileFileCredentialsProvider, provider_config::ProviderConfig};
use aws_sdk_dynamodb::{
    Client,
    client::Waiters,
    config::{
        BehaviorVersion, ProvideCredentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
    },
    operation::create_table::CreateTableError,
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        TableStatus, Tag,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

const ACK: &str = "CREATE-AND-DELETE-TWO-DISPOSABLE-STORELINK-JOURNAL-TABLES";
const CLEANUP_SETTLED_ACK: &str = "ORIGINAL-PRODUCER-AND-ALL-CREATE-REQUESTS-HAVE-SETTLED";
const PREFIX: &str = "storelink-acceptance-journal";
const PURPOSE: &str = "storelink-native-journal-acceptance";
const RUN_TAG: &str = "storelink:acceptance-run";
const PURPOSE_TAG: &str = "storelink:acceptance-purpose";
const ROLE_TAG: &str = "storelink:acceptance-role";
const WAITER_LIMIT: Duration = Duration::from_secs(300);
type Check<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CreatePhase {
    #[default]
    NeverIssued,
    SentUnknown,
    AcceptedUnconfirmed,
    DefinitelyRejected,
    OwnedActive,
}

// In-memory only; survives a create task panic, not process death. No lock is
// held over an await or user code. Poison recovery preserves the last phase.
#[derive(Clone, Default)]
struct Creation(Arc<Mutex<CreatePhase>>);

impl Creation {
    fn phase(&self) -> CreatePhase {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn record(&self, phase: CreatePhase) {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner()) = phase;
    }
}

fn create_failure_phase(error: Option<&CreateTableError>) -> CreatePhase {
    // Deliberately narrow: an explicit existing-table rejection proves this
    // SINGLE attempt did not create. Never generalize to 5xx, transport errors,
    // timeouts, malformed responses, or a rejection after an earlier SDK retry.
    if error.is_some_and(|error| error.is_resource_in_use_exception()) {
        CreatePhase::DefinitelyRejected
    } else {
        CreatePhase::SentUnknown
    }
}

#[derive(Clone, Copy, Debug)]
enum CleanupBasis {
    Automatic(CreatePhase),
    OperatorAcknowledgedSettlement,
}

impl CleanupBasis {
    fn confirm_unobserved_absence(self) -> Check<()> {
        match self {
            Self::Automatic(CreatePhase::SentUnknown | CreatePhase::AcceptedUnconfirmed) => Err(
                "late-create uncertainty: sent CreateTable was not observed owned ACTIVE; \
                 absence does not prove request drain; preserve manifest"
                    .into(),
            ),
            Self::Automatic(
                CreatePhase::NeverIssued
                | CreatePhase::DefinitelyRejected
                | CreatePhase::OwnedActive,
            )
            | Self::OperatorAcknowledgedSettlement => Ok(()),
        }
    }
}

fn cleanup_settlement_ack(value: Option<&str>) -> Check<CleanupBasis> {
    if value != Some(CLEANUP_SETTLED_ACK) {
        return Err("cleanup-only requires STORELINK_AWS_CLEANUP_SETTLED_ACK: acknowledge original producer AND all original create requests have settled; process exit/timeouts are not proof".into());
    }
    // Operator prerequisite only: this flag cannot establish remote quiescence.
    Ok(CleanupBasis::OperatorAcknowledgedSettlement)
}

// Deliberately no credentials, session tokens, caller ARN or SDK config in this
// immutable recovery record. It is an allowlist, never a source of arbitrary names.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    purpose: String,
    run: Uuid,
    account: String,
    region: String,
    profile: String,
    coordination_table: String,
    application_table: String,
}

impl Manifest {
    fn new(account: String, region: String, profile: String, run: Uuid) -> Check<Self> {
        if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
            return Err("expected account must be exactly 12 ASCII digits".into());
        }
        // Intentionally only the commercial AWS partition for this bounded harness.
        // An unsupported region fails in the SDK; no arbitrary endpoint is accepted.
        if region.is_empty()
            || region.len() > 32
            || !region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || !region.contains('-')
            || region.starts_with("cn-")
            || region.starts_with("us-gov-")
            || region.starts_with("us-iso")
        {
            return Err("requires an explicit commercial AWS region".into());
        }
        if profile.is_empty() || profile.trim() != profile || profile.chars().any(char::is_control)
        {
            return Err("requires a nonempty explicit AWS profile".into());
        }
        if run.get_version_num() != 4 || run.get_variant() != uuid::Variant::RFC4122 {
            return Err("run must be a fresh RFC4122 version-4 UUID".into());
        }
        let suffix = run.simple();
        Ok(Self {
            version: 1,
            purpose: PURPOSE.into(),
            run,
            account,
            region,
            profile,
            coordination_table: format!("{PREFIX}-coord-{suffix}"),
            application_table: format!("{PREFIX}-app-{suffix}"),
        })
    }

    fn tables(&self) -> [(&str, &str); 2] {
        [
            (&self.coordination_table, "coordination"),
            (&self.application_table, "application"),
        ]
    }

    fn arn(&self, name: &str) -> String {
        format!(
            "arn:aws:dynamodb:{}:{}:table/{name}",
            self.region, self.account
        )
    }

    fn filename(&self) -> String {
        format!("{PREFIX}-{}.json", self.run)
    }
}

fn required(name: &str) -> Check<String> {
    std::env::var(name).map_err(|_| format!("missing explicit prerequisite {name}"))
}

fn inputs() -> Check<(Manifest, PathBuf)> {
    if required("STORELINK_AWS_ACK")? != ACK {
        return Err("STORELINK_AWS_ACK does not match the documented acknowledgement".into());
    }
    let run_text = required("STORELINK_AWS_RUN_UUID")?;
    let run = Uuid::parse_str(&run_text).map_err(|_| "invalid run UUID")?;
    if run.to_string() != run_text {
        return Err("run UUID must use canonical lowercase hyphenated form".into());
    }
    let manifest = Manifest::new(
        required("STORELINK_AWS_EXPECTED_ACCOUNT")?,
        required("STORELINK_AWS_REGION")?,
        required("STORELINK_AWS_PROFILE")?,
        run,
    )?;
    let dir = PathBuf::from(required("STORELINK_AWS_MANIFEST_DIR")?);
    let path = manifest_path(&dir, &manifest)?;
    Ok((manifest, path))
}

#[cfg(unix)]
fn private_permissions(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn private_permissions(_: &fs::Metadata) -> bool {
    false // Refuse platforms where this harness cannot verify Unix privacy modes.
}

fn manifest_path(dir: &Path, manifest: &Manifest) -> Check<PathBuf> {
    if !dir.is_absolute() {
        return Err("manifest directory must be an absolute path outside the repository".into());
    }
    let dir = dir
        .canonicalize()
        .map_err(|_| "manifest directory must already exist")?;
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .map_err(|_| "cannot resolve repository path")?;
    let metadata = fs::metadata(&dir).map_err(|_| "cannot inspect manifest directory")?;
    if dir.starts_with(repo) || !metadata.is_dir() || !private_permissions(&metadata) {
        return Err(
            "manifest directory must be private (0700), outside the repository, on Unix".into(),
        );
    }
    Ok(dir.join(manifest.filename()))
}

fn write_manifest(path: &Path, manifest: &Manifest) -> Check<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|_| "cannot create NEW cleanup manifest; never overwrite/reuse a run")?;
    // A partial/unflushed manifest is a hard failure BEFORE any CreateTable call.
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|_| "cannot encode cleanup manifest")?;
    file.write_all(&bytes)
        .map_err(|_| "cannot write cleanup manifest")?;
    file.sync_all()
        .map_err(|_| "cannot sync cleanup manifest")?;
    File::open(path.parent().ok_or("missing manifest parent")?)
        .and_then(|dir| dir.sync_all())
        .map_err(|_| "cannot sync cleanup manifest directory")?;
    Ok(())
}

fn read_manifest(path: &Path, expected: &Manifest) -> Check<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| "matching cleanup manifest is required")?;
    if !metadata.is_file() || !private_permissions(&metadata) || metadata.len() > 16 * 1024 {
        return Err(
            "manifest must be a private regular file (0600), not a symlink, at most 16 KiB".into(),
        );
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| "cannot open cleanup manifest")?
        .take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read cleanup manifest")?;
    let actual: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| "invalid cleanup manifest")?;
    if &actual != expected {
        return Err(
            "manifest does not exactly match the run/account/region/profile and generated names"
                .into(),
        );
    }
    Ok(())
}

async fn verified_client(manifest: &Manifest) -> Check<Client> {
    let region = Region::new(manifest.region.clone());
    let timeouts = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(10))
        .operation_attempt_timeout(Duration::from_secs(30))
        .operation_timeout(Duration::from_secs(90))
        .build();
    let retries = RetryConfig::standard().with_max_attempts(3);
    // Explicit profile provider, NOT the environment-first default credential chain.
    // The selected profile is trusted authentication configuration (including any
    // credential_process/SSO/role chain); credential errors are intentionally redacted.
    let provider_config = ProviderConfig::without_region()
        .with_region(Some(region.clone()))
        .with_timeout_config(timeouts.clone())
        .with_retry_config(retries.clone());
    let provider = ProfileFileCredentialsProvider::builder()
        .profile_name(&manifest.profile)
        .configure(&provider_config)
        .build();
    let config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(&manifest.profile)
        .region(region.clone())
        .credentials_provider(provider)
        .timeout_config(timeouts.clone())
        .retry_config(retries.clone())
        .load()
        .await;
    let snapshot = config
        .credentials_provider()
        .ok_or("profile credential provider missing")?
        .provide_credentials()
        .await
        .map_err(|_| "profile credential resolution failed (details redacted)")?;
    // Fresh service builders intentionally do NOT inherit the loaded config's
    // endpoint_url or service_config. Global, service-specific and profile endpoint
    // overrides therefore cannot redirect STS identity verification or DynamoDB.
    // Both clients use the SAME in-memory, non-refreshing credential snapshot.
    let sts = aws_sdk_sts::Client::from_conf(
        aws_sdk_sts::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region.clone())
            .credentials_provider(snapshot.clone())
            .timeout_config(timeouts.clone())
            .retry_config(retries.clone())
            .build(),
    );
    let identity = sts
        .get_caller_identity()
        .send()
        .await
        .map_err(|_| "native STS GetCallerIdentity failed (details redacted)")?;
    if identity.account() != Some(manifest.account.as_str()) {
        return Err("STS account mismatch: refusing all mutations".into());
    }
    let caller = identity.arn().ok_or("STS returned no caller ARN")?;
    let parts: Vec<_> = caller.splitn(6, ':').collect();
    if parts.len() != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[4] != manifest.account {
        return Err("STS caller is outside the expected commercial partition/account".into());
    }
    Ok(Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region)
            .credentials_provider(snapshot)
            .timeout_config(timeouts)
            .retry_config(retries)
            .build(),
    ))
}

async fn absent_before_create(client: &Client, name: &str) -> Check<()> {
    match client.describe_table().table_name(name).send().await {
        Err(e)
            if e.as_service_error()
                .is_some_and(|e| e.is_resource_not_found_exception()) =>
        {
            Ok(())
        }
        Ok(_) => Err(format!(
            "{name}: already exists; refusing adoption or cleanup in this invocation"
        )),
        Err(_) => Err(format!("{name}: cannot confirm absence before creation")),
    }
}

async fn create(
    client: Client,
    manifest: Manifest,
    name: String,
    role: &'static str,
    creation: Creation,
) -> Check<()> {
    // Clone the verified native configuration (same credentials/endpoints/timeouts),
    // but NEVER retry CreateTable. Reads/waiters retain their bounded retry policy.
    let create_client = Client::from_conf(
        client
            .config()
            .to_builder()
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .build(),
    );
    let mut request = create_client
        .create_table()
        .table_name(&name)
        .billing_mode(BillingMode::PayPerRequest);
    for (field, key_type) in [("pk", KeyType::Hash), ("sk", KeyType::Range)] {
        request = request
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(field)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|_| "invalid fixture attribute")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(field)
                    .key_type(key_type)
                    .build()
                    .map_err(|_| "invalid fixture key schema")?,
            );
    }
    // Values are fixed at creation and never updated by this harness. IAM must
    // prevent other actors from modifying/reusing these run/purpose ownership tags.
    for (key, value) in [
        (PURPOSE_TAG, PURPOSE.to_owned()),
        (RUN_TAG, manifest.run.to_string()),
        (ROLE_TAG, role.into()),
    ] {
        request = request.tags(
            Tag::builder()
                .key(key)
                .value(value)
                .build()
                .map_err(|_| "invalid fixture tag")?,
        );
    }
    // Record BEFORE polling send: a timeout or panic may leave a remote request
    // alive. Local task completion never establishes remote request drain.
    creation.record(CreatePhase::SentUnknown);
    match request.send().await {
        Ok(_) => creation.record(CreatePhase::AcceptedUnconfirmed),
        Err(error) => {
            creation.record(create_failure_phase(error.as_service_error()));
            return Err(format!(
                "{name}: CreateTable failed/ambiguous; cleanup required"
            ));
        }
    }
    client
        .wait_until_table_exists()
        .table_name(&name)
        .wait(WAITER_LIMIT)
        .await
        .map_err(|_| format!("{name}: native readiness waiter failed/budget exhausted"))?;
    let owned = confirm_owned(&client, &manifest, &name, role)
        .await?
        .ok_or_else(|| format!("{name}: disappeared after readiness"))?;
    if !owned.active {
        return Err(format!("{name}: owned table not ACTIVE after readiness"));
    }
    creation.record(CreatePhase::OwnedActive);
    Ok(())
}

struct OwnedTable {
    arn: String,
    active: bool,
}

/// None means service-confirmed absence, not an access/transport/parse error.
async fn confirm_owned(
    client: &Client,
    manifest: &Manifest,
    name: &str,
    role: &str,
) -> Check<Option<OwnedTable>> {
    let output = match client.describe_table().table_name(name).send().await {
        Ok(output) => output,
        Err(e)
            if e.as_service_error()
                .is_some_and(|e| e.is_resource_not_found_exception()) =>
        {
            return Ok(None);
        }
        Err(_) => return Err(format!("{name}: DescribeTable failed; refusing deletion")),
    };
    let table = output
        .table()
        .ok_or_else(|| format!("{name}: missing description"))?;
    let expected_arn = manifest.arn(name);
    if table.table_name() != Some(name) || table.table_arn() != Some(expected_arn.as_str()) {
        return Err(format!(
            "{name}: canonical ARN/name/account/region mismatch; refusing deletion"
        ));
    }
    // DynamoDB allows at most 50 tags, but handle native pagination rather than
    // relying on page size. Bound pages and reject repeated pagination tokens.
    let mut tags = Vec::new();
    let mut next = None;
    let mut seen = std::collections::HashSet::new();
    loop {
        let page = client
            .list_tags_of_resource()
            .resource_arn(&expected_arn)
            .set_next_token(next)
            .send()
            .await
            .map_err(|_| format!("{name}: ListTagsOfResource failed; refusing deletion"))?;
        tags.extend_from_slice(page.tags());
        next = page.next_token().map(str::to_owned);
        if let Some(token) = &next {
            if seen.len() >= 10 || !seen.insert(token.clone()) {
                return Err(format!("{name}: invalid tag pagination; refusing deletion"));
            }
        } else {
            break;
        }
    }
    for (key, value) in [
        (PURPOSE_TAG, PURPOSE.to_owned()),
        (RUN_TAG, manifest.run.to_string()),
        (ROLE_TAG, role.into()),
    ] {
        let matching: Vec<_> = tags.iter().filter(|t| t.key() == key).collect();
        if matching.len() != 1 || matching[0].value() != value {
            return Err(format!(
                "{name}: ownership tags mismatch/missing; refusing deletion"
            ));
        }
    }
    Ok(Some(OwnedTable {
        arn: expected_arn,
        active: table.table_status() == Some(&TableStatus::Active),
    }))
}

async fn cleanup_one(
    client: Client,
    manifest: Manifest,
    name: String,
    role: &'static str,
    basis: CleanupBasis,
) -> Check<()> {
    if let Some(owned) = confirm_owned(&client, &manifest, &name, role).await? {
        // This matching observation resolves the one possible create. Delete by
        // its verified exact ARN, never a prefix or enumerated table. A present
        // non-ACTIVE table may reject deletion; the absence checks still apply.
        match client.delete_table().table_name(&owned.arn).send().await {
            Ok(_) => (),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|e| e.is_resource_not_found_exception()) => {}
            // A lost delete response may still have deleted the table. The native
            // absence waiter below must establish the postcondition independently.
            Err(_) => {
                eprintln!("{name}: DeleteTable failed/ambiguous; checking native absence waiter")
            }
        }
    } else {
        basis
            .confirm_unobserved_absence()
            .map_err(|error| format!("{name}: {error}"))?;
    }
    client
        .wait_until_table_not_exists()
        .table_name(&name)
        .wait(WAITER_LIMIT)
        .await
        .map_err(|_| {
            format!("{name}: deletion unconfirmed; preserve manifest and run cleanup-only")
        })?;
    // Do not interpret waiter completion alone as a catch-all success path.
    match client.describe_table().table_name(&name).send().await {
        Err(e)
            if e.as_service_error()
                .is_some_and(|e| e.is_resource_not_found_exception()) =>
        {
            Ok(())
        }
        _ => Err(format!(
            "{name}: final absence confirmation failed; preserve manifest"
        )),
    }
}

fn settled(label: &str, result: Result<Check<()>, tokio::task::JoinError>) -> Check<()> {
    // Do not print arbitrary panic payloads or credential-provider errors.
    result.map_err(|_| format!("{label}: task panicked/cancelled"))?
}

async fn cleanup_all(client: &Client, manifest: &Manifest, basis: [CleanupBasis; 2]) -> Check<()> {
    let coord = tokio::spawn(cleanup_one(
        client.clone(),
        manifest.clone(),
        manifest.coordination_table.clone(),
        "coordination",
        basis[0],
    ));
    let app = tokio::spawn(cleanup_one(
        client.clone(),
        manifest.clone(),
        manifest.application_table.clone(),
        "application",
        basis[1],
    ));
    let (coord, app) = tokio::join!(coord, app);
    let coord = settled("coordination cleanup", coord);
    let app = settled("application cleanup", app);
    if coord.is_err() || app.is_err() {
        return Err(format!(
            "cleanup incomplete; manifest RETAINED: coordination={coord:?}; application={app:?}"
        ));
    }
    eprintln!("both exact generated fixture tables confirmed absent; immutable manifest retained");
    Ok(())
}

async fn provision_and_exercise(
    client: Client,
    manifest: Manifest,
    creations: [Creation; 2],
) -> Check<()> {
    // Independent creates start together and BOTH outcomes settle, including a
    // panic in one task. No try_join! short-circuit can abandon the other create.
    let coord = tokio::spawn(create(
        client.clone(),
        manifest.clone(),
        manifest.coordination_table.clone(),
        "coordination",
        creations[0].clone(),
    ));
    let app = tokio::spawn(create(
        client.clone(),
        manifest.clone(),
        manifest.application_table.clone(),
        "application",
        creations[1].clone(),
    ));
    let (coord, app) = tokio::join!(coord, app);
    let coord = settled("coordination creation", coord);
    let app = settled("application creation", app);
    if coord.is_err() || app.is_err() {
        return Err(format!(
            "fixture creation: coordination={coord:?}; application={app:?}"
        ));
    }
    // Enrollment and every assertion are under the outer JoinHandle boundary.
    let fixture = journal_contract::Fixture::enroll(
        client,
        manifest.coordination_table,
        manifest.application_table,
    )
    .await;
    // Each native operation has a bounded SDK timeout. Do not cancel the whole
    // scenario future: dropping race JoinHandles could detach writes into cleanup.
    journal_contract::exercise(fixture).await;
    Ok(())
}

#[tokio::test]
#[ignore = "REAL AWS MUTATIONS: requires explicit authorization, account/profile/region/run/ack and private cleanup manifest directory"]
async fn native_journal_contract_aws() {
    let (manifest, path) = inputs().expect("AWS acceptance prerequisites failed");
    let client = verified_client(&manifest)
        .await
        .expect("AWS identity gate failed");
    let [(coord, _), (app, _)] = manifest.tables();
    let (coord_absent, app_absent) = tokio::join!(
        absent_before_create(&client, coord),
        absent_before_create(&client, app)
    );
    // No mutations yet. Existing names must not enter the cleanup path at all.
    assert!(
        coord_absent.is_ok() && app_absent.is_ok(),
        "fresh-name gate failed: {coord_absent:?}; {app_absent:?}"
    );
    write_manifest(&path, &manifest).expect("durable cleanup manifest gate failed");
    let creations = [Creation::default(), Creation::default()];
    let result = tokio::spawn(provision_and_exercise(
        client.clone(),
        manifest.clone(),
        creations.clone(),
    ))
    .await;
    // Partial creates, failed enrollment and assertion panic all arrive
    // here. Process kill/abort loses phases and requires operator-acknowledged
    // remote request settlement before cleanup_journal_aws below.
    let cleanup = cleanup_all(
        &client,
        &manifest,
        creations.map(|creation| CleanupBasis::Automatic(creation.phase())),
    )
    .await;
    let contract = settled("native journal contract", result);
    assert!(cleanup.is_ok(), "{cleanup:?}; contract={contract:?}");
    assert!(
        contract.is_ok(),
        "{contract:?}; both tables confirmed absent"
    );
}

#[tokio::test]
#[ignore = "REAL AWS DELETION ONLY: requires matching private manifest/run/account/profile/region/ack AND producer/request settlement acknowledgement; never enumerates tables"]
async fn cleanup_journal_aws() {
    let (manifest, path) = inputs().expect("AWS cleanup prerequisites failed");
    // Validate settlement acknowledgement and immutable allowlist BEFORE loading
    // credentials or contacting AWS. Neither a killed producer nor an SDK timeout
    // establishes that its original remote create requests have settled.
    let basis = cleanup_settlement_ack(
        std::env::var("STORELINK_AWS_CLEANUP_SETTLED_ACK")
            .ok()
            .as_deref(),
    )
    .expect("cleanup settlement acknowledgement gate failed");
    read_manifest(&path, &manifest).expect("cleanup manifest gate failed");
    let client = verified_client(&manifest)
        .await
        .expect("AWS cleanup identity gate failed");
    cleanup_all(&client, &manifest, [basis; 2])
        .await
        .expect("cleanup unconfirmed; manifest retained");
}

// These non-ignored tests are strictly in-memory/filesystem guard tests. They do
// not load AWS configuration, resolve credentials or construct service clients.
#[test]
fn unobserved_absence_requires_resolved_creation() {
    for phase in [
        CreatePhase::NeverIssued,
        CreatePhase::DefinitelyRejected,
        CreatePhase::OwnedActive,
    ] {
        assert!(
            CleanupBasis::Automatic(phase)
                .confirm_unobserved_absence()
                .is_ok()
        );
    }
    // A successful CreateTable response without a matching ACTIVE observation
    // is deliberately no safer than a lost response, timeout, or send panic.
    for phase in [CreatePhase::SentUnknown, CreatePhase::AcceptedUnconfirmed] {
        let error = CleanupBasis::Automatic(phase)
            .confirm_unobserved_absence()
            .unwrap_err();
        assert!(error.contains("late-create uncertainty"));
        assert!(error.contains("preserve manifest"));
    }
}

#[test]
fn single_attempt_rejection_classification_is_narrow() {
    use aws_sdk_dynamodb::types::error::{InternalServerError, ResourceInUseException};
    let rejected =
        CreateTableError::ResourceInUseException(ResourceInUseException::builder().build());
    assert_eq!(
        create_failure_phase(Some(&rejected)),
        CreatePhase::DefinitelyRejected
    );
    let unknown = CreateTableError::InternalServerError(InternalServerError::builder().build());
    assert_eq!(
        create_failure_phase(Some(&unknown)),
        CreatePhase::SentUnknown
    );
    // No modeled service error includes transport/timeout/response failures.
    assert_eq!(create_failure_phase(None), CreatePhase::SentUnknown);
}

#[test]
fn cleanup_requires_exact_explicit_settlement_acknowledgement() {
    // Pure input validation: no environment mutation, configuration or credentials.
    for value in [
        None,
        Some(""),
        Some(ACK),
        Some("true"),
        Some("PROCESS-EXITED"),
    ] {
        assert!(cleanup_settlement_ack(value).is_err());
    }
    assert!(cleanup_settlement_ack(Some(&format!("{CLEANUP_SETTLED_ACK} "))).is_err());
    let basis = cleanup_settlement_ack(Some(CLEANUP_SETTLED_ACK)).unwrap();
    assert!(basis.confirm_unobserved_absence().is_ok());
}

#[tokio::test]
async fn creation_phases_survive_panic_and_both_tasks_settle() {
    let creations = [Creation::default(), Creation::default()];
    assert_eq!(creations[0].phase(), CreatePhase::NeverIssued);
    let first = creations[0].clone();
    let second = creations[1].clone();
    let coord = tokio::spawn(async move {
        first.record(CreatePhase::SentUnknown);
        panic!("synthetic offline send panic");
        #[allow(unreachable_code)]
        Ok(())
    });
    let app = tokio::spawn(async move {
        tokio::task::yield_now().await;
        second.record(CreatePhase::SentUnknown);
        second.record(CreatePhase::AcceptedUnconfirmed);
        second.record(CreatePhase::OwnedActive);
        Ok(())
    });
    let (coord, app) = tokio::join!(coord, app);
    assert!(settled("coordination creation", coord).is_err());
    assert!(settled("application creation", app).is_ok());
    assert_eq!(creations[0].phase(), CreatePhase::SentUnknown);
    assert_eq!(creations[1].phase(), CreatePhase::OwnedActive);
    assert!(
        CleanupBasis::Automatic(creations[0].phase())
            .confirm_unobserved_absence()
            .is_err()
    );
    assert!(
        CleanupBasis::Automatic(creations[1].phase())
            .confirm_unobserved_absence()
            .is_ok()
    );
}

#[test]
fn manifest_names_are_exact_and_bounded() {
    let run = Uuid::parse_str("3bb5b062-251e-48ec-b0e6-a5cb2d35ca67").unwrap();
    let manifest = Manifest::new(
        "000000000000".into(),
        "us-east-1".into(),
        "disposable-test".into(),
        run,
    )
    .unwrap();
    assert_eq!(manifest.tables().len(), 2);
    assert_eq!(
        manifest.coordination_table,
        "storelink-acceptance-journal-coord-3bb5b062251e48ecb0e6a5cb2d35ca67"
    );
    assert_eq!(
        manifest.application_table,
        "storelink-acceptance-journal-app-3bb5b062251e48ecb0e6a5cb2d35ca67"
    );
    assert_ne!(manifest.coordination_table, manifest.application_table);
    assert_eq!(
        serde_json::from_slice::<Manifest>(&serde_json::to_vec(&manifest).unwrap()).unwrap(),
        manifest
    );
    assert!(Manifest::new("123".into(), "us-east-1".into(), "test".into(), run).is_err());
    assert!(
        Manifest::new(
            "000000000000".into(),
            "us-east-1".into(),
            "test".into(),
            Uuid::nil()
        )
        .is_err()
    );
    assert!(
        Manifest::new(
            "000000000000".into(),
            "cn-north-1".into(),
            "test".into(),
            run
        )
        .is_err()
    );
}

#[cfg(unix)]
#[test]
fn manifest_recovery_guards_are_fail_closed() {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt, symlink};
    let manifest = Manifest::new(
        "000000000000".into(),
        "us-east-1".into(),
        "disposable-test".into(),
        Uuid::new_v4(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("storelink-manifest-guard-{}", manifest.run));
    fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    // Keep this temporary diagnostic fixture outside the checkout, just as a real
    // cleanup record must be. No source tree is copied and no service is called.
    let result = std::panic::catch_unwind(|| {
        let path = manifest_path(&dir, &manifest).unwrap();
        assert!(read_manifest(&path, &manifest).is_err());
        write_manifest(&path, &manifest).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let original = fs::read(&path).unwrap();
        assert!(write_manifest(&path, &manifest).is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
        read_manifest(&path, &manifest).unwrap();
        let mut wrong = manifest.clone();
        wrong.account = "111111111111".into();
        assert!(read_manifest(&path, &wrong).is_err());
        wrong = manifest.clone();
        wrong.application_table = "storelink-acceptance-journal-app-arbitrary".into();
        assert!(read_manifest(&path, &wrong).is_err());
        wrong = manifest.clone();
        wrong.profile = "different-profile".into();
        assert!(read_manifest(&path, &wrong).is_err());
        let link = dir.join("symlink.json");
        symlink(&path, &link).unwrap();
        assert!(read_manifest(&link, &manifest).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_manifest(&path, &manifest).is_err());
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(manifest_path(&dir, &manifest).is_err());
        assert!(manifest_path(Path::new(env!("CARGO_MANIFEST_DIR")), &manifest).is_err());
        assert!(manifest_path(Path::new("relative"), &manifest).is_err());
    });
    fs::remove_dir_all(&dir).unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
