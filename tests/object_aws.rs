//! Real AWS only inside the ignored entrypoint. No provisioning or client MPU abort.
//! Ordinary tests use pure guards or a synthetic-credential loopback SDK client;
//! they never load AWS configuration or contact AWS.
#![cfg(all(feature = "aws-acceptance", feature = "s3"))]

use aws_config::{profile::ProfileFileCredentialsProvider, provider_config::ProviderConfig};
use aws_sdk_dynamodb::{
    Client as Dynamo,
    config::{
        BehaviorVersion, ProvideCredentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
    },
    types::{AttributeValue as A, Delete, Put, TableDescription, TableStatus, TransactWriteItem},
};
use aws_sdk_s3::Client as S3;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storelink::{
    claim::{ClaimPolicy, ClaimState},
    clock::{ClockError, ClockObservation, TimestampMs},
    dynamodb::{
        ApplicationActions, AttemptState, CommandJournal, EnrolledTables, IntentState,
        InventoryProgressError, NamespaceSnapshot, OrphanError, OrphanGracePolicy,
        OrphanPreparation, Outcome, PreparedCommand, ReclamationError, ReferenceChange,
        ReferenceId, UploadConstraints, VersionState,
    },
    identity::{DomainId, NamespaceId, ResourceId, StoreId, VersionRef},
    s3::{
        DeletionError, DeletionOutcome, NamespaceConfiguration, NamespaceConfigurationSpec,
        ObservedOrphanObject, OrphanAttribution, QualifiedNamespace, VersionPage,
        delete_retiring_version, list_version_page, observe_orphan_object, qualify_existing,
        qualify_fresh, transfer_upload, verify_candidate,
    },
};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

type Check<T = ()> = Result<T, &'static str>;
type Item = HashMap<String, A>;
const ACK: &str = "WRITE-AND-CLEAN-DISPOSABLE-STORELINK-OBJECT-FIXTURE";
const TAG: &str = "StoreLinkAcceptanceRun";
const ENV: &str = "STORELINK_OBJECT_AWS_";
const MAX_PAYLOAD: usize = 8 * 1024 * 1024 + 17;
const PAGE_SIZE: i32 = 100;
const MAX_LIST: usize = 1000;
const MAX_PAGES: usize = 10;
const PREFLIGHT: Duration = Duration::from_secs(180);
const SCENARIO: Duration = Duration::from_secs(600);
const CLEANUP: Duration = Duration::from_secs(180);
const KNOWN_ORPHAN: &str = "orphan-unselected";
const UNATTRIBUTED_ORPHAN: &str = "orphan-unattributed";
const INVENTORY_LABELS: [&str; 2] = ["inventory-first", "inventory-second"];
const PLANNED_LABELS: [&str; 7] = [
    "empty",
    "small",
    "multipart",
    KNOWN_ORPHAN,
    UNATTRIBUTED_ORPHAN,
    INVENTORY_LABELS[0],
    INVENTORY_LABELS[1],
];

// Errors intentionally discard SDK bodies, native metadata, credentials and paths.
fn checked<T, E>(value: Result<T, E>, label: &'static str) -> Check<T> {
    value.map_err(|_| label)
}
fn require(condition: bool, label: &'static str) -> Check {
    if condition { Ok(()) } else { Err(label) }
}

/// This may be a private JSON value exported by normal Terraform outputs, NOT
/// historical state/adoption input. Credentials and profile are deliberately absent.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Fixture {
    account: String,
    region: String,
    run_uuid: String,
    coordination_table_arn: String,
    application_table_arn: String,
    bucket: String,
}
impl Fixture {
    fn validate(&self) -> Check {
        require(
            self.account.len() == 12 && self.account.bytes().all(|b| b.is_ascii_digit()),
            "invalid account",
        )?;
        let run = checked(Uuid::parse_str(&self.run_uuid), "invalid run UUID")?;
        require(
            run.get_version_num() == 4
                && run.get_variant() == uuid::Variant::RFC4122
                && run.to_string() == self.run_uuid,
            "canonical v4 run UUID required",
        )?;
        let parts: Vec<_> = self.region.split('-').collect();
        require(
            parts.len() >= 3
                && self.region.len() <= 32
                && self
                    .region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !parts.iter().any(|p| p.is_empty())
                && parts
                    .last()
                    .is_some_and(|p| p.bytes().all(|b| b.is_ascii_digit()))
                && !["cn-", "us-gov-", "us-iso"]
                    .iter()
                    .any(|p| self.region.starts_with(p)),
            "commercial region required",
        )?;
        for arn in self.tables() {
            self.table_name(arn)?;
        }
        require(
            self.coordination_table_arn != self.application_table_arn,
            "two distinct exact tables required",
        )?;
        // Reuse the public configuration validator, not a fresh-qualification factory.
        self.configuration("a")?;
        Ok(())
    }
    fn tables(&self) -> [&str; 2] {
        [&self.coordination_table_arn, &self.application_table_arn]
    }
    fn table_name<'a>(&self, arn: &'a str) -> Check<&'a str> {
        let prefix = format!("arn:aws:dynamodb:{}:{}:table/", self.region, self.account);
        let name = arn
            .strip_prefix(&prefix)
            .ok_or("ARN account/region/partition mismatch")?;
        require(
            (3..=255).contains(&name.len())
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.')),
            "exact table ARN required, not index or wildcard",
        )?;
        Ok(name)
    }
    fn prefix(&self) -> String {
        format!("storelink-object/{}/", self.run_uuid)
    }
    fn domain(&self) -> Check<DomainId> {
        checked(
            DomainId::new(format!("object-{}", self.run_uuid)),
            "domain ID",
        )
    }
    fn configuration(&self, slot: &str) -> Check<NamespaceConfiguration> {
        require(matches!(slot, "a" | "b"), "unknown namespace slot")?;
        checked(
            NamespaceConfiguration::new(NamespaceConfigurationSpec {
                id: checked(
                    NamespaceId::new(format!("object-{}-{slot}", self.run_uuid)),
                    "namespace ID",
                )?,
                store: checked(
                    StoreId::new(format!("object-{}", self.run_uuid)),
                    "store ID",
                )?,
                bucket: self.bucket.clone(),
                expected_owner: self.account.clone(),
                region: self.region.clone(),
                key_prefix: format!("{}{slot}/", self.prefix()),
                mpu_max_age_ms: 7_200_000,
                mpu_lifecycle_margin_ms: 3_600_000,
            }),
            "namespace configuration",
        )
    }
    fn owns_key(&self, key: &str) -> bool {
        key.strip_prefix(&self.prefix())
            .is_some_and(|suffix| !suffix.is_empty())
    }
    fn check_description(&self, arn: &str, table: &TableDescription) -> Check {
        require(
            table.table_arn() == Some(arn)
                && table.table_name() == Some(self.table_name(arn)?)
                && table.table_status() == Some(&TableStatus::Active),
            "table identity/status mismatch",
        )?;
        let keys: BTreeSet<_> = table
            .key_schema()
            .iter()
            .map(|k| (k.attribute_name(), k.key_type().as_str()))
            .collect();
        let attrs: BTreeSet<_> = table
            .attribute_definitions()
            .iter()
            .map(|a| (a.attribute_name(), a.attribute_type().as_str()))
            .collect();
        require(
            table.key_schema().len() == 2
                && table.attribute_definitions().len() == 2
                && keys == BTreeSet::from([("pk", "HASH"), ("sk", "RANGE")])
                && attrs == BTreeSet::from([("pk", "S"), ("sk", "S")]),
            "plain pk/sk string schema required",
        )
    }
    fn check_tags<'a>(&self, tags: impl Iterator<Item = (&'a str, &'a str)>) -> Check {
        let values: Vec<_> = tags.filter(|(k, _)| *k == TAG).map(|(_, v)| v).collect();
        require(
            values == [self.run_uuid.as_str()],
            "missing/duplicate/mismatched run ownership tag",
        )
    }
}

fn required(suffix: &str) -> Check<String> {
    let value = checked(
        std::env::var(format!("{ENV}{suffix}")),
        "missing explicit object fixture input",
    )?;
    require(
        !value.is_empty()
            && value.len() <= 4096
            && value.trim() == value
            && !value.chars().any(char::is_control),
        "invalid explicit object fixture input",
    )?;
    Ok(value)
}
#[cfg(unix)]
fn private(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o077 == 0
}
#[cfg(not(unix))]
fn private(_: &fs::Metadata) -> bool {
    false
}

fn private_directory(path: &Path) -> Check<PathBuf> {
    require(path.is_absolute(), "private directory must be absolute")?;
    let resolved = checked(path.canonicalize(), "cannot resolve private directory")?;
    let repo = checked(
        Path::new(env!("CARGO_MANIFEST_DIR")).canonicalize(),
        "cannot resolve checkout",
    )?;
    let meta = checked(fs::metadata(&resolved), "cannot inspect private directory")?;
    require(
        !resolved.starts_with(repo) && meta.is_dir() && private(&meta),
        "private directory must be outside checkout (0700, Unix)",
    )?;
    Ok(resolved)
}
fn read_private_fixture(path: &Path) -> Check<Fixture> {
    private_directory(path.parent().ok_or("manifest parent missing")?)?;
    let meta = checked(
        fs::symlink_metadata(path),
        "cannot inspect fixture manifest",
    )?;
    require(
        meta.is_file() && private(&meta) && meta.len() <= 16 * 1024,
        "manifest must be private regular file <=16KiB, not symlink",
    )?;
    let mut bytes = Vec::new();
    checked(
        checked(File::open(path), "cannot open fixture manifest")?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes),
        "cannot read fixture manifest",
    )?;
    require(bytes.len() <= 16 * 1024, "fixture manifest too large")?;
    checked(
        serde_json::from_slice(&bytes),
        "invalid fixture manifest JSON",
    )
}
fn inputs() -> Check<(Fixture, String, PathBuf)> {
    require(
        required("ACK")? == ACK,
        "explicit disposable fixture acknowledgement required",
    )?;
    let fixture = Fixture {
        account: required("ACCOUNT")?,
        region: required("REGION")?,
        run_uuid: required("RUN_UUID")?,
        coordination_table_arn: required("COORDINATION_TABLE_ARN")?,
        application_table_arn: required("APPLICATION_TABLE_ARN")?,
        bucket: required("BUCKET")?,
    };
    fixture.validate()?;
    let profile = required("PROFILE")?;
    // Optional manifest, when supplied, must match EVERY explicit fixture input.
    if let Some(path) = std::env::var_os(format!("{ENV}FIXTURE_MANIFEST")) {
        require(
            read_private_fixture(Path::new(&path))? == fixture,
            "fixture manifest/env mismatch",
        )?;
    }
    let dir = private_directory(Path::new(&required("EVIDENCE_DIR")?))?;
    Ok((fixture, profile, dir))
}
fn persist_evidence(f: &Fixture, dir: &Path) -> Check {
    let path = dir.join(format!("object-{}.json", f.run_uuid));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = checked(
        options.open(path),
        "cannot create NEW evidence; never reuse run",
    )?;
    let record = serde_json::json!({"format": 1, "fixture": f, "prefix": f.prefix(),
        "keys": PLANNED_LABELS.map(|label| format!("{}a/{label}", f.prefix())),
        "warning": "Any request may have unknown outcome; enumerate exact prefix versions, markers and MPUs. Local exit is not remote settlement. No credentials recorded."});
    let bytes = checked(serde_json::to_vec_pretty(&record), "cannot encode evidence")?;
    require(bytes.len() <= 16 * 1024, "evidence size exceeded")?;
    checked(file.write_all(&bytes), "cannot write evidence")?;
    checked(file.sync_all(), "cannot sync evidence")?;
    checked(
        checked(File::open(dir), "cannot open evidence directory")?.sync_all(),
        "cannot sync evidence directory",
    )
}

async fn clients(f: &Fixture, profile: &str) -> Check<(Dynamo, S3)> {
    let region = Region::new(f.region.clone());
    let timeouts = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(10))
        .operation_attempt_timeout(Duration::from_secs(20))
        .operation_timeout(Duration::from_secs(20))
        .build();
    let retries = RetryConfig::standard().with_max_attempts(1);
    let provider = ProfileFileCredentialsProvider::builder()
        .profile_name(profile)
        .configure(
            &ProviderConfig::without_region()
                .with_region(Some(region.clone()))
                .with_timeout_config(timeouts.clone())
                .with_retry_config(retries.clone()),
        )
        .build();
    let snapshot = checked(
        checked(
            tokio::time::timeout(Duration::from_secs(30), provider.provide_credentials()).await,
            "profile resolution timeout",
        )?,
        "explicit profile resolution failed",
    )?;
    // Fresh service builders, not ambient shared config: no default credentials,
    // endpoint_url or service_config inheritance. Same frozen credentials for ALL.
    let sts = aws_sdk_sts::Client::from_conf(
        aws_sdk_sts::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region.clone())
            .credentials_provider(snapshot.clone())
            .timeout_config(timeouts.clone())
            .retry_config(retries.clone())
            .build(),
    );
    let identity = checked(
        sts.get_caller_identity().send().await,
        "STS identity check failed",
    )?;
    require(
        identity.account() == Some(f.account.as_str()),
        "STS account mismatch",
    )?;
    let arn: Vec<_> = identity
        .arn()
        .ok_or("STS caller ARN missing")?
        .splitn(6, ':')
        .collect();
    require(
        arn.len() == 6 && arn[0] == "arn" && arn[1] == "aws" && arn[4] == f.account,
        "STS partition/account mismatch",
    )?;
    let db = Dynamo::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region.clone())
            .credentials_provider(snapshot.clone())
            .timeout_config(timeouts.clone())
            .retry_config(retries.clone())
            .build(),
    );
    let s3 = S3::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(region)
            .credentials_provider(snapshot)
            .timeout_config(timeouts)
            .retry_config(retries)
            .build(),
    );
    Ok((db, s3))
}
async fn bucket_gate(f: &Fixture, s3: &S3) -> Check {
    let head = checked(
        s3.head_bucket()
            .bucket(&f.bucket)
            .expected_bucket_owner(&f.account)
            .send()
            .await,
        "HeadBucket failed",
    )?;
    require(
        head.bucket_region() == Some(f.region.as_str()),
        "bucket actual region mismatch",
    )?;
    let tags = checked(
        s3.get_bucket_tagging()
            .bucket(&f.bucket)
            .expected_bucket_owner(&f.account)
            .send()
            .await,
        "bucket tags unavailable",
    )?;
    require(tags.tag_set().len() <= 50, "bucket tag limit")?;
    f.check_tags(tags.tag_set().iter().map(|t| (t.key(), t.value())))
}
async fn tables_gate(f: &Fixture, db: &Dynamo) -> Check {
    for arn in f.tables() {
        let out = checked(
            db.describe_table().table_name(arn).send().await,
            "DescribeTable failed",
        )?;
        f.check_description(arn, out.table().ok_or("table description missing")?)?;
        let mut next = None;
        let mut seen = BTreeSet::new();
        let mut tags = Vec::new();
        for page in 0..MAX_PAGES {
            let out = checked(
                db.list_tags_of_resource()
                    .resource_arn(arn)
                    .set_next_token(next)
                    .send()
                    .await,
                "table tag listing failed",
            )?;
            tags.extend_from_slice(out.tags());
            require(tags.len() <= 50, "table tag bound exceeded")?;
            next = out.next_token().map(str::to_owned);
            match &next {
                None => break,
                Some(token) => require(
                    !token.is_empty()
                        && token.len() <= 4096
                        && seen.insert(token.clone())
                        && page + 1 < MAX_PAGES,
                    "partial/invalid table tag pagination",
                )?,
            }
        }
        f.check_tags(tags.iter().map(|t| (t.key(), t.value())))?;
    }
    Ok(())
}

// Strict bounded complete pagination. Missing truncation flags, repeated/partial
// cursors or a budget boundary are NOT evidence of emptiness or complete cleanup.
#[derive(Default)]
struct Pages {
    count: usize,
    pages: usize,
    seen: BTreeSet<(String, String)>,
}
impl Pages {
    fn next(
        &mut self,
        count: usize,
        truncated: Option<bool>,
        key: Option<&str>,
        other: Option<&str>,
    ) -> Check<Option<(String, String)>> {
        self.count += count;
        self.pages += 1;
        require(
            count <= PAGE_SIZE as usize && self.count <= MAX_LIST && self.pages <= MAX_PAGES,
            "listing budget exceeded; incomplete",
        )?;
        match truncated {
            // Native S3 may retain empty OR populated next-marker fields on a
            // terminal response. IsTruncated, not marker absence, is authoritative.
            Some(false) => Ok(None),
            Some(true) => {
                let (key, other) = (
                    key.ok_or("partial pagination cursor")?,
                    other.ok_or("partial pagination cursor")?,
                );
                require(
                    !key.is_empty()
                        && !other.is_empty()
                        && key.len() <= 1024
                        && other.len() <= 4096
                        && self.pages < MAX_PAGES
                        && self.count < MAX_LIST,
                    "pagination exhausted/invalid; incomplete",
                )?;
                let cursor = (key.to_owned(), other.to_owned());
                require(
                    self.seen.insert(cursor.clone()),
                    "repeated pagination cursor",
                )?;
                Ok(Some(cursor))
            }
            None => Err("missing pagination classification"),
        }
    }
}
async fn versions(f: &Fixture, s3: &S3) -> Check<Vec<(String, String)>> {
    let mut rows = Vec::new();
    let mut pages = Pages::default();
    let (mut key, mut version) = (None, None);
    loop {
        let out = checked(
            s3.list_object_versions()
                .bucket(&f.bucket)
                .expected_bucket_owner(&f.account)
                .prefix(f.prefix())
                .max_keys(PAGE_SIZE)
                .set_key_marker(key)
                .set_version_id_marker(version)
                .send()
                .await,
            "version listing failed; incomplete",
        )?;
        for (key, version) in out
            .versions()
            .iter()
            .map(|v| (v.key(), v.version_id()))
            .chain(
                out.delete_markers()
                    .iter()
                    .map(|v| (v.key(), v.version_id())),
            )
        {
            let (key, version) = (
                key.ok_or("listed key missing")?,
                version.ok_or("listed version missing")?,
            );
            require(
                f.owns_key(key)
                    && key.len() <= 1024
                    && !version.is_empty()
                    && version != "null"
                    && version.len() <= 4096,
                "unowned/invalid version; refusing cleanup",
            )?;
            rows.push((key.to_owned(), version.to_owned()));
        }
        match pages.next(
            out.versions().len() + out.delete_markers().len(),
            out.is_truncated(),
            out.next_key_marker(),
            out.next_version_id_marker(),
        )? {
            None => return Ok(rows),
            Some((k, v)) => {
                key = Some(k);
                version = Some(v);
            }
        }
    }
}
async fn multipart_count(f: &Fixture, s3: &S3) -> Check<usize> {
    let mut pages = Pages::default();
    let (mut key, mut upload) = (None, None);
    loop {
        let out = checked(
            s3.list_multipart_uploads()
                .bucket(&f.bucket)
                .expected_bucket_owner(&f.account)
                .prefix(f.prefix())
                .max_uploads(PAGE_SIZE)
                .set_key_marker(key)
                .set_upload_id_marker(upload)
                .send()
                .await,
            "MPU listing failed; incomplete",
        )?;
        for mpu in out.uploads() {
            require(
                mpu.key().is_some_and(|k| f.owns_key(k) && k.len() <= 1024)
                    && mpu
                        .upload_id()
                        .is_some_and(|id| !id.is_empty() && id.len() <= 4096),
                "unowned/invalid MPU listing",
            )?;
        }
        match pages.next(
            out.uploads().len(),
            out.is_truncated(),
            out.next_key_marker(),
            out.next_upload_id_marker(),
        )? {
            None => return Ok(pages.count),
            Some((k, u)) => {
                key = Some(k);
                upload = Some(u);
            }
        }
    }
}
async fn cleanup(f: &Fixture, s3: &S3, successful: bool) -> Result<(), Vec<&'static str>> {
    // Reserve the final minute of the outer cleanup budget for BOTH inventories.
    // A delete timeout/failure must not abandon other independently owned versions.
    let mutation_deadline = tokio::time::Instant::now() + CLEANUP - Duration::from_secs(60);
    let prewrite = async {
        bucket_gate(f, s3).await?;
        let (listed, mpus) = tokio::join!(versions(f, s3), multipart_count(f, s3));
        let listed = listed?;
        // Ownership failure before writes stops deletion, including reassigned tags.
        bucket_gate(f, s3).await?;
        require(
            listed.iter().all(|(key, _)| f.owns_key(key)),
            "cleanup escaped exact UUID prefix",
        )?;
        Ok::<_, &'static str>((listed, mpus))
    };
    let (listed, mpus) = checked(
        tokio::time::timeout_at(mutation_deadline, prewrite).await,
        "cleanup prewrite deadline; RETAIN fixture",
    )
    .and_then(|r| r)
    .map_err(|e| vec![e])?;
    let mut failures = Vec::new();
    for (key, version) in listed {
        if tokio::time::Instant::now() >= mutation_deadline {
            failures.push("delete budget exhausted; RETAIN fixture");
            break;
        }
        let deletion = checked(
            tokio::time::timeout_at(
                mutation_deadline,
                s3.delete_object()
                    .bucket(&f.bucket)
                    .expected_bucket_owner(&f.account)
                    .key(key)
                    .version_id(version)
                    .send(),
            )
            .await,
            "delete budget exhausted; remote outcome unknown",
        )
        .and_then(|r| checked(r, "exact version deletion unconfirmed"));
        if let Err(error) = deletion {
            failures.push(error);
        }
    }
    // Never short-circuit these independent final reads after a deletion failure.
    let final_inventory = tokio::time::timeout(Duration::from_secs(55), async {
        tokio::join!(versions(f, s3), multipart_count(f, s3))
    })
    .await;
    let (remaining, final_mpus) = final_inventory.unwrap_or((
        Err("final version inventory deadline; RETAIN fixture"),
        Err("final MPU inventory deadline; RETAIN fixture"),
    ));
    if let Err(error) =
        remaining.and_then(|v| require(v.is_empty(), "versions remain; cleanup incomplete"))
    {
        failures.push(error);
    }
    for inventory in [mpus, final_mpus] {
        if let Err(error) = inventory.and_then(|n| {
            require(
                n == 0,
                "MPUs remain; cleanup incomplete; RETAIN fixture for provider lifecycle",
            )
        }) {
            failures.push(error);
        }
    }
    // Empty current listings never establish remote settlement after lost results.
    if !successful {
        failures.push("scenario failed/unknown; cleanup observations not settlement proof; RETAIN fixture and evidence");
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

fn sample_clock() -> Result<ClockObservation, ClockError> {
    // FIXTURE ASSUMPTION: operator trusts this host's synchronized wall clock to
    // within 1s. Sampling SystemTime does NOT prove production clock calibration.
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ClockError::Untrusted)?
        .as_millis();
    ClockObservation::trusted(
        TimestampMs::new(u64::try_from(ms).map_err(|_| ClockError::Overflow)?)?,
        1000,
    )
}
fn timing() -> Check<(ClockObservation, TimestampMs)> {
    let clock = checked(sample_clock(), "fixture clock unavailable")?;
    let deadline = checked(
        clock.upper_bound().and_then(|n| n.checked_add(590_000)),
        "fixture deadline overflow",
    )?;
    Ok((clock, deadline))
}
async fn commit(journal: &CommandJournal, command: &PreparedCommand) -> Check {
    let report = checked(
        journal.execute(command).await,
        "native journal execution failed",
    )?;
    require(
        report.outcome == Outcome::Committed(b"object-fixture".to_vec())
            && report.attempt_failure.is_none(),
        "native transaction not committed",
    )
}
fn result() -> Vec<u8> {
    b"object-fixture".to_vec()
}
fn key(pk: String) -> Item {
    HashMap::from([("pk".into(), A::S(pk)), ("sk".into(), A::S("M".into()))])
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn domain_prefix(domain: &DomainId) -> String {
    format!("D#{:x}", Sha256::digest(domain.as_str().as_bytes()))
}
async fn get(db: &Dynamo, table: &str, key: Item) -> Check<Item> {
    checked(
        db.get_item()
            .table_name(table)
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await,
        "native strong GetItem failed",
    )?
    .item
    .ok_or("expected native item missing")
}
async fn counts(db: &Dynamo, f: &Fixture, version: &VersionRef, refs: u64, claims: u64) -> Check {
    // Read-only v0 address/schema inspection; no synthetic reserved-row writes or
    // fake Dynamo expression evaluator. Selection/publication create every row.
    let row = get(
        db,
        &f.coordination_table_arn,
        key(format!(
            "{}#OBJ#{}",
            domain_prefix(&f.domain()?),
            hex(&version.fingerprint())
        )),
    )
    .await?;
    require(
        row.get("references") == Some(&A::N(refs.to_string()))
            && row.get("claims") == Some(&A::N(claims.to_string()))
            && row.get("state") == Some(&A::S("READY".into())),
        "native version accounting mismatch",
    )
}
async fn enroll(
    journal: &CommandJournal,
    s3: &S3,
    config: &NamespaceConfiguration,
) -> Check<NamespaceSnapshot> {
    let fresh = checked(
        qualify_fresh(s3, config).await,
        "native fresh qualification failed",
    )?;
    let context = checked(journal.command_context().await, "enrollment context")?;
    let command = checked(
        journal.prepare_enroll_namespace(context, fresh, result()),
        "prepare namespace enrollment",
    )?;
    commit(journal, &command).await?;
    let ns = checked(
        journal.read_namespace(config.id()).await,
        "read enrolled namespace",
    )?
    .ok_or("namespace missing")?;
    require(
        ns.id() == config.id()
            && ns.store() == config.store()
            && ns.key_prefix() == config.key_prefix()
            && ns.bucket() == config.bucket()
            && ns.expected_owner() == config.expected_owner()
            && ns.region() == config.region(),
        "enrolled namespace binding mismatch",
    )?;
    Ok(ns)
}
#[allow(clippy::too_many_arguments)]
async fn upload_case(
    f: &Fixture,
    db: &Dynamo,
    s3: &S3,
    journal: &CommandJournal,
    ns: &NamespaceSnapshot,
    qualified: &QualifiedNamespace,
    label: &str,
    payload: Vec<u8>,
) -> Check {
    require(payload.len() <= MAX_PAYLOAD, "payload bound")?;
    let policy = checked(ClaimPolicy::new(600_000, 30_000, 1000), "claim policy")?;
    let hash: [u8; 32] = Sha256::digest(&payload).into();
    let context = checked(journal.command_context().await, "intent context")?;
    let intent_id = ResourceId::from_command(context.id());
    let (clock, deadline) = timing()?;
    let command = checked(
        journal.prepare_begin_intent(
            context,
            ns,
            format!("{}{label}", ns.key_prefix()),
            checked(
                UploadConstraints::new(MAX_PAYLOAD as u64, Some(payload.len() as u64), Some(hash)),
                "constraints",
            )?,
            deadline,
            clock,
            &policy,
            result(),
        ),
        "prepare intent",
    )?;
    commit(journal, &command).await?;
    let intent =
        checked(journal.read_intent(&intent_id).await, "read intent")?.ok_or("intent missing")?;
    let context = checked(journal.command_context().await, "attempt context")?;
    let attempt_id = ResourceId::from_command(context.id());
    require(attempt_id != intent_id, "resource identities reused")?;
    let (clock, deadline) = timing()?;
    let mpu_until = checked(
        clock.lower_bound().and_then(|n| n.checked_add(7_200_000)),
        "MPU deadline",
    )?;
    let command = checked(
        journal.prepare_begin_attempt(
            context,
            &intent,
            ns,
            deadline,
            mpu_until,
            clock,
            &policy,
            result(),
        ),
        "prepare attempt",
    )?;
    commit(journal, &command).await?;
    let candidate = checked(
        transfer_upload(
            journal,
            qualified,
            &attempt_id,
            std::io::Cursor::new(&payload),
            &policy,
            &sample_clock,
        )
        .await,
        "native transfer failed/unknown",
    )?;
    require(
        candidate.length() == payload.len() as u64 && candidate.full_sha256() == &hash,
        "transfer length/hash mismatch",
    )?;
    let version = candidate.version().clone();
    let receipt = checked(
        verify_candidate(
            journal,
            qualified,
            &attempt_id,
            &version,
            &policy,
            &sample_clock,
        )
        .await,
        "native exact-version full-hash verification failed",
    )?;
    let context = checked(journal.command_context().await, "selection context")?;
    let (clock, deadline) = timing()?;
    let selection = checked(
        journal.prepare_select_upload(context, receipt, deadline, clock, &policy, result()),
        "prepare selection",
    )?;
    commit(journal, &selection).await?;
    let stage =
        checked(journal.read_staged(&intent_id).await, "read stage")?.ok_or("stage missing")?;
    let intent = checked(
        journal.read_intent(&intent_id).await,
        "read selected intent",
    )?
    .ok_or("selected intent missing")?;
    require(
        stage.version() == &version
            && stage.verified_length() == payload.len() as u64
            && stage.verified_full_sha256() == Some(&hash)
            && stage.claim().state() == ClaimState::Active
            && intent.state() == IntentState::Selected,
        "selection state mismatch",
    )?;
    counts(db, f, &version, 0, 1).await?;
    let slot = checked(
        ReferenceId::new(format!("{}-{label}", f.run_uuid)),
        "reference ID",
    )?;
    let revision = Uuid::new_v4();
    let app_key = key(format!("object-{}-{label}", f.run_uuid));
    let mut item = app_key.clone();
    for (name, value) in [
        ("store", version.store().as_str()),
        ("object_key", version.key()),
        ("version_id", version.version()),
    ] {
        item.insert(name.into(), A::S(value.into()));
    }
    item.insert("revision".into(), A::S(revision.to_string()));
    let put = checked(
        Put::builder()
            .table_name(&f.application_table_arn)
            .set_item(Some(item.clone()))
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .build(),
        "application Put",
    )?;
    let application = checked(
        ApplicationActions::new(vec![TransactWriteItem::builder().put(put).build()]),
        "application actions",
    )?;
    let change = checked(
        ReferenceChange::new(slot.clone(), None, Some(version.clone()), Some(revision)),
        "reference change",
    )?;
    let context = checked(journal.command_context().await, "publication context")?;
    let command = checked(
        journal.prepare_publish_staged(
            context,
            application,
            vec![change],
            vec![],
            vec![(&stage, &intent)],
            checked(sample_clock(), "publication clock")?,
            &policy,
            result(),
        ),
        "prepare publication",
    )?;
    commit(journal, &command).await?;
    require(
        get(db, &f.application_table_arn, app_key.clone()).await? == item,
        "native application item mismatch",
    )?;
    let reference = checked(journal.read_reference(&slot).await, "read reference")?
        .ok_or("reference missing")?;
    require(
        reference.version() == &version,
        "reference exact version mismatch",
    )?;
    counts(db, f, &version, 1, 0).await?;
    let stage = checked(journal.read_staged(&intent_id).await, "read terminal stage")?
        .ok_or("terminal stage missing")?;
    let intent = checked(
        journal.read_intent(&intent_id).await,
        "read consumed intent",
    )?
    .ok_or("consumed intent missing")?;
    require(
        stage.claim().state() == ClaimState::Released
            && intent.state() == IntentState::Consumed
            && stage.verified_length() == payload.len() as u64
            && stage.verified_full_sha256() == Some(&hash),
        "publication state mismatch",
    )?;
    let live_version = checked(
        journal.read_version(&version).await,
        "read protected version",
    )?
    .ok_or("protected version missing")?;
    require(
        matches!(
            journal.retire_unprotected_version(&live_version).await,
            Err(ReclamationError::Ineligible)
        ),
        "durable reference must prevent retirement",
    )?;
    require(
        matches!(
            delete_retiring_version(journal, qualified, &version).await,
            Err(DeletionError::Ineligible)
        ),
        "READY content must not be deleted",
    )?;
    // Diagnostic fixture read only, not a consuming-use pin acceptance shortcut.
    // Always supply exact VersionId. Never retry with a bare/latest key.
    let out = checked(
        s3.get_object()
            .bucket(&f.bucket)
            .expected_bucket_owner(&f.account)
            .key(version.key())
            .version_id(version.version())
            .send()
            .await,
        "published exact-version GET",
    )?;
    require(
        out.version_id() == Some(version.version())
            && out.content_length() == Some(payload.len() as i64),
        "published GET identity/length",
    )?;
    let mut actual = Vec::with_capacity(payload.len());
    checked(
        out.body
            .into_async_read()
            .take(MAX_PAYLOAD as u64 + 1)
            .read_to_end(&mut actual)
            .await,
        "published body read",
    )?;
    require(
        actual == payload && <[u8; 32]>::from(Sha256::digest(&actual)) == hash,
        "published bytes/hash mismatch",
    )?;

    // Normal tracked-version reclamation, not raw teardown: remove the app
    // pointer and its declared edge atomically before requesting retirement.
    let delete = checked(
        Delete::builder()
            .table_name(&f.application_table_arn)
            .set_key(Some(app_key))
            .condition_expression("#revision = :revision")
            .expression_attribute_names("#revision", "revision")
            .expression_attribute_values(":revision", A::S(revision.to_string()))
            .build(),
        "application Delete",
    )?;
    let application = checked(
        ApplicationActions::new(vec![TransactWriteItem::builder().delete(delete).build()]),
        "unlink application actions",
    )?;
    let detach = checked(
        ReferenceChange::new(slot.clone(), Some(reference), None, None),
        "reference detach",
    )?;
    let unlink = checked(
        journal.prepare_reference_update(
            checked(journal.command_context().await, "unlink context")?,
            application,
            vec![detach],
            vec![],
            result(),
        ),
        "prepare atomic unlink",
    )?;
    commit(journal, &unlink).await?;
    require(
        checked(
            journal.read_reference(&slot).await,
            "read detached reference",
        )?
        .is_none(),
        "reference survived unlink",
    )?;
    counts(db, f, &version, 0, 0).await?;
    let unprotected = checked(
        journal.read_version(&version).await,
        "read unprotected version",
    )?
    .ok_or("unprotected version missing")?;
    checked(
        journal.retire_unprotected_version(&unprotected).await,
        "retire tracked version",
    )?;
    let retiring = checked(
        journal.read_version(&version).await,
        "read retiring version",
    )?
    .ok_or("retiring version missing")?;
    require(
        retiring.state() == VersionState::Retiring
            && retiring.references() == 0
            && retiring.claims() == 0,
        "retirement postcondition",
    )?;
    require(
        checked(
            delete_retiring_version(journal, qualified, &version).await,
            "delete exact retired version",
        )? == DeletionOutcome::Deleted,
        "exact deletion outcome",
    )?;
    let deleted = checked(journal.read_version(&version).await, "read deleted version")?
        .ok_or("deleted metadata missing")?;
    require(
        deleted.state() == VersionState::Deleted
            && deleted.references() == 0
            && deleted.claims() == 0,
        "deletion metadata postcondition",
    )?;
    require(
        checked(
            delete_retiring_version(journal, qualified, &version).await,
            "observe repeated deletion",
        )? == DeletionOutcome::AlreadyDeleted,
        "terminal deletion observation",
    )
}
// Two bounded orphan cases in the SAME enrolled namespace and owned scenario.
// No scanner progress, notification simulation or real-cloud fault injection.
async fn retire_observed_orphan(
    journal: &CommandJournal,
    qualified: &QualifiedNamespace,
    observation: ObservedOrphanObject,
    policy: &OrphanGracePolicy,
    clock: &impl Fn() -> Result<ClockObservation, ClockError>,
) -> Check {
    let version = observation.version().clone();
    // A second actual HEAD, saved before registration AND physical deletion.
    // Reusing it later must preserve existing accounting rather than overwrite it.
    let repeated = checked(
        observe_orphan_object(journal, qualified, &version).await,
        "second pre-delete orphan observation",
    )?;
    let context = checked(journal.command_context().await, "orphan retirement context")?;
    let creation = ResourceId::from_command(context.id());
    let prepared = checked(
        journal
            .prepare_orphan_retirement(context, observation, policy, clock, result())
            .await,
        "prepare orphan retirement",
    )?;
    let OrphanPreparation::Prepared(command) = prepared else {
        return Err("new orphan unexpectedly tracked");
    };
    commit(journal, &command).await?;
    let retiring = checked(journal.read_version(&version).await, "read orphan RETIRING")?
        .ok_or("orphan retirement missing")?;
    require(
        retiring.state() == VersionState::Retiring
            && retiring.references() == 0
            && retiring.claims() == 0
            && retiring.creation_id() == &creation
            && retiring.namespace_id() == qualified.configuration().id()
            && retiring.configuration_revision() == repeated.namespace().configuration_revision(),
        "orphan must be created RETIRING with zero counts, never READY",
    )?;
    let repeated = checked(
        journal
            .prepare_orphan_retirement(
                checked(journal.command_context().await, "repeat orphan context")?,
                repeated,
                policy,
                clock,
                result(),
            )
            .await,
        "repeat orphan retirement preparation",
    )?;
    let OrphanPreparation::ExistingTracked(existing) = repeated else {
        return Err("repeat orphan attempted accounting overwrite");
    };
    require(
        existing == retiring,
        "repeat changed orphan creation/accounting",
    )?;
    require(
        checked(
            journal.read_version(&version).await,
            "reread orphan accounting",
        )? == Some(retiring),
        "repeat overwrote orphan metadata",
    )?;
    require(
        checked(
            delete_retiring_version(journal, qualified, &version).await,
            "delete exact orphan version",
        )? == DeletionOutcome::Deleted,
        "orphan deletion unconfirmed",
    )?;
    let deleted = checked(journal.read_version(&version).await, "read orphan DELETED")?
        .ok_or("deleted orphan metadata missing")?;
    require(
        deleted.state() == VersionState::Deleted
            && deleted.references() == 0
            && deleted.claims() == 0
            && deleted.creation_id() == &creation,
        "orphan DELETED accounting/identity mismatch",
    )
}

async fn orphan_cases(
    f: &Fixture,
    s3: &S3,
    journal: &CommandJournal,
    ns: &NamespaceSnapshot,
    qualified: &QualifiedNamespace,
) -> Check {
    // The three normal versions have already been retired and deleted. This
    // empty pass is only an observation, never a permanent absence/drain proof.
    let initial = checked(
        list_version_page(qualified, None, 100).await,
        "initial orphan inventory",
    )?;
    require(
        initial.versions().is_empty()
            && initial.delete_markers().is_empty()
            && initial.next_cursor().is_none(),
        "initial namespace inventory not empty",
    )?;
    let claims = checked(
        ClaimPolicy::new(600_000, 30_000, 1000),
        "orphan claim policy",
    )?;
    let grace = checked(
        OrphanGracePolicy::new(1000, 1000),
        "explicit orphan grace policy",
    )?;
    let payload = b"unselected synthetic object";
    let hash: [u8; 32] = Sha256::digest(payload).into();
    let context = checked(journal.command_context().await, "orphan intent context")?;
    let intent_id = ResourceId::from_command(context.id());
    let (clock, deadline) = timing()?;
    let command = checked(
        journal.prepare_begin_intent(
            context,
            ns,
            format!("{}{KNOWN_ORPHAN}", ns.key_prefix()),
            checked(
                UploadConstraints::new(63, Some(payload.len() as u64), Some(hash)),
                "orphan constraints",
            )?,
            deadline,
            clock,
            &claims,
            result(),
        ),
        "prepare orphan intent",
    )?;
    commit(journal, &command).await?;
    let intent = checked(journal.read_intent(&intent_id).await, "read orphan intent")?
        .ok_or("orphan intent missing")?;
    let context = checked(journal.command_context().await, "orphan attempt context")?;
    let attempt_id = ResourceId::from_command(context.id());
    require(attempt_id != intent_id, "orphan resource identities reused")?;
    let (clock, deadline) = timing()?;
    let mpu_until = checked(
        clock.lower_bound().and_then(|n| n.checked_add(7_200_000)),
        "orphan MPU bound",
    )?;
    let command = checked(
        journal.prepare_begin_attempt(
            context,
            &intent,
            ns,
            deadline,
            mpu_until,
            clock,
            &claims,
            result(),
        ),
        "prepare orphan attempt",
    )?;
    commit(journal, &command).await?;
    // Fresh SystemTime on the normal native transfer path; deliberately no
    // verify/select/publication, so no Version row or staging claim is created.
    let candidate = checked(
        transfer_upload(
            journal,
            qualified,
            &attempt_id,
            std::io::Cursor::new(payload),
            &claims,
            &sample_clock,
        )
        .await,
        "unselected native transfer failed/unknown",
    )?;
    let version = candidate.version();
    require(
        candidate.length() == payload.len() as u64 && candidate.full_sha256() == &hash,
        "unselected transfer constraints mismatch",
    )?;
    let later = checked(
        list_version_page(qualified, None, 100).await,
        "later orphan inventory",
    )?;
    require(
        later.versions() == std::slice::from_ref(version)
            && later.delete_markers().is_empty()
            && later.next_cursor().is_none(),
        "later pass did not find exact late candidate",
    )?;
    let observation = checked(
        observe_orphan_object(journal, qualified, version).await,
        "observe known orphan",
    )?;
    require(
        matches!(observation.attribution(), OrphanAttribution::Known(known)
        if known.intent_id() == &intent_id && known.attempt_id() == &attempt_id),
        "native attribution differs from persisted admission",
    )?;
    let intent = checked(
        journal.read_intent(&intent_id).await,
        "read OPEN orphan intent",
    )?
    .ok_or("OPEN orphan intent missing")?;
    let attempt = checked(
        journal.read_attempt(&attempt_id).await,
        "read ACTIVE orphan attempt",
    )?
    .ok_or("ACTIVE orphan attempt missing")?;
    require(
        intent.state() == IntentState::Open && attempt.state() == AttemptState::Active,
        "orphan must remain OPEN/ACTIVE before cancellation",
    )?;
    require(
        matches!(
            journal
                .prepare_orphan_retirement(
                    checked(
                        journal.command_context().await,
                        "selection-possible context"
                    )?,
                    observation,
                    &grace,
                    &sample_clock,
                    result(),
                )
                .await,
            Err(OrphanError::SelectionPossible)
        ),
        "OPEN/ACTIVE orphan must reject as SelectionPossible",
    )?;
    require(
        checked(
            journal.read_version(version).await,
            "read unregistered orphan",
        )?
        .is_none(),
        "rejected orphan retirement created a Version row",
    )?;
    let cancel = checked(
        journal.prepare_cancel_intent(
            checked(
                journal.command_context().await,
                "cancel orphan intent context",
            )?,
            &intent,
            result(),
        ),
        "prepare native orphan intent cancellation",
    )?;
    commit(journal, &cancel).await?;
    require(
        checked(
            journal.read_intent(&intent_id).await,
            "read cancelled orphan intent",
        )?
        .is_some_and(|i| i.state() == IntentState::Cancelled),
        "orphan intent not CANCELLED",
    )?;
    let observation = checked(
        observe_orphan_object(journal, qualified, version).await,
        "reobserve cancelled exact orphan",
    )?;
    retire_observed_orphan(journal, qualified, observation, &grace, &sample_clock).await?;

    // Explicit SYNTHETIC TEST SETUP ONLY, not an adoption helper: one raw Put,
    // no reserved attribution metadata, at a key already in fsync'd evidence.
    let raw_key = format!("{}{UNATTRIBUTED_ORPHAN}", ns.key_prefix());
    require(
        f.owns_key(&raw_key) && raw_key == format!("{}a/{UNATTRIBUTED_ORPHAN}", f.prefix()),
        "unattributed setup escaped declared owned key",
    )?;
    let raw = checked(
        s3.put_object()
            .bucket(&f.bucket)
            .expected_bucket_owner(&f.account)
            .key(&raw_key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(
                b"synthetic unattributed",
            ))
            .send()
            .await,
        "synthetic unattributed Put failed/unknown",
    )?;
    let raw_version = checked(
        VersionRef::new(
            ns.store().clone(),
            raw_key,
            raw.version_id().ok_or("synthetic Put VersionId missing")?,
        ),
        "synthetic Put invalid/null VersionId",
    )?;
    let observation = checked(
        observe_orphan_object(journal, qualified, &raw_version).await,
        "observe unattributed synthetic version",
    )?;
    require(
        matches!(
            observation.attribution(),
            OrphanAttribution::Unattributed(_)
        ),
        "synthetic object unexpectedly attributed",
    )?;
    let modified = observation.last_modified();
    // Controlled trusted TEST clock boundary, not real elapsed grace or production
    // clock calibration. The normal transfer clock above remains fresh SystemTime.
    let at_modified = || ClockObservation::trusted(modified, 1000);
    require(
        matches!(
            journal
                .prepare_orphan_retirement(
                    checked(journal.command_context().await, "grace-pending context")?,
                    observation,
                    &grace,
                    &at_modified,
                    result(),
                )
                .await,
            Err(OrphanError::GracePending)
        ),
        "unattributed grace must remain pending",
    )?;
    require(
        checked(
            journal.read_version(&raw_version).await,
            "read grace-pending version",
        )?
        .is_none(),
        "grace-pending created a Version row",
    )?;
    let eligible = checked(
        modified
            .checked_add(grace.grace_ms())
            .and_then(|n| n.checked_add(1000)),
        "virtual grace boundary overflow",
    )?;
    let at_boundary = || ClockObservation::trusted(eligible, 1000);
    let observation = checked(
        observe_orphan_object(journal, qualified, &raw_version).await,
        "reobserve unattributed synthetic version",
    )?;
    retire_observed_orphan(journal, qualified, observation, &grace, &at_boundary).await?;
    let final_page = checked(
        list_version_page(qualified, None, 100).await,
        "final orphan inventory",
    )?;
    require(
        final_page.versions().is_empty()
            && final_page.delete_markers().is_empty()
            && final_page.next_cursor().is_none(),
        "orphan exact physical deletion incomplete",
    )
    // Parent cleanup still enumerates ALL owned versions/markers AND MPUs, even
    // on a scenario failure; no request-settlement inference from this empty page.
}

// Considering an inventory page means inspecting EVERY item before checkpointing.
// Here the decision is deliberately retain-for-test, never GC eligibility/deletion.
fn retained_inventory_candidate(page: &VersionPage, expected: &[VersionRef]) -> Check<VersionRef> {
    require(
        page.versions().len() == 1 && page.delete_markers().is_empty(),
        "inventory fixture expected one version and no delete markers",
    )?;
    let candidate = &page.versions()[0];
    require(
        expected.contains(candidate),
        "inventory returned unexpected exact version",
    )?;
    Ok(candidate.clone())
}

async fn inventory_progress_case(
    f: &Fixture,
    s3: &S3,
    journal: &CommandJournal,
    ns: &NamespaceSnapshot,
    qualified: &QualifiedNamespace,
) -> Check {
    // Runs only AFTER the existing normal/orphan cases proved their final page empty.
    // SYNTHETIC FIXTURE SETUP ONLY: both keys are already in fsync'd evidence.
    // No attribution, publication, adoption, multipart upload or client abort.
    let mut expected = Vec::with_capacity(2);
    for (label, payload) in INVENTORY_LABELS.into_iter().zip([
        b"retained inventory fixture one".as_slice(),
        b"retained inventory fixture two".as_slice(),
    ]) {
        let raw_key = format!("{}{label}", ns.key_prefix());
        require(
            payload.len() < 64
                && f.owns_key(&raw_key)
                && raw_key == format!("{}a/{label}", f.prefix())
                && PLANNED_LABELS.contains(&label),
            "inventory setup escaped declared owned key/payload bound",
        )?;
        let raw = checked(
            s3.put_object()
                .bucket(&f.bucket)
                .expected_bucket_owner(&f.account)
                .key(&raw_key)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(payload))
                .send()
                .await,
            "synthetic inventory Put failed/unknown",
        )?;
        expected.push(checked(
            VersionRef::new(
                ns.store().clone(),
                raw_key,
                raw.version_id().ok_or("inventory Put VersionId missing")?,
            ),
            "inventory Put invalid/null VersionId",
        )?);
    }
    let initial = checked(
        journal.load_inventory_progress(qualified).await,
        "load initial inventory progress",
    )?;
    require(
        checked(initial.cursor(), "initial inventory cursor")?.is_none()
            && initial.completed_passes() == 0
            && initial.last_completion().is_none(),
        "inventory progress must initially be empty",
    )?;
    let first = checked(
        list_version_page(
            qualified,
            checked(initial.cursor(), "first input cursor")?,
            1,
        )
        .await,
        "first native inventory page",
    )?;
    // Record the ACTUAL candidate, not a key guessed from lexical order or markers.
    let mut retained = vec![retained_inventory_candidate(&first, &expected)?];
    let next = first
        .next_cursor()
        .ok_or("first inventory page must continue")?;
    let saved_markers = (
        next.key_marker().to_owned(),
        next.version_id_marker().to_owned(),
    );
    // Capture both stale observation and its own native first page BEFORE the CAS.
    let stale = checked(
        journal.load_inventory_progress(qualified).await,
        "load stale inventory observation",
    )?;
    require(
        checked(stale.cursor(), "stale inventory cursor")?.is_none()
            && stale.completed_passes() == 0
            && stale.last_completion().is_none(),
        "stale inventory observation not initial",
    )?;
    let stale_page = checked(
        list_version_page(qualified, checked(stale.cursor(), "stale input cursor")?, 1).await,
        "stale native first page",
    )?;
    let _stale_retained = retained_inventory_candidate(&stale_page, &expected)?;
    require(
        stale_page.next_cursor().is_some(),
        "stale first page must continue",
    )?;
    // Bounded synthetic diagnostic time ONLY, not a trusted GC/retention clock.
    let metric_time = checked(TimestampMs::new(1000), "inventory metric time")?;
    checked(
        journal
            .checkpoint_considered_inventory_page(initial, first, Some(metric_time))
            .await,
        "checkpoint first considered inventory page",
    )?;
    require(
        matches!(
            journal
                .checkpoint_considered_inventory_page(stale, stale_page, Some(metric_time))
                .await,
            Err(InventoryProgressError::Conflict)
        ),
        "stale inventory checkpoint must explicitly Conflict",
    )?;
    // Fresh native load restores the opaque cursor; no in-memory first-page cursor
    // is supplied to the next S3 request. A stale CAS must not alter progress.
    let restored = checked(
        journal.load_inventory_progress(qualified).await,
        "reload inventory progress after stale conflict",
    )?;
    let cursor = checked(restored.cursor(), "restored inventory cursor")?
        .ok_or("durable inventory continuation missing")?;
    require(
        (cursor.key_marker(), cursor.version_id_marker())
            == (saved_markers.0.as_str(), saved_markers.1.as_str())
            && restored.completed_passes() == 0
            && restored.last_completion().is_none(),
        "stale checkpoint changed durable inventory progress",
    )?;
    let last = checked(
        list_version_page(qualified, Some(cursor), 1).await,
        "next native page using restored cursor",
    )?;
    retained.push(retained_inventory_candidate(&last, &expected)?);
    require(
        last.next_cursor().is_none()
            && retained.len() == 2
            && retained[0] != retained[1]
            && expected.iter().all(|version| retained.contains(version)),
        "inventory pass must consider exactly both retained versions and terminate",
    )?;
    checked(
        journal
            .checkpoint_considered_inventory_page(restored, last, Some(metric_time))
            .await,
        "checkpoint terminal considered inventory page",
    )?;
    let completed = checked(
        journal.load_inventory_progress(qualified).await,
        "reload durable completed inventory pass",
    )?;
    require(
        checked(completed.cursor(), "completed inventory cursor")?.is_none()
            && completed.completed_passes() == 1
            && completed.last_completion() == Some(metric_time),
        "terminal inventory progress must durably restart with one completed pass",
    )
    // Both versions remain retained test data. Existing parent cleanup removes
    // their exact versions and checks the WHOLE prefix/MPUs, including on failure.
}

async fn scenario(f: Fixture, db: Dynamo, s3: S3) -> Check {
    let tables = checked(
        EnrolledTables::enroll(
            &db,
            &f.coordination_table_arn,
            std::slice::from_ref(&f.application_table_arn),
        )
        .await,
        "native table enrollment",
    )?;
    let journal = CommandJournal::new(db.clone(), tables, f.domain()?);
    checked(
        journal.initialize_fresh_domain().await,
        "fresh domain initialization",
    )?;
    let config = f.configuration("a")?;
    let ns = enroll(&journal, &s3, &config).await?;
    let binding_key = key(format!(
        "{}#STORE#{}",
        domain_prefix(&f.domain()?),
        hex(config.store().as_str().as_bytes())
    ));
    let binding = get(&db, &f.coordination_table_arn, binding_key.clone()).await?;
    let second = enroll(&journal, &s3, &f.configuration("b")?).await?;
    require(
        ns.configuration_revision() != second.configuration_revision()
            && ns.key_prefix() != second.key_prefix()
            && ns.store() == second.store(),
        "second namespace must have independent configuration, identical StoreId",
    )?;
    require(
        get(&db, &f.coordination_table_arn, binding_key).await? == binding,
        "second enrollment rebound permanent store",
    )?;
    let qualified = checked(
        qualify_existing(&s3, &config).await,
        "native namespace health qualification",
    )?;
    for (label, size) in [("empty", 0), ("small", 37), ("multipart", MAX_PAYLOAD)] {
        // Deterministic synthetic payload, bounded to exactly 8MiB+17 at largest.
        let payload = (0..size).map(|i| (i % 251) as u8).collect();
        Box::pin(upload_case(
            &f, &db, &s3, &journal, &ns, &qualified, label, payload,
        ))
        .await?;
    }
    Box::pin(orphan_cases(&f, &s3, &journal, &ns, &qualified)).await?;
    Box::pin(inventory_progress_case(&f, &s3, &journal, &ns, &qualified)).await
}

/// Read-only diagnostic for a failed profile gate; never initializes metadata or
/// consumes a fresh token. Same explicit fixture ownership inputs are required.
#[tokio::test]
#[ignore = "REAL AWS READS: explicit owned fixture inputs; private log capture required"]
async fn native_namespace_qualification_aws() {
    let (f, profile, _) = inputs().expect("fixture input gate");
    tokio::time::timeout(PREFLIGHT, async {
        let (db, s3) = clients(&f, &profile).await.expect("fixture credentials");
        tables_gate(&f, &db).await.expect("table ownership");
        bucket_gate(&f, &s3).await.expect("bucket ownership");
        for slot in ["a", "b"] {
            let config = f.configuration(slot).expect("namespace configuration");
            if let Err(error) = qualify_fresh(&s3, &config).await {
                // QualificationError Display is intentionally bounded/static;
                // do not use Debug, which would include boxed SDK errors.
                panic!("native qualification: {error}");
            }
        }
    })
    .await
    .expect("read-only qualification deadline");
}

#[tokio::test]
#[ignore = "REAL AWS WRITES: requires pre-provisioned two-table/one-bucket run-tagged fixture and explicit private evidence/identity/ACK inputs"]
async fn native_object_contract_aws() {
    let (f, profile, evidence) = inputs().expect("object fixture input gate failed");
    let (db, s3) = tokio::time::timeout(PREFLIGHT, async {
        let (db, s3) = clients(&f, &profile).await?;
        let (tables, bucket) = tokio::join!(tables_gate(&f, &db), bucket_gate(&f, &s3));
        tables?;
        bucket?;
        Ok::<_, &'static str>((db, s3))
    })
    .await
    .expect("preflight deadline")
    .expect("native identity/ownership gate failed");
    persist_evidence(&f, &evidence).expect("durable evidence gate failed before writes");
    let (owned_f, owned_s3) = (f.clone(), s3.clone());
    // All scenario futures are owned here; no detached child tasks. Deadline drops
    // only this owned future; JoinError catches panic and cleanup still runs.
    let joined = tokio::spawn(async move {
        checked(
            tokio::time::timeout(SCENARIO, Box::pin(scenario(owned_f, db, owned_s3))).await,
            "scenario deadline; remote outcome unknown",
        )?
    })
    .await;
    let contract = checked(
        joined,
        "scenario panicked/cancelled; remote outcome unknown",
    )
    .and_then(|r| r);
    let cleanup = tokio::time::timeout(CLEANUP, cleanup(&f, &s3, contract.is_ok()))
        .await
        .unwrap_or_else(|_| Err(vec!["cleanup deadline; RETAIN fixture"]));
    // This summary uses static labels. Rust's panic hook may already have printed
    // a payload before JoinError; capture ALL stdout/stderr privately (see docs).
    assert!(
        contract.is_ok() && cleanup.is_ok(),
        "contract={contract:?}; cleanup={cleanup:?}; private evidence retained; parent owns table/bucket teardown"
    );
}

// Ordinary guards are pure; SDK regressions below use only explicit loopback
// endpoints and synthetic credentials. No environment loading or AWS traffic.
fn fixture() -> Fixture {
    Fixture {
        account: "123456789012".into(),
        region: "us-west-2".into(),
        run_uuid: "01234567-89ab-4cde-8fab-0123456789ab".into(),
        coordination_table_arn: "arn:aws:dynamodb:us-west-2:123456789012:table/disposable-coord"
            .into(),
        application_table_arn: "arn:aws:dynamodb:us-west-2:123456789012:table/disposable-app"
            .into(),
        bucket: "disposable-object-fixture".into(),
    }
}
#[test]
fn explicit_identity_and_run_guards() {
    let f = fixture();
    assert!(f.validate().is_ok());
    for run in [
        "",
        "00000000-0000-0000-0000-000000000000",
        "0123456789ab4cde8fab0123456789ab",
        "01234567-89AB-4CDE-8FAB-0123456789AB",
    ] {
        let mut bad = f.clone();
        bad.run_uuid = run.into();
        assert!(bad.validate().is_err());
    }
    for arn in [
        "arn:aws:dynamodb:us-east-1:123456789012:table/table",
        "arn:aws:dynamodb:us-west-2:999999999999:table/table",
        "arn:aws-cn:dynamodb:us-west-2:123456789012:table/table",
        "arn:aws:dynamodb:us-west-2:123456789012:table/table/index/index",
        "*",
    ] {
        let mut bad = f.clone();
        bad.application_table_arn = arn.into();
        assert!(bad.validate().is_err());
    }
    let mut bad = f.clone();
    bad.application_table_arn = bad.coordination_table_arn.clone();
    assert!(bad.validate().is_err());
    for region in [
        "",
        "us-gov-west-1",
        "cn-north-1",
        "us-iso-east-1",
        "us-west-2 ",
    ] {
        let mut bad = f.clone();
        bad.region = region.into();
        assert!(bad.validate().is_err());
    }
    assert!(
        f.check_tags([(TAG, f.run_uuid.as_str())].into_iter())
            .is_ok()
    );
    for tags in [
        vec![],
        vec![(TAG, "wrong")],
        vec![(TAG, f.run_uuid.as_str()); 2],
    ] {
        assert!(f.check_tags(tags.into_iter()).is_err());
    }
}
#[test]
fn exact_prefix_and_namespace_ownership() {
    let f = fixture();
    let prefix = f.prefix();
    assert!(f.owns_key(&format!("{prefix}a/small")));
    for key in [
        prefix.clone(),
        "unmanaged/a".into(),
        prefix.replace("01234567", "11234567"),
        format!("{}evil/a", prefix.trim_end_matches('/')),
    ] {
        assert!(!f.owns_key(&key));
    }
    let a = f.configuration("a").unwrap();
    let b = f.configuration("b").unwrap();
    assert_ne!(a.id(), b.id());
    assert_eq!(a.store(), b.store());
    assert!(
        !a.key_prefix().starts_with(b.key_prefix()) && !b.key_prefix().starts_with(a.key_prefix())
    );
    assert!(f.configuration("../unmanaged").is_err());
}
#[test]
fn planned_orphan_and_inventory_keys_are_distinct_and_owned_before_writes() {
    let f = fixture();
    let keys = PLANNED_LABELS.map(|label| format!("{}a/{label}", f.prefix()));
    assert_eq!(keys.iter().collect::<BTreeSet<_>>().len(), 7);
    assert!(keys.iter().all(|key| f.owns_key(key)));
    for label in [KNOWN_ORPHAN, UNATTRIBUTED_ORPHAN]
        .into_iter()
        .chain(INVENTORY_LABELS)
    {
        assert!(keys.contains(&format!(
            "{}{label}",
            f.configuration("a").unwrap().key_prefix()
        )));
    }
}

#[test]
fn partial_pagination_is_never_complete() {
    assert!(
        Pages::default()
            .next(0, Some(false), None, None)
            .unwrap()
            .is_none()
    );
    for args in [
        (0, None, None, None),
        (1, Some(true), Some("key"), None),
        (1, Some(true), None, Some("id")),
        (1, Some(true), Some(""), Some("id")),
        (1, Some(true), Some("key"), Some("")),
        (101, Some(false), None, None),
    ] {
        assert!(
            Pages::default()
                .next(args.0, args.1, args.2, args.3)
                .is_err()
        );
    }
    let mut pages = Pages::default();
    assert!(
        pages
            .next(1, Some(true), Some("key"), Some("id"))
            .unwrap()
            .is_some()
    );
    assert!(pages.next(1, Some(true), Some("key"), Some("id")).is_err());
    let mut pages = Pages::default();
    for n in 0..9 {
        assert!(
            pages
                .next(100, Some(true), Some(&format!("key-{n}")), Some("id"))
                .is_ok()
        );
    }
    assert!(
        pages
            .next(100, Some(true), Some("last"), Some("id"))
            .is_err()
    );
    let mut pages = Pages::default();
    for n in 0..9 {
        pages
            .next(100, Some(true), Some(&format!("key-{n}")), Some("id"))
            .unwrap();
    }
    assert!(pages.next(100, Some(false), None, None).unwrap().is_none());
}
#[test]
fn active_pk_sk_schema_required() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    let f = fixture();
    let arn = &f.coordination_table_arn;
    let mut builder = TableDescription::builder()
        .table_arn(arn)
        .table_name("disposable-coord")
        .table_status(TableStatus::Active);
    for (name, kind) in [("pk", KeyType::Hash), ("sk", KeyType::Range)] {
        builder = builder
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(name)
                    .key_type(kind)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(name)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            );
    }
    let valid = builder.build();
    assert!(f.check_description(arn, &valid).is_ok());
    assert!(
        f.check_description(&f.application_table_arn, &valid)
            .is_err()
    );
    let mut wrong = valid.clone();
    wrong.table_status = Some(TableStatus::Creating);
    assert!(f.check_description(arn, &wrong).is_err());
    let mut wrong = valid;
    wrong.key_schema = None;
    assert!(f.check_description(arn, &wrong).is_err());
}

// Tiny bounded HTTP fixture, not a cloud harness or expression emulator. The
// pinned native SDK actually deserializes these XML responses over loopback.
fn loopback(
    requests: usize,
    mut respond: impl FnMut(&str) -> (u16, String, String) + Send + 'static,
) -> (S3, std::thread::JoinHandle<Vec<String>>) {
    use std::{net::TcpListener, time::Instant};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut lines = Vec::new();
        while lines.len() < requests {
            assert!(Instant::now() < deadline, "loopback request budget expired");
            let (mut socket, _) = match listener.accept() {
                Ok(socket) => socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::park_timeout(Duration::from_millis(5));
                    continue;
                }
                Err(_) => panic!("loopback accept failed"),
            };
            // Accepted sockets can inherit nonblocking mode on macOS. The
            // header reader below is deliberately blocking with a finite timeout.
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                assert!(Instant::now() < deadline, "loopback header budget expired");
                assert!(bytes.len() < 16 * 1024, "loopback header bound");
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                bytes.push(byte[0]);
            }
            let headers = String::from_utf8(bytes).unwrap();
            let line = headers.lines().next().unwrap();
            let (status, extra, body) = respond(line);
            let response = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).unwrap();
            lines.push(line.to_owned());
        }
        lines
    });
    let client = S3::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "synthetic",
                "synthetic",
                None,
                None,
                "loopback",
            ))
            .region(Region::new("us-west-2"))
            .endpoint_url(format!("http://{address}"))
            .force_path_style(true)
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(2))
                    .read_timeout(Duration::from_secs(2))
                    .operation_timeout(Duration::from_secs(3))
                    .operation_attempt_timeout(Duration::from_secs(3))
                    .build(),
            )
            .build(),
    );
    (client, worker)
}

#[tokio::test]
async fn native_sdk_terminal_mpu_markers_are_not_continuations() {
    let mut index = 0;
    let (s3, worker) = loopback(2, move |line| {
        assert!(line.starts_with("GET ") && line.contains("uploads"));
        let (key, upload) = if index == 0 {
            ("", "")
        } else {
            ("last-key", "last-upload")
        };
        index += 1;
        (
            200,
            String::new(),
            format!(
                "<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IsTruncated>false</IsTruncated><NextKeyMarker>{key}</NextKeyMarker><NextUploadIdMarker>{upload}</NextUploadIdMarker></ListMultipartUploadsResult>"
            ),
        )
    });
    for (key, upload) in [("", ""), ("last-key", "last-upload")] {
        let out = s3
            .list_multipart_uploads()
            .bucket("disposable-object-fixture")
            .max_uploads(PAGE_SIZE)
            .send()
            .await
            .unwrap();
        assert_eq!(out.next_key_marker(), Some(key));
        assert_eq!(out.next_upload_id_marker(), Some(upload));
        assert_eq!(out.is_truncated(), Some(false));
        assert!(
            Pages::default()
                .next(
                    out.uploads().len(),
                    out.is_truncated(),
                    out.next_key_marker(),
                    out.next_upload_id_marker()
                )
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(worker.join().unwrap().len(), 2);
}

#[tokio::test]
async fn cleanup_continues_after_delete_failure_and_runs_both_final_inventories() {
    let f = fixture();
    let owned = f.clone();
    let mut version_reads = 0;
    let (s3, worker) = loopback(10, move |line| {
        let body = if line.starts_with("HEAD ") {
            return (
                200,
                "x-amz-bucket-region: us-west-2\r\n".into(),
                String::new(),
            );
        } else if line.starts_with("DELETE ") {
            assert!(line.contains("versionId="));
            if line.contains("/first?") {
                return (
                    503,
                    String::new(),
                    "<Error><Code>ServiceUnavailable</Code></Error>".into(),
                );
            }
            assert!(line.contains("/second?"));
            return (204, String::new(), String::new());
        } else if line.contains("tagging") {
            format!(
                "<Tagging><TagSet><Tag><Key>{TAG}</Key><Value>{}</Value></Tag></TagSet></Tagging>",
                owned.run_uuid
            )
        } else if line.contains("versions") {
            version_reads += 1;
            let rows = if version_reads == 1 {
                format!(
                    "<Version><Key>{}first</Key><VersionId>v1</VersionId></Version><Version><Key>{}second</Key><VersionId>v2</VersionId></Version>",
                    owned.prefix(),
                    owned.prefix()
                )
            } else {
                String::new()
            };
            format!(
                "<ListVersionsResult><IsTruncated>false</IsTruncated>{rows}</ListVersionsResult>"
            )
        } else {
            assert!(line.contains("uploads"));
            "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><NextKeyMarker/><NextUploadIdMarker/></ListMultipartUploadsResult>".into()
        };
        (200, String::new(), body)
    });
    let result = cleanup(&f, &s3, true).await;
    let requests = worker.join().unwrap();
    assert_eq!(result, Err(vec!["exact version deletion unconfirmed"]));
    assert_eq!(
        requests.iter().filter(|r| r.starts_with("DELETE ")).count(),
        2
    );
    assert_eq!(
        requests.iter().filter(|r| r.contains("versions")).count(),
        2
    );
    assert_eq!(requests.iter().filter(|r| r.contains("uploads")).count(), 2);
    let last_delete = requests
        .iter()
        .rposition(|r| r.starts_with("DELETE "))
        .unwrap();
    assert_eq!(requests[last_delete + 1..].len(), 2);
    assert!(
        requests[last_delete + 1..]
            .iter()
            .all(|r| r.starts_with("GET "))
    );
}
