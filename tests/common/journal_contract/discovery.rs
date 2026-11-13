//! Native read/discovery/checkpoint conformance only, NOT per-item reaping or GC.
//! Reuse the parent's two owned tables and panic/cleanup boundary. Synthetic
//! metadata fixtures do not qualify an S3 namespace or authorize historical GC.

use super::*;
use aws_sdk_dynamodb::types::Get;
use storelink::clock::{ClockObservation, TimestampMs};
use storelink::dynamodb::{
    BoundedMaintenanceHint as Hint, MaintenancePolicy, MetadataCursor, MetadataDiscoveryItem,
    MetadataDiscoveryPage, MetadataProgressError,
};

// Scan evaluates the WHOLE shared fixture table, including all earlier domains.
// Each sweep is bounded by evaluated work, never by returned hints. Limit 1 on
// the first page guarantees continuation without depending on native row order.
const PAGE_LIMIT: u16 = 32;
const MAX_PAGES: usize = 128;
const MAX_EVALUATED: usize = MAX_PAGES * PAGE_LIMIT as usize;

pub(super) async fn exercise(f: &Fixture) {
    Box::pin(application_batch(f)).await;
    Box::pin(discovery_and_progress(f)).await;
}

#[test]
fn scenario_future_layout_is_bounded() {
    fn size<F: std::future::Future>(_: fn(&'static Fixture) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    for bytes in [
        size(exercise),
        size(application_batch),
        size(discovery_and_progress),
    ] {
        assert!(bytes <= 128 * 1024, "oversized discovery scenario: {bytes}");
    }
}

fn get(table: &str, name: &str) -> Get {
    Get::builder()
        .table_name(table)
        .set_key(Some(key(name)))
        .build()
        .unwrap()
}
fn domain(label: &str) -> DomainId {
    DomainId::new(format!("discovery-{label}-{}", Uuid::new_v4())).unwrap()
}
fn prefix(domain: &DomainId) -> String {
    format!("D#{:x}", Sha256::digest(domain.as_str().as_bytes()))
}
fn progress_key(domain: &DomainId) -> Item {
    key(format!("{}#METADATA_PROGRESS", prefix(domain)))
}

async fn application_batch(f: &Fixture) {
    let domain = domain("batch");
    let journal = f.journal(&domain);
    journal.initialize_fresh_domain().await.unwrap();
    let names = ["first", "second", "missing"].map(|n| format!("{}-{n}", domain.as_str()));
    let command = journal
        .prepare_metadata_only(
            journal.command_context().await.unwrap(),
            actions(vec![
                put(&f.app, &names[0], Some("attribute_not_exists(pk)")),
                put(&f.app, &names[1], Some("attribute_not_exists(pk)")),
            ]),
            b"batch".to_vec(),
        )
        .unwrap();
    assert_eq!(
        journal.execute(&command).await.unwrap().outcome,
        Outcome::Committed(b"batch".to_vec())
    );
    let policy = MaintenancePolicy::new(1000, 10).unwrap();
    journal.configure_compaction_policy(&policy).await.unwrap();
    journal.advance_and_close_generation(&policy).await.unwrap();
    let control = journal
        .arm_compaction_window(&policy, &|| {
            ClockObservation::trusted(TimestampMs::new(1000)?, 5)
        })
        .await
        .unwrap()
        .observation;
    let before = f.get(&f.coord, super::control(&domain)).await.unwrap();
    assert!(before.contains_key("compaction_not_before_ms"));
    let arn = f
        .client
        .describe_table()
        .table_name(&f.app)
        .send()
        .await
        .unwrap()
        .table
        .unwrap()
        .table_arn
        .unwrap();
    // Mix the verified alias and canonical ARN, deliberately reverse item order
    // and put absence in the middle. Projection aliases remain SDK-native.
    let projected = Get::builder()
        .table_name(&arn)
        .set_key(Some(key(&names[1])))
        .projection_expression("#partition, #sort")
        .expression_attribute_names("#partition", "pk")
        .expression_attribute_names("#sort", "sk")
        .build()
        .unwrap();
    let observed = journal
        .observe_application_items(vec![
            projected,
            get(&f.app, &names[2]),
            get(&f.app, &names[0]),
        ])
        .await
        .unwrap();
    assert_eq!(
        observed.items(),
        &[Some(key(&names[1])), None, Some(key(&names[0]))]
    );
    assert_eq!(observed.fence(), control.fence());
    assert_eq!(f.get(&f.coord, super::control(&domain)).await, Some(before));
    let mut invalid = get(&arn, &names[0]);
    invalid.projection_expression = Some("(".into());
    match journal
        .observe_application_items(vec![invalid, get(&f.app, &names[1])])
        .await
    {
        Err(JournalError::Native(message)) => {
            assert!(message.contains("ValidationException"), "{message}");
            assert!(message.contains("ProjectionExpression"), "{message}");
        }
        _ => panic!("batch read discarded native invalid-projection diagnostic"),
    }
}

// Independent native Scan oracle: exact domain receipts, and all permanent row
// families, with bounded whole-table evaluated work and native LEK following.
async fn rows(f: &Fixture, domain: &DomainId) -> Vec<Item> {
    let mut start = None;
    let mut evaluated = 0;
    let mut rows = Vec::new();
    for _ in 0..MAX_PAGES {
        let page = f
            .client
            .scan()
            .table_name(&f.coord)
            .consistent_read(true)
            .limit(i32::from(PAGE_LIMIT))
            .filter_expression("begins_with(#pk, :domain)")
            .expression_attribute_names("#pk", "pk")
            .expression_attribute_values(":domain", A::S(format!("{}#", prefix(domain))))
            .set_exclusive_start_key(start)
            .send()
            .await
            .unwrap();
        assert!((0..=i32::from(PAGE_LIMIT)).contains(&page.scanned_count));
        evaluated += page.scanned_count as usize;
        assert!(evaluated <= MAX_EVALUATED);
        rows.extend(page.items.unwrap_or_default());
        start = page.last_evaluated_key.filter(|k| !k.is_empty());
        if start.is_none() {
            return rows;
        }
    }
    panic!("native fixture Scan exceeded whole-table budget");
}
fn receipts(rows: &[Item], domain: &DomainId) -> Vec<String> {
    let prefix = format!("{}#CMD#", prefix(domain));
    let mut keys: Vec<_> = rows
        .iter()
        .filter_map(|row| row["pk"].as_s().ok())
        .filter(|pk| pk.starts_with(&prefix))
        .cloned()
        .collect();
    keys.sort();
    keys
}
fn native_key(cursor: &MetadataCursor) -> Item {
    // Test-only inspection of the documented serializable opaque cursor; never
    // manufacture a cursor or assume its key belongs to the scanned domain.
    let wire = serde_json::to_value(cursor).unwrap();
    Item::from([
        ("pk".into(), A::S(wire["pk"].as_str().unwrap().into())),
        ("sk".into(), A::S(wire["sk"].as_str().unwrap().into())),
    ])
}

async fn assert_resume(
    f: &Fixture,
    journal: &CommandJournal,
    domain: &DomainId,
    expected: &MetadataCursor,
) {
    let loaded = journal.load_metadata_progress().await.unwrap();
    assert_eq!(loaded.cursor().unwrap(), Some(expected));
    let page = journal
        .discover_metadata_page(loaded.cursor().unwrap(), PAGE_LIMIT)
        .await
        .unwrap();
    assert_eq!(page.input_cursor(), Some(expected));
    let native = f
        .client
        .scan()
        .table_name(&f.coord)
        .consistent_read(true)
        .limit(i32::from(PAGE_LIMIT))
        .filter_expression("begins_with(#pk, :domain)")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_values(":domain", A::S(prefix(domain)))
        .set_exclusive_start_key(Some(native_key(expected)))
        .send()
        .await
        .unwrap();
    assert_eq!(i32::from(page.scanned_count()), native.scanned_count);
    assert_eq!(page.items().len(), native.items().len());
    assert_eq!(
        page.next_cursor().map(native_key),
        native.last_evaluated_key.filter(|k| !k.is_empty())
    );
}

fn consider(
    page: &MetadataDiscoveryPage,
    expected: &[Hint],
    seen: &mut Vec<Hint>,
    permanent: &mut usize,
) {
    for item in page.items() {
        match item {
            MetadataDiscoveryItem::Hint(hint) => {
                assert!(
                    expected.contains(hint),
                    "foreign or unexpected hint: {hint:?}"
                );
                seen.push(hint.clone());
            }
            MetadataDiscoveryItem::Permanent => *permanent += 1,
            other => panic!(
                "owned valid fixture (including REF/STORE/progress) misclassified: {other:?}"
            ),
        }
    }
}

async fn sweep(journal: &CommandJournal, expected: &[Hint]) {
    let mut seen = Vec::new();
    let mut permanent = 0;
    let mut evaluated = 0;
    let completion = TimestampMs::new(1234).unwrap();
    for index in 0..MAX_PAGES {
        let observation = journal.load_metadata_progress().await.unwrap();
        let limit = if index == 0 { 1 } else { PAGE_LIMIT };
        let page = journal
            .discover_metadata_page(observation.cursor().unwrap(), limit)
            .await
            .unwrap();
        assert_eq!(page.input_cursor(), observation.cursor().unwrap());
        assert!(page.scanned_count() <= limit);
        evaluated += usize::from(page.scanned_count());
        assert!(evaluated <= MAX_EVALUATED);
        let terminal = page.next_cursor().is_none();
        if index == 0 {
            assert!(!terminal, "fixture must exercise native continuation");
        }
        // An empty filtered page is still considered and checkpointed if it has
        // a LEK. Neither returned item count nor provider ordering ends the pass.
        consider(&page, expected, &mut seen, &mut permanent);
        journal
            .checkpoint_considered_metadata_page(observation, page, Some(completion))
            .await
            .unwrap();
        let loaded = journal.load_metadata_progress().await.unwrap();
        assert_eq!(loaded.completed_passes(), u64::from(terminal));
        if terminal {
            assert!(loaded.cursor().unwrap().is_none());
            assert_eq!(loaded.last_completion(), Some(completion));
            assert_eq!(permanent, 4, "control + REF + STORE + progress");
            assert_eq!(seen.len(), expected.len());
            for hint in expected {
                assert!(seen.contains(hint));
            }
            return;
        }
        assert!(loaded.cursor().unwrap().is_some());
        assert!(loaded.last_completion().is_none());
    }
    panic!("metadata sweep exceeded whole-fixture evaluated/page budget");
}

async fn empty_filtered_sweep(f: &Fixture) {
    // No rows for this fresh domain: deterministic empty filtered pages, without
    // requiring a particular interleaving of the other domains' native keys.
    let journal = f.journal(&domain("empty"));
    let mut cursor = None;
    let mut evaluated = 0;
    for index in 0..MAX_PAGES {
        let limit = if index == 0 { 1 } else { PAGE_LIMIT };
        let page = journal
            .discover_metadata_page(cursor.as_ref(), limit)
            .await
            .unwrap();
        assert!(
            page.items().is_empty(),
            "foreign rows must be filtered, not classified"
        );
        assert_eq!(page.input_cursor(), cursor.as_ref());
        evaluated += usize::from(page.scanned_count());
        assert!(page.scanned_count() <= limit && evaluated <= MAX_EVALUATED);
        cursor = page.next_cursor().cloned();
        if index == 0 {
            assert!(cursor.is_some());
        }
        if cursor.is_none() {
            assert!(evaluated > 1);
            return;
        }
    }
    panic!("empty-filtered sweep exceeded whole-fixture budget");
}

async fn discovery_and_progress(f: &Fixture) {
    let domain = domain("progress");
    let journal = f.journal(&domain);
    let incarnation = journal.initialize_fresh_domain().await.unwrap();
    let version = VersionRef::new(
        StoreId::new("synthetic-discovery-store").unwrap(),
        "fresh-fixture/object",
        "synthetic-version",
    )
    .unwrap();
    // Reuse the owned native fixture codecs. Complete immutable provenance is
    // synthetic TEST input, not S3 verification, selection or namespace adoption.
    let mut row = ready_version(&domain, &version);
    for (name, value) in [
        ("kind", "VERSION".into()),
        ("namespace_id", "synthetic-discovery".into()),
        ("configuration_revision", Uuid::new_v4().to_string()),
        ("creation_domain", domain.as_str().into()),
        ("creation_incarnation", incarnation.to_string()),
        ("creation_uuid", Uuid::new_v4().to_string()),
    ] {
        row.insert(name.into(), A::S(value));
    }
    row.insert("creation_generation".into(), A::N("1".into()));
    f.put(&f.coord, row).await;
    seed_source(f, &journal, &domain, &version).await;
    assert!(journal.read_version(&version).await.unwrap().is_some());
    let store_hex: String = version
        .store()
        .as_str()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut store = key(format!("{}#STORE#{store_hex}", prefix(&domain)));
    for (name, value) in [
        ("record", "store-binding-v1"),
        ("domain", domain.as_str()),
        ("store", version.store().as_str()),
        ("bucket", "fixture-not-s3-verified"),
        ("expected_owner", "123456789012"),
        ("region", "us-west-2"),
        ("provider_profile", "aws-s3-versioned-lifecycle-v0"),
    ] {
        store.insert(name.into(), A::S(value.into()));
    }
    f.put(&f.coord, store).await;
    let command = f
        .prepare(
            &journal,
            &format!("{}-app", domain.as_str()),
            b"own-receipt",
        )
        .await;
    assert_eq!(
        journal.execute(&command).await.unwrap().outcome,
        Outcome::Committed(b"own-receipt".to_vec())
    );
    let expected = [
        Hint::Receipt(command.token().clone()),
        Hint::Version(version),
    ];
    let before = receipts(&rows(f, &domain).await, &domain);
    assert_eq!(before.len(), 1);
    Box::pin(empty_filtered_sweep(f)).await;

    let absent = journal.load_metadata_progress().await.unwrap();
    assert!(!absent.requires_reset());
    assert!(absent.cursor().unwrap().is_none());
    assert_eq!(absent.completed_passes(), 0);
    assert!(absent.last_completion().is_none());
    assert!(f.get(&f.coord, progress_key(&domain)).await.is_none());
    let page = journal.discover_metadata_page(None, 1).await.unwrap();
    let next = page.next_cursor().unwrap().clone();
    consider(&page, &expected, &mut Vec::new(), &mut 0);
    journal
        .checkpoint_considered_metadata_page(absent, page, None)
        .await
        .unwrap();
    Box::pin(assert_resume(f, &journal, &domain, &next)).await;

    // Both opaque observations see the SAME existing UUID revision. Sequential
    // competing writes deterministically exercise CAS, without timing races.
    let first = journal.load_metadata_progress().await.unwrap();
    let stale = journal.load_metadata_progress().await.unwrap();
    let old_row = f.get(&f.coord, progress_key(&domain)).await.unwrap();
    let page = journal
        .discover_metadata_page(first.cursor().unwrap(), 1)
        .await
        .unwrap();
    let stale_page = journal
        .discover_metadata_page(stale.cursor().unwrap(), 1)
        .await
        .unwrap();
    consider(&page, &expected, &mut Vec::new(), &mut 0);
    consider(&stale_page, &expected, &mut Vec::new(), &mut 0);
    journal
        .checkpoint_considered_metadata_page(first, page, None)
        .await
        .unwrap();
    let winner_row = f.get(&f.coord, progress_key(&domain)).await.unwrap();
    assert_ne!(winner_row["revision"], old_row["revision"]);
    assert_eq!(
        journal
            .checkpoint_considered_metadata_page(stale, stale_page, None)
            .await,
        Err(MetadataProgressError::Conflict)
    );
    assert_eq!(
        f.get(&f.coord, progress_key(&domain)).await,
        Some(winner_row)
    );
    let fresh = journal.load_metadata_progress().await.unwrap();
    let page = journal
        .discover_metadata_page(fresh.cursor().unwrap(), 1)
        .await
        .unwrap();
    consider(&page, &expected, &mut Vec::new(), &mut 0);
    journal
        .checkpoint_considered_metadata_page(fresh, page, None)
        .await
        .unwrap();
    journal
        .reset_metadata_progress(journal.load_metadata_progress().await.unwrap())
        .await
        .unwrap();
    Box::pin(sweep(&journal, &expected)).await;

    let stale = journal.load_metadata_progress().await.unwrap();
    let page = journal
        .discover_metadata_page(stale.cursor().unwrap(), 1)
        .await
        .unwrap();
    consider(&page, &expected, &mut Vec::new(), &mut 0);
    // Administrative TEST-ONLY rotation through the owned fixture client. The
    // native guard binds the observed incarnation/revision; no namespace change,
    // restore workflow, or production assumption is introduced.
    let old_control = f.get(&f.coord, control(&domain)).await.unwrap();
    let mut rotated = old_control.clone();
    rotated.insert("incarnation".into(), A::S(Uuid::new_v4().to_string()));
    rotated.insert("revision".into(), A::N("2".into()));
    f.client
        .put_item()
        .table_name(&f.coord)
        .set_item(Some(rotated.clone()))
        .condition_expression("#inc = :inc AND #rev = :rev")
        .expression_attribute_names("#inc", "incarnation")
        .expression_attribute_names("#rev", "revision")
        .expression_attribute_values(":inc", old_control["incarnation"].clone())
        .expression_attribute_values(":rev", old_control["revision"].clone())
        .send()
        .await
        .unwrap();
    let old_progress = f.get(&f.coord, progress_key(&domain)).await.unwrap();
    assert_eq!(
        journal
            .checkpoint_considered_metadata_page(stale, page, None)
            .await,
        Err(MetadataProgressError::Conflict)
    );
    assert_eq!(
        f.get(&f.coord, progress_key(&domain)).await,
        Some(old_progress)
    );
    let historical = journal.load_metadata_progress().await.unwrap();
    assert!(historical.requires_reset());
    assert_eq!(
        historical.cursor(),
        Err(MetadataProgressError::ResetRequired)
    );
    assert_eq!(historical.completed_passes(), 1);
    let stale_reset = journal.load_metadata_progress().await.unwrap();
    journal.reset_metadata_progress(historical).await.unwrap();
    assert_eq!(
        journal.reset_metadata_progress(stale_reset).await,
        Err(MetadataProgressError::Conflict)
    );
    let reset = journal.load_metadata_progress().await.unwrap();
    assert!(!reset.requires_reset());
    assert!(reset.cursor().unwrap().is_none());
    assert_eq!(reset.completed_passes(), 0);
    assert!(reset.last_completion().is_none());
    assert_eq!(f.get(&f.coord, control(&domain)).await, Some(rotated));
    Box::pin(sweep(&journal, &expected)).await;
    assert_eq!(
        receipts(&rows(f, &domain).await, &domain),
        before,
        "progress/reset must not create CMD receipts"
    );
}
