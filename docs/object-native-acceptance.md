# Native object acceptance (opt-in, disposable fixture only)

`tests/object_aws.rs` is acceptance **test code**, not a deployment root, adoption
helper or production configuration. Compilation, ordinary offline guard tests and
loopback SDK tests are **not AWS enrollment or service acceptance evidence**.
Only a separately authorized successful ignored run exercises native AWS.

## Fixture owner and explicit inputs

An external operator provisions a **fresh disposable fixture** through reviewed,
temporary Terraform outside this repository: exactly two regional DynamoDB tables
and one general-purpose S3 bucket. This entrypoint never creates/deletes tables or
buckets, changes lifecycle, IAM or tags, or invokes client multipart abort. It does
not scan/list tables or discover additional buckets. Terraform owns final fixture
teardown, including all table rows. Do not use existing application resources.

Both tables must be ACTIVE, have precisely string `pk` (HASH) / `sk` (RANGE) keys,
and be in the expected commercial AWS account/region. Both tables and the bucket
must carry exactly one `StoreLinkAcceptanceRun=<fresh canonical v4 UUID>` tag.
Keep ownership tags immutable for the run. Require exclusive fixture writers;
matching tags and empty listings cannot prove IAM exclusivity or request drain.

The bucket must have versioning enabled, no Object Lock, no completed-object
expiration/unsafe transitions, and a continuously maintained matching
AbortIncompleteMultipartUpload lifecycle rule with eligibility **at least one
day**. Public access should be blocked. Preserve lifecycle even when listings are
empty. Keep Terraform `force_destroy=false`; never force destruction around an
unfinished MPU. Public `qualify_fresh` checks the native profile and fresh namespace
scope. Optional Intelligent-Tiering archive configurations are **not inspected**;
operators must keep restore-required optional tiers disabled for managed objects.
This omitted check does not impose STANDARD-only storage and must not be counted
as a verified absence of archive configuration. The service-owned in-flight-part
reclamation obligation remains the assumption in protocol section 9.

For a newly versioned bucket, the operator must also allow **15 minutes after first
versioning enablement before object PUT/DELETE**, following [AWS's propagation
recommendation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/manage-versioning-examples.html).
A successful `GetBucketVersioning` or qualification observation does not prove this
interval elapsed. Record the provisioning completion and the resulting not-before
time in private fixture evidence; do not turn this into a per-request delay or
weaken exact-VersionId checks to accommodate propagation.

Set these variables explicitly; there are no default account/profile/region or
resource selections. Prefix every suffix below with `STORELINK_OBJECT_AWS_`:

| Suffix | Value |
| --- | --- |
| `ACK` | `WRITE-AND-CLEAN-DISPOSABLE-STORELINK-OBJECT-FIXTURE` |
| `ACCOUNT` | `<12-digit-account>` |
| `PROFILE` | `<explicit-authentication-profile>` |
| `REGION` | `<commercial-region>` |
| `RUN_UUID` | `<fresh-canonical-v4-uuid>` |
| `COORDINATION_TABLE_ARN` | `<exact-coordination-table-arn>` |
| `APPLICATION_TABLE_ARN` | `<different-exact-application-table-arn>` |
| `BUCKET` | `<fixture-bucket-name>` |
| `EVIDENCE_DIR` | `<absolute-private-directory-outside-checkout>` |
| `FIXTURE_MANIFEST` | Optional, preferred: `<absolute-private-json-file>` |

Prefer exporting a normal Terraform output value as a private JSON manifest with
these exact fields (not a Terraform state file or the full output wrapper):

```json
{
  "account": "<12-digit-account>",
  "region": "<commercial-region>",
  "run_uuid": "<fresh-canonical-v4-uuid>",
  "coordination_table_arn": "<exact-coordination-table-arn>",
  "application_table_arn": "<exact-application-table-arn>",
  "bucket": "<fixture-bucket-name>"
}
```

The optional manifest must match **all six explicit environment values**. Unknown
JSON fields are rejected. Keep manifest/evidence directories Unix mode 0700 and
files 0600, outside the published checkout. Do not commit captured identities,
profile data, secrets, state, test logs or recovery evidence. The profile itself is
trusted authentication configuration, including its SSO/role/process providers.
Credential resolution is explicitly profile-based, bounded and snapshotted once.
Fresh STS, DynamoDB and S3 service builders use that **same frozen credential
snapshot and explicit region**; they do not inherit shared-config endpoint
redirects or the environment-first credential chain. STS verifies account and
commercial partition before any mutation. Native DescribeTable, paginated table
tags, expected-owner HeadBucket/actual region and bucket tags gate the run.

IAM must allow the guarded reads (STS identity, table description/tags, bucket
profile/lifecycle/versioning/Object Lock reads and listings),
regional DynamoDB GetItem/TransactGetItems/TransactWriteItems and **dynamodb:PutItem
on the coordination table** for `initialize_fresh_domain`, S3 PutObject and
multipart creation/parts/completion, exact-version GetObject and DeleteObjectVersion.
Scope permissions to the two fixture tables, bucket and run prefix where possible.
Do not grant the test table/bucket creation/deletion, lifecycle mutation or
AbortMultipartUpload permissions. Native permission/configuration failures fail
closed; they are not interpreted as absence.

## Scenario and limits

The exact owned prefix is `storelink-object/<RUN_UUID>/`. Namespace `a/` and `b/`
are disjoint children with distinct IDs/configuration revisions and the same
StoreId/bucket. Real fresh qualification is consumed by public journal enrollment;
strong reads confirm persisted namespace bindings. The second native enrollment
must commit without changing the permanent StoreId binding row. Conflicting
physical bucket fields remain covered by existing private action-shape unit tests:
a native conflicting-bucket qualification would require a second bucket, which
this fixture deliberately does not have. No qualification is forged.

For separate target keys `a/empty`, `a/small` and `a/multipart`, the scenario creates
fresh intent/attempt identities using the public journal, calls `transfer_upload`
with an AsyncRead source, verifies the returned exact candidate with the native
full-body SHA-256 verifier, selects it, reads staging, and publishes one conditional
application Put plus one ReferenceChange in the same native transaction. Strong
native reads verify the application item, reference, READY counts (0 refs/1 claim
before publication; 1 ref/0 claims afterward), and RELEASED/CONSUMED stage/intent.
An explicit VersionId GET checks published bytes and SHA-256; there is no latest-key
fallback. This final diagnostic read does **not** claim use-pin acceptance.
The application pointer and reference are then atomically removed; tracked
retirement must reject the earlier referenced READY version, then permit the
unprotected version to become RETIRING (0 references/0 claims). Exact-version
`delete_retiring_version` must produce DELETED; repetition observes AlreadyDeleted.

After all three normal objects are retired, the same namespace `a/` exercises two
additional bounded cases, with no new harness or resources:

- **Known, unselected version (`a/orphan-unselected`):** `list_version_page`
  first observes an empty namespace. Persist a fresh intent and attempt, transfer
  a payload under 64 bytes through the normal native adapter, but never select or
  publish it. A later page must find exactly that candidate: the earlier empty
  pass is not permanent absence. Native exact-version observation must recover
  the admitting identities. While the intent/attempt remain OPEN/ACTIVE,
  `prepare_orphan_retirement` must specifically return SelectionPossible and leave
  no Version row. Cancel the intent with the native journal command, reobserve
  the exact candidate, then prepare/execute guarded orphan retirement directly
  into RETIRING with zero counts, never READY.
- **Unattributed fixture (`a/orphan-unattributed`):** one explicit synthetic raw
  native Put, under the predeclared owned key and expected bucket owner, supplies
  no reserved metadata. Its returned VersionId must be non-null and the exact
  observer must classify it as Unattributed. An explicit positive 1000-ms grace
  policy with 1000-ms uncertainty is tested using a **controlled trusted virtual
  test clock**: at LastModified, preparation returns GracePending with no Version
  row; at LastModified + grace + uncertainty, its conservative lower bound reaches
  eligibility and guarded registration creates RETIRING/0/0. This is a virtual
  clock boundary test, **not real elapsed grace or production clock calibration**.
  Raw Put is synthetic test setup only, not a production adoption helper.

For each case a second actual HEAD observation is captured before registration
and deletion. Repeating preparation with that observation must return
ExistingTracked with the same complete snapshot/creation identity, without
writing over accounting. The existing exact-version deletion adapter must then
produce DELETED/0/0 with the original creation identity. A final native page must
be empty; parent cleanup still checks the entire owned prefix's versions, delete
markers and MPUs. Unknown/future-schema and foreign attribution stay in unit and
loopback coverage; no deliberately abandoned real MPU is added. These cases do
not implement a recurring scanner, fabricate GC progress, or prove full GC
convergence, late in-flight request settlement or metadata compaction.

Only after that final empty page, a bounded **inventory scheduling-progress** case
uses two more predeclared keys, `a/inventory-first` and `a/inventory-second`:

- Two explicit fixture-only raw PutObject requests write distinct synthetic
  payloads under 64 bytes, with expected-owner checks and non-null returned
  VersionIds. They create no MPU and are not library adoption/publication.
- `load_inventory_progress(&qualified)` must initially expose no cursor, zero
  completed passes and no completion metric. A native `list_version_page` with
  limit **1** must return one of those exact VersionRefs and a continuation.
  The test inspects every returned item, rejects delete markers/unexpected data,
  and records the actual candidate as **retained test data**, not GC work.
- Before checkpointing, capture another initial progress observation and another
  native first page; inspect that page too. Checkpoint the first considered page,
  then require the stale checkpoint to return specifically
  `InventoryProgressError::Conflict`. Other errors and unexpected success fail;
  a fresh native load must preserve the first page's markers, zero completed
  passes and absent completion metric.
- Supply the **restored opaque cursor from that load** to the next native S3 page,
  not the original in-memory cursor or reconstructed markers. Check that the two
  recorded candidates are distinct and exactly the two expected VersionRefs,
  independent of alphabetical ordering. Native next markers are never assumed
  to identify the first returned entry.
- The terminal page checkpoint clears continuation and increments completed
  passes. A final native reload must show cursor None, completed passes **1**, and
  the supplied bounded diagnostic TimestampMs(1000). This synthetic metric is
  scheduling evidence, never a trusted GC/retention clock or elapsed-time claim.

This is durable cursor reload/restart acceptance, not process-crash injection or
recurring scanner/GC convergence acceptance. The extension adds exactly two Puts,
three limit-1 S3 listings (including the stale page), four progress loads and three
checkpoint attempts (including the required conflict) on its success path: **12
scenario requests**, with no retries or new resources. Existing parent cleanup
adds two exact-version deletions and retains its whole-prefix version/marker/MPU
checks even after failure. The 600-second scenario and 180-second cleanup budgets
are unchanged; exhaustion still fails rather than claiming progress. Neither of
these retained synthetic versions is garbage-collected by the library.

Payloads are synthetic: zero bytes, 37 bytes and **8 MiB + 17 bytes**. The current
transfer adapter uses a single Put for the first two and two 8-MiB-bounded parts for
the third. No giant payloads, intentional abandoned MPU or real-cloud fault
injection are used; loopback tests own transfer error/no-abort coverage.

Fixture-only policy: MPU supported age two hours plus one-hour lifecycle margin;
claim lifetime ceiling ten minutes, new deadlines 590 seconds ahead, renewal
margin 30 seconds and uncertainty one second. A fresh SystemTime observation is
sampled for admission/RPC checks, never a captured old timestamp. **The operator
assumes this test host is synchronized within one second.** This is not a
production clock calibration proof or a recommended production policy. The suite
does not renew claims, add use pins or require another worker's protection API.

All native clients use one attempt (no SDK retry), 5-second connect, 10-second read
and 20-second operation/attempt timeouts. Preflight is capped at 180 seconds,
scenario at 600 seconds and cleanup at 180 seconds (16 minutes total asynchronous
budget, plus bounded local filesystem work). The profile resolution sub-budget is
30 seconds. Final GET buffering stops at 8 MiB + 18 bytes. Cleanup listings request
100 entries/page and reject incomplete traversal beyond ten pages or 1000 entries
per versions/markers or MPU traversal. A missing IsTruncated flag fails. Truncated
pages require nonempty, bounded, nonrepeating continuation markers; explicit
IsTruncated=false is terminal even when native S3 retains empty or populated next
markers. Neither missing markers nor budget exhaustion means empty. Public
qualification additionally has its own bounded configuration
listings (at most 1000 configurations) and the enclosing scenario deadline.

## Cleanup, unknown outcomes and evidence

Before **any writes**, create-new, fsync'd private `object-<RUN_UUID>.json` evidence
records the exact fixture, UUID prefix and all seven planned keys (`a/empty`,
`a/small`, `a/multipart`, `a/orphan-unselected`, `a/orphan-unattributed`,
`a/inventory-first`, `a/inventory-second`), including both orphan keys and both
synthetic inventory keys **before their S3 writes**. A reused evidence name
fails before mutation. It intentionally records no credentials, profile, payload,
attribution metadata or response body. This prefix allowlist supports unknown
Put/Create/Complete outcomes where no candidate version was returned. Evidence is
retained even after success; it is diagnostic, not an executable recovery input.

The entire owned scenario runs inside a joined Tokio task. Returned errors,
assertion panics and the scenario deadline all lead to bounded cleanup; there are
no detached writer tasks. Local future settlement **does not prove remote request
settlement**. Ordinary checked errors and the final summary use fixed labels and
preserve both scenario and cleanup results. **JoinError does not redact Rust's
panic hook:** a panic can print its payload before being caught. There is no global
panic-hook override. Capture **all stdout and stderr privately** for every native
ignored run; use umask 077, a new log file inside the private evidence directory,
and never paste/upload that log into a public issue, repository or chat. A killed
process/abort cannot run teardown: retain private evidence and the fixture for
operator investigation.

Cleanup revalidates bucket ownership and completes bounded version/delete-marker
and MPU listings. Before deletion it rechecks ownership, then deletes **only
exact VersionIds beneath the exact UUID prefix**, always with expected owner.
A failed delete is recorded without retrying it; other independently validated
versions are still attempted within the mutation budget. Both final inventories
are always attempted after the delete phase, even after a delete failure. The
mutation phase has at most 120 seconds, reserving the final minute of cleanup for
inventory (55-second inner budget plus scheduling slack). Failure labels accumulate
and fail the result even if the final listings are empty. Pre-write ownership
failure still stops deletion. It never deletes tables/buckets, touches other
prefixes or issues AbortMultipartUpload. This is explicit removal of
synthetic fixture versions, **not StoreLink garbage-collection acceptance**.
The successful normal cases unlink their application/reference records; failure
may leave them live. All remaining table rows belong to parent fixture teardown.

If either MPU listing contains unfinished uploads, cleanup is incomplete: retain
the fixture for the continuously maintained provider lifecycle. No busy wait,
automatic retry loop, force-destroy or claim of emptiness is permitted. A scenario
error/panic/timeout conservatively makes cleanup incomplete even if current
listings are empty, because late writes/unknown MPUs may still materialize. Retain
the bucket/lifecycle/evidence and resolve settlement externally before teardown.
An incomplete/failed listing, delete or cleanup timeout also fails the test.

## Validation and separately authorized execution

Compile only (no cloud calls or ignored execution):

```sh
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/storelink-verification-target \
  cargo test --locked --offline --all-features --test object_aws --no-run
```

Ordinary tests in this target check identity/ARN/run tags, namespace/prefix
ownership, all seven distinct predeclared keys including both retained inventory
keys, ACTIVE pk/sk schemas and
incomplete-pagination classification. Two
additional regressions construct synthetic-credential, explicit-loopback SDK
clients: the pinned SDK deserializes terminal MPU XML with empty and populated
markers, and cleanup continues after a failed delete through both final
inventories. They do not load real credentials/configuration or contact AWS.
Compilation does not run these ordinary tests. To run only offline tests:

```sh
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/storelink-verification-target \
  cargo test --locked --offline --all-features --test object_aws -- --skip native_object_contract_aws
```

Strict Clippy for this target (also no ignored execution or AWS calls):

```sh
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/storelink-verification-target \
  cargo clippy --locked --offline --all-features --test object_aws -- -D warnings
```

Only after separate review, provisioning and explicit authorization, an operator
may remove `--no-run` and select the ignored test:

```sh
(
  umask 077
  set -C  # refuse to overwrite an existing private log
  CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/storelink-object-target \
    cargo test --all-features --test object_aws native_object_contract_aws -- --ignored --exact \
    > "${STORELINK_OBJECT_AWS_EVIDENCE_DIR}/native-${STORELINK_OBJECT_AWS_RUN_UUID}.log" 2>&1
)
```

The private evidence directory must already exist with mode 0700 outside the
checkout. Do not replace private capture with `tee` to a public terminal/log sink;
panic output may contain sensitive data even though checked SDK errors are redacted.

Do not treat this narrow success path as proof of whole-protocol safety/liveness,
GC, lifecycle convergence, clock health, simultaneous-reader protection, lost
response recovery or cross-provider compatibility. Record actual native results
privately and report unexecuted acceptance honestly.
