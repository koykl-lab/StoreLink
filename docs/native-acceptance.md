# Native journal acceptance: evidence and guarded fixtures

## Status and scope

**Authoring, formatting or compiling this harness is not real-AWS acceptance.**
The shared journal scenario completed successfully against regional AWS DynamoDB
on 2026-11-09, including canonical-ARN transaction addressing. Both disposable
fixture tables were deleted and absence was independently rechecked. This is
metadata-journal acceptance only, not whole-library or filesystem acceptance.
Every additional live execution still requires the explicit authorization and
fixture guards below.
No cloud execution is implicit in a build, ordinary `cargo test`, or enabling
`aws-acceptance`. Both service-facing entrypoints are explicitly ignored. A
concrete disposable run requires separate operator authorization, including its
account, region, profile, run UUID, costs and cleanup boundary.

`tests/common/journal_contract.rs` is the **single** native scenario implementation.
`tests/journal_local.rs` and `tests/journal_aws.rs` supply native DynamoDB clients
and fresh two-table fixtures to it. There is no alternate mocked transaction
engine, relaxed enrollment, or smaller AWS-only success case. The scenarios cover:

- explicit fresh-domain initialization and immutable command/token identity;
- atomic application effects and binary historical result receipts;
- exact retry, lost-response resolution, and changed current application state;
- application conditional cancellation with indexed native reasons and no partial effects;
- native duplicate-item, malformed-expression and oversized-result rejection;
- eight barrier-coordinated execute/cancel races and outcome/effect agreement;
- canonical enrollment, reserved coordination identities and unenrolled rejection;
- 100-action execution versus a rejected 101-action plan;
- digest/domain/future-generation validation;
- admission fencing, closed-generation absence, compaction and incarnation expiry;
- deliberately malformed control and receipt records.

The suite also exercises coordinated slot attachment, replacement, detachment,
shared-version count consolidation, same-version/zero-net moves, stale revision
rejection, deterministic cancellation, and a reference-versus-retirement race.
Those cases seed synthetic READY records directly: they test **existing-version
reference coordination**, not a production upload-registration/adoption path.

A successful regional AWS run of the enhanced suite was verified on 2026-11-09,
with both exact fixture tables subsequently confirmed absent. This is still not
S3 upload, lifecycle-provider, multipart, reclamation-convergence, consumer
integration, or complete StoreLink protocol acceptance. Native unit/model tests likewise do not
prove service behavior.

### Native scenario stack budget

The combined debug-build scenario aborted with a stack overflow during admission
coverage. Its two fixture tables were recovered with the exact manifest-guarded
cleanup path and independently confirmed absent. The scenario driver now runs
journal, reference and admission phases behind separate boxed boundaries, with
an offline future-layout regression bound. That bound measures future storage,
not the entire debug poll stack.

For the long, explicitly invoked native suite use **one test thread** and
`RUST_MIN_STACK=8388608` (8 MiB). The corrected suite passed against regional AWS
with that bounded test-only setting and confirmed cleanup. This does not change
production runtime stack settings or establish full serving-runtime acceptance.

### Current emulator limit

The observed DynamoDB Local run fails with `ResourceNotFoundException` on
canonical-ARN `TransactGetItems`. Changing the signing region to `ddblocal` did
**not** fix it. A described table or passing unit test is not proof that the
emulator supports the required canonical ARN transaction addressing.

The local wrapper keeps the canonical ARN safeguards intact and fails honestly.
It accepts **only** literal `http://127.0.0.1:<nonzero port>`, uses synthetic
credentials without a credential/config chain, and cannot select live AWS by
changing an endpoint variable. It creates exactly two UUID-suffixed local tables
and attempts both deletions after provisioning/enrollment/assertion failure.
It does not launch an emulator or Docker. No fixture is assumed to be running.

For a separately authorized, already-running local emulator:

```sh
STORELINK_DYNAMODB_LOCAL_ENDPOINT='http://127.0.0.1:<port>' \
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$BUILD_TARGET_DIR" RUST_MIN_STACK=8388608 \
cargo test --locked --no-default-features --features dynamodb \
  --test journal_local native_journal_contract -- --ignored --exact --nocapture --test-threads=1
```

Substitute a real port and an operator-selected build directory. Do not interpret
the currently expected emulator failure as real AWS failure or compatibility.

## Compile without contacting AWS

The feature is optional and is not a default/runtime feature. It adds native AWS
configuration and STS tooling only for the opt-in harness.

```sh
CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR="$BUILD_TARGET_DIR" CARGO_NET_OFFLINE=true \
cargo test --locked --no-default-features --features aws-acceptance \
  --test journal_aws --no-run
```

Dependencies must already be cached for offline compilation. Run long compiles
as supervised background operations and inspect their terminal exit/log evidence;
a launched process is not a successful compile. Limit compilation to two jobs.
Formatting may be scoped to `tests/journal_local.rs`, `tests/journal_aws.rs` and
`tests/common/journal_contract.rs`. To format only the AWS harness, use
`rustfmt --edition 2024 --config skip_children=true tests/journal_aws.rs`.
Neither compiling nor formatting executes the ignored tests. No Python process,
Docker launch or cloud mutation is needed.

The ordinary, nonignored guard tests exercise phase/absence classification,
conservative single-attempt rejection classification, phase retention through a
task panic, explicit cleanup acknowledgement and manifest guards. They do not
load AWS configuration, credentials or service clients:

```sh
CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR="$BUILD_TARGET_DIR" CARGO_NET_OFFLINE=true \
cargo test --locked --no-default-features --features aws-acceptance \
  --test journal_aws
```

Do **not** add `--ignored` to this offline guard-test command.

## Real-AWS gates

The harness currently supports the commercial `aws` partition on Unix systems
where private file modes can be checked. Prerequisites are mandatory: explicitly
invoking an ignored test without them **fails**, never silently skips.

Required environment variables:

| Variable | Meaning |
| --- | --- |
| `STORELINK_AWS_ACK` | Exact acknowledgement below; not a substitute for operator authorization |
| `STORELINK_AWS_EXPECTED_ACCOUNT` | Exactly 12 ASCII digits; verified against native STS before any DynamoDB mutation |
| `STORELINK_AWS_REGION` | Explicit commercial AWS region, not an endpoint URL |
| `STORELINK_AWS_PROFILE` | Explicit trusted native AWS profile |
| `STORELINK_AWS_RUN_UUID` | Fresh version-4 UUID in lowercase hyphenated form |
| `STORELINK_AWS_MANIFEST_DIR` | Existing private directory (0700) at an absolute path **outside** the repository |
| `STORELINK_AWS_CLEANUP_SETTLED_ACK` | **Cleanup-only additionally requires** the exact settlement acknowledgement below, before credentials/network access; not required or used by automatic cleanup |

Authentication uses the native profile credentials provider, not the environment-
first default credential chain. Native configuration is loaded with that profile
and region. A single credential snapshot is resolved **in memory**, then used by
both native STS `GetCallerIdentity` and DynamoDB clients. The snapshot is not
refreshed mid-run; provide credentials valid long enough for provisioning,
scenarios and cleanup. The returned account must exactly equal the expected
account, and the caller must belong to the commercial partition. Credential
provider/STS errors are redacted. No credentials or session tokens are printed or
written to the manifest.

The STS and DynamoDB service clients use fresh native SDK configuration builders:
they do not inherit `endpoint_url` or `service_config` from loaded configuration.
Global, service-specific and profile endpoint overrides cannot redirect those
clients. The **selected profile is trusted authentication configuration**, including
its source profiles, credential processes, SSO and role acquisition; native profile
authentication can itself perform network I/O or launch a configured credential
process. Inspect it before authorizing a run. Do not configure Python credential
processes or use untrusted profile/credential files for this harness.

Exactly two names are derived from the UUID's 32 hexadecimal digits:

```text
storelink-acceptance-journal-coord-<uuid-without-hyphens>
storelink-acceptance-journal-app-<uuid-without-hyphens>
```

There are no arbitrary table-name inputs and no table enumeration. Both names
must be confirmed absent before entering the mutation/cleanup phase. Existing
names abort without adoption or deletion. Each table uses on-demand billing,
string `pk`/`sk` keys, and creation-time tags:

| Tag | Value |
| --- | --- |
| `storelink:acceptance-purpose` | `storelink-native-journal-acceptance` |
| `storelink:acceptance-run` | Exact canonical run UUID |
| `storelink:acceptance-role` | `coordination` or `application` |

Run/purpose/role values are immutable **within this harness**; it never retags a
table. DynamoDB tags are not intrinsically immutable. Authorization must ensure
exclusive ownership of these fresh names and prevent another actor from replacing
tables or changing ownership tags during the run/recovery window. Native
`DeleteTable` has no conditional-tag predicate: Describe/Tags/Delete is not an
atomic authorization primitive. The ARN-and-tag gate supplements, not replaces,
least-privilege IAM and exclusive disposable ownership.

Before either create call, the harness writes and syncs a nonsecret, versioned JSON
manifest with mode 0600 and `create_new`, then syncs its parent directory. The file
name is `storelink-acceptance-journal-<canonical-run-uuid>.json`. It records only
purpose, run, expected account, region, profile and the two deterministic names.
No manifest overwrite/reuse is accepted. A partial write/sync failure aborts
before creation. Preserve this record outside published sources; account/profile
metadata is operational information even though it is not credential material.
A UUID must never be reused, even with a different manifest directory. Retained
manifests enforce reuse rejection in the selected directory, not globally.

The two independent creates start concurrently and both local tasks settle,
including panic, before cleanup starts. Each table has exactly one CreateTable
send with SDK `max_attempts = 1`, using a dedicated native client cloned from the
verified client's configuration. There is no application create retry. Other
operations retain bounded SDK retries (at most three attempts) and timeouts;
native `table_exists` waiters have five-minute bounds. A single attempt can still
have an unknown remote outcome: local completion is **not** remote request drain.

Each table's creation phase is recorded **in memory before send** and survives a
task panic: never issued, sent/unknown, accepted but unconfirmed, definitively
rejected, or observed owned ACTIVE. Only an explicit `ResourceInUseException`
from that single attempt is classified as definite rejection; other errors remain
unknown conservatively. A successful create response or readiness waiter alone
is not the owned ACTIVE observation: exact ARN/name and ownership tags must also
match. These phases are not written into the immutable manifest.

Provisioning, enrollment and scenario assertions are inside a task boundary so an
assertion panic or partial create still reaches cleanup. The whole scenario is
not forcibly dropped in the middle of a race. Both race competitors settle before
their results are asserted.

### Least-privilege disposable access

Restrict the run's principal to the two expected table ARNs in the expected region
and account, with creation request tags constrained to the values above. The
harness uses CreateTable (including permission to attach its creation tags),
DescribeTable, ListTagsOfResource, DeleteTable, GetItem, PutItem, DeleteItem,
TransactGetItems and TransactWriteItems. Native STS GetCallerIdentity is also used.
There is no S3 access, ListTables, broad deletion or existing-resource adoption.
Do not grant broad production access simply to make this test pass.

## Authorized invocation template — NOT executed by compilation

Only after reviewing and authorizing a concrete disposable run, choose the
account/profile/region and a private manifest directory. Use a new UUID each time.
The example contains placeholders, not a personal account or preselected target.

```sh
export STORELINK_AWS_ACK='CREATE-AND-DELETE-TWO-DISPOSABLE-STORELINK-JOURNAL-TABLES'
export STORELINK_AWS_EXPECTED_ACCOUNT='<12-digit-disposable-account>'
export STORELINK_AWS_REGION='<authorized-region>'
export STORELINK_AWS_PROFILE='<authorized-profile>'
export STORELINK_AWS_RUN_UUID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
export STORELINK_AWS_MANIFEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/storelink-acceptance.XXXXXXXX")"
# Securely retain these NONSECRET run coordinates outside the repository.

CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$BUILD_TARGET_DIR" RUST_MIN_STACK=8388608 \
cargo test --locked --no-default-features --features aws-acceptance \
  --test journal_aws native_journal_contract_aws -- --ignored --exact --nocapture --test-threads=1
```

Run the command as a supervised background operation, inspect terminal evidence,
and record the contract result **and** each cleanup result. Do not invoke a bare
`--ignored` filter: the two entrypoints have different purposes. Never run the
acceptance and cleanup-only entrypoints concurrently for the same run.

## Cleanup and process-death recovery

For **each exact generated name independently**, cleanup:

1. Describes the table. Only `ResourceNotFoundException` establishes **current**
   absence; access, transport and other failures do not. Automatic cleanup refuses
   to report completion for an absent table whose sent create remains unknown or
   accepted-but-unconfirmed and was never observed owned ACTIVE: it reports
   **late-create uncertainty** and retains the manifest. An absence waiter cannot
   erase that uncertainty. Never-issued or definitively rejected single attempts,
   or a previously observed owned ACTIVE table, may safely take the absence path.
2. If present, requires the returned name and canonical ARN to equal the expected
   commercial partition, account, region and exact generated name.
3. Lists tags on that verified ARN (including pagination) and requires exact
   purpose, run and role matches. Missing/mismatched tags refuse deletion.
4. If a matching owned table is actually observed, deletes by the verified
   canonical ARN, never a prefix. This observation resolves the one possible
   create, even if its response was lost. A failed/ambiguous deletion response is
   not success; the postcondition must be established. A non-ACTIVE table may
   reject deletion and leave cleanup unconfirmed.
5. Uses the bounded native `table_not_exists` waiter and a final DescribeTable
   absence confirmation. Both independent cleanup outcomes settle even if one
   fails or panics.

Unconfirmed cleanup fails the test and preserves the manifest. The manifest is
also retained on success as an immutable recovery/audit record; it is not rewritten
with a fabricated success marker. An assertion failure remains a failed contract
even if both tables were successfully removed.

Process termination, abort, machine loss, expired credentials or persistent
service failure can prevent in-process cleanup. In-memory phases are lost on
process death. Cleanup-only therefore **additionally requires an explicit operator
acknowledgement that the original producer AND all original CreateTable requests
have settled**. This flag is an operational prerequisite, **not proof** of remote
quiescence; setting it does not cancel requests, fence the producer, or verify
settlement. Process exit, SDK timeout, a five-minute waiter expiry, and repeated
absence observations are not substitutes. If settlement cannot be established,
do not acknowledge it or claim cleanup complete; retain the manifest and resolve
the uncertainty with the provider/operator.

Once that prerequisite is actually satisfied, use the **same** run, directory,
account, region, profile and original acknowledgement, plus the settlement flag.
The flag and manifest are checked before credential loading; symlinks, nonprivate
files, unknown fields, edited names and mismatched coordinates are rejected.
Renew the same profile's login if needed, without changing the expected account.
Obtain authorization for recovery if it was not included in the run approval.

```sh
# Re-export the exact retained STORELINK_AWS_* coordinates and acknowledgement.
# Do NOT generate a new UUID or select a different manifest directory here.
# ONLY after establishing original producer AND all original create requests settled:
export STORELINK_AWS_CLEANUP_SETTLED_ACK='ORIGINAL-PRODUCER-AND-ALL-CREATE-REQUESTS-HAVE-SETTLED'
CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR="$BUILD_TARGET_DIR" \
cargo test --locked --no-default-features --features aws-acceptance \
  --test journal_aws cleanup_journal_aws -- --ignored --exact --nocapture
```

This entrypoint never creates tables and never enumerates the account. It repeats
the exact STS identity and per-table ARN/tag/absence gates. Its ability to accept
absence without the lost in-memory phases is conditional on the operator's
settlement acknowledgement being true, not on the flag providing evidence. A
five-minute waiter expiry, a temporarily unavailable ownership tag, or a
still-creating/deleting table is **unconfirmed cleanup**, not permission to bypass guards. Preserve the
manifest, investigate, and retry the exact cleanup after the service settles.
If a manifest write failed before creates, do not infer that its partial contents
authorize deletion. A prefix match is never sufficient.

No finite observation proves a still-in-flight remote CreateTable can never
arrive later. Process-death recovery requires request quiescence; preserve the
manifest for another exact-name check when an outcome was ambiguous. Never claim
leak-free acceptance solely from a killed process or one early absence response.
