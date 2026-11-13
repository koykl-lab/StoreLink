# StoreLink v0 protocol — working implementation contract

The user authorized v0 implementation and deployment work after design review.
This contract replaces the earlier non-expiring pins and permanent per-operation
tombstones. Implementation remains incomplete; authorization is not implementation
acceptance or a production proof. Review, model testing and native acceptance remain
required gates.

**Provider dependency for user review:** Section 9 delegates abandoned multipart
reclamation to mandatory S3 lifecycle, without early client abort. Its liveness
claim explicitly assumes that service-owned reclamation covers finitely many
in-flight parts. AWS documents the overall cleanup outcome, but not that precise
race. This is a stated backend obligation, not a universally proven S3 behavior.

## Reading guide: what StoreLink owns

An application stores logical metadata in its database and bytes as object-store
versions. For example, a filesystem might say `/notes/a.txt` uses object version V7;
a media application might associate V7 with a photo. These are application meanings, not
StoreLink's schema.

| Responsibility | Owner |
| --- | --- |
| Filesystem tree, inventory records, permissions and domain-specific conditions | Application |
| Choice of same-key versions versus new object keys | Application |
| Upload-intent tracking and exact-version verification | StoreLink with its object-store adapter |
| Reference/temporary-protection accounting and retirement rules | StoreLink |
| Combining application writes and that accounting into one metadata transaction | StoreLink's metadata adapter, using native database primitives |
| Outcome receipts, safe retry/resolution and bounded compaction | StoreLink |
| Reconciliation logic | StoreLink; the application/deployment must run it fairly without client traffic |
| Byte storage, native transactions and lifecycle-managed MPU reclamation | Storage providers under the stated adapter contracts |

StoreLink need not proxy the bytes. A caller may use SDK transfers or constrained
presigned URLs, but must obey the intent, attribution and finalization contract.
The library is not a replacement S3 server, filesystem or generic saga engine.

### Native transaction participation

The application authors its native conditional actions; StoreLink constructs and
submits the final coordinated transaction and resolves its outcome. Providing a
configured native client/transport is not permission to omit library guards or
replace outcome handling with a caller's Boolean success wrapper. An immutable
inspection/budget view may be exposed without making caller-edited executable
plans the correctness boundary.

Do not merge arbitrary application Update/Condition expressions. Applications
combine their own conditions into valid native actions. Preflight known structural
bounds and library-owned records; native DynamoDB remains authoritative for
expression validity, duplicate-item rejection and final item sizes that cannot be
inferred from an opaque Update expression. Native rejection must have no partial
publication.

An application business receipt can remain in the application write set alongside
the StoreLink command outcome. They are not automatically interchangeable: a
capture/job may span several coordinated effects and have a different retention
or replay contract.

### Terms used below

- **Upload intent (the 'ticket' in discussion):** a persisted record created before
  transfer, identifying the intended destination/attempts and finite claim lifetime.
  It is not an S3 UploadId, credential or successful application publication.
- **Verified upload receipt:** evidence identifying a completed exact object
  version; it does not establish that application metadata references it.
- **Application reference:** an application-owned pointer to an object version.
  A corresponding library reference edge records that the version must remain.
- **Temporary protection/claim:** a finite renewable reason to retain a version
  during staging or consuming I/O. This is not a client DAV lock or S3 Object Lock.
- **Publication:** committing the application pointer and matching protection
  accounting together in the metadata database.
- **Command receipt:** the recorded terminal result of a metadata operation,
  written atomically with its effects, so a lost response does not cause duplicate
  effects or speculative rollback. Native database idempotency is used where
  applicable; the documented outcome protocol handles its broader recovery needs.
- **Retirement:** irreversibly prohibiting new references before physical deletion.

### Worked example: publish V7

The application supplies an operation ID, its conditional metadata changes, the
reference transition from V6 to V7, and its intended result (such as revision R9).
It owns the meaning and validation of those application conditions.

StoreLink's adapter submits ONE database transaction containing those changes plus
checks that V7 is publishable, removal/addition of the matching reference edges,
consumption of staging protection, and the terminal result for that operation.
S3 is not inside this transaction. If the response is lost, replay resolves the
recorded operation rather than applying the accounting twice. If the operation
never publishes, finite claims and reconciliation make its unused content eligible
for reclamation. Sections 4–10 define the actual state and failure rules.

The caller must not update a managed pointer independently and notify StoreLink
later. Unrelated application metadata does not inherently need StoreLink.

### Application-neutral integration contract

Filesystem and media workloads both inform the design. The library must be
reusable across applications using S3-compatible versioned storage and regional
DynamoDB; it is not an application schema helper with a later generic wrapper.

- Applications choose their tables, keys, fields, object-key policy, authorization
  and domain conditions. StoreLink's reserved records are explicitly separated.
- Application changes are conditional native DynamoDB actions composed into the
  same transaction as declared reference/protection changes. No mandatory ORM or
  invented general database language is required.
- StoreLink identifiers are opaque; no `users/`, `media/`, home ID, filesystem path,
  image/audio type, DAV token or directory ancestry is built into the core.
- Server SDK uploads, constrained browser uploads and derived-object writes use
  the same intent/receipt/publication rules. Bytes need not traverse StoreLink.
- Lifecycle policies are explicit configuration subject to the protocol's safety
  and progress requirements, not inferred from a file or photo schema.
- S3 compatibility requires the stated version, integrity and reclamation
  capabilities. Accepting PUT/GET alone is not conformance. The first concrete
  provider profile is AWS S3; alternate providers need adapter acceptance.

Required consumer-shaped conformance examples:

1. Filesystem-style conditional replacement of a file's content reference, including
   an old-version reader and concurrent deletion/reclamation.
2. Media-application-style eager browser upload followed by a separate draft/photo/capture
   reference publication, plus server-generated derived content and abandoned
   temporary media.

These examples must use different application schemas/reference-field names.
Consumer application refactors are separate projects. Use their workloads to
challenge the abstraction, not to preserve existing internals or make an individual
consumer's adoption readiness a prerequisite. Library development does not itself
include consumer rewrites, production enrollment or repair of historical leaks.
The caller must declare all managed references; StoreLink cannot infer hidden
pointers in arbitrary DynamoDB values. That is a prescribed integration obligation,
not a reason to impose either application's domain model on the library.

## 1. Required guarantees and assumptions

**Safety:** no valid application reference or permitted protection is reclaimed.
**Conditional liveness:** after abandonment, finite outstanding requests settle,
finite temporary protections/retention end, providers recover, clocks become
healthy, and reconciliation runs fairly with sufficient capacity, abandoned
managed payloads and temporary metadata eventually disappear.

Live application data and explicitly retained history/backups are not leaks.
A bounded set of domain/fence configuration records is permitted; an ever-growing
list of historical tombstones is not. No hard wall-time cleanup promise is made
through permanent provider failure, starvation, or legitimately renewed activity.

Initial adapters require regional DynamoDB transactions/strong reads and regional
versioned S3. Application and protection changes share one metadata transaction;
S3 does not participate in it. CAS-only stores do not implement this capability
without a separately proven protocol.

## 2. Ownership, identity and admission

Each `ManagedNamespace` binds a logical store, physical bucket/owner and explicit
exclusive managed key scope. Existing application media or other historical objects
must be enrolled/migrated or kept outside a fresh managed scope before collection.
No out-of-band version adoption or mutation of library metadata is permitted.
Optional Intelligent-Tiering archive configuration is currently not inspected by
backend qualification. Operators must keep restore-required optional tiers disabled
for managed objects; no absence claim is inferred from the omitted inspection.
Versioning, Object Lock and lifecycle expiry/transition/MPU checks remain enforced.

Within a domain, a StoreId permanently identifies one provider/bucket/expected-owner/
region binding. Fresh namespace enrollment must atomically create this bounded
configuration record or assert the identical existing binding alongside namespace
creation. Concurrent conflicting first enrollments must not both commit. The
record is immutable configuration, not an operation receipt or expiring claim;
namespace retirement and operation compaction must not rebind it. A different
physical store requires a new StoreId. Multiple namespaces may share an identical
store binding, but their independently managed physical scopes must not overlap.
Cross-domain/store-alias prefix exclusivity remains an explicit operator/IAM
obligation; a store-binding point check is not a prefix-overlap proof. Read/use
routing must also validate the version record's owning namespace/configuration,
not merely accept any qualified configuration with the same StoreId.

An object reference is `(store_id, key, non_null_version_id)`. Exact-version
reads/deletes are mandatory. Bare-key latest, ETags and URLs are not version identity.
The upper layer chooses same-key/new-version or new-key updates.

A domain has a bounded fence record:

```text
incarnation, current_generation, closed_through, compacted_through, revision
```

Creation IDs for commands, intents, attempts, claims and other temporary resources
include the domain incarnation, creation generation and unique creator identity.
They are never reassigned. A command cannot recreate an old resource by supplying
its old ID under a fresh generation. Application reference slots may be updated,
but transitions require expected slot/application revisions and fresh mutation IDs.

Every state-creating, renewing or effect-producing metadata transaction checks
its admitted command generation in the native transaction. Closing a generation
therefore fences late transactions. Existing live resources may be operated on by
fresh-generation commands; closing their creation generation does not delete them.

Reads and naturally idempotent terminal cleanup need not mint new per-command
receipts just to delete earlier receipts. Their conditional cleanup rules below
provide replay safety without an endless bookkeeping chain.

## 3. Renewable claims and time

Intents, individual attempts, staging protection and consuming-use pins have finite
lifetimes. Merge records where possible, but do not let a renewed intent keep an
abandoned attempt or staging protection alive indefinitely.

Claims contain an immutable ID, ACTIVE/REVOKED/RELEASED state, revision and absolute
deadline. Renewal binds its expected revision and fixed proposed absolute deadline
before submission, within an allowed lifetime horizon. Retry cannot recalculate
`now + duration`. Renewal tasks must stop when the actual caller/handle ends.

DynamoDB does not supply a server-NOW condition. Deadlines are eligibility for a
conditional revocation using documented clock bounds; time passing is not itself
a state transition or an S3 cancellation fence. Reapers use a conservative cutoff
and stop time-based decisions while their clock is untrusted.

Renewal, publication and revocation condition on the same claim/revision and update
protection accounting transactionally. If renewal wins, stale revocation fails;
if revocation wins, renewal cannot revive that claim. An already submitted command
may win before revocation even if its response arrives after the nominal deadline.
Its fixed deadline cannot be extended indefinitely by delayed retries.

Callers stop initiating consuming I/O/publication once they cannot establish valid
protection, and revalidate after suspension. Existing S3 I/O is not magically
cancelled. A handle can fail after claim loss; no uninterrupted read guarantee is
made beyond its protected lifetime. Publication rights are rechecked transactionally.

## 4. Upload and verified receipt

```text
Intent:  OPEN -> SELECTED(version) -> CONSUMED(publication)
         OPEN/SELECTED -> CANCELLED
Attempt: ACTIVE -> SEALED
```

Persist an intent and each attempt before issuing storage I/O or upload instructions.
Bind immutable per-version S3 user metadata to domain, namespace, destination,
intent/attempt creation IDs and declared constraints. Reserved attribution fields
cannot be overridden by caller metadata; presigned instructions bind the required
headers. Mutable S3 tags are not the attribution proof.

A browser-provided version ID is a candidate. A verified receipt comes from a
trusted acknowledged adapter result or verified observation of that exact version.
Verification binds its store/key/version, persisted admitting attempt and declared
length/integrity constraints. Required full-body hashes use applicable validated
checksums or an exact-version read, never an ETag or asserted uploader metadata.

V0 assumes cooperating adapters and exclusive write ownership, not an extra custom
metadata-signing service. If that trust model changes, authenticated attribution
and verification-key lifecycle need an explicit design; a signature would still
not replace payload verification.

Selection atomically checks the OPEN intent and admitting attempt, selects one
version, creates its READY record and finite staging protection, and seals the
attempt. Selection cannot replace its selected version or resurrect a missing
terminal attempt. Retries may produce several S3 versions; this is not S3 PUT
idempotence. Unselected versions are reconciled separately.

## 5. Publication and durable references

Object states are irreversible:

```text
READY -> RETIRING -> DELETED
```

Every application pointer must be covered by an explicitly identified reference
ownership relationship. Aggregate counts equal recorded references/protections
and never underflow. Applications declare reference intent, not raw counter
increments/decrements. The library computes bookkeeping from its authoritative
reference records.

The public API should expose precise slot transitions and/or complete desired sets
for a bounded opaque owner, with the necessary expected identities/revisions. A
complete owned-set helper can avoid caller-computed stale-delta leaks; it cannot
infer whether the application's intended operation was append, replace or merge.
Large/unbounded owner sets and asynchronous owner closure require separate limit
and protocol review, not silent splitting.

Applications must provide domain concurrency conditions and complete declarations
for the retention scope they change. Hidden pointers, mismatched declared sets,
or direct writes to reserved records violate the integration contract.

A native transaction:

1. Checks command admission and absent terminal outcome.
2. Checks original application preconditions/revisions and required authorization.
3. Checks selected intents and staging claims being consumed.
4. Requires newly referenced versions to be READY.
5. Changes application pointers, reference edges and combined version-count deltas.
6. Consumes staging protection and terminalizes relevant intents.
7. Records COMMITTED(result, request_digest).

Replacement removes the old edge and creates the new edge atomically. Unlink
removes its pointer and edge together. Existing-version references require an
existing READY record; no API silently adopts an arbitrary untracked version.
Application references, intentional history and backups persist according to
explicit application policy—not the lifetime of the original uploading client.

Temporary drafts require an expiring draft policy. Durable drafts require a real,
recoverable application reference. Clients must not discard their original content
based solely on an S3 upload acknowledgement if they require durable publication.

## 6. Outcomes and replay

Terminal outcomes are COMMITTED(result,digest) or CANCELLED(digest). The outcome
and its effects commit together. An ID cannot be reused with different inputs.

Only receipt-bearing application commands use this historical-result interface.
Naturally idempotent maintenance actions instead report/check their guarded
postcondition; a missing receipt is not used to infer their historical execution.

While its generation admits execution, a missing response/receipt is unknown, not
rollback. Identical retry or explicit cancellation competes on the same terminal
record. Upload cancellation also terminalizes its intent and releases selected
staging protection atomically; another command ID cannot bypass that terminal state.

Resolve fence and receipt coherently:

- Receipt present: return the recorded historical result.
- Generation open, receipt absent: unresolved; do not speculate about rollback.
- Generation closed and receipt retention still guaranteed: absence can establish
  no committed outcome for that command.
- Generation compacted, receipt absent: **Expired / historical outcome unavailable /
  not replayable**, never KnownAborted or permission to rerun it as a new command.

Applications may inspect current state, but it need not prove a historical result.
A fresh command after expiry is a new authorized decision, not transparent replay.

## 7. Reads, copies and retirement

Opening a use pin atomically checks its declared authority and adds finite
protection to the exact READY version. For published content, the authority is
the application's observed pointer/revision, expressed through native guards.
For selected but unpublished content, it is the matching SELECTED intent and
ACTIVE staging claim: this permits inspection or derived-content generation before
application publication. A bare version tuple or stale upload receipt alone is
not that authority; the backing state must still validate.

The acquired use pin has its own finite lifetime. Consuming/cancelling staging or
replacing/unlinking a published pointer does not remove an already acquired valid
use pin. Those transitions can prevent new acquisitions through the old authority.
Authorization and derivation policy remain application-owned. Renewal/close follow
Section 3. A crashed owner eventually stops renewing; expiry cleanup revokes the
pin and removes its counted protection conditionally. No manual proof of process
death is required for the normal abandoned-pin path.

A required history hold must be established before the last other protection can
disappear. Retention expiry removes its application/history state and protection
consistently. The meaning and duration of history/backup retention remain policy
choices; they are not inferred from S3 current/noncurrent classification.

Retirement changes READY to RETIRING only with zero references, pins and required
holds. Protection acquisition and retirement contend on that same version record.
Once RETIRING, no new reference or pin can attach; it is never reopened.

Delete only the recorded S3 version. Retry an ambiguous deletion against that same
identity. Mark DELETED only on confirmed deletion/absence; AccessDenied, missing
bucket and transport failure are not confirmed absence. Delayed deletion remains
harmless to any different version identity.

Remote asynchronous consumption must have a defined lifetime before it can safely
use a finite source pin. V0 need not expose an operation whose source-use lifetime
cannot be bounded or resolved. Logical file copies can retain an existing version
without server-side byte copying.

## 8. Orphan discovery, including late writes

Recurring complete version enumeration/inventory is mandatory; notifications are
only accelerators. Revisit earlier key ranges because late writes can appear after
an earlier pass. Authoritative metadata absence requires a strong read or native
transaction condition, never a GSI miss.

Before creating retirement metadata for an untracked inventory candidate, make a
fresh exact-version observation. Confirmed absence with no managed record is a
no-op, not a reason to manufacture a new tombstone/receipt for a stale event.

For each exact version inside enrolled managed scope:

- READY: preserve its live/reference/retention accounting.
- RETIRING: continue deletion.
- No version record: transactionally establish that selection is impossible and
  install RETIRING, serializing against receipt registration.

Selection is impossible when the matching attempt is SEALED, its intent is terminal,
or the required record is absent and its creation generation is permanently closed.
An existing live old-generation intent/attempt is not an orphan merely because its
creation epoch closed. Expired claims must first be conditionally revoked/sealed.

After original intent/attempt metadata is compacted, immutable attribution and the
closed-generation fence still prevent a late version from being published. New
commands cannot recreate those old identities. Repeated inventory finds and retires
late PUT/completion results without retaining one intent tombstone forever.

Invalid/unattributable versions are not eligible for normal registration. Within
explicitly exclusive enrolled scope, apply a finite diagnostic grace policy, alert,
then transactionally verify absence of managed protection and register retirement.
Do not silently quarantine forever. Outside enrolled scope, never collect such data.
Metadata corruption or bypassed caller invariants are incidents requiring recovery,
not evidence authorizing speculative deletion of otherwise protected versions.

## 9. Multipart uploads: lifecycle-owned reclamation profile

CreateMultipartUpload can succeed while its response is lost. Persist a known
`(store,key,UploadId)` association when available. Retries may create another MPU.
ListMultipartUploads does not establish custom intent attribution or request drain.
Several independent uploads can share a key.

### Do not remove the discovery path with an early abort

In the proposed v0 profile, StoreLink does NOT issue AbortMultipartUpload on error,
expiry or cancellation. SDK/transfer-manager auto-abort paths must also be disabled
or avoided. Cancellation seals publication eligibility and stops further authorized
part/completion admission; it does not promise cancellation of already issued I/O.
Unfinished uploads remain in progress for service-owned lifecycle reclamation.

This avoids the problematic sequence 'client abort -> empty parts inspection ->
forget UploadId -> a previously in-flight part finishes'. An empty inspection or
client timeout is not a safe-compaction proof. An out-of-band early abort violates
this profile and cannot silently inherit its reclamation argument. A separately
proven fast-abort capability could be added later; it is not assumed in v0.

### Mandatory backend obligation

A matching AbortIncompleteMultipartUpload lifecycle rule is REQUIRED, not an
optional alternative to client enumeration. Enrollment verifies its coverage and
parameters; monitoring detects loss of coverage. Preserve it throughout the
managed bucket/prefix lifetime, including after local UploadId records compact.
An empty current MPU listing is not permission to remove that rule. Do not repurpose
managed scope in a way that withdraws cleanup for delayed creates.

Required provider-level property: with the rule continuously maintained and S3
making progress, every covered MPU that does not successfully complete eventually
terminates and all associated part storage is reclaimed, including parts arising
from finitely many requests already in flight when lifecycle acts.

AWS documents that lifecycle stops eligible incomplete uploads and deletes their
associated parts, for both existing and future MPUs. The explicit in-flight-request
quantification above is a provider assumption interpreting that advertised outcome;
the public lifecycle documentation does not detail its internal race handling or a
completion-time bound. AWS clarification would strengthen that basis. Tests can
verify configuration, adapter compliance and observed convergence, not universal
provider behavior. If a backend cannot meet this obligation, this profile cannot
claim multipart leak freedom on it.

### Completion, compaction and age

Each MPU has a finite maximum supported age, independent of renewable intent life.
Before that age, complete it or stop using it and start a fresh attempt. Lifecycle
eligibility exceeds the supported age by explicit clock/scheduling/granularity
margins. Abandonment can retain billable parts until lifecycle acts; that is an
explicit cost/latency trade-off, not prompt physical deletion.

A real successful CompleteMultipartUpload result transfers cleanup to completed
version reconciliation. HTTP 200 alone is insufficient because completion errors
can be embedded in that response. Lost completion results remain unknown, and late
completed versions are discovered and retired under Section 8.

UploadId associations can compact after metadata fencing and other obligations
are satisfied when unfinished-part reclamation is wholly delegated to the verified
lifecycle profile. No client proof of physical absence is claimed. Unknown or delayed
Create results are likewise covered from their actual S3 initiation time, even if
StoreLink never learns their UploadIds. Listings remain useful for observation and
acceptance, not an enumerator-only substitute for the provider's obligation.

Sources:
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html

## 10. Bounded metadata compaction

Close command generations irreversibly before pruning records that prevent replay.
Advance `compacted_through` BEFORE deleting receipts, so a miss cannot be interpreted
under an obsolete replay promise. Keep a documented minimum resolution window.

One bounded scheduling option retains a candidate closed boundary and a conservative
not-before time in the fence record. Hold that boundary fixed for the resolution
window, publish its compaction watermark, then select a later boundary. This may
retain records longer than the minimum but does not keep a row per past epoch.
Clock assumptions affect the advertised retention window; fencing, not timing,
prevents old effects from executing again.

Terminal claims/attempts/intents compact after their creation generation closes
and remaining cleanup is complete OR reconstructible from attribution, ownership,
closed-generation evidence and authoritative inventories. Active records remain
until their legitimate lifetime ends. Every renewal/release requires the existing
matching incarnation; it cannot upsert a missing old record.

DELETED version tombstones compact after physical absence is confirmed and future
registration is impossible. Existing-version publication still requires READY;
selection still requires an existing admitting attempt/intent. A stale inventory
may cause another bounded retirement check, never recreate READY.

Cleanup must be naturally idempotent: for example, delete an expired pin and
decrement its count in one conditional transaction; a retry finding the pin absent
must not decrement again. Do not create an endless chain of durable cleanup receipts.

## 11. Required maintenance and operational limits

Reconciliation runs without user traffic and provides progress/backlog/oldest-age
signals. Exhausting a retry budget defers/requeues work; it is not successful cleanup.
Required work includes claim revocation, orphan versions, MPU lifecycle coverage
and observation, retirement and metadata compaction. Fair discovery and capacity sufficient for the admitted load
are part of deployment acceptance.

V0 policies must define finite upload/attempt/use lifetimes, maximum MPU age,
resolution retention, generation rotation, inventory cadence and orphan grace.
Legitimate history/backup retention is separate. Blanket age-based noncurrent
version expiry is unsafe: noncurrent versions may still be live. Lifecycle rules
must respect these protections and the application's explicit retention policy.

Metadata restore/rollback requires maintenance fencing, invariant validation and a
fresh non-reused domain incarnation before resuming callers/GC. Restoring an old
fence must not re-authorize delayed historical requests.

Native limits apply to complete plans: 100 distinct transaction items, 4 MiB total,
400 KiB per item. Merge same-item deltas; reject unsupported plans without partial
publication. No advertised atomic operation is silently split.

## 12. Required conformance evidence

Model-check or property-test safety AND convergence after failures stop. Cover:

- Claim expiry versus renewal, reference publication, reader open and unlink.
- Staged-content use acquisition racing staging consumption/cancellation, without
  inventing a published application pointer or allowing a stale receipt to grant use.
- Stale renewal replay with fixed deadlines and a paused/resumed caller.
- Late command after receipt/intent/version tombstone compaction.
- Wrapping an old resource identity in a fresh command generation.
- Active old-generation intent continuing through fresh commands.
- PUT/Complete after cancellation, after a sweep, and after intent GC.
- Lost CreateMultipartUpload response; multiple MPUs sharing one key; delayed parts
  and completion around service lifecycle action.
- Cancellation/error paths never issue client abort, including hidden SDK cleanup;
  out-of-band abort is detected/reported as outside the supported profile.
- Lifecycle coverage survives metadata compaction and late creates; completed
  versions are still collected independently.
- Missing notifications, stale inventory entries and repeated full sweeps.
- Committed publication with lost acknowledgement versus genuinely abandoned work.
- No clients returning: maintenance reaches the expected live-state baseline.
- No endless accumulation caused by cleanup's own bookkeeping.
- Unenrolled historical data remains untouched.

Real DynamoDB/S3 acceptance supplements the model; mocks are not service proof.
Before implementation, settle policy durations, caller/handle behavior on claim
loss, native transaction-participation API, enrollment and restoration procedures.
StoreLink remains a small set of prescribed coordination primitives, not a general
saga engine or filesystem implementation.
