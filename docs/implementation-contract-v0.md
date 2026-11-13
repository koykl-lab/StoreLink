# V0 implementation contract

This refines the approved direction into implementation boundaries. V0 is under
development; this document is not a claim of complete protocol or deployment
acceptance. The safety/liveness obligations in `protocol-v0.md` remain binding.

## Package and execution ownership

One Rust crate, `storelink`, exposes protocol identity/lifetime types and
qualified regional-DynamoDB/versioned-S3 components. Native SDK clients are supplied
by the host application. The `aws` feature is enabled by default; disabling it is
for primitive/model testing, not an alternative production coordination backend.

The application supplies native DynamoDB actions; StoreLink alone submits the
complete coordinated transaction. Coordination records use a dedicated table with
string `pk` and `sk`; application tables retain their own schemas. Enrollment
obtains canonical table identities and verifies the shared account/Region scope.
Application actions cannot target the coordination table through any enrolled alias.
The SDK/service validates native expressions and final item limits. Never split a
commit to evade the 100-action/4-MiB/400-KiB native bounds.

## Immutable commands and replay

V0 freezes a complete native submission before sending it. Its command ID,
request digest, native ClientRequestToken, result bytes, fixed deadlines and
conditional inputs cannot change on retry. There is no arbitrary retry callback.

The original application operation may have multiple coordinated commands, but a
changed application/reference plan requires a fresh command identity only after
prior non-commit is established or cancellation wins. A transport timeout alone
never permits a fresh business attempt. Expired historical outcomes do not prove
non-commit. Native transaction idempotency supplements, not replaces, the outcome
record and generation guard.

Preparing commands may read state and produce a stale plan; that is not publication.
Execution reports distinct application-condition, library-condition, native
validation and unresolved-outcome classes. Conditional failure from one attempt
must not be confused with proof that an earlier ambiguous attempt never committed.
The library resolves its outcome coherently before exposing a final classification.

Preparation and execution expose inspection/budget information, not a mutable
caller-submitted request. Result receipts are historical evidence, not automatically
current read/write capabilities. Capability use revalidates its backing state.

## Identities and time

- `DomainId`, `StoreId` and `NamespaceId` are opaque library labels, not paths or
  user identities. Each is nonempty, at most 128 UTF-8 bytes.
- `Scope` binds a domain, non-nil incarnation UUID and positive creation generation.
  `ResourceId` and `CommandId` are distinct types, each with a scope and non-nil UUID.
  A new temporary resource uses its creating command's scope and UUID. V0 uses
  separate creating commands for intents, attempts and claims. Creation checks
  both generation and creator UUID, preventing another command in the same open
  generation from recreating an already cleaned-up temporary resource.
- `VersionRef` binds store, exact UTF-8 key and non-null version ID. Keys are 1–1024
  UTF-8 bytes; v0 version IDs are nonempty, not the literal `null`, and at most
  1024 UTF-8 bytes. Never decode/normalize application keys or substitute an ETag.
- A SHA-256 address fingerprint is an index, not permission to treat unequal tuples
  as equal: stored full identities must match or operations fail closed.
- Generation controls satisfy `compacted_through <= closed_through < current`.
  Creation under an old scope cannot be smuggled through a fresh command. Operating
  on an existing old-generation resource is different from recreating one.
- Time is explicit UTC milliseconds with checked arithmetic. A trusted clock
  observation includes an uncertainty bound. Acquisition uses the conservative
  upper bound; expiry eligibility uses the lower bound. Untrusted observations
  cannot authorize time-based use or revocation.
- Renewal fixes expected revision, proposed absolute deadline and new revision
  before execution; it never computes a new deadline during replay. Revocation
  and release are terminal. Cleanup changes counts only on the guarded transition.

Lifetime and scheduling policy is explicit configuration. Validate positivity,
renewal margins, clock bounds and MPU supported-age versus lifecycle eligibility.
Do not silently turn test durations into production deployment policy.

## Reference and transfer interfaces

The first core reference operation is an opaque slot transition with an expected
prior exact version (or expected absence), and a desired exact version (or removal).
The library loads/conditions its own records and computes consolidated per-version
count deltas. The app must describe all changed retention scopes and supply matching
concurrency conditions. No public numeric counter-adjustment API exists.

Bounded desired-reference-set replacement can build on the same primitives. It
must condition its authoritative set revision and account for its entire transaction
budget. Large-set closure is not implicitly supported or silently split.

Upload intents and attempts persist before S3 I/O. Selected versions, staging
claims and their verification evidence are immutable bindings, not caller-asserted
version tuples. Server streams and constrained presigned uploads share attribution
and finalization. No error/Drop path may invoke early multipart abort under the
accepted lifecycle-owned profile.

Use acquisition accepts either native guards on a published application pointer
or the matching selected intent/active staging authority for the exact READY
version. It creates an independently finite use pin. Parent staging cancellation
or consumption does not retroactively remove an already acquired valid pin.
Application authorization remains outside StoreLink.

## Validation boundaries

Primitive tests cover constructor/deserialization invariants, clock arithmetic,
fixed renewal deadlines, terminal transitions and creation/admission fences.
Protocol model tests must separately exercise safety and eventual convergence.
Actual DynamoDB Local tests exercise the native SDK transaction boundary and
failure cases; controlled S3 transport tests exercise real SDK requests and bodies.
AWS acceptance, native client compatibility, continuous reconciliation, multi-replica
filesystem/lock behavior and publication provenance are separate delivery gates.
