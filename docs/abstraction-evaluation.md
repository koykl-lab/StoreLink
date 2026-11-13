# Abstraction boundaries

StoreLink shares **versioned-object lifetime coordination composed with native
application transactions**. It is not a filesystem, application schema, backup
format or general workflow engine.

The design is informed by two different workloads: a filesystem with conditional
file updates and a media application with eager uploads, derived content and
retained history. Supporting both does not mean sharing their domain models or
requiring either application's existing implementation to remain unchanged.

## What belongs where

| StoreLink | Filesystem consumer | Media consumer |
| --- | --- | --- |
| Upload admission, exact-version verification, abandonment cleanup | HTTP write handling and file update policy | Browser upload endpoints and derivation policy |
| Native transaction composition, library guards and outcome recovery | Tree/lock conditions and filesystem operation results | Inventory/capture conditions and business outcomes |
| Reference accounting and finite-use protection | File/history ownership and read-handle semantics | Original/derived/history/draft ownership and delivery policy |
| Inventory, retirement, protocol metadata compaction | Lock and recursive-operation maintenance | Application history/draft expiry and workflow maintenance |

## Native application fragments, library-owned execution

Applications supply native conditional DynamoDB actions. StoreLink composes those
with its guards, accounting and outcome record, then owns submission and recovery.
Returning a mutable plan for every caller to execute would duplicate the most
error-prone part of the protocol in every consumer.

Application tables retain arbitrary key/field schemas. Coordination uses a separate
table in the same account/Region transaction domain. Enrollment verifies canonical
table identities; a lookup by cross-account ARN does not establish a bare-name
alias. Verified aliases are qualified to their canonical ARNs before freezing a
command, so later credential refresh cannot silently retarget a bare name.

An application combines its own same-item conditions and updates. StoreLink does
not merge arbitrary database expressions, infer hidden references from arbitrary
items, or implement another database language. Native DynamoDB remains authoritative
for expression validity and final item sizes that an opaque Update cannot reveal.

## Reference intent, not numeric adjustments

The application describes a reference slot transition or complete desired set for
a bounded retention owner. StoreLink computes accounting from its authoritative
records. It must not ask callers to maintain numerical reference counts themselves.

Consider two callers that both read a media array `[old]`, then independently write
`[old,b]` and `[old,c]`. Blindly adding reference protections for `b` and `c` permits
a final array `[old,c]` with an unmatched protection for `b`. Atomic transaction
composition alone cannot correct an incomplete declaration.

An expected-media condition can reject the stale overwrite atomically. A complete
owned-set replacement can additionally derive accounting from the actual previous
set, rather than a stale caller-derived delta. Neither mechanism can infer that the
application intended an append instead of a replacement. Business concurrency and
reference completeness remain explicit consumer obligations.

Large histories must not become one unbounded item. Native transaction/item limits
apply to the complete plan. Bulk closure or other staged primitives need their own
reviewed protocol; an advertised atomic operation must not be silently split.

## Three distinct receipts

1. **Verified upload evidence** identifies exact stored bytes.
2. **StoreLink command outcome** witnesses one coordinated metadata effect.
3. **Application outcome** describes a business operation, which may span several
   coordinated effects and have a different retention/replay contract.

Application outcomes can coexist in the same native write set without being
absorbed into StoreLink's schema. A historical receipt is not automatically a
currently valid read/write capability.

## Transfers and use lifetimes

Server streams, constrained browser uploads and derived content share admission,
attribution and finalization. StoreLink need not proxy all bytes. Native SDKs own
wire/authentication mechanics; adapters must preserve the protocol's request and
multipart-cleanup constraints.

Selected but unpublished media may need inspection or derivation. A use pin can
be acquired from its matching selected intent and active staging authority, rather
than inventing an application pointer. The acquired pin has its own finite lifetime;
consuming staging does not retroactively remove a valid existing pin.

Signed URLs are delivery capabilities, not object identities or lifetime proofs.
Version selection must reach signatures and cache keys. URL expiry does not prove
that already-started reads have drained; remote consumption needs an explicit
supported lifetime contract.

## Garbage collection is not backup

A noncurrent S3 version may still be referenced and must not expire just because
it is old. Reference-aware reclamation is separate from an independent, restorable
application backup. Retaining coupled live metadata and blobs does not by itself
prove recovery from corruption of that state.

## Evidence boundaries

Primitive/property tests, native SDK request tests, DynamoDB Local transactions,
real provider acceptance and consumer-specific tests establish different things.
None alone proves the whole protocol. In particular, a coordination unit does not
prove application reference completeness, and a lifecycle configuration test cannot
prove every internal provider race. See [the protocol](protocol-v0.md) and
[reclamation requirements](reclamation-requirements.md) for the full obligations.
