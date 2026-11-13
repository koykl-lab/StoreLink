# Bounded late-version and orphan retirement

This defines the missing protocol section 8 bridge from S3 inventory to tracked
RETIRING records. It is not a second upload/READY registration path. Discovery
never authorizes deletion by itself, and this unit does not claim a complete fair
maintenance scheduler.

## One bounded discovery page

Read exact S3 versions under the enrolled namespace's exclusive prefix, using its
qualified captured client and expected owner. No latest-key fallback. Return a
bounded page plus native continuation markers; missing/ambiguous pagination is an
error, never completion. Delete markers may be reported separately for later
namespace cleanup, but are not content-version candidates or automatically deleted
by this content-retirement path. Namespace/store configuration and lifecycle remain
in place after operation records compact.

## Exact observation and admission

Before preparing an orphan transition, HEAD the exact non-null VersionId under
that same namespace. Validate returned version identity, object length, timestamp
and storage binding. Produce an opaque, non-deserializable observation; do not
accept raw caller assertions that an object is an orphan. Errors, missing bucket,
denial and ambiguous absence never produce such evidence.

For valid schema-1 upload attribution, decode canonical domain/incarnation,
namespace, intent/attempt creation IDs and target fingerprint. Require the full
expected namespace and destination fingerprint, not just a matching prefix or
metadata label. Metadata is immutable per version under the cooperating-adapter
and exclusive-writer contract; no custom signing service is added.

A coherently read existing version record wins:
- READY: preserve it; its reference/claim accounting governs retirement.
- RETIRING/DELETED: use the existing terminal path/current observation, never reopen.

If there is no version record, establish selection impossibility with native
transaction guards. Sufficient evidence is one of:
- The matching admitting attempt exists and is SEALED; or
- The matching intent exists and is terminal (CONSUMED/CANCELLED); or
- The admitting attempt is absent and its creation generation is closed in the
  same valid incarnation, or that incarnation is irreversibly obsolete.

An ACTIVE attempt or OPEN intent is not expired merely because a scan says so.
First use the guarded expiry/sealing operations; then re-observe. Missing records
in an open generation are not proof of abandonment. SELECTED intents alone are
not a shortcut: preserve their selected content and use sealed-attempt or terminal
proof for other versions.

Unattributed/malformed-attribution objects may be considered only under the fresh,
exclusively enrolled managed scope, after an explicit configured grace interval
based on immutable provider LastModified and a trusted conservative clock lower
bound. They cannot be selected through the validated-attribution publication API.
A different valid attribution domain/namespace is a scope conflict requiring
operator investigation, not permission to delete another owner's data. Do not
reinterpret unknown future attribution schema versions as malformed garbage.

## Conditional RETIRING registration

Use a fresh admitted StoreLink command to atomically create an absent-only version
record directly in RETIRING with zero references/claims. Include immutable current
namespace/configuration and creation provenance. Guard the exact observed control,
namespace, and selection-impossibility evidence/absence in that same transaction,
plus the ordinary command admission/outcome. An overlapping selection either wins
and creates READY, or loses because its admitting state cannot validate. No effect
can replace an existing version record.

The opaque observation is evidence about immutable bytes, not perpetual metadata
authority. Native guards and current trusted time are revalidated at preparation;
retirement must never be prepared from stale proof alone. A lost outcome uses the
same frozen command and journal resolution; a fresh attempt requires re-observation.
The subsequent exact-version deletion and DELETED transition use the already
reviewed tracked-retirement adapter. No direct raw S3 deletion or MPU abort is
added here.

The optional Intelligent-Tiering configuration inspection remains explicitly
omitted as requested; this proposal does not silently restore it or add a storage
class restriction. Existing profile/operator assumptions still apply.

## Acceptance

Test native request shapes and actual DynamoDB races against selection, sealing,
terminal intent cancellation, generation closure, and tombstone pruning. Include
late completion after an earlier empty inventory, mismatched attribution, missing
records in open generations, unknown future schemas, grace/clock boundaries and
provider errors. Show that only RETIRING can be created, never READY, and that
repeated stale inventory cannot bypass exact-version identity or live accounting.
Fair repeated inventory, capacity/backlog observability and traffic-independent
scheduling remain separate required deployment work.
