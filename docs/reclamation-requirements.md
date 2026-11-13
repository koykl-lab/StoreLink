# Eventual reclamation — required revision criteria

This requirement supersedes the previous proposal to retain operational pins,
intents or terminal outcomes indefinitely. It is a requirement/proof checklist,
not a completed replacement protocol.

## Safety and progress are both required

**Safety:** no version protected by a valid application reference, permitted use,
or explicit history/backup retention is reclaimed by the protocol.

**Conditional liveness:** after an operation is abandoned, all finite outstanding
external requests have settled, its temporary claims and explicit retention have
ended, services are available and reconciliation is fairly scheduled, its
unreferenced managed payloads AND temporary metadata eventually disappear.

The remaining baseline may contain live application/reference records and a
bounded set of store/domain fencing records. One permanent tombstone per past
operation/version is not an acceptable substitute for compaction.

No absolute wall-clock guarantee is claimed while a provider is permanently
unavailable, a caller legitimately renews a live claim, or cleanup cannot run.
The operational design must provide reconciliation without relying on future user
traffic and must expose stalled cleanup rather than silently abandoning work.

## Every object needs a lifetime rule

Account for:

- Upload intents and staging claims.
- Completed but unpublished S3 versions, including duplicate retries.
- Unknown/lost CreateMultipartUpload results, incomplete parts and uploads.
- Use/read/copy pins after caller failure.
- Cancelled or indeterminate metadata commands.
- Committed/cancelled operation outcomes after their replay window.
- RETIRING/DELETED version records and deletion tasks.
- Consumer-owned recursive-operation parents, member intents and result indexes.
- Retired namespace/lock bookkeeping that no live resource requires.

A live, enumerable application reference is intentional retention, not garbage.
The application must remove obsolete references through the prescribed atomic
boundary; the library cannot infer hidden application reachability.

## Required mechanisms to evaluate

1. Finite, renewable claims. Renewal requests bind an absolute deadline; replaying
   an old request must not extend protection relative to the new retry time.
   Renewer lifetime must be tied to the actual caller/handle, not an accidentally
   self-retaining background task.
2. Conditional revocation. A timestamp makes cleanup eligible; retirement/revocation
   races with renewal/publication through native transaction conditions. A stale
   caller cannot regain publication authority after revocation.
3. Late-I/O handling. Cancelling metadata does not stop issued S3 requests. Late
   versions remain attributable and unpublishable, and later sweeps reclaim them.
4. Non-reusable identities and an irreversible generation/fence scheme. Old
   commands cannot become fresh commands after their result records are pruned.
   A pruned result returns Expired/not-replayable, not KnownAborted.
5. Compact closed-generation evidence. Keep bounded watermarks rather than an
   unbounded list of terminal IDs. Live long-running intents may remain protected
   across command-generation changes, but must never be recreated after retirement.
6. Authoritative inventory backstops. Notifications are accelerators, not the only
   discovery mechanism. Enumeration must include versions, not just latest keys.
7. MPU lifetime bounds and provider-owned reclamation. V0's proposed profile avoids
   client/SDK auto-abort and requires continuously maintained managed-prefix S3
   lifecycle cleanup. Supported upload age must fit below lifecycle eligibility.
   State the provider obligation for in-flight parts explicitly; do not equate an
   empty parts listing with request drain or substitute enumerator-only cleanup.
8. Cleanup operations must not generate an endless chain of new per-operation
   records merely to delete earlier cleanup records. Idempotent conditional terminal
   deletion can have a different execution contract from user publication.

## Namespace ownership is a prerequisite

Automatic collection is confined to an explicitly enrolled, exclusively managed
namespace. Never enable it over existing application media or other historical data
without enrollment/migration or a fresh managed prefix.

Attribution uses immutable per-version metadata constrained by the upload path,
not mutable tags alone. A candidate without valid attribution cannot acquire a
managed publication receipt. The policy for malformed/unattributable objects in an
exclusively managed namespace must be explicit and bounded; silent permanent
quarantine cannot be called leak-free. Unmanaged data outside that namespace is
not eligible for collection.

Restoring/rolling back metadata requires a maintenance/fencing procedure and
validation of live references before GC resumes. The normal crash model does not
silently include arbitrary administrative corruption or resurrected old epochs.

## Required adversarial tests

For every external-call and transaction boundary, inject a crash, lost response,
delay or duplicate, then restore service and advance time/work scheduling.

Verify both no premature deletion and eventual return to the expected live-state
baseline. Include:

- PUT/MPU completion after intent cancellation, after a sweep, and after intent
  metadata compaction.
- Abort -> empty inspection -> UploadId compaction -> late part is a negative
  control: the v0 adapter must not perform that early client-abort path. Model the
  stated lifecycle-owned provider obligation separately, and verify late/unknown
  creation, parts and completion without claiming a client timeout proves drain.
- A stale begin/renew/publish request delivered after its receipt/tombstone is gone.
- Reader failure, renewal racing revocation, and use after loss of protection.
- Publication racing retirement and cancellation racing a previously sent commit.
- Deletion succeeding with response loss, including retries after metadata pruning.
- A selected upload whose application publish never happens.
- A committed publication whose client never learns the outcome.
- Missing/duplicated notifications, repeated inventories and cleanup retry exhaustion
  followed by successful later work.
- Quiescence with no API traffic: no continuing accumulation of cleanup metadata.

Before approving an implementation, the revised protocol must explain why each
case terminates safely and what finite-lifetime/provider/scheduler assumptions it
uses. These tests do not justify weakening the application-reference invariant.
