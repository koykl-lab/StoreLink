# Bounded generation and receipt-retention control

This refines protocol section 10's single-candidate scheduling option. It is a
metadata primitive, not a complete maintenance worker, orphan inventory, or
application workflow engine. No production durations are implied.

## One bounded control record

Retain the existing domain/incarnation/current/closed/compacted/revision fields.
Add an optional positive `resolution_retention_ms` configuration and an optional
pair `compaction_candidate` / `compaction_not_before_ms`. No row per generation.
All values are checked scalars; preserve the compact-record bound and reject
malformed/partial candidate fields. A candidate satisfies
`compacted_through < candidate <= closed_through`; its presence requires a
configured retention period. The period is immutable within an incarnation.

The host supplies explicit retention and trusted-clock uncertainty policy. A
missing policy does not select a default or permit compaction. Bootstrap installs
it before enabling maintenance. Changing it is not an ordinary per-call override;
no policy migration or incarnation-rotation API is introduced by this unit.

## Native operations

Each operation strongly reads the canonical control record and uses a full old-row
conditional replacement with an incremented checked revision. Missing records are
not recreated. Native failures remain conflicts or unknown outcomes; callers
reread the current postcondition, never infer historical rollback. No maintenance
command receipts or resource IDs are minted.

1. **Configure retention:** set the explicit period only when absent; identical
   existing configuration is an observed no-op; a different period is rejected.
   Do not alter generation or candidate state.
2. **Advance and close:** increment current generation and close through the old
   current generation in the same conditional write. Preserve a pending candidate
   and its fixed deadline. Existing live resources remain usable under the normal
   existing-resource rules; old effect-creating commands become fenced.
3. **Arm a compaction window:** require installed matching policy, no pending
   candidate, and a closed boundary beyond the compacted watermark. Only AFTER
   the strong read has observed that boundary already closed, sample fresh trusted
   time and set `candidate = observed closed_through`,
   `not_before = clock.upper_bound + retention`. Keep the boundary fixed. A
   concurrent control change invalidates the full-row condition.
4. **Publish the candidate:** reread and validate the pending pair; sample fresh
   trusted time. Only when `clock.lower_bound >= not_before`, advance
   `compacted_through` to that exact candidate and clear the pair. Preserve the
   configured period. Subsequent arming may select a later closed boundary.

The ordering in step 3 is essential: a timestamp chosen while preparing the
closure could precede its actual delayed commit and shorten retention. An upper
bound sampled after observing closure cannot precede closure; delayed persistence
of that timestamp is therefore safe. Publishing uses a conservative lower bound.
Untrusted clocks stop time-dependent operations, not reinterpret them as due.

No eligible boundary, an existing candidate, or a not-yet-due candidate are current
observations, not historical execution results. Closing may continue while a
candidate waits. All progress remains caller-scheduled without traffic-dependent
renewal, timers, or a leader-lease subsystem in this primitive.

## Deletion and recovery boundary

Publishing the watermark precedes any receipt deletion. This unit does not delete
receipts or terminal resources yet. Their later deletion must validate the
appropriate fence/identity and terminal/cleanup predicates; active resources are
never deleted merely because their creation generation is compacted. A present
receipt still returns its recorded outcome. An absent compacted receipt is
Expired, never NotCommitted or permission to replay under a new identity.

Authorized metadata restoration still requires maintenance fencing, invariant
validation and a fresh incarnation. It does not promise retention of old-incarnation
history. Durable exact-version provenance is not current command authority; do not
rewrite it merely to renew or clean up independent current-incarnation pins.

## Verification

Use native SDK request/observation fixtures and real DynamoDB transactions, not an
expression evaluator. Cover configure idempotence/mismatch, missing/corrupt rows,
revision and generation overflow, monotone closure, unchanged pending boundaries,
post-read clock sampling, lower/upper-bound edges, untrusted clocks, stale control
conditions, and lost replies resolved through current control/outcome observation.
Test that old commands cannot execute after closure and that absent receipts become
Expired only after the published watermark, with existing receipt precedence.
