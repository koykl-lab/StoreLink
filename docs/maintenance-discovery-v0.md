# Bounded maintenance discovery and traversal progress

Status: design and implemented Scan/progress primitives independently reviewed.
Native metadata discovery/checkpoint and S3 cursor-restart conformance passed on
owned disposable fixtures. A continuously scheduled host and traffic-free
reclamation acceptance remain separate requirements; point APIs and progress
records alone do not establish continuous reclamation.

## Native discovery, not an index of callbacks

Coordination records use distributed partition keys under the private hashed
domain prefix, not one shared partition suitable for Query. Add one native Scan
page at a time on the enrolled canonical coordination ARN, with domain-prefix
filter, ConsistentRead, an explicit evaluated-item limit (1..=128), and the native
ExclusiveStartKey. The service's 1-MiB Scan page is not an atomic snapshot. A
zero-result filtered page with a continuation is NOT completion or emptiness.
Never use traffic, notifications, one earlier empty pass, or TTL as discovery.

The opaque serializable cursor binds canonical table ARN and domain. It contains
only the native pk/sk continuation, with checked String key types/lengths. The
native last evaluated key can belong to ANOTHER domain because filtering follows
evaluation; do not incorrectly require its pk to match this domain prefix. It
cannot authorize mutation and cannot be used against a different journal/table.
Do not invent ordering over DynamoDB Scan keys or require a marker to name a
returned item. A native read error does not advance the cursor.

Return bounded typed hints for receipts, intents, attempts, staged handles, use
pins, versions and namespaces, plus explicit permanent-row/unsupported/corrupt
observations. Hints are locators only, not eligibility or mutation authority.
Reuse existing checked identity/record decoders and canonical key generation;
never infer absence/terminality from malformed rows. Do not return receipt result
payloads or print full native rows in errors. Unknown/corrupt items must not make
an otherwise valid page's continuation disappear: expose per-item issues so the
host can alert while still visiting other records and subsequent pages. Validate
cursor/response structure failures as whole-page errors rather than skipping to
an invented position. Do not implement a DynamoDB expression interpreter.

## Bounded durable traversal progress

One permanent progress row per domain stores metadata traversal state; at most
one further row per enrolled namespace stores S3 version-inventory traversal.
These are configuration-sized progress records, not per-operation/per-version
work receipts, lease pins, or an indefinitely growing retry queue. Keep each row
within the existing compact-item bound. Store only namespace ID/configuration
revision plus native key/version markers for S3 progress; do not duplicate full
bucket/prefix configuration into every cursor row.

Progress is scheduling data, NEVER deletion authority. CAS the row's fresh
revision after attempting a bounded page; keep a cursor unchanged when the page
was not completely considered (deadline/crash). Per-item failures are reported
and retried on later complete sweeps, not silently declared clean. On a terminal
page persist reset-to-start and a completed-pass marker. Continue round-robin
passes even without user traffic. Crash/redundant workers can repeat a page;
point operations must reread/recheck their existing native guards. Bound EACH
candidate attempt, including native timeout/unknown outcomes, and size the page
so every candidate can be considered plus a reserved progress-write budget. An
item timeout is an explicit failed consideration, not permission to repeatedly
abort before visiting the rest. Crash/deadline tests must demonstrate that a slow
first candidate cannot starve later candidates across ticks. No leader lease
or per-item ownership protocol is needed for correctness. Failed progress CAS
requires re-observation and must not turn into a busy retry loop.

A restore or configuration change requires an explicitly validated progress
reset, not conversion of stale progress into object authority. Namespace/store
configuration remains intact while versions may need discovery. Do not change
or recreate the coordination control schema merely to save a cursor.

## Host execution and acceptance (still mandatory)

The service runs periodic bounded metadata and namespace-inventory work with an
explicit item/concurrency/time budget and timer independent of requests. Each
hint goes to its corresponding coherent point API: expire active claims/abandoned
operations first, prune terminal metadata only behind published frontiers; retire
READY/0/0, finish exact RETIRING deletion, observe untracked versions and prepare
guarded orphan retirement. Existing tracked versions are never overwritten.
Lifecycle—not client AbortMultipartUpload—continues to own MPU reclamation.
Delete-marker handling must use an explicit owning-namespace contract, not a
latest-key deletion fallback or blanket lifecycle expiry.

Maintain published compaction through the reviewed fixed-window control API;
never advance it merely because one scan was empty. Application-owned logical
records, access edges and zero-link nodes need their separate typed application
traversal/reapers, not guessed treatment by StoreLink. Respect the same work and
fairness rules there.

Expose completed-pass age, work/error/defer counts, oldest observed eligible work
and progress-CAS contention without high-cardinality object keys/user payloads.
Document provisioning/capacity assumptions: sustained maintenance capacity must
cover whole-table evaluated Scan work (including other domains and filtered-out
rows), not just returned hints or creation of reclaimable work. Failures in one
candidate do not hide or starve independent candidates. Persisted traversal progress plus recurring timers
and bounded point operations must be demonstrated in a traffic-free real-service
acceptance, including restart/duplicate workers/partial failures and late versions.
A successful point test or this document alone is not that proof.
