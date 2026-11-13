# Conditional terminal metadata pruning

This implements the deletion side of protocol section 10 after the bounded
compaction watermark is published. It is not a scanner, workflow engine, or
physical orphan collector. No production timing defaults or new cleanup receipts.

## Eligibility and observation

Each operation reads control and its exact candidate item in ONE native
TransactGetItems. Validate canonical control, domain/table identity and the
candidate's existing checked codec. Within the current incarnation, every relevant
creation generation must be at or below `compacted_through`. A valid different
incarnation is historically fenced; do not compare its generation numerically
with the current incarnation. Maintenance-fenced, invariant-validated restoration
remains a prerequisite, not something pruning implements.

The API reports current metadata observations or acknowledged deletion, never a
historical non-commit result. An absent item is only absent at this observation;
it is not evidence of physical object absence or permission to recreate it.

## Candidate families

- **Command outcome:** use its checked command token/digest and native receipt
  decoder. Both COMMITTED and CANCELLED are eligible after the watermark. Existing
  outcome validation/digest checks remain; no incorrect token can erase a receipt.
- **Use pin:** only RELEASED or REVOKED. ACTIVE pins, including expired ones, must
  first take the counted terminal transition. Pruning never decrements a count.
- **Staging row:** only a terminal staging claim; check the claim's creation scope
  as well as its intent/address scope. Its counted release and intent transition
  were already atomic. Do not require the version tombstone to remain present.
- **Upload intent:** only CONSUMED or CANCELLED, never OPEN/SELECTED. Remaining
  attempts cannot select through a missing terminal intent; late S3 effects remain
  discoverable through immutable attribution, closed epochs and retained namespace
  ownership/lifecycle configuration.
- **Upload attempt:** only SEALED. Late effects remain discoverable as above.
- **Version tombstone:** only DELETED with zero references and claims. This state
  can be written only after confirmed exact deletion; check its historical creation
  scope. Pruning does not itself issue S3 requests or adopt an absent version.

Every deletion is a native transaction containing the full observed control guard
and full observed item guard. A concurrent renewal/state change, control change,
or prior cleanup invalidates the stale condition. No blind DeleteItem, TTL,
extra tombstone, new resource ID, or cleanup receipt is introduced. Service errors
and conditional conflicts require rereading; they do not establish rollback.

## Replay and retention boundary

Generations are irreversibly closed before their watermark is published. Original
creators cannot recreate deleted temporary identities; new commands cannot reuse
another creating command's resource ID. Selection requires the existing admitting
intent/attempt, while new references/pins require READY. DELETED cannot reopen.
A delayed inventory observation may cause another guarded check, never recreate
READY. Configuration/ownership records are not pruning candidates.

These APIs must later be called by fair, bounded maintenance even without user
traffic. Their existence alone does not prove eventual reclamation. Native tests
must cover pre-watermark rejection, active/nonterminal rejection, stale guards,
terminal deletion without count changes, repeated absence, recorded receipt
precedence before deletion and Expired resolution afterwards.
