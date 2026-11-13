# Application-owned terminal pruning (v0 extension)

Status: design independently reviewed and approved;
implemented by `CommandJournal::observe_application_pruning_candidate` and
`CommandJournal::prune_application_item`. Native-cloud acceptance and application
integration proof remain separate gates; loopback tests are not DynamoDB proof.
The implementation has eight focused application-pruning tests. Validation must
be rerun after integration changes. The wire test checks exactly two actions and
preserves the official SDK's automatic `ClientRequestToken`. This ephemeral
service idempotency value is not a durable StoreLink command ID, resource or
receipt. Suppressing it would add unnecessary interception and would not improve
metadata reclamation. Native retry remains disabled for this point operation.

## Need and boundary

Applications own their logical records and terminal semantics. Library receipt
pruning cannot remove a filesystem logical result: it may have terminalized in a
later generation than its creator. Requiring another journaled deletion would
create receipts for cleanup. Allowing applications to construct StoreLink's
private control predicates would split transaction ownership.

Add one bounded, receipt-free application deletion seam. It is not a workflow
engine, scheduler, TTL mechanism, permission check, reference-release operation,
or historical outcome oracle. The trusted application must have completed all
counted releases and declare every scope whose retention protects the item.
Applications remain responsible for their schema, complete declarations and
native item CAS, just as they own native business actions and pointer declarations
in normal transactions. Untrusted clients must not receive this server API.

## Observation

`observe_application_pruning_candidate(Get)` canonicalizes an independently
enrolled application table/key and rejects projections. One native TransactGet
reads the full control and full application item. Parse control with the complete
maintenance decoder, including immutable retention state. A malformed/missing
control, incomplete response or native error is not absence.

The `ApplicationPruningObservation` result is opaque, non-serializable and consumed
by pruning (no cloning or executable-plan API). It exposes the item option and fence
for application decoding, and privately retains canonical target, coordination
identity, domain and the exact control observation. A projected/general-purpose
application observation cannot be upgraded into this token. Merely observing an
item or its absence authorizes no deletion or historical conclusion.

## Deletion

`prune_application_item(observation, native Delete, retained_scopes)`:

- Accept only the observation's same canonical enrolled table/key and same
  coordination ARN/domain. Never allow coordination-table application actions.
- Require a nonempty native condition and at least one retained scope. These
  scopes are a complete application declaration, not evidence inferred by
  StoreLink from an arbitrary schema. They include creator and terminal scopes
  for filesystem logical operations, plus any additional retained identity scopes.
- Require every declared scope to belong to this domain and be covered by the
  observed published compaction frontier, or belong to a historical incarnation.
  Closed-but-not-compacted generations do not suffice.
- The application condition must assert the decoded terminal state and an ABA-safe
  immutable identity/mutation revision that changes for EVERY semantic mutation,
  including adding attributes/protected inputs. Equality of only the attributes
  that happened to exist is not an exact-item-shape guard in DynamoDB. The library
  cannot validate arbitrary application schemas; document this obligation.
- Validate native application action/key/expression size constraints through the
  existing action/table machinery. No caller-supplied control action is accepted.
- Assemble exactly the library's full observed-control ConditionCheck and the
  conditional application Delete. Submit one native SDK attempt, without a new
  command/resource ID, receipt, generation record, or implicit retry task.
- The observation alone never freezes state: the native CAS and full control
  predicate must still hold at deletion. Schema/result changes, ABA, restore or
  concurrent maintenance must conflict rather than weaken those guards.

An acknowledged delete reports Removed. Conditional conflict reports Conflict
and requires reobservation; an ambiguous response reports Unknown and requires
reobservation, never rollback/recreation. `ApplicationPruningError` retains the
existing `PruningError::Conflict`/`OutcomeUnknown` classification and native
diagnostic in `NativeWrite`; observation errors retain native diagnostics. Native
expression syntax, schema and service limits remain service-authoritative, not
interpreted by a custom expression evaluator. An absent observation may report only
ObservationAbsent after validating binding; it is not proof of earlier execution,
legitimate pruning, historical cancellation, or physical object absence.

## Application integration obligations

Applications must decode their complete supported schema and establish a terminal
state before deletion. A timeout alone is not terminality. If the application
retains a result under both creation and completion scopes, both must be declared
and covered by published compaction before the result is deleted. A missing item
must not be converted into historical success or cancellation when evidence is
insufficient.

All owned input protection must have completed its counted release before pruning.
Adding a new application record type requires checking its own terminal-state,
retention and input-release contract; it cannot inherit eligibility merely because
another type used the same maintenance API.

## Verification requirements

Native SDK wire and pure tests: target/coordination/domain binding, projection
rejection, empty declarations/conditions, current/open/closed-not-compacted
scopes, historical incarnations, malformed control, missing/incomplete rows,
exactly two native actions/no durable receipt or command identity, native SDK
idempotency preserved, one-attempt unknown/conflict handling,
no executable-plan exposure. Real DynamoDB: terminal generation later than
creator remains retained; frontier then permits deletion; ABA/CAS change and
control rotation reject stale observations; absent reread is only an observation.
Application tests must cover unsupported schemas and input-release obligations.

Fair discovery, durable cursor progress, scheduling, capacity/backlog metrics and
S3 cleanup remain separate mandatory deployment work; this point operation alone
does not establish eventual collection.
