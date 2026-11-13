# Coherent application observation sets

Extend the existing one-item observation with
`observe_application_items(Vec<Get>) -> ApplicationItemsObservation`.

- Require 1..=99 application Gets, plus exactly one library-owned control Get in
  ONE native TransactGetItems. The 100-item/4-MiB service limits remain native;
  oversize/error responses must not be truncated or treated as absence.
- Canonicalize every independently enrolled application target before network I/O.
  Reject reserved/unknown aliases using existing table machinery. Preserve native
  key/projection/expression semantics and native validation errors/diagnostics.
- Require exactly N+1 responses, parse the same checked domain fence as the
  existing API, and preserve application input order and optional item presence.
  All reported values and fence come from that one native transactional read.
- Return an opaque observation with read-only item/fence access, no public
  constructor or deserialization. Cloning observations does not refresh them.
- Delegate the existing single-item API to this implementation without changing
  its behavior or authority. A projected/missing item is still not command history,
  permission, resource identity, attachment authority, or pruning evidence.
- Do not create command IDs, receipts, durable records, retry loops or custom
  expression evaluation. No caller-supplied control target or predicate.

Verification covers bounds, mixed enrolled aliases/canonical ARNs, ordering and
missing items, incomplete responses, invalid control, rejected reserved targets
before I/O, native validation diagnostics, and the unchanged one-item API. Native
application writers must still CAS their facts in the final mutation; this read
alone does not freeze state. Consumer applications own their tighter read budgets
and schema/identity/authorization checks.

Status: independently reviewed and implemented. Native SDK tests and real
DynamoDB conformance cover ordered coherent observation and diagnostic behavior.
This remains an observation seam, not operation authority.
