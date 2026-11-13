# StoreLink

V0 implements the reviewed protocol and failure model, including the explicit
lifecycle-owned MPU cleanup provider assumption. Implementation and deployment
acceptance must still prove the contracts in `docs/protocol-v0.md`; partial
components or model tests are not a complete/proven protocol. Consumer application
refactors belong to their own projects. Do not enable collection over historical
data. This library is intended for public source distribution: keep secrets,
consumer-specific operational evidence and machine paths outside published files.

StoreLink owns reusable DB/object-store coordination primitives and their
correctness tests. Both no premature deletion and eventual reclamation after
abandonment are required; permanent pins or per-operation tombstones are not an
acceptable final fallback. Read `docs/reclamation-requirements.md` as well as the
protocol. It does not own filesystem paths, WebDAV, application permissions or a
general saga engine. Callers may be required to obey explicit
transaction/reference/lifetime contracts; do not promise safety for hidden or
bypassed application references.

Initial adapters target regional DynamoDB transactions and versioned S3. A
CAS-only store does not satisfy the transaction adapter merely by implementing
similarly named methods. Unknown outcomes are not rollbacks, and object keys alone
are not immutable version identity.

Use independent Release Please versions and GitHub release artifacts initially.
Python bindings and global storage protocols are future work. Preserve native
SDK/service behavior; do not replace official clients or validation tools with
bespoke implementations without a demonstrated need.
