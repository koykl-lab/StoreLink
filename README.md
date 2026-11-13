# StoreLink

StoreLink is a Rust library being developed for coordinating application metadata
with versioned object storage. It is not a filesystem, WebDAV implementation,
object-storage server, or general saga/workflow engine.

**Status: v0 implementation in progress; not production-ready.** Identity/lifetime
primitives and native request-boundary components are being implemented against
the reviewed protocol. No complete coordinator, release or deployment acceptance
is claimed.

The v0 adapters target regional DynamoDB transactions and versioning-enabled
S3. Applications retain ownership of their schemas and publication policy, while
StoreLink owns explicit operation outcomes, object-reference protection, replay,
and retirement invariants.

Read the [abstraction evaluation](docs/abstraction-evaluation.md),
[v0 protocol](docs/protocol-v0.md), and the superseding
[eventual-reclamation requirements](docs/reclamation-requirements.md). The reviewed
contract requires finite claims, generation fencing and compaction. Design review
does not establish implementation or deployment acceptance. Permanent pins/terminal
records are not an acceptable final recovery strategy. The core supports filesystem and media
workloads without owning either application's schema, permissions or workflows.
Consumer refactoring and language bindings are separate delivery decisions.

The repository is intended for public distribution under the [MIT license](LICENSE).
Release Please manages independent versions, with GitHub release artifacts initially;
crates.io publication is not enabled. Consumers use native Cargo Git release tags,
which Renovate can update together with Cargo.lock. See the [release contract](docs/releases.md).
Publication still requires a content review and working acceptance gates. The implementation contract is in
[`docs/implementation-contract-v0.md`](docs/implementation-contract-v0.md).

Primitive/model checks can run without AWS adapters:

```sh
cargo test --locked --no-default-features
cargo clippy --locked --no-default-features --all-targets -- -D warnings
```

Native integration suites require their explicit fixture invocation. An ignored
integration test or a passing primitive suite is not backend/deployment acceptance.
