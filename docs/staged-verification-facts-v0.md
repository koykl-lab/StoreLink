# Durable staging verification facts

Independently reviewed and implemented with discriminator `staged-verified-v1`.
Owned native S3/DynamoDB conformance verified selected facts by rereading them
and comparing with the empty, small and multipart fixture payloads. This is not
an old-state upgrade or complete host-reclamation claim.

Selection already consumes an opaque verified upload receipt and checks its
actual length/hash against the admitted constraints. Persist the receipt's actual
verified length and optional actually verified full SHA-256 in the immutable part
of StagedSnapshot. Expose read-only accessors. Constraints are NOT verification
facts: an absent expected length is not zero, and a caller-supplied expected hash
is not evidence that the object was hashed.

Only verified-receipt selection populates these fields. No public constructor,
deserialization, caller setter or raw-tuple promotion is added. Native decoding
must check integer/hash types, bounds and consistency with stored constraints;
renewal, release, publication and cleanup preserve the facts while guarding the
complete observed staging row. Include fields in complete-row CAS, canonical
encoding, sizing, discovery and pruning tests. A matching receipt with no full
hash verification persists no verified hash.

The staging discriminator is `record = staged-verified-v1`. Its immutable
fields include `max_length`, optional `expected_length` / `full_sha256`
constraints, required numeric `verified_length`, and optional 32-byte binary
`verified_full_sha256`. Constraint hashes and verified hashes are separate fields;
selection copies facts only from the opaque verified receipt. Decoding rejects
invalid types, noncanonical/out-of-range lengths, malformed hashes and facts
inconsistent with constraints. Lifecycle operations also compare the stored
constraints with the selected intent and guard optional-field absence.

This is pre-release
schema development: no StoreLink release or production deployment has established
a persisted-schema compatibility contract. Do not fabricate missing facts for
old rows or add a deployment-specific migration/adoption helper. Reject unsupported
schemas explicitly and document the fresh-fixture/prerelease boundary before
release. Future released-schema upgrades need their own compatibility/cleanup
proof; rejection must not be advertised as automatic old-state reclamation.
Old `staged` rows and unknown/future discriminators are unsupported, including
for pruning; missing facts are never inferred. Native acceptance must use fresh
fixtures written with this schema. No release or production deployment has used
the old staging contract. The currently active garbage-reclamation proof uses a
pinned pre-change binary and is NOT evidence of schema-upgrade safety, cleanup,
or this new persistence contract. There is no automatic old-state cleanup or
migration helper.

Acceptance: opaque receipt-derived fields survive selection and native reread,
including unknown-length inputs and absent verified hashes; forged/missing/wrong
facts reject; all existing lifecycle/publication/reference tests remain valid;
complete transaction budgets and actual native persistence verified. These facts
are immutable metadata, not read/attach permissions or a lifetime mechanism.
