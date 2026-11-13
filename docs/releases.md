# Native source releases and Cargo consumption

StoreLink is MIT-licensed. Release Please owns its independent Rust version,
changelog, rolling release PR and `vX.Y.Z` tags. Distribution initially uses
GitHub releases; `publish = false` prevents registry publication. No crates.io
publication is configured or implied.

## Release flow

1. Ordinary PR and merge-queue verification exercise both native architectures,
   feature combinations, doctests and Cargo package verification.
2. On main, the enabled Release Please job uses the repository-scoped release App
   to maintain its native release PR and publish merged releases.
3. GitHub's `release.published` event runs verification/package creation against
   the **released tag's commit**, not the main-push commit that invoked Release
   Please. After both architectures pass, that run attaches the verified `.crate`
   and `SHA256SUMS` using the repository's normal Actions token.

This accepts Release Please's native model: it discovers merged release PRs on
live main, and their merge commit can differ from a workflow's main-push SHA.
There is no custom draft finalizer, per-commit release coordinator, singleton
merge-queue requirement, or attempt to force Release Please to release a different
commit. Existing queue batching is compatible with the release-event build.

The tag/release can exist before its downloadable assets finish building. Cargo
Git consumers use the tag, not those assets, and consumer verification remains
required when adopting an update. No pre-tag artifact-completion guarantee is
claimed. Release assets never come from an unrelated later main build.

## Initial setup and permissions

The manifest starts as `{}` because there is no prior release. Release Please's
Rust strategy starts at `0.1.0` when eligible Conventional Commits exist; it owns
subsequent Cargo.toml, Cargo.lock, manifest and changelog updates.

Before enabling `RELEASE_PLEASE_ENABLED=true`:

- Create the repository and review the exact public source/history for secrets,
  private consumer details and machine-specific evidence.
- Install the source workflow and normal branch/check/tag protections. Verify real
  PR and merge-queue runs; do not bypass checks to bootstrap routine releases.
- Provision `RELEASE_APP_ID` and `RELEASE_APP_PRIVATE_KEY` through the authorized
  App bootstrap, restricted to this repository. The workflow requests Contents
  and Pull requests write, with no organization-wide token.
- Verify App-authored PRs and published releases trigger their expected workflows.
  `GITHUB_TOKEN` is deliberately not a substitute for the release App: its events
  generally do not trigger subsequent workflows.
- Complete implementation and acceptance for the intended release. Configuring
  this workflow does not establish a production-ready protocol or a published tag.

Verification jobs have read-only repository permissions and no release App key or
cloud credentials. The App token appears only in the main-only release job.
The release-asset job receives Contents write only after verification and does not
check out or execute source/package contents.

## Failure and recovery

A failed package build or upload does not erase the native release/tag. Inspect
that release's own run and retained verified artifact. Correct the failure and
rerun the relevant jobs, or attach missing verified assets through native GitHub
release operations. Uploads do not use `--clobber`: existing assets require byte/
checksum verification before deciding any recovery, never an implicit overwrite.
Do not move a published tag to fix source; publish a corrected version instead.

## Native Cargo and Renovate adoption

A consumer declares a semantic Git tag and commits Cargo.lock:

```toml
[dependencies]
storelink = { git = "https://github.com/koykl-lab/StoreLink.git", tag = "v0.1.0" }
```

This example is not evidence that the tag currently exists. Renovate's native
Cargo manager discovers GitHub tags and proposes manifest/lockfile updates.
Consumer CI uses `cargo build --locked` and its normal tests before adoption.
There is no custom archive downloader, regex version injection or private registry.
Published tags must not move. Source releases and downstream application/image
releases remain independent.

References:
- https://github.com/googleapis/release-please/blob/v17.6.0/docs/manifest-releaser.md
- https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#release
- https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html
- https://docs.renovatebot.com/modules/manager/cargo/
