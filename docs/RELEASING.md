# Release process

redis-tower uses the manual `Release` GitHub Actions workflow. Preparing a
release pull request and publishing immutable crates are separate maintainer
actions. Merging ordinary code never publishes a package.

## Current published state

The complete 13-crate public workspace is available on crates.io. These
versions were verified against the workspace manifests, crates.io search, and
GitHub releases on **2026-09-25**:

| Package group | Published version |
|---|---:|
| `redis-tower-protocol`, `redis-tower`, `redis-tower-cluster`, `redis-tower-sentinel` | 0.1.3 |
| `redis-tower-core`, `redis-tower-commands`, `redis-tower-sync`, `redis-tower-modules`, `redis-tower-client`, `redis-tower-primitives` | 0.1.2 |
| `redis-tower-auth-aws`, `redis-tower-auth-azure`, `redis-tower-test` | 0.1.1 |

This is a dated status snapshot, not a version source of truth. Before a
release, compare the manifests, crates.io, tags, GitHub releases, and each
crate's changelog.

## Prepare

1. Confirm that no open pull request already covers the release and that
   `main` is green.
2. Run the local gates:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo test --lib --all-features
   cargo test --workspace --doc --all-features
   RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
   mdbook build
   mdbook test
   python3 scripts/test_check_docs_links.py -v
   python3 scripts/check_docs_links.py
   python3 scripts/check_release_hygiene.py --check-package-contents
   ```

3. Ask release-plz to prepare version and changelog changes:

   ```bash
   gh workflow run Release --ref main -f command=release-pr
   ```

4. Review the generated pull request. Check that every changed public package
   has the intended semver bump and changelog entry, workspace dependency
   requirements remain resolvable, and no unrelated package was bumped.
5. Merge only after the release pull request and required checks are complete.

A downstream `cargo publish --dry-run` cannot resolve a new workspace
dependency until that exact dependency version exists on crates.io. Dry-run the
first dependency tier (`redis-tower-protocol`) and rely on the full workspace
gates for downstream code before publication. The real workflow waits for each
published dependency to become visible before continuing.

## Publish

Publish only from a clean, fully validated `main`:

```bash
gh workflow run Release --ref main -f command=release
```

The workspace dependency graph requires this order:

1. `redis-tower-protocol`
2. `redis-tower-core`
3. `redis-tower-commands` and `redis-tower-test`
4. `redis-tower`
5. Cluster, Sentinel, modules, primitives, sync, and cloud-auth crates
6. `redis-tower-client`

GitHub serializes release workflow runs and skips publication in forks. The
repository must retain a `CARGO_REGISTRY_TOKEN` Actions secret; the first
publication of a crate cannot be bootstrapped with crates.io trusted publishing.

Do not retry a partially successful workflow blindly. Inspect the workflow,
crates.io, tags, and releases first. Published versions are immutable: continue
from the first missing tier, and bump only packages whose attempted version can
no longer be published.

## Verify

For every package reported as published:

- confirm the expected version exists and is not yanked on crates.io;
- confirm the docs.rs build completed with the expected features;
- confirm the GitHub tag and release exist;
- install the facade from crates.io in a fresh temporary project and run a
  basic `PING` against a supported Redis version;
- verify README badges and public documentation links resolve to the release.

Keep publication manual. It changes external immutable state and remains a
deliberate action after review and validation.

## Recovery history

The initial June 2026 publication attempt left six versions yanked and seven
packages unpublished. Yanked versions cannot be reused, and release-plz could
not infer a next version when every registry version was yanked.

[PR #690](https://github.com/joshrotenberg/redis-tower/pull/690) merged on
2026-08-28 with explicit patch bumps, changelogs for all public crates, staged
publication order, and the manual operation-specific workflow. The first
complete staged relaunch was published on 2026-09-18; follow-up releases on
2026-09-24 established the current version groups above. That incident explains
the recovery safeguards but is no longer the active release plan.

See release-plz's [yanked-package guidance](https://release-plz.dev/docs/extra/yanked-packages)
if a future release encounters the same registry state.
