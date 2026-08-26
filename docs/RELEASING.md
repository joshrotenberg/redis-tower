# Release process

The workspace is released through the manual `Release` GitHub Actions
workflow. Preparing versions and publishing them are separate operations so a
release can be reviewed and validated before anything reaches crates.io.

## Current recovery release

The first public attempt left six versions yanked and seven packages
unpublished. A yanked version cannot be republished, and release-plz cannot
infer the next version when every registry version is yanked. This release
therefore carries the required patch bumps in the release-readiness pull
request.

| Package | Next version | Registry state before this release |
|---|---:|---|
| `redis-tower-protocol` | 0.1.2 | 0.1.0 and 0.1.1 yanked |
| `redis-tower-core` | 0.1.1 | 0.1.0 yanked |
| `redis-tower-commands` | 0.1.1 | 0.1.0 yanked |
| `redis-tower` | 0.1.1 | 0.1.0 yanked |
| `redis-tower-cluster` | 0.1.1 | 0.1.0 yanked |
| `redis-tower-sentinel` | 0.1.1 | 0.1.0 yanked |
| `redis-tower-sync` | 0.1.0 | first publication |
| `redis-tower-modules` | 0.1.0 | first publication |
| `redis-tower-client` | 0.1.0 | first publication |
| `redis-tower-primitives` | 0.1.0 | first publication |
| `redis-tower-auth-aws` | 0.1.0 | first publication |
| `redis-tower-auth-azure` | 0.1.0 | first publication |
| `redis-tower-test` | 0.1.0 | first publication |

See release-plz's [yanked-package guidance](https://release-plz.dev/docs/extra/yanked-packages)
before changing this recovery plan.

## Prepare

1. Confirm there is no overlapping release pull request and that `main` is
   green.
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

   A `cargo publish --dry-run` for a downstream package cannot resolve a new
   workspace dependency until that dependency has reached crates.io. Before a
   staged workspace release, dry-run the first dependency tier
   (`redis-tower-protocol`) and use the full workspace gates above for the
   downstream crates. During publication, validate each completed tier before
   proceeding to the next one.

3. For an ordinary future release, ask release-plz to prepare version and
   changelog changes:

   ```bash
   gh workflow run Release --ref main -f command=release-pr
   ```

4. Review and merge the generated release pull request. If a deliberately
   prepared pull request already contains the version and changelog changes,
   as with the yanked-version recovery above, this preparation dispatch is not
   needed.

## Publish

Publish only from a clean, fully validated `main`:

```bash
gh workflow run Release --ref main -f command=release
```

The workspace dependency graph requires this publication order:

1. `redis-tower-protocol`
2. `redis-tower-core`
3. `redis-tower-test`
4. `redis-tower-commands`
5. `redis-tower`
6. Cluster, Sentinel, modules, primitives, sync, and cloud-auth crates
7. `redis-tower-client`

Do not retry a partially successful workflow blindly. First inspect crates.io
and the workflow output to determine which immutable versions already exist;
then bump only the packages that require a new version.

GitHub serializes release workflow runs and skips the job in forks. The
repository must retain a `CARGO_REGISTRY_TOKEN` Actions secret; the first
publication of a crate cannot be bootstrapped with crates.io trusted
publishing.

## Verify

For every package reported as published:

- confirm that its expected version is present and not yanked on crates.io;
- confirm that its docs.rs build completed with the expected features;
- confirm that the GitHub tag and release exist;
- install the facade from crates.io in a fresh temporary project and run a
  basic `PING` example against a supported Redis version;
- verify that the README badges and documentation links resolve to the new
  release.

Keep the release workflow manual. Publishing changes external immutable state
and should remain a deliberate maintainer action after the release pull request
and required checks are complete.
