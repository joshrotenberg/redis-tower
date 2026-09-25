# redis-rs migration compatibility harness

This standalone crate compiles the migration guide's central examples against
the exact versions published on crates.io. Its independent workspace prevents
the repository's path dependencies from replacing released packages.

```bash
cargo check --locked \
  --manifest-path release-tests/redis-rs-migration/Cargo.toml
```

When the guide advances to a new release, update every redis-tower package
version together, regenerate `Cargo.lock`, and keep the documentation CI step
green.
