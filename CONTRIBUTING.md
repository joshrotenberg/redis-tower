# Contributing to redis-tower

Thanks for your interest in contributing!

## Development setup

redis-tower is a Cargo workspace. You need a recent stable Rust toolchain (the
MSRV is **1.88**) and, for the integration tests, a local `redis-server` binary
on your `PATH` -- the tests start and manage their own instances.

## Before you open a pull request

Run the same checks CI runs:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
cargo test --test '*' --all-features        # standalone integration tests
cargo deny check                            # supply-chain policy
```

Cluster and sentinel integration tests are gated behind `#[ignore]` and run
single-threaded:

```bash
cargo test -p redis-tower-cluster  --test cluster_integration  -- --ignored
cargo test -p redis-tower-sentinel --test sentinel_integration -- --ignored
```

## Definition of done

A change is complete when it ships, in the same pull request:

- **Tests** -- unit and/or integration as appropriate. New behavior gets a test
  that would fail without it.
- **Documentation** -- doc comments on any new public API, plus updates to the
  relevant README or guide where behavior or usage changes.

Code that merely compiles is not done.

## Conventions

- Rust 2024 edition; no `unsafe` -- every crate is `#![forbid(unsafe_code)]`.
- `thiserror` for library errors.
- Public APIs carry doc comments with examples.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).

## Licensing

By contributing, you agree that your contributions are dual-licensed under the
MIT and Apache-2.0 licenses, matching the project.
