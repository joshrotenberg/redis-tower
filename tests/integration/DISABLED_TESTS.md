# Temporarily Disabled Integration Tests

The following integration tests are disabled while migrating from testcontainers to docker-wrapper:

- integration_advanced.rs
- integration_blocking.rs
- integration_bloom.rs
- integration_core.rs
- integration_pubsub.rs
- integration_scripting.rs
- integration_streams.rs
- integration_transactions.rs

## Status

- ✅ integration_cluster.rs - migrated (tests ignored due to Docker networking)
- ✅ integration_sentinel.rs - migrated
- ⏳ Others - need migration

## To Re-enable

Each test file needs to:
1. Remove testcontainers imports
2. Create helper in tests/integration/helpers/ using docker-wrapper
3. Update test setup to use new helper
