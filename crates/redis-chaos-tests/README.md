# redis-chaos-tests

Docker-backed compatibility and fault-injection tests for redis-tower.

Normal workspace test runs compile this crate but never start Docker. Every
Docker test is `#[ignore]`-gated and runs only from the scheduled compatibility
workflow or an explicit local command:

```bash
REDIS_TEST_IMAGE=redis:8.8-alpine \
  cargo test -p redis-chaos-tests --test docker_smoke -- --ignored
```

The nightly version matrix starts each server image as a GitHub Actions service
and points the existing `redis-tower` standalone integration suite at it through
`REDIS_URL`. Future true network-partition and container-only fault scenarios
belong in this crate; process-level crash, freeze, ACL, replication, and
failover tests remain in the regular integration suites.
