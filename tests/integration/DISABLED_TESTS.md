# Integration Tests Status

## ✅ Working (7 tests, 67 test cases)

- **integration_cluster.rs** - 7 tests - Cluster operations (requires `./scripts/setup-test-cluster.sh start`)
- **integration_sentinel.rs** - Sentinel failover
- **integration_core.rs** - 13 tests - Basic Redis commands
- **integration_pubsub.rs** - 13 tests - Pub/Sub functionality  
- **integration_blocking.rs** - 12 tests - Blocking list operations
- **integration_advanced.rs** - 10 tests - HyperLogLog, Geo commands
- **integration_streams.rs** - 13 tests - Redis Streams

## ⚠️ Needs Fixing (3 tests)

- **integration_transactions.rs** - Uses Transaction API that needs RedisConnection, not RedisClient
- **integration_scripting.rs** - Runtime test failures (Lua script issues)
- **integration_bloom.rs** - Missing bloom module imports (requires bloom feature)

## Running Tests

```bash
# Start standalone Redis for most tests
redis-server --daemonize yes --port 6379

# Start cluster for cluster tests
./scripts/setup-test-cluster.sh start

# Run all working integration tests
cargo test --test integration_core
cargo test --test integration_pubsub
cargo test --test integration_streams
cargo test --test integration_blocking  
cargo test --test integration_advanced
cargo test --test integration_cluster --features cluster
```
