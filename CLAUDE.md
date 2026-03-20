# Redis-Tower Client

## Project Status

**Version**: 0.1.0
**Commands**: 518/518 implemented (100% coverage)
**Tests**: 828 lib tests passing
**Parser**: Integrated internally (`src/parser/`)

## Project Vision

A production-ready Redis client using Tower's middleware architecture for composable
resilience, observability, and type safety. Built on a high-performance RESP parser
with strongly-typed commands and compile-time validation.

### Why Tower-Based?

- No existing Rust Redis client uses Tower (fred and redis-rs don't)
- Composable resilience via middleware (circuit breakers, retries, timeouts)
- First-class observability with tracing and metrics built in
- Connection pooling via Tower's `Buffer` and `Balance` services
- Type safety with strongly typed commands and responses

## Core Architecture

### Command Trait

```rust
pub trait Command {
    type Response;
    fn to_frame(&self) -> Frame;
    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError>;
}
```

### Tower Service Stack

`RedisConnection`, `RedisClient`, and `ResilientRedisClient` all implement
`Service<Cmd>` for any `Cmd: Command`. This allows composing with Tower middleware:

```rust
let client = ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    .layer(CircuitBreakerLayer::new(5, Duration::from_secs(30)))
    .layer(RetryLayer::new(ExponentialBackoff::default()))
    .service(RedisConnection::connect("localhost:6379").await?);
```

**Backpressure caveat**: `poll_ready` always returns `Ready` because the connection
uses `Arc<Mutex<>>` internally. For proper backpressure, wrap with `tower::buffer::Buffer`.

### Connection Types

- `RedisConnection` - Core connection, implements `Service<Cmd>`
- `RedisClient` - High-level wrapper, implements `Service<Cmd>`
- `ResilientRedisClient` - Auto-reconnecting variant, implements `Service<Cmd>` (requires `Cmd: Clone`)

## Project Structure

```
redis-tower/
  Cargo.toml
  src/
    lib.rs              # Public API and re-exports
    client.rs           # RedisConnection, RedisClient, ResilientRedisClient
    codec.rs            # RESP codec (Tokio Decoder/Encoder)
    config.rs           # ClientConfig builder
    connection_pool.rs  # ResilientConnection with auto-reconnect
    health.rs           # Connection health checking
    hooks.rs            # Error/reconnect hooks
    metrics.rs          # Command/connection metrics
    tcp.rs              # TCP configuration
    tls.rs              # TLS configuration (native-tls, rustls)
    tracing.rs          # Tracing configuration
    url.rs              # Redis URL parsing
    pipeline.rs         # Pipeline builder
    transaction.rs      # Transaction builder (MULTI/EXEC)
    parser/             # Integrated RESP2/3 parser (from resp-parser-rs)
    commands/           # 518 strongly-typed command implementations
      mod.rs            # Command trait
      strings.rs, hashes.rs, lists.rs, sets.rs, sorted_sets.rs
      streams.rs, geo.rs, hyperloglog.rs, bitmap.rs
      keys.rs, server.rs, pubsub.rs, transactions.rs
      scripting.rs, functions.rs, acl.rs
      cluster.rs, connection.rs, latency.rs, module.rs
    modules/            # Redis Stack modules (feature-gated)
      bloom.rs, cuckoo.rs, cms.rs, topk.rs, tdigest.rs
      json.rs, search.rs, timeseries.rs, graph.rs, vector.rs
    types/              # Type conversion traits and response types
    cluster/            # Cluster routing (SlotMap, CRC16)
    sentinel/           # Sentinel master discovery
  examples/             # 20+ examples
  tests/
    commands/           # Command integration tests (real Redis)
    integration/        # Higher-level integration tests
    parser/             # Parser test suite
```

## Feature Flags

```toml
[dependencies]
redis-tower = "0.1"                                    # Core only
redis-tower = { version = "0.1", features = ["cluster"] }    # + Cluster
redis-tower = { version = "0.1", features = ["sentinel"] }   # + Sentinel
redis-tower = { version = "0.1", features = ["bloom"] }      # + RedisBloom
redis-tower = { version = "0.1", features = ["json"] }       # + RedisJSON
redis-tower = { version = "0.1", features = ["search"] }     # + RediSearch
redis-tower = { version = "0.1", features = ["timeseries"] } # + RedisTimeSeries
redis-tower = { version = "0.1", features = ["graph"] }      # + RedisGraph (deprecated)
redis-tower = { version = "0.1", features = ["deprecated"] } # + Deprecated commands
redis-tower = { version = "0.1", features = ["serde-json"] } # + Serde JSON integration

# TLS backends (pick one)
redis-tower = { version = "0.1", features = ["tls-native-tls"] }
redis-tower = { version = "0.1", features = ["tls-rustls"] }
```

## Core Dependencies

- **tower** 0.5 (Service trait, Buffer, Balance, Timeout, Reconnect)
- **tower-resilience** 0.3.7 (circuit breakers, retries, rate limiting)
- **tokio** 1.42 (async runtime)
- **tokio-util** 0.7 (codec for Framed streams)
- **bytes** 1.9 (zero-copy byte buffers)
- **thiserror** 2.0 (library error types)

## Known Limitations

### poll_ready Backpressure
`poll_ready` always returns `Ready`. The `Arc<Mutex<>>` design serializes requests
inside `call()` rather than signaling backpressure. Wrap with `tower::buffer::Buffer`
for proper flow control.

### LCS IDX Response Parsing
The `LCS` command with `IDX` returns a placeholder instead of parsing the complex
nested array. Use LCS without IDX or parse the raw response manually.

### Cluster Keyless Commands
Cluster client rejects commands without extractable keys (PING, TIME, etc.).
Connect directly to nodes for these commands.

## Development Standards

### Pre-commit Checklist
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
cargo test --test '*' --all-features
```

### Git Workflow
- Check current branch before commits: `git status`
- Create feature branches: `git checkout -b type/description`
- Never commit directly to main
- Branch naming: `fix/`, `feat/`, `docs/`, `refactor/`, `test/`
- Conventional commits: `type: description`

### Code Quality
- No emojis in code, commits, or documentation
- `thiserror` for library errors, `anyhow` for application errors
- All public APIs must have doc comments
- Rust 2024 edition, MSRV 1.87

### GitHub Issues
- Every piece of work should reference a tracking issue
- Use `Closes #N` or `Part of #N` in commit messages
- Apply labels: `area:`, `priority:`, `type:`
