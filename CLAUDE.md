# Redis-Tower Client Experimental Project

## Project Vision

Build an experimental Redis **client** (not proxy) using Tower's middleware architecture. This explores using Tower's patterns for client-side resilience, observability, and connection management. We'll use your existing RESP2/3 parser as the protocol layer and provide strongly-typed commands and responses.

## Why a Tower-Based Redis Client?

### The Motivation
- **No good Tower Redis client exists** (fred and redis-rs don't use Tower)
- **Composable resilience** - Circuit breakers, retries, timeouts as middleware
- **First-class observability** - Tracing and metrics built in
- **Connection pooling** - Tower's `Buffer` and `Balance` services
- **Type safety** - Strongly typed commands and responses (unlike redis-rs strings)
- **Learning project** - Explore Tower patterns in a real protocol

### What Makes This Different from fred/redis-rs
- **Tower-native**: Service trait at the core
- **Middleware-first**: Resilience isn't bolted on
- **Protocol agnostic**: Your RESP parser as a codec
- **Composable**: Users can add their own Tower middleware
- **Type-safe**: No stringly-typed APIs, compile-time command validation
- **Modern async**: Built on latest Tokio patterns

## Core Architecture

### The Tower Service Stack

```rust
// Users can compose their Redis client like this:
let redis_client = ServiceBuilder::new()
    // Observability
    .layer(TraceLayer::new_for_redis())
    .layer(MetricsLayer::new())
    
    // Resilience  
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    .layer(CircuitBreakerLayer::new(5, Duration::from_secs(30)))
    .layer(RetryLayer::new(ExponentialBackoff::default()))
    
    // Connection management
    .layer(BufferLayer::new(100))  // Tower's request buffering
    
    // Load balancing across connections
    .layer(BalanceLayer::new(discover_endpoints()))
    
    // Core Redis service
    .service(RedisConnection::new("localhost:6379"));

// Usage with strongly typed commands
let value: Option<String> = redis_client
    .call(Get::new("user:123"))
    .await?
    .into_value()?;

let count: i64 = redis_client
    .call(Incr::new("counter"))
    .await?
    .into_value()?;
```

### Strongly Typed Commands and Responses

```rust
// Each command knows its response type
pub trait RedisCommand {
    type Response: FromResp;
    
    fn to_frame(&self) -> Frame;
    fn parse_response(frame: Frame) -> Result<Self::Response, Error>;
}

// Strongly typed GET command
pub struct Get {
    key: String,
}

impl RedisCommand for Get {
    type Response = Option<Bytes>;  // GET returns optional bytes
    
    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(b"GET".to_vec()),
            Frame::BulkString(self.key.as_bytes().to_vec()),
        ])
    }
    
    fn parse_response(frame: Frame) -> Result<Option<Bytes>, Error> {
        match frame {
            Frame::BulkString(data) => Ok(Some(data.into())),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(Error::Redis(e)),
            _ => Err(Error::UnexpectedResponse),
        }
    }
}
```

## Implementation Roadmap

### Phase 1: Core Connection & Types (Day 1-2)
- [ ] Wrap your RESP parser in Tokio codec
- [ ] Basic `RedisConnection` struct with Framed stream
- [ ] Implement Tower `Service<Command>` trait
- [ ] Strongly typed GET/SET/DEL commands
- [ ] Type-safe response parsing

### Phase 2: Tower Integration (Day 3-4)
- [ ] Add timeout middleware
- [ ] Add retry middleware
- [ ] Add circuit breaker
- [ ] Connection pooling with Balance
- [ ] Request coalescing middleware

### Phase 3: Advanced Types (Day 5-6)
- [ ] Pipeline builder with type safety
- [ ] Transaction builder with type safety
- [ ] JSON serialization support
- [ ] Custom derive macros for responses
- [ ] Pub/Sub with typed messages

### Phase 4: Production Features (Day 7-8)
- [ ] Cluster support with automatic routing
- [ ] Redis Sentinel support
- [ ] TLS support
- [ ] Unix socket support
- [ ] Benchmarks vs fred and redis-rs

## Success Criteria

### Must Have
- [ ] Strongly typed commands and responses
- [ ] Tower middleware actually works (timeout, retry)
- [ ] Type-safe pipeline and transaction builders
- [ ] Performance within 2x of fred/redis-rs
- [ ] Your RESP parser integrates cleanly

### Nice to Have
- [ ] Full Redis command coverage
- [ ] Derive macros for custom types
- [ ] Better performance than fred/redis-rs
- [ ] Cluster and Sentinel support
- [ ] Becomes a real crate others use

## Project Structure

```
redis-tower/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Public API
│   ├── client.rs        # Core RedisConnection Service
│   ├── codec.rs         # Your RESP parser as Tokio codec
│   ├── commands/
│   │   ├── mod.rs       # Command trait and impls
│   │   ├── strings.rs   # GET, SET, INCR, etc.
│   │   ├── hashes.rs    # HGET, HSET, etc.
│   │   ├── lists.rs     # LPUSH, RPOP, etc.
│   │   └── sets.rs      # SADD, SREM, etc.
│   ├── types/
│   │   ├── mod.rs       # Type conversion traits
│   │   ├── response.rs  # Response types
│   │   └── value.rs     # Redis value types
│   ├── middleware/
│   │   ├── mod.rs       # Custom Redis middleware
│   │   ├── coalescing.rs
│   │   ├── cluster.rs
│   │   └── routing.rs
│   ├── pipeline.rs      # Pipeline builder
│   ├── transaction.rs   # Transaction builder
│   └── pool.rs          # Connection pooling
├── examples/
│   ├── basic.rs         # Simple typed usage
│   ├── pipeline.rs      # Pipeline with types
│   ├── transaction.rs   # Transactions
│   └── resilient.rs     # Full middleware stack
└── benches/
    └── comparison.rs    # Benchmark vs fred/redis-rs
```

## Development Standards

Follow the same high standards as the redis-proxy project:

### Code Quality
- No emojis in any code, commits, or documentation
- Always run cargo test and clippy after every task
- Run before committing:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test --lib --all-features
  cargo test --test '*' --all-features
  ```

### Rust Development
- Use `anyhow` for application errors, `thiserror` for library errors
- Follow Rust 2024 edition idioms
- All public APIs must have doc comments
- Maintain minimum 70% test coverage

### Git Workflow
- **ALWAYS** check current branch before making any commits
- **ALWAYS** create feature branch BEFORE making changes
- **NEVER** commit directly to main branch
- Branch naming: `feat/`, `fix/`, `docs/`, `refactor/`, `test/`
- Use conventional commit format (no emojis, no "Generated with Claude Code")

## Current Status

**Project Created**: 2025-10-23

**Next Steps**:
1. Set up Cargo.toml with dependencies
2. Create basic module structure
3. Integrate RESP parser as codec
4. Implement first typed command (GET)

This experimental client could become the most type-safe, composable Redis client in the Rust ecosystem!
