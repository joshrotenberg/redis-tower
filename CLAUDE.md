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

## Core Dependencies

### Tower Ecosystem
- **tower** (0.5): Core Service trait and middleware composition
- **tower-layer** (0.3): Layer trait for middleware
- **tower-service** (0.3): Service trait definitions
- **tower-resilience** (0.3.5, features = ["full"]): Pre-built resilience patterns
  - Circuit breakers
  - Retry with backoff
  - Rate limiting
  - Timeouts
  - Bulkheads
  - Caching

### Protocol Layer
- **resp-parser** (local: `../resp-parser-rs`): Your high-performance RESP2/3 parser
  - Zero-copy parsing
  - ~34-48ns/iter performance
  - 4.8-8.0 GB/s throughput
  - Features: `resp2`, `resp3`

### Runtime & Utilities
- **tokio** (1.42, features = ["full"]): Async runtime
- **tokio-util** (0.7, features = ["codec"]): Codec helpers for Framed streams
- **bytes** (1.9): Zero-copy byte buffers
- **futures** (0.3): Future combinators
- **thiserror** (2.0): Error handling
- **anyhow** (1.0): Application-level errors
- **serde** (1.0, features = ["derive"]): Serialization
- **serde_json** (1.0): JSON support for typed values
- **tracing** (0.1): Structured logging
- **tracing-subscriber** (0.3, features = ["env-filter"]): Log output

## Core Architecture

### The Tower Service Stack

```rust
// Users can compose their Redis client like this:
use tower::ServiceBuilder;
use tower_resilience::{
    CircuitBreakerLayer, RetryLayer, TimeoutLayer,
};

let redis_client = ServiceBuilder::new()
    // Observability
    .layer(TraceLayer::new_for_redis())
    .layer(MetricsLayer::new())
    
    // Resilience from tower-resilience
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

### Integrating resp-parser as Tokio Codec

```rust
use tokio_util::codec::{Decoder, Encoder};
use resp_parser::{parse_resp2, parse_resp3, RespType};
use bytes::{Buf, BufMut, BytesMut};

pub struct RespCodec {
    version: RespVersion,
}

impl Decoder for RespCodec {
    type Item = RespType;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Use your resp-parser's zero-copy parsing
        match self.version {
            RespVersion::Resp2 => {
                match parse_resp2(src) {
                    Ok((remaining, frame)) => {
                        let consumed = src.len() - remaining.len();
                        src.advance(consumed);
                        Ok(Some(frame))
                    }
                    Err(nom::Err::Incomplete(_)) => Ok(None),
                    Err(e) => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("RESP parse error: {:?}", e)
                    )),
                }
            }
            RespVersion::Resp3 => {
                match parse_resp3(src) {
                    Ok((remaining, frame)) => {
                        let consumed = src.len() - remaining.len();
                        src.advance(consumed);
                        Ok(Some(frame))
                    }
                    Err(nom::Err::Incomplete(_)) => Ok(None),
                    Err(e) => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("RESP parse error: {:?}", e)
                    )),
                }
            }
        }
    }
}

impl Encoder<RespType> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespType, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Use resp-parser's encoding functions
        // (you may need to add these to resp-parser if not present)
        item.write_to(dst);
        Ok(())
    }
}
```

### Strongly Typed Commands and Responses

```rust
// Each command knows its response type
pub trait RedisCommand {
    type Response: FromResp;
    
    fn to_frame(&self) -> RespType;
    fn parse_response(frame: RespType) -> Result<Self::Response, Error>;
}

// Strongly typed GET command
pub struct Get {
    key: String,
}

impl RedisCommand for Get {
    type Response = Option<Bytes>;  // GET returns optional bytes
    
    fn to_frame(&self) -> RespType {
        RespType::Array(vec![
            RespType::BulkString(b"GET".to_vec()),
            RespType::BulkString(self.key.as_bytes().to_vec()),
        ])
    }
    
    fn parse_response(frame: RespType) -> Result<Option<Bytes>, Error> {
        match frame {
            RespType::BulkString(data) => Ok(Some(data.into())),
            RespType::Null => Ok(None),
            RespType::Error(e) => Err(Error::Redis(e)),
            _ => Err(Error::UnexpectedResponse),
        }
    }
}
```

### Connection Layer with resp-parser

```rust
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tower::Service;

pub struct RedisConnection {
    framed: Framed<TcpStream, RespCodec>,
}

impl RedisConnection {
    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(addr).await?;
        let codec = RespCodec::new(RespVersion::Resp2);
        let framed = Framed::new(stream, codec);
        
        Ok(Self { framed })
    }
}

impl<Cmd: RedisCommand> Service<Cmd> for RedisConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;
    
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check if connection is ready for another request
        Poll::Ready(Ok(()))
    }
    
    fn call(&mut self, command: Cmd) -> Self::Future {
        use futures::SinkExt;
        use futures::StreamExt;
        
        let mut framed = self.framed.clone();
        
        Box::pin(async move {
            // Use your RESP parser via codec to encode
            let frame = command.to_frame();
            framed.send(frame).await?;
            
            // Use your RESP parser via codec to decode
            let response_frame = framed.next().await
                .ok_or(RedisError::ConnectionClosed)??;
            
            // Type-safe response parsing
            Cmd::parse_response(response_frame)
        })
    }
}
```

## Implementation Roadmap

### Phase 1: Core Connection & Types (Day 1-2)
- [x] Project skeleton created
- [x] Dependencies added (tower, tower-resilience, resp-parser)
- [ ] Wrap resp-parser in Tokio codec (RespCodec)
- [ ] Basic `RedisConnection` struct with Framed stream
- [ ] Implement Tower `Service<Command>` trait
- [ ] Wire up strongly typed GET/SET/DEL commands
- [ ] Type-safe response parsing with RespType

### Phase 2: Tower Integration (Day 3-4)
- [ ] Add timeout middleware (tower-resilience TimeoutLayer)
- [ ] Add retry middleware (tower-resilience RetryLayer)
- [ ] Add circuit breaker (tower-resilience CircuitBreakerLayer)
- [ ] Connection pooling with Tower Balance
- [ ] Request coalescing middleware (custom)

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
- [ ] Tower middleware actually works (timeout, retry from tower-resilience)
- [ ] Type-safe pipeline and transaction builders
- [ ] Performance within 2x of fred/redis-rs
- [ ] resp-parser integrates cleanly as Tokio codec

### Nice to Have
- [ ] Full Redis command coverage
- [ ] Derive macros for custom types
- [ ] Better performance than fred/redis-rs
- [ ] Cluster and Sentinel support
- [ ] Becomes a real crate others use

## Project Structure

```
redis-tower/
├── Cargo.toml            # Dependencies configured
├── CLAUDE.md             # This file
├── README.md
├── src/
│   ├── lib.rs           # Public API
│   ├── client.rs        # Core RedisConnection Service
│   ├── codec.rs         # resp-parser as Tokio codec
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
│   │   ├── coalescing.rs # Request deduplication
│   │   ├── cluster.rs   # Cluster routing
│   │   └── routing.rs   # Command routing
│   ├── pipeline.rs      # Pipeline builder
│   ├── transaction.rs   # Transaction builder
│   └── pool.rs          # Connection pooling
├── examples/
│   ├── basic.rs         # Simple typed usage
│   ├── resilient.rs     # Using tower-resilience layers
│   ├── pipeline.rs      # Pipeline with types
│   └── transaction.rs   # Transactions
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

**Dependencies Configured**:
- ✅ Tower ecosystem (tower, tower-layer, tower-service)
- ✅ tower-resilience with full features
- ✅ resp-parser (local path: ../resp-parser-rs)
- ✅ Tokio runtime and utilities
- ✅ All supporting libraries

**Next Immediate Steps**:
1. Update `src/codec.rs` to integrate resp-parser properly
2. Implement `RedisConnection` as Tower Service
3. Wire up GET/SET/DEL commands to use resp-parser frames
4. Create example showing tower-resilience middleware

## Why This is Worth Building

1. **Type safety** - No Redis client has this level of type safety
2. **Tower ecosystem** - Leverage tower-resilience for free
3. **Your RESP parser** - Perfect use case showcasing its performance
4. **Modern patterns** - Showcase Rust's type system
5. **Real need** - Redis clients could be much better with Tower patterns

This experimental client could become the most type-safe, composable Redis client in the Rust ecosystem!
