# redis-tower Design Document

## Motivation

There is no Tower-native Redis client in the Rust ecosystem. `redis-rs` and `fred`
are mature but neither exposes `tower::Service` as the fundamental abstraction.
This means users who build on Tower (which is most of the async Rust ecosystem at
this point) can't compose Redis operations with the same middleware they use for
HTTP, gRPC, and everything else -- circuit breakers, retries, timeouts, rate
limiting, load balancing, observability -- all as stackable layers.

redis-tower exists to fill that gap: a Redis client where every connection is a
`Service<Cmd>`, commands are typed request/response pairs, and resilience is
composed rather than baked in.

## Design Principles

1. **Service all the way down.** Every connection type implements
   `Service<Cmd> for Cmd: Command`. This is the only way to send commands.
   Tower middleware works without adaptation.

2. **Commands are types.** Each Redis command is a struct with a typed `Response`.
   `Get` returns `Option<Bytes>`, `Incr` returns `i64`. No stringly-typed
   dispatch, no runtime type errors for well-formed commands.

3. **Core before surface area.** The connection, protocol, and service layers must
   be solid before expanding to 500+ commands. A small correct client is more
   useful than a large fragile one.

4. **Feature-gated layers.** Cluster, Sentinel, modules, TLS backends -- these are
   opt-in via Cargo features. The default build is minimal: single-node, plaintext,
   core commands only.

5. **Workspace separation.** Concerns that can stand alone should be separate
   crates. The parser, the command definitions, and the client runtime have
   different change velocities and different downstream consumers.

## Architecture

### Layer Diagram

```
                    User Code
                       |
               tower::ServiceBuilder
              (Timeout, Retry, CircuitBreaker, Buffer, ...)
                       |
                 +-----------+
                 |  Service  |  <-- tower::Service<Cmd>
                 +-----------+
                       |
              +------------------+
              |  RedisConnection |  core connection, owns the codec
              +------------------+
                       |
                 +----------+
                 |  Codec   |  tokio_util::codec::{Encoder, Decoder}
                 +----------+
                       |
                 +----------+
                 |  Parser  |  RESP2/RESP3 wire protocol
                 +----------+
                       |
                    TCP / TLS / Unix
```

### Workspace Structure

```
redis-tower/
  Cargo.toml                  # workspace root

  crates/
    redis-tower-protocol/     # Frame, Parser, Serializer, Codec
      src/
        frame.rs              # Frame enum (RESP2 + RESP3 types)
        parser.rs             # zero-copy streaming parser
        serializer.rs         # Frame -> wire bytes
        codec.rs              # tokio Encoder/Decoder adapter
        error.rs

    redis-tower-core/         # Command trait, connection, Service impl
      src/
        command.rs            # Command trait
        connection.rs         # RedisConnection (Service impl)
        stream.rs             # RedisStream (TCP/TLS/Unix abstraction)
        config.rs             # ClientConfig builder
        url.rs                # redis:// URL parsing
        error.rs
        tcp.rs                # socket tuning
        tls.rs                # TLS backends (feature-gated)

    redis-tower-commands/     # typed command structs (depends on protocol + core)
      src/
        strings.rs
        hashes.rs
        lists.rs
        sets.rs
        sorted_sets.rs
        keys.rs
        server.rs
        connection.rs
        ...                   # add incrementally

    redis-tower/              # "batteries included" facade crate
      src/
        lib.rs                # re-exports from core, commands
        client.rs             # RedisClient (ergonomic wrapper)
        resilient.rs          # ResilientRedisClient (auto-reconnect)
        pool.rs               # ConnectionPool
        pipeline.rs           # Pipeline builder
        transaction.rs        # MULTI/EXEC
        health.rs             # health checking
        hooks.rs              # lifecycle callbacks
        metrics.rs            # command/connection metrics
        tracing.rs            # tracing integration
        pubsub.rs             # Pub/Sub mode
        streaming.rs          # SCAN/XREAD iterators

    redis-tower-cluster/      # cluster routing (feature-gated from facade)
    redis-tower-sentinel/     # sentinel discovery (feature-gated from facade)
    redis-tower-modules/      # Redis Stack module commands (bloom, json, search, ...)
```

The key insight: `redis-tower-protocol` and `redis-tower-core` should be small,
stable, and independently useful. Someone building a custom Redis tool can depend
on just the protocol crate. Someone building a minimal client can use core without
pulling in 500 command structs.

### The Command Trait

```rust
pub trait Command: Send + 'static {
    type Response;

    /// Serialize this command to a RESP frame for the wire.
    fn to_frame(&self) -> Frame;

    /// Parse a RESP response frame into the typed response.
    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError>;
}
```

This trait lives in `redis-tower-core` and depends only on `Frame` from
`redis-tower-protocol`. Command structs live in `redis-tower-commands`.

Open question: should `parse_response` take `&self` instead of being an
associated function? Taking `&self` would allow response parsing to depend on
command parameters (e.g., `LCS` with vs. without `IDX` returns different
shapes). The v1 design used an associated function, which made the LCS IDX case
awkward. Leaning toward `&self` for v2.

Open question: should there be a `fn name(&self) -> &str` method for
observability (metrics keys, tracing spans) without needing `std::any::type_name`?

### The Service Implementation

```rust
impl<Cmd: Command> Service<Cmd> for RedisConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        // ...
    }
}
```

**The `poll_ready` problem.** In v1, `poll_ready` always returns `Ready` because
the connection is behind `Arc<Mutex<>>` and actual serialization happens inside
`call()`. This breaks Tower's backpressure contract -- a `Buffer` layer can't
know the connection is saturated.

Options for v2:

1. **Keep it, document it.** `poll_ready` is `Ready`, wrap with `Buffer` for
   backpressure. Simple, pragmatic. This is what v1 does.

2. **Use a channel internally.** Connection owns a background task with an mpsc
   channel. `poll_ready` checks channel capacity. More correct, but adds
   complexity and a task per connection.

3. **Require `&mut self`.** Drop the `Arc<Mutex<>>`, make `RedisConnection`
   non-Clone. Users who need sharing use `Buffer` (which handles the `&mut`
   requirement). Most correct Tower usage, but less ergonomic for simple cases.

Leaning toward option 3 for the core `RedisConnection` and providing the
ergonomic `RedisClient` wrapper that uses `Buffer` internally for the common
case.

### Connection and Stream

```rust
pub struct RedisConnection {
    framed: Framed<RedisStream, RespCodec>,
}

pub enum RedisStream {
    Tcp(TcpStream),
    Unix(UnixStream),
    #[cfg(feature = "tls-native-tls")]
    NativeTls(Box<tokio_native_tls::TlsStream<TcpStream>>),
    #[cfg(feature = "tls-rustls")]
    Rustls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}
```

`RedisStream` implements `AsyncRead + AsyncWrite` so it works with
`tokio_util::codec::Framed`. TLS variants are feature-gated so the enum
doesn't pull in TLS dependencies by default.

### Protocol: Frame and Codec

```rust
pub enum Frame {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Frame>>),
    Null,
    // RESP3 extensions (feature-gated or always present TBD)
    Double(f64),
    Boolean(bool),
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
    Push(Vec<Frame>),
    BigNumber(Bytes),
    VerbatimString { encoding: Bytes, data: Bytes },
}
```

The parser is zero-copy, using `bytes::Bytes` throughout. The codec
(`RespCodec`) adapts the parser for Tokio's `Framed` transport.

v1 observation: the parser (originally from `resp-parser-rs`) is solid and
well-tested. It should be extracted cleanly into `redis-tower-protocol` as the
foundation. The benchmarks show 4.8-8.0 GB/s throughput -- this is not the
bottleneck.

### Resilience Stack

Rather than building resilience into the client, compose it from Tower layers:

```rust
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;
use tower::buffer::BufferLayer;
use tower_resilience::{
    circuit_breaker::CircuitBreakerLayer,
    retry::RetryLayer,
};

let connection = RedisConnection::connect("localhost:6379").await?;

let client = ServiceBuilder::new()
    .layer(BufferLayer::new(64))                         // backpressure + cloneable
    .layer(TimeoutLayer::new(Duration::from_secs(5)))    // per-command timeout
    .layer(CircuitBreakerLayer::new(5, Duration::from_secs(30)))
    .layer(RetryLayer::new(ExponentialBackoff::default()))
    .service(connection);
```

The `ResilientRedisClient` from v1 (auto-reconnect, health checks, hooks) is
still valuable as a batteries-included option, but it should be a composition of
these layers rather than a monolithic implementation.

### Connection Pool

v1 built a full connection pool with dynamic scaling, validation strategies,
reaper threads, and wait queues. This is a lot of code (39KB).

For v2, consider whether `tower::buffer::Buffer` + `tower::balance` gives us
enough pooling for free. A `Buffer` gives us a queue of requests over a single
connection. A `Balance` over multiple `Buffer`ed connections gives us a pool
with load balancing. This is more Tower-idiomatic than a custom pool.

If custom pooling is still needed, it should be a separate layer that produces
`Service` instances, not a bespoke connection manager.

### Pipeline and Transaction

**Pipeline:** Batch multiple commands into a single write, read all responses.
This is a protocol-level optimization (fewer syscalls, better throughput).
It operates below the Service layer -- it's a method on the connection that
takes a `Vec<Frame>` and returns a `Vec<Frame>`.

**Transaction:** MULTI/EXEC wraps a pipeline in atomicity guarantees. Same
level as pipeline, with added MULTI prefix and EXEC suffix.

Both should be available on `RedisConnection` directly. The v1 implementation
works well here; the main improvement is better type safety for transaction
results.

### Pub/Sub

Pub/Sub fundamentally changes the connection's behavior -- it stops being
request/response and becomes a message stream. v1 handled this with a separate
`PubSubConnection` type, which is the right call. A Pub/Sub connection is not
a `Service` -- it's a `Stream<Item = Message>`.

### Cluster and Sentinel

These are significant complexity and should be separate workspace crates that
depend on `redis-tower-core`:

- **Cluster:** Slot-based routing, MOVED/ASK redirect handling, topology
  discovery. Wraps multiple connections and implements `Service<Cmd>` with
  routing logic.

- **Sentinel:** Master discovery, failover detection. Wraps a connection with
  automatic reconnection to the current master.

Both were implemented in v1 but were hard to test and maintain alongside the
core. Workspace separation makes them independently developable and testable.

## What Worked in v1

- **The Command trait.** Simple, effective, good ergonomics. Builder patterns
  for complex commands (Set with EX/NX/GET options) work well.

- **The parser.** Fast, correct, well-tested. Handles RESP2 and RESP3.
  Worth preserving as-is.

- **Tower integration concept.** Being a `Service` is genuinely useful. The
  composability story is the whole reason this project exists.

- **Feature gating.** Keeping cluster, sentinel, and modules behind features
  keeps the default build lean.

- **URL parsing.** Small but important for usability.

- **Typed responses.** `Get` returning `Option<Bytes>` instead of a dynamic
  value type catches real bugs at compile time.

## What to Change in v2

### 1. Workspace from day one

v1 was a single crate that grew to ~50 source files and 518 command structs.
This made compilation slow, testing expensive, and changes risky. Splitting
into protocol/core/commands/client crates from the start keeps each piece
small and focused.

### 2. Commands are not the product

v1 raced to 518 commands (100% Redis coverage) before the connection layer
was battle-tested. For v2, ship core with maybe 20-30 essential commands
(GET, SET, DEL, EXPIRE, PING, SELECT, AUTH, SUBSCRIBE, etc.) and grow the
command set based on actual usage.

### 3. Fix the Service contract

The `poll_ready` always-ready design is a known compromise. For v2, make
`RedisConnection` require `&mut self` (standard Tower pattern) and provide
`Buffer`-wrapped variants for the shared/cloneable use case.

### 4. Simplify the pool

Evaluate whether `tower::buffer::Buffer` + `tower::balance` eliminates the
need for a custom pool. If not, build a much simpler pool -- fixed size,
checkout/return, health check on checkout. The dynamic scaling, reaper
threads, and multiple validation strategies in v1 added complexity without
proven demand.

### 5. Resilience as composition, not integration

v1's `ResilientRedisClient` baked in reconnection, health checks, hooks,
and metrics. For v2, these should be Tower layers that any user can compose.
Provide a convenience constructor that assembles the recommended stack, but
don't hide the composition.

### 6. parse_response should take &self

Allow response parsing to depend on command configuration. This fixes the
LCS IDX problem and similar cases where the response shape depends on the
command's options.

### 7. Error types per crate

Each workspace crate should have its own error type. `redis-tower-protocol`
has parse errors. `redis-tower-core` has connection errors. The facade crate
ties them together. v1 had a single `RedisError` enum that grew unwieldy.

## Build Phases

### Phase 1: Protocol

Extract and clean up the parser into `redis-tower-protocol`. This crate has
no async dependencies -- just `bytes` and the parser. It provides `Frame`,
`Parser`, `RespCodec`, and serialization. It should have excellent tests and
benchmarks from v1 to carry forward.

Deliverable: `redis-tower-protocol` on crates.io, independently useful for
anyone building Redis tooling.

### Phase 2: Core

Build `redis-tower-core` with `RedisConnection`, the `Command` trait,
`RedisStream`, config, URL parsing, and TLS support. Implement `Service<Cmd>`
with proper `poll_ready` semantics. Include a handful of essential commands
(GET, SET, DEL, PING, AUTH, SELECT) for testing and bootstrapping.

Deliverable: a working Redis client that's a proper Tower service. Can be
wrapped with any Tower middleware.

### Phase 3: Client Ergonomics

Build the `redis-tower` facade crate with `RedisClient` (Buffer-wrapped for
easy cloning), pipeline, transaction, pub/sub, and the convenience resilient
client. This is where health checks, hooks, metrics, and tracing live.

Deliverable: the crate most users depend on. Ergonomic, batteries-included,
but built on composable pieces.

### Phase 4: Commands

Expand `redis-tower-commands` incrementally. Prioritize by usage frequency:
strings, hashes, lists, sets, sorted sets, keys, server. Streams, geo,
hyperloglog, bitmap come later. Module commands (bloom, json, search) are
separate feature-gated modules.

### Phase 5: Cluster and Sentinel

Build as separate workspace crates once the core is stable. These have their
own test infrastructure (docker-compose clusters) and release cadence.

## Open Questions

- **RESP3 by default?** v1 supports both RESP2 and RESP3. Should v2 default
  to RESP3 (HELLO 3) and support RESP2 as fallback? Redis 6+ supports RESP3.
  RESP3 gives us typed responses (doubles, booleans, maps) without guessing.

- **Connection multiplexing?** v1 is one-request-at-a-time per connection.
  Redis supports pipelining at the protocol level -- should the connection
  multiplex automatically (send commands without waiting for responses)?
  This would improve throughput significantly but adds complexity.

- **Derive macro for commands?** A proc macro could generate Command impls
  from a declarative spec, reducing boilerplate. Risk: macros are hard to
  debug and maintain. Maybe start manual and add a macro later if the pattern
  is stable.

- **no_std protocol crate?** If `redis-tower-protocol` avoids std, it could
  be used in embedded Redis tooling. Probably not worth the effort unless
  there's demand, but worth considering in API design.

- **Async traits vs. boxed futures?** With Rust 2024 edition and RPITIT
  stabilized, `Service` could potentially use `impl Future` instead of
  `Pin<Box<dyn Future>>`. Check if Tower 0.5 supports this or if it's
  Tower 0.6 territory.
