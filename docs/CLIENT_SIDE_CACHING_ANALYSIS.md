# Client-Side Caching Analysis

## Overview

Redis client-side caching is a server-assisted feature that enables clients to cache responses locally and receive invalidation notifications when cached keys are modified.

## Requirements

### Core Prerequisites
1. **RESP3 Protocol** - Required for push message support
2. **CLIENT TRACKING** command - Enable/disable tracking on server
3. **Push Message Handling** - Non-request/response pattern for invalidations
4. **Local Cache Storage** - LRU with TTL support
5. **Cache Invalidation Logic** - Handle server invalidation messages

## How It Works

### 1. Tracking Modes

Redis supports two tracking modes:

**RESP3 Mode (In-band)**:
- Client uses RESP3 protocol
- Invalidation messages sent as push messages on same connection
- Simpler architecture - single connection handles both requests and invalidations

**RESP2 Mode (Out-of-band)**:
- Client uses RESP2 protocol
- Requires separate Pub/Sub connection for invalidation messages
- More complex - two connections to coordinate

### 2. Message Flow

```
Client                    Redis Server
  |                            |
  |-- CLIENT TRACKING ON ---->|  Enable tracking
  |<-------- +OK -------------|
  |                            |
  |------- GET key1 --------->|  Track key1 for this client
  |<------ "value1" ----------|
  |                            |
  [Cache: key1 -> "value1"]    |
  |                            |
  |------- GET key1 --------->|  (could serve from cache)
  |<------ "value1" ----------|
  |                            |
                               |  Another client: SET key1 "new"
  |<-- Push: Invalidate -------|  Server sends invalidation
  |    [key1]                  |
  |                            |
  [Clear cache: key1]          |
  |                            |
  |------- GET key1 --------->|  Cache miss - fetch from server
  |<------ "new" --------------|
```

### 3. Push Message Format (RESP3)

Invalidation messages arrive as:
```
>2
$10
invalidate
*1
$4
key1
```

Parsed as:
```rust
Value::Push {
    kind: PushKind::Invalidate,
    data: vec![
        Value::Array(vec![
            Value::BulkString("key1".into())
        ])
    ]
}
```

## Architecture Analysis

### redis-rs Implementation

**Key Components**:
1. **CacheManager** - Central cache coordinator
2. **ShardedLRU** - Sharded LRU cache for performance
3. **CacheConfig** - Configuration (mode, size, TTL)
4. **CacheStatistics** - Hit/miss/invalidation tracking

**Cache Modes**:
- `CacheMode::All` - Cache everything (opt-out)
- `CacheMode::OptIn` - Only cache commands with explicit opt-in

**Flow**:
1. Client enables RESP3 protocol
2. Connection configured with CacheConfig
3. On command execution:
   - Check local cache first (hit → return cached value)
   - Miss → send to Redis, cache response with TTL
4. Background: Listen for push messages
5. On invalidation push → remove key from cache

**TTL Logic**:
- Client-side TTL (max time in cache)
- Server-side TTL (fetched via PTTL command)
- Uses minimum of both TTLs

### Challenges for redis-tower

#### 1. **RESP3 Support**
- **Current State**: We support RESP2 via resp-parser
- **Needed**: RESP3 protocol support with push messages
- **Complexity**: Medium - resp-parser may already support RESP3

#### 2. **Async Push Message Handling**
- **Current State**: Request/response pattern only
- **Needed**: Background task listening for push messages
- **Complexity**: High - requires architecture change

Our current connection model:
```rust
pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
```

This is synchronous request/response. We need:
```rust
// Separate receiver for push messages
let (push_tx, push_rx) = mpsc::channel();

// Background task
tokio::spawn(async move {
    loop {
        if let Some(frame) = framed.next().await {
            match frame {
                Frame::Push { kind, data } => {
                    push_tx.send((kind, data)).await;
                }
                Frame::Response => {
                    // Handle normal response
                }
            }
        }
    }
});
```

#### 3. **Connection Multiplexing**
- **Current State**: Single in-flight request per connection
- **Needed**: Multiple in-flight requests + push messages
- **Complexity**: High - requires request/response matching

#### 4. **Tower Integration**
- **Current State**: Tower Service trait is request/response
- **Needed**: Side channel for push messages
- **Complexity**: High - Tower doesn't have built-in push support

#### 5. **Cache Storage**
- **Current State**: None
- **Needed**: Thread-safe LRU cache with TTL
- **Complexity**: Medium - can use existing crates or implement

## Implementation Options

### Option 1: Full Implementation (High Complexity)

**Pros**:
- Complete feature parity with redis-rs
- Best performance (local cache hits)
- Modern RESP3 support

**Cons**:
- Requires significant architecture changes
- Breaks Tower's request/response model
- Complex connection multiplexing
- High development time

**Components Needed**:
1. RESP3 protocol support in codec
2. Connection multiplexing layer
3. Push message router
4. LRU cache with TTL
5. Cache invalidation handler
6. CLIENT TRACKING command
7. Background push message processor

### Option 2: Tower Layer Approach (Medium Complexity)

Use Tower's Service wrapping for caching:

```rust
let cached_client = ServiceBuilder::new()
    .layer(CacheLayer::new(CacheConfig::default()))
    .service(redis_client);
```

**Implementation**:
- Layer wraps Service calls
- Maintains local cache
- On cache miss → call inner service
- Uses polling or separate connection for invalidations

**Pros**:
- Fits Tower architecture
- Composable middleware
- Easier to opt-in/out

**Cons**:
- Still needs push message handling
- May require separate invalidation connection
- More overhead than integrated approach

### Option 3: External Cache (Low Complexity)

Users manage their own cache with helper utilities:

```rust
let cache = RedisCache::new(client.clone(), CacheConfig::default());

// User code
if let Some(value) = cache.get("key1").await? {
    return Ok(value);
}

let value = client.execute(Get::new("key1")).await?;
cache.set("key1", value.clone()).await?;
```

**Pros**:
- Minimal changes to core
- Users control cache behavior
- Simpler implementation

**Cons**:
- Less automatic
- Users must remember to use cache
- Still needs invalidation handling

## Recommendation

Given redis-tower's goals (experimental, Tower-native, type-safe), I recommend:

### **Phased Approach**:

**Phase 1: RESP3 Support** (Foundation)
- Add RESP3 protocol to codec
- Parse push messages correctly
- This unblocks other features (Pub/Sub improvements, etc.)

**Phase 2: Basic Caching** (Experimental)
- Simple LRU cache as Tower Layer
- Polling-based invalidation (CLIENT TRACKING with BCAST)
- Opt-in per command
- Document as experimental

**Phase 3: Full Implementation** (If needed)
- Connection multiplexing
- True push message handling
- Production-ready caching

## Similar Patterns in Ecosystem

- **Tower's load balancing** - Uses Discover trait for dynamic endpoints
- **Tower's rate limiting** - Uses background tasks with channels
- **Tonic (gRPC)** - Handles bidirectional streaming with separate channels

## Next Steps

1. **Check resp-parser RESP3 support** - Does it already handle push messages?
2. **Prototype RESP3 in codec** - Can we parse push frames?
3. **Design cache layer API** - What does the Tower layer look like?
4. **Implement basic LRU** - Local cache without invalidation
5. **Add CLIENT TRACKING** - Command support
6. **Prototype invalidation** - Simplest working version

## References

- Redis Client-Side Caching: https://redis.io/docs/latest/develop/reference/client-side-caching/
- CLIENT TRACKING: https://redis.io/docs/latest/commands/client-tracking/
- redis-rs implementation: `/tmp/redis-rs/redis/src/caching/`
- RESP3 Specification: https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md
