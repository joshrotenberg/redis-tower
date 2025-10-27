# Feature Parity Analysis: redis-tower vs fred.rs and redis-rs

**Analysis Date**: 2025-10-27  
**redis-tower Version**: 0.2.0  
**fred.rs Version**: 10.1.0  
**redis-rs Version**: 0.32.7 (stable), 1.0.0-rc.1 (beta)

## Executive Summary

redis-tower v0.2.0 has achieved **production readiness** with all HIGH priority features implemented. This document identifies remaining gaps compared to fred.rs and redis-rs to guide v0.3.0 and v1.0.0 development.

### Current Status
- ✅ **HIGH Priority**: 5/5 complete (100%)
- ⚠️ **MEDIUM Priority**: 0/8 complete (0%)
- 🟢 **LOW Priority**: 0/12 complete (0%)

### Overall Feature Parity
- **fred.rs**: ~75% parity (missing MEDIUM/LOW features)
- **redis-rs**: ~85% parity (missing fewer critical features)

---

## HIGH Priority Features (COMPLETE)

All critical production features have been implemented in v0.2.0:

| Feature | redis-tower | fred.rs | redis-rs | Status |
|---------|-------------|---------|----------|--------|
| TLS Support | ✅ Both backends | ✅ Both | ✅ Both | ✅ COMPLETE |
| Auto-Reconnect | ✅ tower-resilience | ✅ Built-in | ⚠️ Manager only | ✅ COMPLETE |
| Health Checks | ✅ PING validation | ✅ Built-in | ⚠️ Manager only | ✅ COMPLETE |
| Tracing | ✅ Full tokio-tracing | ✅ Full/Partial | ❌ None | ✅ COMPLETE |
| Metrics | ✅ Comprehensive | ✅ Comprehensive | ❌ None | ✅ COMPLETE |

**Result**: redis-tower matches or exceeds fred.rs/redis-rs on all HIGH priority features.

---

## MEDIUM Priority Features (v0.3.0 Target)

These features would bring redis-tower to near-complete parity with mature clients.

### 1. Client-Side Caching (RESP3 Server-Assisted)

**Status**: ❌ Not Implemented  
**Priority**: 🟡 MEDIUM  
**Complexity**: HIGH  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Full support with CLIENT TRACKING
- redis-rs: ❌ No support (experimental in 1.0.0-rc.1 via cache-aio)
- Lettuce: ✅ Full support

**Implementation Requirements**:
1. RESP3 CLIENT TRACKING command integration
2. Server push notification handling for invalidations
3. Local cache storage (HashMap or LRU cache)
4. Automatic cache invalidation on key modifications
5. Per-connection or global cache options
6. Cache TTL and memory limits

**Benefits**:
- Significant performance improvement for read-heavy workloads
- Reduced Redis server load
- Lower latency for cached values

**Code Changes**:
```rust
// src/cache.rs (NEW)
pub struct ClientCache {
    storage: Arc<Mutex<LruCache<String, Bytes>>>,
    tracking_enabled: bool,
}

// src/client.rs
impl RedisClient {
    pub fn with_cache(cache: ClientCache) -> Self {
        // Enable CLIENT TRACKING
    }
}
```

**References**:
- fred.rs: `src/modules/inner.rs` (tracking implementation)
- Redis docs: https://redis.io/docs/latest/develop/use/client-side-caching/

---

### 2. Auto-Pipelining

**Status**: ❌ Not Implemented (manual pipelining only)  
**Priority**: 🟡 MEDIUM  
**Complexity**: MEDIUM  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Full auto-pipelining support
- redis-rs: ❌ Manual pipelining only
- redis-tower: ⚠️ Manual pipelining via builder

**Implementation Requirements**:
1. Command buffering layer (collect commands for N ms)
2. Automatic flushing on buffer size or timeout
3. Response demultiplexing (route responses to correct futures)
4. Tower middleware for transparent batching
5. Opt-in via builder flag

**Benefits**:
- Automatic performance optimization
- Reduced network round-trips
- No code changes required for users

**Code Changes**:
```rust
// src/middleware/autopipeline.rs (NEW)
pub struct AutoPipelineLayer {
    max_batch_size: usize,
    flush_interval: Duration,
}

impl<S> Layer<S> for AutoPipelineLayer {
    type Service = AutoPipelineService<S>;
    
    fn layer(&self, inner: S) -> Self::Service {
        AutoPipelineService {
            inner,
            buffer: Arc::new(Mutex::new(Vec::new())),
            config: self.clone(),
        }
    }
}
```

**References**:
- fred.rs: `src/protocol/responders.rs` (auto-pipeline implementation)
- Tower batching: tower::buffer::Buffer

---

### 3. Mocking Interface

**Status**: ❌ Not Implemented  
**Priority**: 🟡 MEDIUM  
**Complexity**: MEDIUM  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Built-in mock client via `Mocks` struct
- redis-rs: ❌ No mocking support
- redis-py: ✅ Built-in fakeredis

**Implementation Requirements**:
1. MockRedisClient implementing Service trait
2. Command interception and response injection
3. Assertion helpers for command verification
4. Builder pattern for mock configuration
5. Support for all command types

**Benefits**:
- Testing without Redis instance
- Faster test execution
- Deterministic testing

**Code Changes**:
```rust
// src/mock.rs (NEW)
pub struct MockRedisClient {
    expectations: Vec<MockExpectation>,
    responses: HashMap<String, RespType>,
}

impl MockRedisClient {
    pub fn expect<C: RedisCommand>(cmd: C) -> MockExpectation {
        // Record expected command
    }
    
    pub fn respond_with<C: RedisCommand>(cmd: C, response: C::Response) {
        // Set canned response
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_with_mock() {
        let mut mock = MockRedisClient::new();
        mock.expect(Get::new("key")).respond_with(Some(b"value".to_vec()));
        
        let result = mock.call(Get::new("key")).await.unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }
}
```

**References**:
- fred.rs: `src/clients/mod.rs` (Mocks struct)
- mockall crate: https://crates.io/crates/mockall

---

### 4. Sentinel Authentication

**Status**: ❌ Not Implemented  
**Priority**: 🟡 MEDIUM  
**Complexity**: LOW  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Separate sentinel and Redis auth
- redis-rs: ❌ No sentinel-specific auth
- Jedis: ✅ Full sentinel auth
- Lettuce: ✅ Full sentinel auth

**Implementation Requirements**:
1. Separate `SentinelConfig` with auth fields
2. Use sentinel credentials for sentinel connections
3. Use Redis credentials for master/replica connections
4. Update SentinelClient to handle both auth types

**Benefits**:
- Security: Different credentials for sentinel vs data nodes
- Compliance: Meet security audit requirements
- Flexibility: Separate access control

**Code Changes**:
```rust
// src/sentinel.rs
pub struct SentinelConfig {
    pub sentinel_addresses: Vec<String>,
    pub sentinel_username: Option<String>,
    pub sentinel_password: Option<String>,
    pub redis_username: Option<String>,
    pub redis_password: Option<String>,
    pub master_name: String,
}

impl SentinelClient {
    pub async fn connect(config: SentinelConfig) -> Result<Self, Error> {
        // Use sentinel_* for sentinel connections
        // Use redis_* for master/replica connections
    }
}
```

**References**:
- fred.rs: `src/clients/sentinel.rs` (separate auth)
- Redis Sentinel docs: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/

---

### 5. Error/Reconnect Hooks

**Status**: ⚠️ Partial (reconnect hooks exist via tower-resilience)  
**Priority**: 🟡 MEDIUM  
**Complexity**: LOW  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Full error/reconnect hooks
- redis-rs: ❌ No hooks
- redis-tower: ⚠️ Reconnect hooks via ReconnectPolicy, no error hooks

**Implementation Requirements**:
1. Add error hook callback to ClientConfig
2. Invoke on Redis errors, connection errors, protocol errors
3. Add command retry hook (pre-retry callback)
4. Add connection failure hook
5. Thread-safe callback mechanism

**Benefits**:
- Custom error handling and logging
- Metrics collection on failures
- Alerting integration
- Debug logging

**Code Changes**:
```rust
// src/config.rs
pub struct ClientConfig {
    // ... existing fields
    pub on_error: Option<Arc<dyn Fn(&RedisError) + Send + Sync>>,
    pub on_retry: Option<Arc<dyn Fn(&RedisCommand, u32) + Send + Sync>>,
    pub on_connection_failed: Option<Arc<dyn Fn(&str, &Error) + Send + Sync>>,
}

// src/client.rs
impl RedisConnection {
    async fn handle_error(&self, error: RedisError) {
        if let Some(on_error) = &self.config.on_error {
            on_error(&error);
        }
        // ... existing error handling
    }
}
```

**References**:
- fred.rs: `src/clients/mod.rs` (event listeners)

---

### 6. Dynamic Pool Scaling

**Status**: ⚠️ Basic (fixed pool size per node)  
**Priority**: 🟡 MEDIUM  
**Complexity**: MEDIUM  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Load-based scaling with min/max limits
- redis-rs: ❌ Fixed pool sizes (via r2d2/bb8)
- redis-tower: ⚠️ Fixed pool size (default 3 per node)

**Implementation Requirements**:
1. Monitor pool utilization (active/total connections)
2. Scale up when utilization > threshold (e.g., 80%)
3. Scale down when utilization < threshold (e.g., 20%)
4. Enforce min/max connection limits
5. Exponential backoff for scaling operations
6. Metrics for pool size changes

**Benefits**:
- Efficient resource usage
- Handle traffic spikes automatically
- Reduce idle connections during low load

**Code Changes**:
```rust
// src/connection_pool.rs
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub scale_up_threshold: f32, // 0.0-1.0
    pub scale_down_threshold: f32, // 0.0-1.0
    pub scale_interval: Duration,
}

impl ConnectionPool {
    async fn monitor_and_scale(&mut self) {
        loop {
            tokio::time::sleep(self.config.scale_interval).await;
            
            let utilization = self.active_connections() as f32 / self.total_connections() as f32;
            
            if utilization > self.config.scale_up_threshold {
                self.scale_up().await;
            } else if utilization < self.config.scale_down_threshold {
                self.scale_down().await;
            }
        }
    }
}
```

**References**:
- fred.rs: `src/modules/pool.rs` (dynamic scaling)

---

### 7. Unix Socket Support

**Status**: ❌ Not Implemented  
**Priority**: 🟡 MEDIUM  
**Complexity**: LOW  
**Timeline**: v0.3.0 (Q1-Q2 2026)

**Current State**:
- fred.rs: ✅ Unix socket support
- redis-rs: ✅ Unix socket support
- Lettuce: ✅ Unix domain socket support

**Implementation Requirements**:
1. Parse unix:// URLs in connect string
2. Use tokio::net::UnixStream instead of TcpStream
3. Abstract ConnectionType enum (Tcp vs Unix)
4. Update ClientConfig to handle both types
5. TLS not applicable for Unix sockets

**Benefits**:
- Lower latency for local Redis instances
- No TCP overhead
- Common deployment pattern for sidecar Redis

**Code Changes**:
```rust
// src/client.rs
pub enum ConnectionType {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl RedisClient {
    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let stream = if addr.starts_with("unix://") {
            let path = &addr[7..];
            ConnectionType::Unix(UnixStream::connect(path).await?)
        } else {
            ConnectionType::Tcp(TcpStream::connect(addr).await?)
        };
        
        // ... rest of connection setup
    }
}
```

**References**:
- redis-rs: `src/connection.rs` (unix socket handling)
- tokio::net::UnixStream

---

### 8. Multiple Async Runtime Support

**Status**: ❌ Tokio Only  
**Priority**: 🟢 LOW (but mentioned for completeness)  
**Complexity**: HIGH  
**Timeline**: v1.0.0 (Future)

**Current State**:
- fred.rs: ✅ Tokio only
- redis-rs: ✅ Tokio, async-std, smol via feature flags

**Implementation Requirements**:
1. Abstract runtime traits (Spawn, Sleep, TcpStream)
2. Feature flags for each runtime
3. Conditional compilation for runtime-specific code
4. Test coverage for all runtimes

**Benefits**:
- Flexibility for users with non-Tokio codebases
- Embedded systems (smol)

**Decision**: Deferred to v1.0.0 - Tokio is dominant async runtime.

---

## LOW Priority Features (v1.0.0+ Target)

These features are nice-to-have but not critical for feature parity.

### 9. MONITOR Command Support

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: LOW

**Current State**:
- fred.rs: ✅ Full MONITOR support with streaming
- redis-rs: ❌ No MONITOR support
- Jedis: ✅ MONITOR support

**Use Case**: Debugging, log analysis, traffic monitoring

---

### 10. Custom DNS Resolution

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: MEDIUM

**Current State**:
- fred.rs: ✅ Custom DNS resolver support
- redis-rs: ❌ Uses system DNS

**Use Case**: Service discovery, k8s headless services, custom routing

---

### 11. Streaming API

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: MEDIUM

**Current State**:
- Lettuce: ✅ Reactive streams for large datasets

**Use Case**: Stream large SCAN results, large list/set operations

---

### 12. Blocking Encoding

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: LOW

**Current State**:
- fred.rs: ✅ Blocking encoding for CPU-bound serialization

**Use Case**: Offload JSON encoding to blocking threads

---

### 13. Keyspace Event Notifications

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: LOW

**Current State**:
- fred.rs: ✅ Keyspace event support
- Lettuce: ✅ Keyspace event support

**Use Case**: React to key changes (expiration, deletion, modification)

---

### 14. Credential Provider Pattern

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: LOW

**Current State**:
- fred.rs: ✅ Dynamic credential provider
- Jedis: ✅ Token-based auth

**Use Case**: AWS IAM, token rotation, dynamic secrets

---

### 15. TCP Configuration Options

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: LOW

**Current State**:
- fred.rs: ✅ TCP nodelay, keepalive, user timeout
- redis-rs: ✅ TCP configuration

**Use Case**: Fine-tune network performance

---

### 16. BigInt Support

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: LOW

**Current State**:
- redis-rs: ✅ BigInt support for large numbers

**Use Case**: Handle integers larger than i64

---

### 17. Custom Codecs

**Status**: ❌ Not Implemented  
**Priority**: 🟢 LOW  
**Complexity**: MEDIUM

**Current State**:
- Lettuce: ✅ Custom codec support

**Use Case**: Custom serialization formats (MessagePack, CBOR, etc.)

---

## Feature Parity Summary

### vs fred.rs 10.1.0

| Category | redis-tower | fred.rs | Gap |
|----------|-------------|---------|-----|
| **Core Protocol** | ✅ Complete | ✅ Complete | None |
| **Connection Mgmt** | ✅ Complete | ✅ Complete | None |
| **HIGH Priority** | ✅ 5/5 | ✅ 5/5 | None |
| **MEDIUM Priority** | ❌ 0/8 | ✅ 6/8 | 6 features |
| **LOW Priority** | ❌ 0/9 | ⚠️ 4/9 | 5 features |
| **Type Safety** | ✅ Superior | ⚠️ Good | redis-tower ahead |
| **Tower Integration** | ✅ Unique | ❌ None | redis-tower ahead |

**Overall Parity**: ~75% (missing MEDIUM/LOW features)

**Key Gaps**:
- Client-side caching
- Auto-pipelining (fred.rs unique among Rust clients)
- Mocking interface
- MONITOR support
- Custom DNS resolution

---

### vs redis-rs 0.32.7

| Category | redis-tower | redis-rs | Gap |
|----------|-------------|----------|-----|
| **Core Protocol** | ✅ Complete | ✅ Complete | None |
| **Connection Mgmt** | ✅ Superior | ⚠️ Basic | redis-tower ahead |
| **HIGH Priority** | ✅ 5/5 | ⚠️ 2/5 | redis-tower ahead |
| **MEDIUM Priority** | ❌ 0/8 | ❌ 1/8 | Similar |
| **LOW Priority** | ❌ 0/9 | ⚠️ 2/9 | redis-rs ahead |
| **Type Safety** | ✅ Superior | ⚠️ Weak | redis-tower ahead |
| **Tower Integration** | ✅ Unique | ❌ None | redis-tower ahead |

**Overall Parity**: ~85% (ahead on HIGH priority, behind on LOW priority)

**Key Gaps**:
- Unix sockets (redis-rs has)
- Multiple async runtime support (redis-rs has)
- BigInt support (redis-rs has)

**redis-tower Advantages**:
- Better connection management (health checks, auto-reconnect)
- Tracing and metrics (redis-rs has none)
- Type safety (redis-rs is stringly-typed)

---

## Prioritized Roadmap

### v0.3.0 (Q1-Q2 2026) - Feature Parity
**Goal**: Close MEDIUM priority gaps with fred.rs

**Must Have**:
1. ✅ Client-side caching (RESP3)
2. ✅ Auto-pipelining
3. ✅ Mocking interface
4. ✅ Sentinel authentication

**Should Have**:
5. ✅ Error/reconnect hooks
6. ✅ Dynamic pool scaling
7. ✅ Unix socket support

**Estimated Effort**: 6-8 weeks (1-2 weeks per feature)

---

### v1.0.0 (Q3-Q4 2026) - Feature Complete
**Goal**: Address LOW priority gaps, polish

**Nice to Have**:
- MONITOR command support
- Custom DNS resolution
- Streaming API
- Blocking encoding
- Keyspace events
- Credential provider
- TCP configuration
- BigInt support
- Custom codecs

**Estimated Effort**: 4-6 weeks

---

## Implementation Order (Recommended)

### Phase 1: Quick Wins (2-3 weeks)
1. **Sentinel Authentication** (1 week) - Low complexity, high value
2. **Error Hooks** (1 week) - Low complexity, completes reconnect hooks
3. **Unix Sockets** (1 week) - Low complexity, common use case

### Phase 2: Core Features (3-4 weeks)
4. **Mocking Interface** (1.5 weeks) - Medium complexity, high testing value
5. **Dynamic Pool Scaling** (1.5 weeks) - Medium complexity, performance benefit
6. **Auto-Pipelining** (2 weeks) - Medium complexity, significant performance win

### Phase 3: Advanced Features (2-3 weeks)
7. **Client-Side Caching** (2-3 weeks) - High complexity, major performance improvement

**Total Timeline**: 7-10 weeks for v0.3.0

---

## Risk Assessment

### Low Risk (Proven Patterns)
- Sentinel authentication (straightforward auth flow)
- Error hooks (callbacks pattern)
- Unix sockets (standard Tokio API)
- Mocking interface (Tower Service trait makes this clean)

### Medium Risk (Complexity)
- Dynamic pool scaling (requires careful monitoring logic)
- Auto-pipelining (response demultiplexing is tricky)

### High Risk (Significant Effort)
- Client-side caching (RESP3 push notifications, cache invalidation logic)

**Mitigation**:
- Start with low-risk features to build momentum
- Reference fred.rs implementations for complex features
- Comprehensive testing for medium/high risk features
- Feature flags for experimental features

---

## Success Metrics

### v0.3.0 Success Criteria
- ✅ 7/8 MEDIUM priority features implemented
- ✅ Feature parity with fred.rs reaches 90%+
- ✅ All features have integration tests
- ✅ Performance benchmarks show <10% overhead vs fred.rs
- ✅ Documentation complete for all new features

### v1.0.0 Success Criteria
- ✅ 100% feature parity with fred.rs on critical features
- ✅ Production adoption by 5+ projects
- ✅ Performance within 5% of fred.rs
- ✅ Complete API stability (no more breaking changes)

---

## Conclusion

**redis-tower v0.2.0 is production-ready** with all HIGH priority features complete. The path to v0.3.0 is clear:

**Strengths (Maintain)**:
- ✅ Superior type safety
- ✅ Unique Tower integration
- ✅ Excellent tracing and metrics
- ✅ Production-ready core features

**Gaps (Address in v0.3.0)**:
- Client-side caching
- Auto-pipelining
- Mocking interface
- Dynamic pool scaling
- Unix sockets

**Estimated Timeline**: 7-10 weeks to feature parity with fred.rs

**Recommendation**: Proceed with v0.3.0 implementation following the phased approach (quick wins → core features → advanced features).
