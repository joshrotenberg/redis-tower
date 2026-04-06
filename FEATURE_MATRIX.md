# Redis Client Feature Matrix

Comparative analysis of 8 popular Redis client libraries, compiled to inform
the design and feature roadmap of `redis-tower`.

**Libraries analyzed** (source snapshots in `tmp/`):

| Library | Language | Version | MSRV / Runtime |
|---------|----------|---------|----------------|
| **redis-rs** | Rust | latest | MSRV 1.85 |
| **fred.rs** | Rust | 10.1.0 | MSRV 1.75 |
| **go-redis** | Go | 9.18.0 | Go 1.24 |
| **jedis** | Java | latest | Java 8+ |
| **lettuce** | Java | latest | Java 8+ (Netty) |
| **node-redis** | TypeScript | 5.11.0 | Node 18+ |
| **redis-py** | Python | 7.3.0 | Python 3.10+ |
| **redis_client_ex** | Elixir | 0.6.0 | Elixir 1.17 / OTP 27 |

---

## 1. Connection Types

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Standalone | Y | Y | Y | Y | Y | Y | Y | Y |
| Cluster | Y | Y | Y | Y | Y | Y | Y | Y |
| Sentinel | Y | Y | Y | Y | Y | Y | Y | Y |
| Master/Replica | - | Y | Y | - | Y | - | - | - |
| Unix Socket | Y | Y | Y | Y | Y | Y | Y | Y |
| TLS (native-tls) | Y | Y | - | Y | - | - | - | - |
| TLS (rustls) | Y | Y | - | - | - | - | - | - |
| TLS (platform) | - | - | Y | Y | Y | Y | Y | Y |
| Multi-DB Failover | - | - | - | Y | - | - | Y | - |

**Key takeaway**: All libraries support the core trio (standalone, cluster, sentinel) plus TLS
and unix sockets. Master/replica routing is a differentiator in fred.rs, go-redis, and lettuce.
Jedis and redis-py have experimental multi-database failover with circuit breakers.

---

## 2. Protocol

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| RESP2 | Y | Y | Y | Y | Y | Y | Y | Y |
| RESP3 | Y | Y | Y (default) | Y | Y | Y | Y | Y (default) |
| Pipelining | Y | Y | Y | Y | Y | Y (auto) | Y | Y |
| MULTI/EXEC | Y | Y | Y | Y | Y | Y | Y | Y |
| WATCH | Y | Y | Y | Y | Y | Y | Y | Y (auto-retry) |
| Pub/Sub | Y | Y | Y | Y | Y | Y | Y | Y |
| Sharded Pub/Sub | Y | - | Y | Y | - | Y | - | Y |
| Streams | Y | Y | Y | Y | Y | Y | Y | Y |
| Lua Scripting | Y | Y | Y | Y | Y | Y | Y | Y |
| Functions (FCALL) | - | - | Y | Y | Y | Y | Y | - |
| RESP3 Push | Y | Y | Y | Y | Y | Y | Y | Y |

**Key takeaway**: Universal RESP2/RESP3, pipelining, transactions, pub/sub, streams,
and scripting support. FCALL (Redis Functions) is notably absent from both Rust clients.
Sharded pub/sub coverage varies. node-redis does auto-pipelining within event loop ticks.
redis_client_ex has automatic WATCH retry with configurable attempts.

---

## 3. Command Coverage

| Command Group | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Strings | Y | Y | Y | Y | Y | Y | Y | Y |
| Hashes | Y | Y | Y | Y | Y | Y | Y | Y |
| Lists | Y | Y | Y | Y | Y | Y | Y | Y |
| Sets | Y | Y | Y | Y | Y | Y | Y | Y |
| Sorted Sets | Y | Y | Y | Y | Y | Y | Y | Y |
| HyperLogLog | Y | Y | Y | Y | Y | Y | Y | Y |
| Geo | Y | Y | Y | Y | Y | Y | Y | Y |
| Bitmaps | Y | Y | Y | Y | Y | Y | Y | Y |
| Streams | Y | Y | Y | Y | Y | Y | Y | Y |
| ACL | Y | Y | Y | Y | Y | Y | Y | Y |
| Cluster mgmt | Y | Y | Y | Y | Y | Y | Y | Y |
| Server/Admin | Y | Y | Y | Y | Y | Y | Y | Y |
| Hash Field TTL | Y | Y | - | - | - | Y | - | - |
| Vector Sets | Y | - | Y | Y | Y | Y | Y | - |

**Key takeaway**: All 8 libraries have comprehensive coverage of core data structures.
Differentiators are in newer Redis 8.0+ features like vector sets and hash field expiration.

---

## 4. Redis Stack / Module Support

| Module | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|--------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| JSON | Y | Y | Y | Y | Y | Y | Y | Y |
| Search (FT) | partial | Y | Y | Y | Y | Y | Y | Y |
| TimeSeries | - | Y | Y | Y | - | Y | Y | Y |
| Bloom Filter | - | - | Y | Y | - | Y | Y | Y |
| Cuckoo Filter | - | - | Y | Y | - | Y | Y | Y |
| Count-Min Sketch | - | - | Y | Y | - | Y | Y | Y |
| Top-K | - | - | Y | Y | - | Y | Y | Y |
| T-Digest | - | - | Y | Y | - | Y | Y | Y |
| Vector Sets | Y | - | Y | Y | Y | Y | Y | - |

**Key takeaway**: go-redis, jedis, node-redis, redis-py, and redis_client_ex have the
broadest Redis Stack coverage. The Rust clients (redis-rs, fred.rs) lag behind significantly
on probabilistic data structures. Lettuce covers JSON, Search, and Vector Sets but skips
TimeSeries and probabilistic types. **This is a major opportunity for redis-tower.**

---

## 5. Async / Execution Models

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Sync (blocking) | Y | - | Y | Y | Y | - | Y | Y |
| Async (futures) | Y | Y | - | - | Y | Y | Y | Y |
| Reactive (streams) | - | - | - | - | Y | - | - | - |
| Runtime: Tokio | Y | Y | - | - | - | - | - | - |
| Runtime: Smol | Y | - | - | - | - | - | - | - |
| Runtime: Glommio | - | Y (exp) | - | - | - | - | - | - |
| Auto-pipelining | - | - | - | - | - | Y | - | - |

**Key takeaway**: redis-rs is the only Rust client offering both sync and async.
fred.rs is async-only. Lettuce uniquely offers reactive (Project Reactor) streams.
node-redis has automatic pipelining within event loop ticks.

---

## 6. Connection Management

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Connection Pool | Y (r2d2, bb8) | Y (built-in) | Y (built-in) | Y (Commons Pool) | Y (Commons Pool) | Y (built-in) | Y (built-in) | Y (built-in) |
| Multiplexed conn | Y | - | - | - | - | - | - | - |
| Dynamic pool | - | Y | - | - | - | - | - | - |
| Auto-reconnect | Y | Y | Y | Y | Y | Y | Y | Y |
| Exponential backoff | Y | Y | Y | - | Y | Y | Y | Y |
| Configurable jitter | Y | Y | Y | - | - | Y | Y | - |
| Multiple backoff strategies | - | Y (3) | - | - | - | - | Y (7) | - |
| Circuit breaker | - | - | - | Y | - | - | Y | Y (opt) |
| Rate limiter | - | - | Y | - | - | - | - | Y (opt) |

**Key takeaway**: redis-rs uniquely offers multiplexed connections (many commands over one
socket). fred.rs has dynamic pool scaling. redis-py has the richest backoff strategy
selection (7 strategies). Circuit breakers are found in jedis, redis-py, and redis_client_ex.

---

## 7. Client-Side Caching

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Server-assisted (RESP3) | Y | Y | partial | Y | Y | Y | Y | Y |
| Built-in local cache | Y (LRU) | - | - | Y (LRU) | Y | Y (LRU/FIFO) | Y (TTL/LRU) | Y (ETS, LRU/FIFO) |
| Configurable TTL | Y | - | - | - | - | Y | Y | Y |
| Configurable max entries | Y | - | - | Y | - | Y | Y | Y |
| Cache statistics | Y | - | - | Y | - | - | Y | Y |
| Pluggable backend | - | - | - | - | Y | - | Y | Y |

**Key takeaway**: redis-rs has solid built-in caching with LRU. fred.rs exposes CLIENT
TRACKING but leaves cache storage to the user. redis_client_ex, redis-py, and node-redis
have the most configurable caching implementations with multiple eviction policies.

---

## 8. Observability & Instrumentation

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Tracing (spans) | - | Y (tracing) | - | - | Y (Brave) | - | - | - |
| Metrics | - | Y (built-in) | Y (OTel) | - | Y (Micrometer) | Y (OTel) | Y (OTel) | - |
| OpenTelemetry | - | - | Y | - | - | Y | Y | - |
| Telemetry events | - | - | - | - | - | Y (diag_channel) | - | Y (:telemetry) |
| Hooks/interceptors | - | - | Y | - | Y | - | Y | - |
| Command logging | - | Y (network-logs) | - | - | - | - | - | - |

**Key takeaway**: Observability is a mixed bag. go-redis, node-redis, and redis-py have
OpenTelemetry integration. Lettuce has deep Micrometer/Brave integration. fred.rs has
built-in metrics and tracing crate support. **Neither Rust client has OpenTelemetry support
-- another opportunity for redis-tower with its Tower middleware approach.**

---

## 9. Middleware / Extensibility

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Hook/middleware system | - | - | Y | - | Y | - | Y | - |
| Custom command support | Y | Y | Y | Y | Y | Y | Y | Y |
| Custom codec/serializer | Y | Y | Y | Y | Y | Y | Y | - |
| Mock support | - | Y | - | Y | - | - | - | Y |
| Dynamic commands | Y | - | - | Y | Y | - | - | - |

**Key takeaway**: go-redis has the cleanest hook system (DialHook, ProcessHook,
ProcessPipelineHook). This is where redis-tower's Tower middleware architecture is a
**major differentiator** -- composable Service layers for retry, timeout, metrics, etc.
are more powerful than any hook system in the current landscape.

---

## 10. Authentication

| Feature | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Password auth | Y | Y | Y | Y | Y | Y | Y | Y |
| ACL (user + pass) | Y | Y | Y | Y | Y | Y | Y | Y |
| Credential provider | Y | - | Y | Y | Y | Y | Y | Y (MFA tuple) |
| Token rotation | - | - | Y | Y | - | - | Y | - |
| Azure Entra ID | Y | - | Y | Y | - | Y | - | - |

---

## 11. Testing Comparison

| Metric | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|--------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Test files | 22 | 29 | 156 | 376 | 528 | 546 | 127 | 66 |
| Test LOC | ~19K | ~8K | ~36K | large | large | large | ~86K | ~12K |
| Unit tests | Y | Y | Y | Y | Y | Y | Y | Y |
| Integration tests | Y | Y | Y | Y | Y | Y | Y | Y |
| Property-based | - | - | - | - | - | - | - | Y |
| Mock support | Y | Y | - | Y | - | - | Y | Y |
| CI platform | GitHub Actions | CircleCI | GitHub Actions | GitHub Actions | GitHub Actions | GitHub Actions | GitHub Actions | GitHub Actions |
| Redis versions tested | 6.2-8.6 + Valkey | 6.2+ | 8.0-8.6 | 7.2-8.6 | 7.2-8.8 | 7.4-8.6 | 7.2-8.8 | 7, 8, Stack |
| Multi-protocol (R2/R3) | Y | Y | - | - | - | - | Y | - |
| Benchmarks | Y (criterion) | - | Y | Y | Y | - | - | Y (benchee) |
| Coverage reporting | Y | - | - | Y (JaCoCo) | Y (JaCoCo) | Y (nyc) | Y (Codecov) | - |

**Key takeaway**: The Java clients (jedis, lettuce) and node-redis have the largest test
suites. redis-py has the highest test LOC. redis-rs tests across the widest Redis version
range including Valkey. Property-based testing is rare (only redis_client_ex).
**redis-tower should target comprehensive integration tests with multi-version CI,
RESP2/RESP3 matrix, and consider property-based testing for the protocol layer.**

---

## 12. Architecture Patterns

| Pattern | redis-rs | fred.rs | go-redis | jedis | lettuce | node-redis | redis-py | redis_client_ex |
|---------|:--------:|:-------:|:--------:|:-----:|:-------:|:----------:|:--------:|:---------------:|
| Trait-based commands | Y | Y | Y (interface) | Y (interface) | Y (interface) | - | Y (mixin) | Y (behaviour) |
| Command builder | Y | - | - | Y | Y | Y | - | Y |
| Type-safe responses | Y | Y | Y | Y | Y | Y | - | - |
| Feature-gated modules | Y | Y | - | - | - | - | - | - |
| Codec abstraction | Y | - | - | - | Y | Y | Y | - |
| Event/broadcast system | Y (push) | Y | - | - | Y | Y | Y | Y (:telemetry) |
| Service/middleware layer | - | - | Y (hooks) | Y (executors) | Y (interceptors) | - | - | - |

---

## Summary: Gaps and Opportunities for redis-tower

### Universal features (table stakes -- must have)

All 8 clients support these; redis-tower must too:

- Standalone, Cluster, Sentinel connections
- TLS/SSL, Unix sockets
- RESP2 and RESP3 with push notifications
- Pipelining, MULTI/EXEC transactions, WATCH
- Pub/Sub (including sharded)
- Streams with consumer groups
- Lua scripting (EVAL/EVALSHA)
- All core data structures (strings, hashes, lists, sets, sorted sets, HLL, geo, bitmaps)
- Connection pooling with auto-reconnect and exponential backoff
- ACL authentication
- Client-side caching (server-assisted)

### Differentiators to target

These features separate the best clients and represent opportunities:

1. **Tower middleware stack** -- No other client has composable Service middleware.
   This is redis-tower's primary differentiator. Retry, timeout, rate limiting,
   circuit breaking, metrics, and tracing as stackable layers.

2. **Full Redis Stack coverage** -- Both Rust clients lag behind on probabilistic
   data structures (Bloom, Cuckoo, CMS, TopK, T-Digest) and TimeSeries. Matching
   go-redis/jedis/node-redis/redis-py coverage would make redis-tower the most
   complete Rust option.

3. **Redis Functions (FCALL)** -- Neither Rust client supports this. 5 of 6
   non-Rust clients do.

4. **OpenTelemetry integration** -- Neither Rust client has OTel support. A Tower
   layer for OTel tracing/metrics would be a strong selling point.

5. **Vector Sets** -- Redis 8.0+ feature supported by most clients but not fred.rs.
   Early support in redis-tower would be forward-looking.

6. **Multiple backoff strategies** -- redis-py has 7 strategies. Most clients have
   1-3. Tower middleware makes this trivially composable.

7. **Property-based testing** -- Only redis_client_ex does this. Proptest for the
   protocol codec layer would catch edge cases others miss.

### Testing targets

Based on the landscape, a competitive test suite should include:

- Integration tests against real Redis (standalone + cluster + sentinel)
- Multi-version CI matrix (Redis 7.x, 8.x, and Valkey)
- RESP2/RESP3 protocol matrix
- Redis Stack module tests
- Benchmarks (criterion)
- Mock support for unit testing without Redis
- Coverage reporting
- Consider property-based tests for RESP codec (proptest)
