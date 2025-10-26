# Redis Client Types Guide

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
└─────────────────────────────────────────────────────────────┘
                            │
                    ┌───────┴───────┐
                    │               │
            ┌───────▼──────┐  ┌────▼──────────┐
            │ High-Level   │  │  Low-Level    │
            │   Clients    │  │   Clients     │
            └───────┬──────┘  └────┬──────────┘
                    │               │
        ┌───────────┼───────────────┼───────────┐
        │           │               │           │
   ┌────▼────┐ ┌───▼──────┐  ┌────▼─────┐ ┌───▼──────────┐
   │ Redis   │ │Resilient │  │ Redis    │ │ Connection   │
   │ Client  │ │  Redis   │  │Connection│ │    Pool      │
   │         │ │  Client  │  │          │ │              │
   └────┬────┘ └────┬─────┘  └────┬─────┘ └───┬──────────┘
        │           │              │           │
        │ wraps     │ wraps        │           │ manages N×
        │           │              │           │
   ┌────▼───────────▼──────────────▼───────────▼──────┐
   │          Single RedisConnection                   │
   │  (TCP socket + RESP codec + Arc<Mutex<Framed>>)  │
   └───────────────────────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  Redis Server  │
                    └────────────────┘
```

## 1. RedisConnection

**What it is:** Direct TCP connection to Redis with RESP codec.

**Architecture:**
```rust
pub struct RedisConnection {
    framed: Arc<Mutex<Framed<RedisStream, RespCodec>>>,
}
```

**API:**
```rust
// Connect
let conn = RedisConnection::connect("127.0.0.1:6379").await?;

// Execute commands (note: .execute, not .call)
let value: Option<Bytes> = conn.execute(Get::new("key")).await?;

// Good for transactions
let mut tx = Transaction::new(&conn);
tx.queue(Set::new("key", "value")).await?;
let results = tx.exec().await?;
```

**When to use:**
- ✅ Transactions (MULTI/EXEC)
- ✅ Pipelines
- ✅ Direct protocol control
- ✅ Testing/debugging
- ✅ When you need `&RedisConnection` (for Transaction API)

**When NOT to use:**
- ❌ Production apps (no auto-reconnect)
- ❌ High concurrency (single connection)
- ❌ Long-running processes (connection can drop)

**Concurrency:** Single connection, serialized requests via mutex

---

## 2. RedisClient

**What it is:** Thin wrapper around RedisConnection with nicer API.

**Architecture:**
```rust
pub struct RedisClient {
    connection: RedisConnection,  // Just ONE connection
}
```

**API:**
```rust
// Connect
let client = RedisClient::connect("127.0.0.1:6379").await?;

// Execute commands (note: .call, not .execute)
let value: Option<Bytes> = client.call(Get::new("key")).await?;

// Can clone cheaply (Arc internally)
let client2 = client.clone();
tokio::spawn(async move {
    client2.call(Incr::new("counter")).await?;
});
```

**When to use:**
- ✅ Simple CLI tools
- ✅ Scripts
- ✅ Low concurrency apps (<10 req/sec)
- ✅ When you prefer `.call()` over `.execute()`
- ✅ Can clone and share across tasks

**When NOT to use:**
- ❌ Production apps (no auto-reconnect)
- ❌ High concurrency (still just 1 connection)
- ❌ Long-running processes

**Concurrency:** Single connection, can be cloned but requests still serialized

---

## 3. ResilientRedisClient

**What it is:** RedisConnection with automatic reconnection logic.

**Architecture:**
```rust
pub struct ResilientRedisClient {
    connection: ResilientConnection,  // ONE connection with auto-reconnect
}

// ResilientConnection wraps RedisConnection and handles failures
pub struct ResilientConnection {
    inner: Arc<Mutex<Option<RedisConnection>>>,
    reconnect_config: Arc<ReconnectConfig>,
    health_checker: HealthChecker,
}
```

**API:**
```rust
// Connect with configuration
let config = ClientConfig::builder()
    .address("127.0.0.1:6379")
    .reconnect_exponential(
        Duration::from_millis(100),  // min delay
        Duration::from_secs(5),       // max delay
    )
    .max_reconnect_attempts(10)
    .health_check_interval(Duration::from_secs(30))
    .build();

let client = ResilientRedisClient::connect_with_full_config(config).await?;

// Execute commands - auto-reconnects on failure!
let value: Option<Bytes> = client.call(Get::new("key")).await?;
```

**When to use:**
- ✅ Production applications
- ✅ Long-running background workers
- ✅ Unreliable networks
- ✅ Microservices that need resilience
- ✅ When network can drop temporarily

**When NOT to use:**
- ❌ High concurrency (still just 1 connection)
- ❌ When you need pooling

**Concurrency:** Single connection with auto-reconnect, requests serialized

**Reconnection behavior:**
- Exponential backoff: 100ms → 200ms → 400ms → ... → 5s
- Configurable max attempts (or unlimited)
- Health checking to detect stale connections
- Metrics tracking (reconnections, failures)

---

## 4. ConnectionPool

**What it is:** Pool of multiple RedisConnection instances with round-robin distribution.

**Architecture:**
```rust
pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<PooledConnection>>>,
    config: PoolConfig,
    stats: Arc<PoolStats>,
}

// Each PooledConnection wraps a RedisConnection
struct PooledConnection {
    conn: RedisConnection,
    created_at: Instant,
}
```

**API:**
```rust
// Create pool
let config = PoolConfig::builder()
    .max_size(10)              // Up to 10 connections
    .min_idle(2)               // Keep 2 idle
    .connection_timeout(Duration::from_secs(5))
    .max_lifetime(Duration::from_secs(300))  // Recycle after 5 min
    .build();

let pool = ConnectionPool::new("127.0.0.1:6379", config).await?;

// Get connection from pool
let conn = pool.get().await?;

// Use like RedisConnection (note: .execute)
let value: Option<Bytes> = conn.execute(Get::new("key")).await?;

// Connection returns to pool when dropped
drop(conn);

// Use in concurrent tasks
for i in 0..100 {
    let pool = pool.clone();
    tokio::spawn(async move {
        let conn = pool.get().await.unwrap();
        conn.execute(Incr::new("counter")).await.unwrap();
    });
}
```

**When to use:**
- ✅ Web servers (Axum, Actix, etc.)
- ✅ High concurrency (100s-1000s req/sec)
- ✅ Multiple concurrent tasks
- ✅ When single connection is bottleneck

**When NOT to use:**
- ❌ Simple scripts (overkill)
- ❌ Low concurrency apps
- ❌ When you need auto-reconnect (pool doesn't have it yet)

**Concurrency:** Multiple connections, round-robin distribution

**Features:**
- Connection reuse
- Automatic cleanup of stale connections
- Health checking before returning connection
- Stats (active, idle, wait time)
- Configurable min/max pool size

---

## Comparison Table

| Feature | RedisConnection | RedisClient | ResilientRedisClient | ConnectionPool |
|---------|----------------|-------------|----------------------|----------------|
| **Connections** | 1 | 1 | 1 | N (pooled) |
| **API Method** | `.execute()` | `.call()` | `.call()` | `.execute()` |
| **Auto-reconnect** | ❌ | ❌ | ✅ | ❌ |
| **Connection pooling** | ❌ | ❌ | ❌ | ✅ |
| **Health checking** | ❌ | ❌ | ✅ | ✅ |
| **Metrics** | ❌ | ❌ | ✅ | ✅ |
| **Clonable** | ❌ | ✅ | ✅ | ✅ |
| **Transactions** | ✅ | ❌ | ❌ | ✅ |
| **Concurrency** | Low | Low | Low | High |
| **Use case** | Testing, transactions | Simple apps | Production | High-concurrency |

---

## Common Usage Patterns

### Pattern 1: Simple CLI Tool
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RedisClient::connect("127.0.0.1:6379").await?;
    
    let value: Option<Bytes> = client.call(Get::new("key")).await?;
    println!("Value: {:?}", value);
    
    Ok(())
}
```

### Pattern 2: Background Worker
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .address("127.0.0.1:6379")
        .reconnect_exponential(Duration::from_millis(100), Duration::from_secs(5))
        .unlimited_reconnect_attempts()
        .build();
    
    let client = ResilientRedisClient::connect_with_full_config(config).await?;
    
    // Process queue forever - will auto-reconnect if Redis restarts
    loop {
        let job: Option<Bytes> = client.call(RPop::new("queue")).await?;
        if let Some(job) = job {
            process_job(job).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
```

### Pattern 3: Web Server (Axum)
```rust
use axum::{Router, extract::State};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    pool: ConnectionPool,
}

#[tokio::main]
async fn main() {
    let pool_config = PoolConfig::builder()
        .max_size(50)  // Handle 50 concurrent requests
        .min_idle(10)
        .build();
    
    let pool = ConnectionPool::new("127.0.0.1:6379", pool_config).await.unwrap();
    
    let app = Router::new()
        .route("/incr/:key", axum::routing::post(increment))
        .with_state(AppState { pool });
    
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn increment(
    State(state): State<AppState>,
    axum::extract::Path(key): axum::extract::Path<String>,
) -> String {
    let conn = state.pool.get().await.unwrap();
    let value: i64 = conn.execute(Incr::new(&key)).await.unwrap();
    format!("{}", value)
}
```

### Pattern 4: Transactions with Pool
```rust
let pool = ConnectionPool::new("127.0.0.1:6379", pool_config).await?;

// Get connection from pool
let conn = pool.get().await?;

// Use for transaction (needs &RedisConnection)
let mut tx = Transaction::new(&*conn);  // Deref PooledConnection to &RedisConnection
tx.queue(Set::new("key1", "value1")).await?;
tx.queue(Get::new("key1")).await?;
let results = tx.exec().await?;

// Connection returns to pool when dropped
```

### Pattern 5: Tower Middleware Stack
See the detailed Tower Resilience section below for comprehensive examples.

---

## Tower Middleware Integration

One of redis-tower's unique strengths is first-class Tower middleware support. Since all clients implement Tower's `Service` trait, you can compose them with any Tower middleware.

### Available tower-resilience Middleware

redis-tower uses **tower-resilience** for production-ready resilience patterns:

| Middleware | Purpose | When to Use |
|------------|---------|-------------|
| `TimeoutLayer` | Request timeout | Prevent hanging requests |
| `RetryLayer` | Retry with backoff | Handle transient failures |
| `CircuitBreakerLayer` | Stop cascading failures | Protect failing Redis instance |
| `RateLimitLayer` | Request throttling | Prevent overwhelming Redis |
| `BulkheadLayer` | Limit concurrency | Prevent resource exhaustion |

### Basic Tower Stack

```rust
use tower::ServiceBuilder;
use tower_resilience::{TimeoutLayer, RetryLayer, ExponentialBackoff};
use std::time::Duration;

// Start with any client
let client = RedisClient::connect("127.0.0.1:6379").await?;

// Wrap with Tower layers
let mut service = ServiceBuilder::new()
    // Timeout: fail fast if Redis doesn't respond
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    
    // Retry: handle transient failures with exponential backoff
    .layer(RetryLayer::builder()
        .max_attempts(3)
        .backoff(ExponentialBackoff::new(Duration::from_millis(100)))
        .build())
    
    .service(client);

// Use through Tower Service trait
use tower::ServiceExt;
let value: Option<Bytes> = service.call(Get::new("key")).await?;
```

### Complete Resilience Stack

```rust
use tower::ServiceBuilder;
use tower_resilience::{
    TimeoutLayer, RetryLayer, CircuitBreakerLayer, RateLimitLayer,
    ExponentialBackoff, CircuitBreakerConfig,
};
use std::time::Duration;

let client = RedisClient::connect("127.0.0.1:6379").await?;

let mut service = ServiceBuilder::new()
    // 1. Rate Limiting: Max 1000 requests/sec
    .layer(RateLimitLayer::new(1000, Duration::from_secs(1)))
    
    // 2. Timeout: Fail after 5 seconds
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    
    // 3. Circuit Breaker: Open at 50% failure rate
    .layer(CircuitBreakerLayer::builder()
        .failure_rate_threshold(0.5)
        .sliding_window_size(100)
        .wait_duration_in_open(Duration::from_secs(30))
        .build())
    
    // 4. Retry: 3 attempts with exponential backoff
    .layer(RetryLayer::builder()
        .max_attempts(3)
        .backoff(ExponentialBackoff::new(Duration::from_millis(100)))
        .on_retry(|attempt, delay| {
            tracing::warn!("Retry attempt {} after {:?}", attempt, delay);
        })
        .build())
    
    .service(client);

// This request will:
// - Be rate-limited to 1000/sec
// - Timeout after 5 seconds
// - Open circuit breaker if 50% of requests fail
// - Retry up to 3 times with 100ms → 200ms → 400ms backoff
use tower::ServiceExt;
let value: Option<Bytes> = service.call(Get::new("key")).await?;
```

### Circuit Breaker Pattern

Perfect for protecting a failing Redis instance:

```rust
use tower_resilience::CircuitBreakerLayer;

let client = RedisClient::connect("127.0.0.1:6379").await?;

let mut service = ServiceBuilder::new()
    .layer(CircuitBreakerLayer::builder()
        .failure_rate_threshold(0.5)      // Open at 50% failures
        .sliding_window_size(100)          // Over 100 requests
        .wait_duration_in_open(Duration::from_secs(60))  // Wait 60s before retry
        .permitted_calls_in_half_open(10)  // Try 10 requests when half-open
        .build())
    .service(client);

// Circuit breaker states:
// CLOSED: Normal operation, requests flow through
// OPEN: Too many failures, reject requests immediately (fail fast)
// HALF_OPEN: Testing if service recovered (allow limited requests)
```

### Retry with Custom Logic

```rust
use tower_resilience::{RetryLayer, ExponentialBackoff};

let client = RedisClient::connect("127.0.0.1:6379").await?;

let mut service = ServiceBuilder::new()
    .layer(RetryLayer::builder()
        .max_attempts(5)
        .backoff(ExponentialBackoff::new(Duration::from_millis(50)))
        .on_retry(|attempt, delay| {
            // Custom logging
            tracing::warn!(
                "Redis request failed, retry {} after {:?}",
                attempt,
                delay
            );
            
            // Could emit metrics here
            metrics::increment_counter!("redis.retries", "attempt" => attempt.to_string());
        })
        .build())
    .service(client);
```

### Rate Limiting

Prevent overwhelming Redis:

```rust
use tower_resilience::RateLimitLayer;

let client = RedisClient::connect("127.0.0.1:6379").await?;

let mut service = ServiceBuilder::new()
    // Allow max 1000 requests per second
    .layer(RateLimitLayer::new(1000, Duration::from_secs(1)))
    .service(client);

// Requests beyond 1000/sec will wait (back pressure)
```

### Bulkhead Pattern

Limit concurrent requests to Redis:

```rust
use tower_resilience::BulkheadLayer;

let client = RedisClient::connect("127.0.0.1:6379").await?;

let mut service = ServiceBuilder::new()
    // Max 50 concurrent requests
    .layer(BulkheadLayer::new(50))
    .service(client);

// 51st concurrent request will wait for an available slot
```

### Combining with Connection Pool

For maximum throughput + resilience:

```rust
use tower::ServiceBuilder;
use tower_resilience::{TimeoutLayer, CircuitBreakerLayer};

// Create pool with multiple connections
let pool = ConnectionPool::new("127.0.0.1:6379", pool_config).await?;

// Wrap individual connections with Tower layers
async fn get_redis_service(pool: ConnectionPool) -> impl tower::Service<Get> {
    let conn = pool.get().await.unwrap();
    
    ServiceBuilder::new()
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .layer(CircuitBreakerLayer::new(/* config */))
        .service(conn)
}
```

### Custom Middleware

You can write your own Tower middleware:

```rust
use tower::{Layer, Service};
use std::task::{Context, Poll};
use std::future::Future;
use std::pin::Pin;

// Custom metrics layer
#[derive(Clone)]
struct MetricsLayer;

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsService { inner: service }
    }
}

struct MetricsService<S> {
    inner: S,
}

impl<S, Req> Service<Req> for MetricsService<S>
where
    S: Service<Req>,
    Req: redis_tower::commands::Command,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let start = std::time::Instant::now();
        let fut = self.inner.call(req);
        
        Box::pin(async move {
            let result = fut.await;
            let duration = start.elapsed();
            
            // Record metrics
            metrics::histogram!("redis.request.duration", duration);
            if result.is_ok() {
                metrics::increment_counter!("redis.requests.success");
            } else {
                metrics::increment_counter!("redis.requests.error");
            }
            
            result
        })
    }
}

// Use it
let client = RedisClient::connect("127.0.0.1:6379").await?;
let mut service = ServiceBuilder::new()
    .layer(MetricsLayer)
    .service(client);
```

### Production Web Server Example

Combining everything for a production Axum web server:

```rust
use axum::{Router, extract::State, routing::get};
use tower::ServiceBuilder;
use tower_resilience::{TimeoutLayer, CircuitBreakerLayer, RateLimitLayer};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    pool: ConnectionPool,
}

#[tokio::main]
async fn main() {
    // Create connection pool
    let pool_config = PoolConfig::builder()
        .max_size(100)
        .min_idle(10)
        .build();
    
    let pool = ConnectionPool::new("127.0.0.1:6379", pool_config).await.unwrap();
    
    let app = Router::new()
        .route("/incr/:key", get(increment_with_resilience))
        .with_state(AppState { pool });
    
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn increment_with_resilience(
    State(state): State<AppState>,
    axum::extract::Path(key): axum::extract::Path<String>,
) -> Result<String, StatusCode> {
    // Get connection from pool
    let conn = state.pool.get().await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    
    // Wrap with resilience layers
    let mut service = ServiceBuilder::new()
        .layer(RateLimitLayer::new(1000, Duration::from_secs(1)))
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .layer(CircuitBreakerLayer::builder()
            .failure_rate_threshold(0.5)
            .sliding_window_size(100)
            .wait_duration_in_open(Duration::from_secs(30))
            .build())
        .service(conn);
    
    // Execute command through Tower stack
    use tower::ServiceExt;
    let value: i64 = service.call(Incr::new(&key)).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    Ok(format!("{}", value))
}
```

### Observability with Tracing

All Tower layers integrate with `tracing`:

```rust
use tracing_subscriber;

// Initialize tracing
tracing_subscriber::fmt()
    .with_env_filter("redis_tower=debug,tower_resilience=debug")
    .init();

let client = RedisClient::connect("127.0.0.1:6379").await?;

let mut service = ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    .layer(RetryLayer::builder()
        .max_attempts(3)
        .backoff(ExponentialBackoff::new(Duration::from_millis(100)))
        .on_retry(|attempt, delay| {
            tracing::warn!(attempt, ?delay, "Retrying Redis request");
        })
        .build())
    .service(client);

// Logs will show:
// DEBUG redis_tower: Executing command: GET "key"
// WARN tower_resilience: Retrying Redis request attempt=1 delay=100ms
// DEBUG redis_tower: Command succeeded
```

### When to Use Which Middleware

| Scenario | Recommended Middleware |
|----------|----------------------|
| **Unreliable network** | `RetryLayer` + `TimeoutLayer` |
| **Cascading failures** | `CircuitBreakerLayer` |
| **Rate limits** | `RateLimitLayer` |
| **High traffic** | `ConnectionPool` + `BulkheadLayer` |
| **Production app** | All of the above! |
| **Development** | Just `TimeoutLayer` |

### Tower vs ResilientRedisClient

**Question:** If I use `ResilientRedisClient`, do I still need Tower layers?

**Answer:** It depends!

| Feature | ResilientRedisClient | Tower Layers |
|---------|---------------------|--------------|
| Auto-reconnect | ✅ Built-in | ❌ Not included |
| Health checking | ✅ Built-in | ❌ Not included |
| Retry logic | ❌ Not included | ✅ `RetryLayer` |
| Circuit breaker | ❌ Not included | ✅ `CircuitBreakerLayer` |
| Rate limiting | ❌ Not included | ✅ `RateLimitLayer` |
| Timeout | ❌ Not included | ✅ `TimeoutLayer` |

**Best practice:** Use `ResilientRedisClient` **AND** Tower layers for complete resilience:

```rust
// Start with auto-reconnecting client
let config = ClientConfig::builder()
    .address("127.0.0.1:6379")
    .reconnect_exponential(Duration::from_millis(100), Duration::from_secs(5))
    .build();

let client = ResilientRedisClient::connect_with_full_config(config).await?;

// Add Tower layers for additional resilience
let mut service = ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    .layer(CircuitBreakerLayer::new(/* config */))
    .layer(RetryLayer::new(/* config */))
    .service(client);

// Now you have:
// - Auto-reconnect (from ResilientRedisClient)
// - Health checking (from ResilientRedisClient)
// - Timeouts (from Tower)
// - Circuit breaking (from Tower)
// - Retries (from Tower)
```

---

## Decision Tree

```
Do you need transactions?
├─ Yes → Use RedisConnection or ConnectionPool
└─ No
    │
    Do you have high concurrency (>50 req/sec)?
    ├─ Yes → Use ConnectionPool
    └─ No
        │
        Is this a long-running process?
        ├─ Yes → Use ResilientRedisClient
        └─ No → Use RedisClient
```

---

## Performance Characteristics

| Client Type | Throughput | Latency | Memory | Reconnect Time |
|-------------|------------|---------|--------|----------------|
| RedisConnection | Low (1 conn) | Low | Minimal | Manual |
| RedisClient | Low (1 conn) | Low | Minimal | Manual |
| ResilientRedisClient | Low (1 conn) | Low + reconnect | Small | 100ms-5s |
| ConnectionPool (N=10) | High (10× throughput) | Low | Medium | Manual per conn |

**Benchmarks** (on localhost):
- Single connection: ~10,000 req/sec
- ConnectionPool (10 conns): ~80,000 req/sec
- ResilientRedisClient: ~9,500 req/sec (slight overhead for health checks)

---

## Migration Guide

### From redis-rs
```rust
// redis-rs
let client = redis::Client::open("redis://127.0.0.1:6379")?;
let mut conn = client.get_connection()?;
let value: String = conn.get("key")?;

// redis-tower equivalent
let client = RedisClient::connect("127.0.0.1:6379").await?;
let value: Option<Bytes> = client.call(Get::new("key")).await?;
```

### From fred
```rust
// fred
let client = RedisClient::default();
client.connect();
let value: Option<String> = client.get("key").await?;

// redis-tower equivalent
let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
let value: Option<Bytes> = client.call(Get::new("key")).await?;
```
