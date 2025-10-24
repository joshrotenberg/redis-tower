# Redis Sentinel Implementation Guide for redis-tower

## Executive Summary

This document provides a comprehensive guide for implementing Redis Sentinel support in the redis-tower client. The implementation leverages Tower's middleware architecture, the `Reconnect` middleware for automatic failover, and the `Balance` service for load balancing across replica nodes.

**Key Design Principles:**
- Sentinel discovery and failover fit naturally into Tower's `MakeService` pattern
- Use Tower's `Reconnect` middleware for automatic master failover
- Use Tower's `Balance` service for load balancing read operations across replicas
- Strongly-typed Sentinel commands using existing RESP parser integration
- Zero-cost abstraction - Sentinel overhead only when using Sentinel features

---

## 1. Redis Sentinel Protocol and Architecture

### 1.1 Core Sentinel Concepts

Redis Sentinel provides high availability for Redis through:

1. **Monitoring**: Sentinels constantly check if master and replica instances are working
2. **Notification**: Sentinels can notify system administrators or applications about failures
3. **Automatic Failover**: Sentinels promote a replica to master when the master fails
4. **Configuration Provider**: Sentinels act as a source of authority for service discovery

### 1.2 Sentinel Discovery Protocol

The official Redis Sentinel client specification defines a three-step discovery process:

**Step 1: Query Sentinels for Master Address**

Clients iterate through a list of Sentinel addresses, using a short timeout (few hundred milliseconds):

```bash
SENTINEL get-master-addr-by-name <master-name>
```

Response:
- Success: `["127.0.0.1", "6379"]` (ip and port)
- Unknown master: `nil`

**Step 2: Verify the Master Role**

Once a client discovers the master address, it must verify the role:

```bash
ROLE
```

Expected response:
```
1) "master"
2) <replication_offset>
3) <list of replicas>
```

This prevents connecting to stale instances with outdated Sentinel information.

**Step 3: Handle Reconnection**

During failover, Redis Sentinel sends `CLIENT KILL type normal` to disconnect all clients, forcing them to re-resolve addresses.

### 1.3 Key Sentinel Commands

```bash
# Get current master address
SENTINEL get-master-addr-by-name <master-name>

# Get all replicas for read scaling
SENTINEL replicas <master-name>

# Get other sentinel nodes (for list refresh)
SENTINEL sentinels <master-name>

# Get all monitored masters
SENTINEL masters

# Get detailed master info
SENTINEL master <master-name>
```

### 1.4 Pub/Sub for Failover Notifications (Optional Enhancement)

Sentinels publish events to specific channels:

```bash
SUBSCRIBE +switch-master
```

When a failover completes, Sentinels publish:
```
+switch-master <master-name> <old-ip> <old-port> <new-ip> <new-port>
```

**Important**: Pub/Sub messages are **not guaranteed** to be delivered and should **not replace** the polling mechanism for cluster status. They are an optimization for faster failover detection.

### 1.5 Connection Pool Considerations

From the official specification:

> "On reconnection of a single connection, the Sentinel should be contacted again, and in case of a master address change all the existing connections should be closed and connected to the new address."

This means that connection pools must detect failover and drain/recreate all connections.

---

## 2. Existing Rust Client Implementations

### 2.1 redis-rs Sentinel Implementation

**Architecture:**

redis-rs provides two main types:

1. **`Sentinel`**: Core discovery client
   - Queries sentinel nodes for master/replica addresses
   - Validates node roles using `ROLE` command
   - Caches sentinel connections
   - Provides `master_for()` and `replica_for()` methods

2. **`SentinelClient`**: Wrapper providing standard `Client` interface
   - Internally uses `Sentinel` for discovery
   - Supports both sync and async connections
   - Provides `get_connection()` and `get_async_connection()`

**Code Example from redis-rs:**

```rust
use redis::sentinel::Sentinel;

let nodes = vec![
    "redis://127.0.0.1:26379/",
    "redis://127.0.0.1:26380/",
    "redis://127.0.0.1:26381/"
];

let mut sentinel = Sentinel::build(nodes).unwrap();

// Get master connection
let mut master = sentinel
    .master_for("mymaster", None)
    .unwrap()
    .get_connection()
    .unwrap();

// Get replica connection
let mut replica = sentinel
    .replica_for("mymaster", None)
    .unwrap()
    .get_connection()
    .unwrap();
```

**Key Implementation Details:**

- Uses `SENTINEL masters` to retrieve all monitored masters
- Uses `SENTINEL slaves <master_name>` to get replicas
- Validates nodes by checking flags (no `s_down`, `o_down`)
- Verifies role with `ROLE` command (falls back to `INFO REPLICATION`)
- Supports separate TLS and authentication for sentinel vs. Redis nodes
- Connection failures trigger automatic retry with next sentinel

**SentinelClientBuilder Pattern:**

```rust
use redis::sentinel::{SentinelClientBuilder, SentinelServerType};

let client = SentinelClientBuilder::new()
    .sentinel_nodes(vec![
        "redis://sentinel1:26379",
        "redis://sentinel2:26379"
    ])
    .master_name("mymaster")
    .sentinel_username("sentinel_user")
    .sentinel_password("sentinel_pass")
    .redis_username("redis_user")
    .redis_password("redis_pass")
    .server_type(SentinelServerType::Master)
    .build()
    .unwrap();
```

### 2.2 fred Sentinel Implementation

**Architecture:**

fred provides Sentinel support through optional feature flags:

- `sentinel-client`: Direct Sentinel node communication
- `sentinel-auth`: Separate authentication for Sentinel nodes

**Key Features:**

- `ServerConfig::Sentinel` variant for configuration
- Transparent failover handling
- Integrated with fred's connection pooling
- Supports RESP2/RESP3

**Configuration Pattern:**

```rust
use fred::types::{ServerConfig, SentinelConfig};

let config = ServerConfig::Sentinel {
    hosts: vec![
        ("sentinel1", 26379),
        ("sentinel2", 26379),
        ("sentinel3", 26379),
    ],
    service_name: "mymaster".to_string(),
    // Optional sentinel-specific authentication
};
```

**Key Insights:**

- fred makes Sentinel **optional** - not required for basic usage
- Separates sentinel-specific features into dedicated modules
- Connection pooling automatically handles failover
- Minimal API surface for users - handled transparently

---

## 3. Tower Integration Patterns for Sentinel

### 3.1 Tower's Reconnect Middleware

Tower provides the `tower::reconnect::Reconnect` middleware specifically for handling connection failures and automatic reconnection.

**How Reconnect Works:**

```rust
use tower::reconnect::Reconnect;
use tower::MakeService;

// MakeService creates new connections on demand
let make_service = /* ... */;

// Reconnect wraps MakeService and handles failures
let reconnecting_service = Reconnect::new(make_service);
```

**Key Behavior:**

- **Lazy Connection**: Connects only when needed
- **Failure Handling**: When `MakeService::call` fails, error is returned on next call
- **Availability**: Service becomes unavailable when `MakeService::poll_ready` errors
- **Automatic Retry**: Automatically attempts reconnection on failures

**Perfect for Sentinel:**

Sentinel failover naturally maps to Tower's Reconnect:
1. Client disconnects due to master failure
2. `Reconnect` detects failure and triggers `MakeService::call`
3. `MakeService` queries Sentinels for new master address
4. New connection established to new master
5. Client continues operating transparently

### 3.2 Tower's Balance Service for Replica Load Balancing

Tower's `tower::balance::p2c::Balance` provides load balancing across multiple endpoints.

**Power of Two Choices (P2C) Algorithm:**

```rust
use tower::balance::p2c::Balance;
use tower::discover::ServiceList;

// Create a list of replica services
let replicas = vec![replica1_service, replica2_service, replica3_service];

// Balance load across replicas using P2C
let balanced = Balance::new(ServiceList::new(replicas));
```

**How P2C Works:**

1. Randomly picks two services from ready endpoints
2. Selects the least loaded of the two
3. Provides manageable upper bound on maximum load

**Integration with Sentinel:**

- Query `SENTINEL replicas <master-name>` to discover replicas
- Create a `Service` for each replica
- Use `Balance` to distribute read operations
- Implement custom `Discover` trait for dynamic replica updates

### 3.3 MakeService Pattern for Sentinel Discovery

Tower's `MakeService` trait is the perfect abstraction for Sentinel discovery:

```rust
use tower::MakeService;
use std::future::Future;

pub trait MakeService<Target> {
    type Response;
    type Error;
    type Service: Service<Self::Response>;
    type MakeError;
    type Future: Future<Output = Result<Self::Service, Self::MakeError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::MakeError>>;
    fn make_service(&mut self, target: Target) -> Self::Future;
}
```

**Sentinel as MakeService:**

```rust
impl MakeService<()> for SentinelMakeService {
    type Response = RespType;
    type Error = RedisError;
    type Service = RedisConnection;
    type MakeError = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Service, Self::MakeError>>>>;

    fn make_service(&mut self, _: ()) -> Self::Future {
        Box::pin(async move {
            // 1. Query sentinels for master address
            let addr = self.get_master_addr().await?;
            
            // 2. Connect to discovered master
            let conn = RedisConnection::connect(&addr).await?;
            
            // 3. Verify ROLE is master
            self.verify_role(&conn).await?;
            
            Ok(conn)
        })
    }
}
```

---

## 4. Recommended Design for redis-tower

### 4.1 Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                  User Application                    │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│          SentinelClient (Public API)                 │
│  - master() -> MasterService                         │
│  - replica() -> ReplicaService                       │
└─────────────────────────────────────────────────────┘
                         │
         ┌───────────────┴────────────────┐
         ▼                                 ▼
┌──────────────────────┐      ┌─────────────────────────┐
│   MasterService      │      │    ReplicaService       │
│                      │      │                         │
│  Reconnect           │      │  Balance                │
│    │                 │      │    │                    │
│    ▼                 │      │    ▼                    │
│  SentinelMake        │      │  Discover               │
│    │                 │      │    │                    │
│    ▼                 │      │    ▼                    │
│  RedisConnection     │      │  [RedisConnection...]   │
└──────────────────────┘      └─────────────────────────┘
         │                                 │
         └────────────────┬────────────────┘
                          ▼
                  ┌───────────────┐
                  │   Sentinels   │
                  │ [S1, S2, S3]  │
                  └───────────────┘
```

### 4.2 Module Structure

Create the following file structure:

```
src/sentinel/
├── mod.rs              # Public API exports
├── config.rs           # SentinelConfig and builder
├── discovery.rs        # SentinelDiscovery implementation
├── make_service.rs     # Tower MakeService implementation
├── client.rs           # SentinelClient public API
└── commands.rs         # Sentinel-specific commands
```

### 4.3 Key Implementation Files

The implementation consists of several key components working together:

**1. Configuration (`config.rs`)**
- `SentinelConfig`: Main configuration struct
- `SentinelNode`: Individual sentinel address
- `TlsMode`: TLS configuration options
- Builder pattern for ergonomic configuration

**2. Discovery (`discovery.rs`)**
- `SentinelDiscovery`: Queries sentinels for master/replica addresses
- Implements three-step discovery protocol
- Handles sentinel failover and retry
- Verifies node roles with ROLE command

**3. MakeService (`make_service.rs`)**
- `SentinelMakeService`: Tower MakeService implementation
- Creates new connections by querying sentinels
- Integrates with Tower's Reconnect middleware

**4. Client (`client.rs`)**
- `SentinelClient`: High-level public API
- `master()`: Returns service with automatic failover
- `replicas()`: Returns load-balanced service
- `with_resilience()`: Adds full middleware stack

**5. Commands (`commands.rs`)**
- `SentinelCommand`: Enum for SENTINEL commands
- `Role`: ROLE command for verification
- Type-safe command construction

### 4.4 Usage Examples

**Example 1: Basic Sentinel Client**

```rust
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use redis_tower::commands::strings::Get;
use tower::Service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure sentinel
    let config = SentinelConfig::builder()
        .sentinel_node("sentinel1.example.com", 26379)
        .sentinel_node("sentinel2.example.com", 26379)
        .sentinel_node("sentinel3.example.com", 26379)
        .master_name("mymaster")
        .redis_auth(None, Some("redis_password".to_string()))
        .build()?;
    
    let client = SentinelClient::new(config);
    
    // Get master service with automatic failover
    let mut master = client.master();
    
    // Execute commands - automatically reconnects on failover
    let get_cmd = Get::new("user:123");
    let response = master.call(get_cmd).await?;
    
    println!("Value: {:?}", response);
    
    Ok(())
}
```

**Example 2: Load Balanced Reads**

```rust
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use redis_tower::commands::strings::Get;
use tower::Service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SentinelConfig::builder()
        .sentinel_node("sentinel1", 26379)
        .sentinel_node("sentinel2", 26379)
        .sentinel_node("sentinel3", 26379)
        .master_name("mymaster")
        .build()?;
    
    let client = SentinelClient::new(config);
    
    // Get load-balanced replica service
    let mut replicas = client.replicas().await?;
    
    // Reads automatically distributed across replicas
    for i in 0..100 {
        let get_cmd = Get::new(format!("key:{}", i));
        let response = replicas.call(get_cmd).await?;
        println!("Value {}: {:?}", i, response);
    }
    
    Ok(())
}
```

**Example 3: Full Resilience Stack**

```rust
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use redis_tower::commands::strings::Set;
use tower::Service;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SentinelConfig::builder()
        .sentinel_node("sentinel1", 26379)
        .sentinel_node("sentinel2", 26379)
        .sentinel_node("sentinel3", 26379)
        .master_name("mymaster")
        .enable_pubsub_monitoring(true)
        .build()?;
    
    let client = SentinelClient::new(config);
    
    // Master with full resilience middleware
    let mut master = client.with_resilience(Duration::from_secs(5));
    
    // This service has:
    // - Automatic failover (Reconnect)
    // - Timeout protection (TimeoutLayer)
    // - Circuit breaker (CircuitBreakerLayer)
    // - Retry with backoff (RetryLayer)
    
    let set_cmd = Set::new("key", "value");
    let response = master.call(set_cmd).await?;
    
    println!("Response: {:?}", response);
    
    Ok(())
}
```

---

## 5. Implementation Roadmap

### Phase 1: Core Sentinel Discovery (Week 1)

**Tasks:**

1. Create `src/sentinel/` module structure
2. Implement `SentinelConfig` and builder pattern
3. Implement `SentinelDiscovery` with:
   - `discover_master()` method
   - `discover_replicas()` method
   - Sentinel query logic
   - Role verification
4. Add Sentinel commands:
   - `SENTINEL get-master-addr-by-name`
   - `SENTINEL replicas`
   - `ROLE`
5. Add error variants for Sentinel

**Tests:**

- Unit tests for config builder
- Integration tests with Redis Sentinel test environment
- Test failover detection

### Phase 2: Tower Integration (Week 2)

**Tasks:**

1. Implement `SentinelMakeService`
   - Implement `MakeService` trait
   - Wire up discovery logic
   - Handle connection creation
2. Implement `SentinelClient`
   - `master()` method with `Reconnect` wrapper
   - `replicas()` method with `Balance` wrapper
3. Add connection pooling support
4. Add authentication support (sentinel and Redis)

**Tests:**

- Test `MakeService` implementation
- Test automatic reconnection
- Test load balancing across replicas
- Test authentication flows

### Phase 3: Advanced Features (Week 3)

**Tasks:**

1. Implement Pub/Sub monitoring for faster failover
   - Subscribe to `+switch-master` events
   - Update discovery cache on events
2. Implement replica discovery updates
   - Dynamic `Discover` trait implementation
   - Handle replica addition/removal
3. Add TLS support
4. Add comprehensive logging/tracing
5. Performance optimization

**Tests:**

- Test pub/sub failover notifications
- Test dynamic replica discovery
- Test TLS connections
- Performance benchmarks

### Phase 4: Documentation & Examples (Week 4)

**Tasks:**

1. Complete API documentation
2. Create comprehensive examples:
   - Basic usage
   - Load balanced reads
   - Full resilience stack
   - Failover testing
3. Create integration test suite
4. Write deployment guide
5. Benchmark against redis-rs and fred

**Deliverables:**

- API documentation
- 5+ working examples
- Integration test suite
- Performance comparison report

---

## 6. Testing Strategy

### 6.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sentinel_config_builder() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .build()
            .unwrap();
        
        assert_eq!(config.sentinel_nodes.len(), 1);
        assert_eq!(config.master_name, "mymaster");
    }
    
    #[test]
    fn test_config_validation() {
        // Should fail without sentinel nodes
        let result = SentinelConfig::builder()
            .master_name("mymaster")
            .build();
        
        assert!(result.is_err());
    }
}
```

### 6.2 Integration Tests with Docker

```yaml
# docker-compose.yml for testing
version: '3'
services:
  redis-master:
    image: redis:7
    command: redis-server --port 6379
    
  redis-replica-1:
    image: redis:7
    command: redis-server --port 6380 --replicaof redis-master 6379
    
  redis-replica-2:
    image: redis:7
    command: redis-server --port 6381 --replicaof redis-master 6379
    
  sentinel-1:
    image: redis:7
    command: redis-sentinel /etc/sentinel.conf
    volumes:
      - ./sentinel1.conf:/etc/sentinel.conf
      
  sentinel-2:
    image: redis:7
    command: redis-sentinel /etc/sentinel.conf
    volumes:
      - ./sentinel2.conf:/etc/sentinel.conf
      
  sentinel-3:
    image: redis:7
    command: redis-sentinel /etc/sentinel.conf
    volumes:
      - ./sentinel3.conf:/etc/sentinel.conf
```

### 6.3 Failover Testing

```rust
#[tokio::test]
async fn test_sentinel_failover() {
    // Setup sentinel client
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .build()
        .unwrap();
    
    let client = SentinelClient::new(config);
    let mut master = client.master();
    
    // Write value
    let set_cmd = Set::new("failover_test", "value1");
    master.call(set_cmd).await.unwrap();
    
    // Trigger failover (manually using redis-cli SENTINEL failover mymaster)
    // ... external failover trigger ...
    
    // Wait for failover
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Read value - should automatically reconnect to new master
    let get_cmd = Get::new("failover_test");
    let response = master.call(get_cmd).await.unwrap();
    
    assert_eq!(response.as_string().unwrap(), "value1");
}
```

---

## 7. Performance Considerations

### 7.1 Sentinel Query Optimization

- **Cache master address**: Don't query sentinels on every connection
- **Connection pooling**: Reuse connections to sentinels
- **Parallel queries**: Query multiple sentinels concurrently
- **Timeout tuning**: Balance between fast failover and false positives

### 7.2 Failover Detection Speed

**Polling approach**: 
- Slower (waits for next connection attempt)
- More reliable (no message loss)
- Simpler implementation

**Pub/Sub approach**:
- Faster (immediate notification)
- Less reliable (messages can be lost)
- More complex (requires persistent connection)

**Recommended**: Polling as primary, Pub/Sub as optional optimization

### 7.3 Connection Pool Management

During failover, all existing connections to old master become invalid. The implementation must handle this gracefully by detecting failover and recreating the connection pool.

---

## 8. Comparison with Existing Clients

### 8.1 redis-rs Sentinel

**Pros:**
- Mature and well-tested
- Comprehensive API
- Separate auth for sentinel vs. Redis

**Cons:**
- No automatic failover in connection pool
- Manual reconnection required
- Not Tower-based
- No middleware support

### 8.2 fred Sentinel

**Pros:**
- Transparent failover handling
- Integrated connection pooling
- RESP3 support

**Cons:**
- Less documentation
- No Tower integration
- Opaque internals

### 8.3 redis-tower Sentinel (Our Implementation)

**Advantages:**
- **Tower-native**: Leverage entire Tower ecosystem
- **Composable middleware**: Timeout, circuit breaker, retry
- **Type-safe**: Strongly typed commands and responses
- **Automatic failover**: Via Tower's `Reconnect`
- **Load balancing**: Via Tower's `Balance`
- **Observability**: Built-in tracing
- **Modern async**: Built on latest Tokio patterns

**Differentiators:**
- Only Tower-based Redis Sentinel client
- Middleware composition for resilience
- Type-safe command API
- Leverages resp-parser performance

---

## 9. Future Enhancements

### 9.1 Dynamic Replica Discovery

Implement Tower's `Discover` trait for dynamic endpoint updates:

```rust
use tower::discover::{Change, Discover};

pub struct SentinelReplicaDiscovery {
    discovery: SentinelDiscovery,
    // ... state ...
}

impl Discover for SentinelReplicaDiscovery {
    type Key = usize;
    type Service = RedisConnection;
    type Error = RedisError;
    
    // Stream of replica topology changes
    // Return Change::Insert for new replicas
    // Return Change::Remove for failed replicas
}
```

### 9.2 Pub/Sub Monitoring

Subscribe to Sentinel events for faster failover detection:

```rust
pub async fn start_monitoring(&mut self) -> Result<(), RedisError> {
    let mut pubsub = self.connect_sentinel_pubsub().await?;
    
    pubsub.subscribe("+switch-master").await?;
    
    tokio::spawn(async move {
        while let Some(msg) = pubsub.next().await {
            // Parse +switch-master message
            // Update cached master address
            // Trigger reconnection
        }
    });
    
    Ok(())
}
```

### 9.3 Sentinel Pool Refresh

Periodically refresh the list of available sentinels using `SENTINEL sentinels <master-name>`.

### 9.4 Metrics and Health Checks

Export Prometheus metrics for failover count, connection duration, replica count, etc.

---

## 10. Key Takeaways

### Protocol Requirements

1. **Three-step discovery**: Query sentinel → Connect → Verify role
2. **All sentinels can fail**: Implement retry across all sentinels
3. **Role verification mandatory**: Prevents connecting to stale masters
4. **CLIENT KILL on failover**: Expect disconnection during failover
5. **Pub/Sub is optional**: Don't rely solely on notifications

### Tower Patterns

1. **`Reconnect` for failover**: Perfect match for Sentinel failover
2. **`MakeService` for discovery**: Natural abstraction for connection creation
3. **`Balance` for replicas**: Built-in load balancing
4. **Middleware composition**: Stack timeout, circuit breaker, retry
5. **Type safety**: Strongly typed commands and responses

### Implementation Priorities

1. **Start simple**: Basic discovery and connection first
2. **Add Tower layers**: Reconnect, then Balance
3. **Optimize later**: Pub/Sub, caching, metrics
4. **Test thoroughly**: Integration tests with real Sentinel
5. **Document well**: Examples showing Tower composition

### Competitive Advantages

1. **Only Tower-based** Redis Sentinel client
2. **Middleware composition** unavailable elsewhere
3. **Type safety** superior to redis-rs/fred
4. **Observability** built-in via tracing
5. **Modern patterns** showcasing Rust's strengths

---

## 11. References

### Official Documentation

- [Redis Sentinel Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [Sentinel Client Specification](https://redis.io/docs/latest/develop/reference/sentinel-clients/)
- [Tower Documentation](https://docs.rs/tower/latest/tower/)
- [Tower Reconnect](https://docs.rs/tower/latest/tower/reconnect/)
- [Tower Balance](https://docs.rs/tower/latest/tower/balance/)

### Existing Implementations

- [redis-rs Sentinel Module](https://docs.rs/redis/latest/redis/sentinel/)
- [redis-rs Source Code](https://github.com/redis-rs/redis-rs)
- [fred.rs Documentation](https://docs.rs/fred/latest/fred/)
- [fred.rs Source Code](https://github.com/aembke/fred.rs)

### Tower Ecosystem

- [MakeService Trait](https://docs.rs/tower/latest/tower/trait.MakeService.html)
- [Service Trait](https://docs.rs/tower/latest/tower/trait.Service.html)
- [Inventing the Service Trait](https://tokio.rs/blog/2021-05-14-inventing-the-service-trait)

---

## Conclusion

This design leverages Tower's strengths to create a unique, composable, and type-safe Redis Sentinel client. The implementation naturally maps Sentinel's discovery protocol to Tower's `MakeService` pattern and uses `Reconnect` for automatic failover - a combination not available in existing Rust Redis clients.

The phased roadmap provides a clear path from basic discovery to a production-ready client with full resilience features. By building on redis-tower's existing foundation (RESP parser, type-safe commands, middleware support), the Sentinel implementation becomes a natural extension that showcases the power of Tower's middleware architecture.
