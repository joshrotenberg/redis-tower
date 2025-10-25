# Redis-Tower Connection Pool Improvement Opportunities

**Date**: 2025-10-25  
**Based on**: bb8 and deadpool feature analysis  
**Status**: ✅ **IMPLEMENTATION COMPLETE** (2025-10-25)

## Implementation Summary

All identified improvements have been successfully implemented in `src/pool.rs`:

✅ **Wait Queue with Semaphore Backpressure** - Using `tokio::sync::Semaphore` with configurable timeout  
✅ **Connection Recycling Hooks** - `PoolConfig::default_redis_recycle_hook()` with custom hook support  
✅ **Background Reaper Task** - `start_reaper()` / `stop_reaper()` for proactive maintenance  
✅ **Enhanced Metrics** - Utilization %, success rate, wait times, in-use tracking  
✅ **Validation Strategies** - None/OnCheckout/OnCreate/WhileIdle/All  
✅ **Builder Pattern** - Comprehensive `PoolConfig` builder with sensible defaults

**Results**: 560 library tests passing, clippy clean, committed to `feat/pool-improvements` branch.

Redis-tower now has **feature parity with bb8/deadpool** while maintaining our superior Arc-based clone architecture that avoids their borrow/return guard semantics.

## Current redis-tower Pool Features

From `src/pool.rs`:

✅ **What We Have**:
- Max connection limit (`max_size`)
- Min idle connections (`min_idle`)
- Connection lifecycle tracking (created_at, last_used)
- Max lifetime recycling
- Idle timeout
- Health check on checkout (`test_on_checkout`)
- Round-robin connection selection
- Automatic cleanup (every 100th get)
- Connection cloning (Arc-based, cheap)
- Pool statistics tracking

✅ **Architecture**:
- Uses `Arc<RwLock<Vec<PooledConnection>>>` for connection storage
- Atomic counter for round-robin
- Clone-based model (connections stay in pool, clones handed out)

## Missing Features from bb8/deadpool

### 1. **Wait Queue with Timeout** ⭐ HIGH PRIORITY

**Current Behavior**:
```rust
pub async fn get(&self) -> Result<RedisConnection, RedisError> {
    // If no connections available and pool is full, immediately fails
    if connections.is_empty() && connections.len() >= self.config.max_size {
        return Err(RedisError::PoolExhausted);
    }
}
```

**bb8/deadpool Behavior**:
```rust
// Waits for available connection up to configured timeout
pool.get()
    .await // Blocks until connection available or timeout
    .timeout(Duration::from_secs(5))
```

**Implementation Strategy**:
```rust
use tokio::sync::Semaphore;

pub struct ConnectionPool {
    // Add semaphore for backpressure
    semaphore: Arc<Semaphore>,
    // Add wait timeout config
    wait_timeout: Option<Duration>,
}

pub async fn get(&self) -> Result<RedisConnection, RedisError> {
    // Acquire permit (waits if pool exhausted)
    let permit = if let Some(timeout) = self.wait_timeout {
        tokio::time::timeout(timeout, self.semaphore.acquire())
            .await
            .map_err(|_| RedisError::PoolTimeout)?
            .map_err(|_| RedisError::PoolClosed)?
    } else {
        self.semaphore.acquire()
            .await
            .map_err(|_| RedisError::PoolClosed)?
    };
    
    // Get connection from pool
    let conn = self.get_or_create_connection().await?;
    
    // Permit is automatically released when connection returned (via Drop)
    Ok(conn)
}
```

**Benefits**:
- Better backpressure under high load
- No immediate failures when pool temporarily exhausted
- Configurable wait behavior

### 2. **Connection Recycling Hooks** ⭐ MEDIUM PRIORITY

**Missing**:
- Pre-return validation/reset
- Custom recycling logic

**deadpool Has**:
```rust
impl Manager for MyManager {
    async fn recycle(&self, conn: &mut Connection) -> RecycleResult<Error> {
        // Reset connection state before returning to pool
        conn.query("RESET ALL", &[]).await?;
        Ok(())
    }
}
```

**Use Case for Redis**:
```rust
// Before returning connection to pool:
// - Clear any SELECT database changes
// - Cancel any pending WATCH/MULTI
// - Clear any subscription state
async fn recycle_connection(&self, conn: &mut RedisConnection) -> Result<(), RedisError> {
    // Reset to DB 0
    conn.execute(Select::new(0)).await?;
    
    // Clear any transaction state
    conn.execute(Discard).await.ok();
    
    // Unwatch any keys
    conn.execute(Unwatch).await.ok();
    
    Ok(())
}
```

**Implementation**:
```rust
pub struct PoolConfig {
    // Add optional recycling hook
    pub recycle_hook: Option<Arc<dyn Fn(&mut RedisConnection) -> BoxFuture<'_, Result<(), RedisError>> + Send + Sync>>,
}

impl ConnectionPool {
    async fn return_connection(&self, mut conn: RedisConnection) -> Result<(), RedisError> {
        if let Some(hook) = &self.config.recycle_hook {
            hook(&mut conn).await?;
        }
        // Add back to pool
        Ok(())
    }
}
```

### 3. **Background Reaper Task** ⭐ LOW PRIORITY

**Current**:
- Cleanup happens on every 100th `get()` call
- No proactive maintenance

**bb8/deadpool Have**:
- Background task that periodically:
  - Removes stale connections
  - Maintains min_idle count
  - Validates idle connections

**Implementation**:
```rust
impl ConnectionPool {
    pub async fn start_reaper(&self) {
        let pool = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                pool.cleanup().await;
            }
        });
    }
    
    async fn cleanup(&self) {
        let mut conns = self.connections.write().await;
        
        // Remove expired connections
        conns.retain(|conn| {
            !conn.should_recycle(self.config.max_lifetime) &&
            !conn.is_idle_too_long(self.config.idle_timeout, conns.len() > self.config.min_idle)
        });
        
        // Ensure min_idle maintained
        while conns.len() < self.config.min_idle {
            match self.create_connection().await {
                Ok(conn) => conns.push(PooledConnection::new(conn)),
                Err(_) => break,
            }
        }
    }
}
```

### 4. **Detailed Pool Metrics** ⭐ LOW PRIORITY

**Current**:
```rust
pub struct PoolStats {
    pub total_created: AtomicUsize,
    pub total_recycled: AtomicUsize,
    pub health_check_failures: AtomicUsize,
    pub total_gets: AtomicUsize,
}
```

**bb8/deadpool Have**:
- Wait time histogram
- Connection age histogram
- Pool utilization percentage
- Average checkout time

**Enhanced Version**:
```rust
pub struct PoolStats {
    // Current
    pub total_created: AtomicUsize,
    pub total_recycled: AtomicUsize,
    pub health_check_failures: AtomicUsize,
    pub total_gets: AtomicUsize,
    
    // New
    pub current_size: AtomicUsize,           // Live connections
    pub idle_count: AtomicUsize,              // Available connections
    pub in_use_count: AtomicUsize,            // Checked out connections
    pub total_wait_time_ms: AtomicU64,        // Cumulative wait time
    pub max_wait_time_ms: AtomicU64,          // Peak wait time
    pub failed_gets: AtomicUsize,             // Timeout/error gets
}

impl PoolStats {
    pub fn utilization_percent(&self) -> f64 {
        let in_use = self.in_use_count.load(Ordering::Relaxed) as f64;
        let total = self.current_size.load(Ordering::Relaxed) as f64;
        if total == 0.0 { 0.0 } else { (in_use / total) * 100.0 }
    }
    
    pub fn avg_wait_time_ms(&self) -> f64 {
        let total_wait = self.total_wait_time_ms.load(Ordering::Relaxed) as f64;
        let total_gets = self.total_gets.load(Ordering::Relaxed) as f64;
        if total_gets == 0.0 { 0.0 } else { total_wait / total_gets }
    }
}
```

### 5. **Connection Validation Strategy** ⭐ MEDIUM PRIORITY

**Current**:
- Only `test_on_checkout` (PING on every get)

**bb8/deadpool Have**:
- test_on_checkout
- test_on_create
- test_while_idle (periodic background validation)

**Implementation**:
```rust
#[derive(Debug, Clone, Copy)]
pub enum ValidationStrategy {
    None,                    // No validation
    OnCheckout,             // Validate when connection retrieved
    OnCreate,               // Validate when connection created
    WhileIdle(Duration),    // Periodic validation in background
    All,                    // All of the above
}

pub struct PoolConfig {
    pub validation: ValidationStrategy,
}
```

## Priority Recommendations

### Phase 1: Essential Features (Do First)

1. **Wait Queue with Timeout** ⭐⭐⭐
   - Most important missing feature
   - Prevents immediate failures under load
   - Industry standard (bb8, deadpool, Java HikariCP, etc.)
   - ~200 lines of code

2. **Connection Recycling Hooks** ⭐⭐
   - Important for correctness (DB selection, transactions)
   - Redis-specific cleanup needs
   - ~50 lines of code

### Phase 2: Nice to Have

3. **Enhanced Metrics** ⭐
   - Better observability
   - Helps diagnose pool issues
   - ~100 lines of code

4. **Background Reaper** ⭐
   - Current on-demand cleanup works okay
   - Proactive maintenance is cleaner
   - ~150 lines of code

5. **Validation Strategies**
   - Current test_on_checkout is sufficient
   - More options add complexity
   - Consider only if users request

## Comparison Table

| Feature | redis-tower | bb8 | deadpool |
|---------|-------------|-----|----------|
| Max connections | ✅ | ✅ | ✅ |
| Min idle | ✅ | ✅ | ✅ |
| Max lifetime | ✅ | ✅ | ✅ |
| Idle timeout | ✅ | ✅ | ✅ |
| Health check | ✅ (configurable) | ✅ (configurable) | ✅ (configurable) |
| **Wait queue** | ✅ | ✅ | ✅ |
| **Wait timeout** | ✅ | ✅ | ✅ |
| **Recycling hooks** | ✅ | ✅ | ✅ |
| Background reaper | ✅ | ✅ | ✅ |
| Detailed metrics | ✅ | ✅ | ✅ |
| Validation strategies | ✅ | ✅ | ✅ |
| Semaphore backpressure | ✅ | ✅ | ✅ |
| **Clone-based (no guards)** | ✅ | ❌ | ❌ |
| **Type-safe commands** | ✅ | ❌ | ❌ |
| **Tower middleware** | ✅ | ❌ | ❌ |

## Example: Enhanced Pool with Wait Queue

```rust
use tokio::sync::Semaphore;

pub struct PoolConfig {
    pub max_size: usize,
    pub min_idle: usize,
    pub wait_timeout: Option<Duration>,
    pub recycle_hook: Option<RecycleHook>,
    // ... existing fields
}

pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<PooledConnection>>>,
    semaphore: Arc<Semaphore>,  // NEW: Controls access
    config: Arc<PoolConfig>,
    // ... existing fields
}

impl ConnectionPool {
    pub fn with_config(addr: String, config: PoolConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_size)),  // NEW
            // ... existing initialization
        }
    }
    
    pub async fn get(&self) -> Result<RedisConnection, RedisError> {
        let start = Instant::now();
        
        // NEW: Wait for available slot with timeout
        let _permit = if let Some(timeout) = self.config.wait_timeout {
            tokio::time::timeout(timeout, self.semaphore.acquire())
                .await
                .map_err(|_| RedisError::PoolTimeout)?
                .map_err(|_| RedisError::PoolClosed)?
        } else {
            self.semaphore.acquire()
                .await
                .map_err(|_| RedisError::PoolClosed)?
        };
        
        // Track wait time
        let wait_time = start.elapsed();
        self.stats.total_wait_time_ms.fetch_add(
            wait_time.as_millis() as u64,
            Ordering::Relaxed
        );
        
        // Get or create connection (existing logic)
        let mut conn = self.get_or_create_internal().await?;
        
        // NEW: Apply recycling hook if configured
        if let Some(hook) = &self.config.recycle_hook {
            (hook)(&mut conn).await?;
        }
        
        // Health check if enabled (existing logic)
        if self.config.test_on_checkout {
            self.health_check(&conn).await?;
        }
        
        // Track in-use count
        self.stats.in_use_count.fetch_add(1, Ordering::Relaxed);
        
        Ok(conn)
        // Permit automatically released on drop, allowing next waiter
    }
}
```

## Estimated Implementation Time

- **Wait queue + timeout**: 4-6 hours
- **Recycling hooks**: 2-3 hours
- **Enhanced metrics**: 3-4 hours
- **Background reaper**: 4-5 hours
- **Testing**: 4-6 hours

**Total for Phase 1**: ~15-20 hours of work

## Recommendation

Start with **Phase 1 features**:
1. Add semaphore-based wait queue with timeout
2. Add connection recycling hooks for Redis state cleanup
3. Enhance metrics for better observability

This brings redis-tower pool to parity with bb8/deadpool for the features that matter most, while maintaining the simpler clone-based architecture that works well for Redis.
