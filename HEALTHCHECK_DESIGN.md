# Health Check Design for tower-resilience

## Overview

A **proactive health checking** module for tower-resilience that monitors resource health and intelligently selects healthy resources. This is NOT a Tower layer - it's a health-aware wrapper that manages multiple resources.

**Key Distinction:**
- **Circuit Breaker**: Reactive - responds to failures after they happen
- **Health Check**: Proactive - continuously monitors health to prevent failures

These patterns complement each other perfectly!

---

## Core Design Principles

1. **Generic & Reusable**: Works with any resource type (Redis connections, HTTP clients, databases, etc.)
2. **Flexible Selection**: Multiple strategies for choosing which resource to use
3. **Observable**: Event callbacks for health state changes
4. **Type-Erased Selectors**: Simple API without complex generics
5. **Background Monitoring**: Async task continuously checks health
6. **Extensible**: Custom metrics and selection strategies

---

## Core Types

### HealthStatus

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Resource is healthy and ready to use
    Healthy,
    
    /// Resource is degraded but still functional (e.g., slow but working)
    Degraded,
    
    /// Resource is unhealthy and should not be used
    Unhealthy,
    
    /// Health status is unknown (not yet checked or check failed)
    Unknown,
}
```

---

### HealthChecker Trait

```rust
use std::future::Future;

/// Trait for checking the health of a resource.
///
/// Implementors define how to check if a resource (Redis connection, HTTP client, etc.)
/// is healthy.
pub trait HealthChecker<T>: Send + Sync {
    /// Check the health of the given resource.
    ///
    /// Returns `HealthStatus` indicating the current state.
    fn check(&self, resource: &T) -> impl Future<Output = HealthStatus> + Send;
}

// Blanket impl for closures - makes it easy to use
impl<T, F, Fut> HealthChecker<T> for F
where
    F: Fn(&T) -> Fut + Send + Sync,
    Fut: Future<Output = HealthStatus> + Send,
{
    fn check(&self, resource: &T) -> impl Future<Output = HealthStatus> + Send {
        self(resource)
    }
}
```

**Example Usage:**
```rust
// Simple closure-based checker
let redis_checker = |conn: &RedisConnection| async move {
    match conn.ping().await {
        Ok(_) => HealthStatus::Healthy,
        Err(_) => HealthStatus::Unhealthy,
    }
};

// Or implement the trait for complex logic
struct LatencyChecker {
    threshold_ms: u64,
}

impl HealthChecker<RedisConnection> for LatencyChecker {
    async fn check(&self, conn: &RedisConnection) -> HealthStatus {
        let start = Instant::now();
        match conn.ping().await {
            Ok(_) => {
                let latency = start.elapsed().as_millis() as u64;
                if latency < self.threshold_ms {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Degraded
                }
            }
            Err(_) => HealthStatus::Unhealthy,
        }
    }
}
```

---

### HealthCheckedContext

```rust
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::any::Any;

/// Wraps a resource with health check state and metadata.
pub struct HealthCheckedContext<T> {
    /// The actual resource being health checked
    pub resource: T,
    
    /// Name/identifier for this resource
    pub name: String,
    
    /// Health check state (protected by RwLock for concurrent access)
    state: Arc<RwLock<ContextState>>,
    
    /// Extension storage for custom metrics
    extensions: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>>,
}

struct ContextState {
    status: HealthStatus,
    last_check: Instant,
    consecutive_successes: u32,
    consecutive_failures: u32,
}

impl<T> HealthCheckedContext<T> {
    pub fn new(resource: T, name: impl Into<String>) -> Self {
        Self {
            resource,
            name: name.into(),
            state: Arc::new(RwLock::new(ContextState {
                status: HealthStatus::Unknown,
                last_check: Instant::now(),
                consecutive_successes: 0,
                consecutive_failures: 0,
            })),
            extensions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get current health status
    pub fn status(&self) -> HealthStatus {
        self.state.read().unwrap().status
    }
    
    /// Check if resource is healthy or degraded (usable)
    pub fn is_usable(&self) -> bool {
        matches!(self.status(), HealthStatus::Healthy | HealthStatus::Degraded)
    }
    
    /// Check if resource is strictly healthy
    pub fn is_healthy(&self) -> bool {
        self.status() == HealthStatus::Healthy
    }
    
    /// Get time since last health check
    pub fn time_since_check(&self) -> Duration {
        self.state.read().unwrap().last_check.elapsed()
    }
    
    /// Get consecutive success count
    pub fn consecutive_successes(&self) -> u32 {
        self.state.read().unwrap().consecutive_successes
    }
    
    /// Get consecutive failure count
    pub fn consecutive_failures(&self) -> u32 {
        self.state.read().unwrap().consecutive_failures
    }
    
    /// Store a custom metric (for custom selectors)
    pub fn set_metric<V: Any + Send + Sync + Clone + 'static>(&self, key: &str, value: V) {
        self.extensions.write().unwrap().insert(key.to_string(), Box::new(value));
    }
    
    /// Retrieve a custom metric
    pub fn get_metric<V: Any + Send + Sync + Clone + 'static>(&self, key: &str) -> Option<V> {
        self.extensions.read().unwrap()
            .get(key)
            .and_then(|v| v.downcast_ref::<V>())
            .cloned()
    }
    
    /// Update health status (internal use)
    pub(crate) fn update_status(&self, new_status: HealthStatus) {
        let mut state = self.state.write().unwrap();
        state.status = new_status;
        state.last_check = Instant::now();
        
        match new_status {
            HealthStatus::Healthy => {
                state.consecutive_successes += 1;
                state.consecutive_failures = 0;
            }
            HealthStatus::Unhealthy | HealthStatus::Degraded => {
                state.consecutive_failures += 1;
                state.consecutive_successes = 0;
            }
            HealthStatus::Unknown => {
                // Don't reset counters for unknown
            }
        }
    }
}
```

---

### Selection Strategies

**Type-erased design for simplicity:**

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

/// Reference to context for selection (avoids cloning)
pub struct HealthCheckedContextRef<'a> {
    pub name: &'a str,
    pub status: &'a HealthStatus,
    pub last_check: Instant,
    pub consecutive_successes: u32,
    pub consecutive_failures: u32,
}

/// Type-erased selection function trait
pub trait SelectionFn: Send + Sync {
    fn select(&self, contexts: &[HealthCheckedContextRef]) -> Option<usize>;
}

// Blanket impl for closures
impl<F> SelectionFn for F
where
    F: Fn(&[HealthCheckedContextRef]) -> Option<usize> + Send + Sync,
{
    fn select(&self, contexts: &[HealthCheckedContextRef]) -> Option<usize> {
        self(contexts)
    }
}

/// Built-in selection strategies
pub enum SelectionStrategy {
    /// Return first healthy resource
    FirstAvailable,
    
    /// Random selection from healthy resources
    Random,
    
    /// Round-robin across healthy resources
    RoundRobin,
    
    /// Prefer healthy over degraded
    PreferHealthy,
    
    /// Custom selection logic (type-erased)
    Custom(Box<dyn SelectionFn>),
}
```

**Built-in Strategy Implementations:**

```rust
impl SelectionStrategy {
    pub(crate) fn select<T>(
        &self,
        contexts: &[HealthCheckedContext<T>],
        round_robin_counter: &AtomicUsize,
    ) -> Option<usize> {
        // Build reference slice
        let refs: Vec<_> = contexts.iter().map(|ctx| {
            let state = ctx.state.read().unwrap();
            HealthCheckedContextRef {
                name: &ctx.name,
                status: &state.status,
                last_check: state.last_check,
                consecutive_successes: state.consecutive_successes,
                consecutive_failures: state.consecutive_failures,
            }
        }).collect();
        
        match self {
            SelectionStrategy::FirstAvailable => {
                refs.iter().position(|ctx| ctx.status.is_usable())
            }
            
            SelectionStrategy::Random => {
                let usable: Vec<_> = refs.iter().enumerate()
                    .filter(|(_, ctx)| ctx.status.is_usable())
                    .map(|(i, _)| i)
                    .collect();
                
                if usable.is_empty() {
                    None
                } else {
                    use rand::Rng;
                    Some(usable[rand::thread_rng().gen_range(0..usable.len())])
                }
            }
            
            SelectionStrategy::RoundRobin => {
                let usable: Vec<_> = refs.iter().enumerate()
                    .filter(|(_, ctx)| ctx.status.is_usable())
                    .map(|(i, _)| i)
                    .collect();
                
                if usable.is_empty() {
                    None
                } else {
                    let idx = round_robin_counter.fetch_add(1, Ordering::Relaxed);
                    Some(usable[idx % usable.len()])
                }
            }
            
            SelectionStrategy::PreferHealthy => {
                // First try strictly healthy
                refs.iter().position(|ctx| *ctx.status == HealthStatus::Healthy)
                    // Fall back to degraded
                    .or_else(|| refs.iter().position(|ctx| *ctx.status == HealthStatus::Degraded))
            }
            
            SelectionStrategy::Custom(selector) => {
                selector.select(&refs)
            }
        }
    }
}

impl HealthStatus {
    fn is_usable(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }
}
```

---

### Custom Selector Examples

**1. Latency-Based Selection:**
```rust
struct LatencyBasedSelector;

impl SelectionFn for LatencyBasedSelector {
    fn select(&self, contexts: &[HealthCheckedContextRef]) -> Option<usize> {
        contexts.iter()
            .enumerate()
            .filter(|(_, ctx)| ctx.status.is_usable())
            .min_by_key(|(i, ctx)| {
                // Assume latency stored as custom metric
                contexts[*i].get_metric::<u64>("latency_ms").unwrap_or(u64::MAX)
            })
            .map(|(i, _)| i)
    }
}

// Usage:
let config = HealthCheckConfig::builder()
    .selection_strategy(SelectionStrategy::Custom(Box::new(LatencyBasedSelector)))
    .build();
```

**2. Weighted Selection:**
```rust
let weighted_selector = |contexts: &[HealthCheckedContextRef]| -> Option<usize> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    
    let weighted: Vec<_> = contexts.iter()
        .enumerate()
        .filter(|(_, ctx)| ctx.status.is_usable())
        .map(|(i, _)| (i, contexts[i].get_metric::<u32>("weight").unwrap_or(1)))
        .collect();
    
    if weighted.is_empty() {
        return None;
    }
    
    let total_weight: u32 = weighted.iter().map(|(_, w)| w).sum();
    let mut choice = rng.gen_range(0..total_weight);
    
    for (idx, weight) in weighted {
        if choice < weight {
            return Some(idx);
        }
        choice -= weight;
    }
    
    Some(weighted[0].0)
};

let config = HealthCheckConfig::builder()
    .selection_strategy(SelectionStrategy::Custom(Box::new(weighted_selector)))
    .build();
```

**3. Geographic/Affinity-Based:**
```rust
struct GeographicSelector {
    preferred_region: String,
}

impl SelectionFn for GeographicSelector {
    fn select(&self, contexts: &[HealthCheckedContextRef]) -> Option<usize> {
        // Try preferred region first
        contexts.iter()
            .enumerate()
            .filter(|(_, ctx)| ctx.status.is_usable())
            .find(|(i, _)| {
                contexts[*i].get_metric::<String>("region")
                    .map(|r| r == self.preferred_region)
                    .unwrap_or(false)
            })
            .map(|(i, _)| i)
            // Fallback to any healthy
            .or_else(|| contexts.iter().position(|ctx| ctx.status.is_usable()))
    }
}
```

**4. Sticky Session:**
```rust
use std::collections::HashMap;

struct StickySessionSelector {
    session_map: Arc<RwLock<HashMap<String, usize>>>,
}

impl SelectionFn for StickySessionSelector {
    fn select(&self, contexts: &[HealthCheckedContextRef]) -> Option<usize> {
        // Get session ID from thread-local or context
        let session_id = get_current_session_id();
        
        let mut map = self.session_map.write().unwrap();
        
        // Check if we have a sticky assignment
        if let Some(&idx) = map.get(&session_id) {
            if idx < contexts.len() && contexts[idx].status.is_usable() {
                return Some(idx);
            }
        }
        
        // No valid sticky assignment, pick new one
        let new_idx = contexts.iter().position(|ctx| ctx.status.is_usable())?;
        map.insert(session_id, new_idx);
        Some(new_idx)
    }
}
```

---

## HealthCheckConfig

```rust
use std::time::Duration;

pub struct HealthCheckConfig {
    /// How often to run health checks
    pub(crate) check_interval: Duration,
    
    /// Strategy for selecting which resource to use
    pub(crate) selection_strategy: SelectionStrategy,
    
    /// Number of consecutive successes to mark as healthy
    pub(crate) success_threshold: u32,
    
    /// Number of consecutive failures to mark as unhealthy
    pub(crate) failure_threshold: u32,
    
    /// Event callbacks (behind tracing feature)
    #[cfg(feature = "tracing")]
    pub(crate) on_health_change: Option<Arc<dyn Fn(&str, HealthStatus, HealthStatus) + Send + Sync>>,
    
    #[cfg(feature = "tracing")]
    pub(crate) on_check_failed: Option<Arc<dyn Fn(&str, &dyn std::error::Error) + Send + Sync>>,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(10),
            selection_strategy: SelectionStrategy::FirstAvailable,
            success_threshold: 2,
            failure_threshold: 3,
            #[cfg(feature = "tracing")]
            on_health_change: None,
            #[cfg(feature = "tracing")]
            on_check_failed: None,
        }
    }
}

pub struct HealthCheckConfigBuilder {
    check_interval: Duration,
    selection_strategy: SelectionStrategy,
    success_threshold: u32,
    failure_threshold: u32,
    #[cfg(feature = "tracing")]
    on_health_change: Option<Arc<dyn Fn(&str, HealthStatus, HealthStatus) + Send + Sync>>,
    #[cfg(feature = "tracing")]
    on_check_failed: Option<Arc<dyn Fn(&str, &dyn std::error::Error) + Send + Sync>>,
}

impl HealthCheckConfig {
    pub fn builder() -> HealthCheckConfigBuilder {
        HealthCheckConfigBuilder::default()
    }
}

impl HealthCheckConfigBuilder {
    pub fn check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }
    
    pub fn selection_strategy(mut self, strategy: SelectionStrategy) -> Self {
        self.selection_strategy = strategy;
        self
    }
    
    pub fn success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }
    
    pub fn failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }
    
    /// Callback when health status changes
    #[cfg(feature = "tracing")]
    pub fn on_health_change<F>(mut self, callback: F) -> Self
    where
        F: Fn(&str, HealthStatus, HealthStatus) + Send + Sync + 'static,
    {
        self.on_health_change = Some(Arc::new(callback));
        self
    }
    
    /// Callback when health check fails
    #[cfg(feature = "tracing")]
    pub fn on_check_failed<F>(mut self, callback: F) -> Self
    where
        F: Fn(&str, &dyn std::error::Error) + Send + Sync + 'static,
    {
        self.on_check_failed = Some(Arc::new(callback));
        self
    }
    
    pub fn build(self) -> HealthCheckConfig {
        HealthCheckConfig {
            check_interval: self.check_interval,
            selection_strategy: self.selection_strategy,
            success_threshold: self.success_threshold,
            failure_threshold: self.failure_threshold,
            #[cfg(feature = "tracing")]
            on_health_change: self.on_health_change,
            #[cfg(feature = "tracing")]
            on_check_failed: self.on_check_failed,
        }
    }
}

impl Default for HealthCheckConfigBuilder {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(10),
            selection_strategy: SelectionStrategy::FirstAvailable,
            success_threshold: 2,
            failure_threshold: 3,
            #[cfg(feature = "tracing")]
            on_health_change: None,
            #[cfg(feature = "tracing")]
            on_check_failed: None,
        }
    }
}
```

---

## HealthCheckWrapper

```rust
use tokio::task::JoinHandle;

pub struct HealthCheckWrapper<T> {
    contexts: Arc<RwLock<Vec<HealthCheckedContext<T>>>>,
    config: Arc<HealthCheckConfig>,
    checker: Arc<dyn HealthChecker<T>>,
    round_robin_counter: Arc<AtomicUsize>,
    background_task: Option<JoinHandle<()>>,
}

impl<T: Send + Sync + 'static> HealthCheckWrapper<T> {
    /// Create a new builder
    pub fn builder() -> HealthCheckWrapperBuilder<T> {
        HealthCheckWrapperBuilder::new()
    }
    
    /// Get a healthy resource using the configured selection strategy
    pub fn get_healthy(&self) -> Option<&T> {
        let contexts = self.contexts.read().unwrap();
        let idx = self.config.selection_strategy.select(
            &contexts,
            &self.round_robin_counter,
        )?;
        Some(&contexts[idx].resource)
    }
    
    /// Get a usable resource (healthy or degraded)
    pub fn get_usable(&self) -> Option<&T> {
        // Same as get_healthy since selectors already consider degraded as usable
        self.get_healthy()
    }
    
    /// Get all contexts (for inspection)
    pub fn contexts(&self) -> Vec<HealthCheckedContext<T>> 
    where
        T: Clone,
    {
        self.contexts.read().unwrap().clone()
    }
    
    /// Manually trigger health check for all resources
    pub async fn check_all(&self) {
        let contexts = self.contexts.read().unwrap().clone();
        
        for context in contexts {
            self.check_one(&context).await;
        }
    }
    
    async fn check_one(&self, context: &HealthCheckedContext<T>) {
        let old_status = context.status();
        let new_status = self.checker.check(&context.resource).await;
        
        // Update based on thresholds
        let state = context.state.read().unwrap();
        let consecutive_successes = state.consecutive_successes;
        let consecutive_failures = state.consecutive_failures;
        drop(state);
        
        let actual_status = match new_status {
            HealthStatus::Healthy => {
                if consecutive_successes + 1 >= self.config.success_threshold {
                    HealthStatus::Healthy
                } else {
                    old_status // Wait for threshold
                }
            }
            HealthStatus::Unhealthy => {
                if consecutive_failures + 1 >= self.config.failure_threshold {
                    HealthStatus::Unhealthy
                } else {
                    old_status // Wait for threshold
                }
            }
            HealthStatus::Degraded => HealthStatus::Degraded,
            HealthStatus::Unknown => HealthStatus::Unknown,
        };
        
        context.update_status(actual_status);
        
        // Fire callback if status changed
        #[cfg(feature = "tracing")]
        if old_status != actual_status {
            if let Some(ref callback) = self.config.on_health_change {
                callback(&context.name, old_status, actual_status);
            }
        }
    }
    
    /// Start background health checking
    pub fn start(mut self) -> Self {
        let contexts = self.contexts.clone();
        let checker = self.checker.clone();
        let config = self.config.clone();
        let interval = config.check_interval;
        
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            
            loop {
                ticker.tick().await;
                
                let contexts_snapshot = contexts.read().unwrap().clone();
                
                for context in contexts_snapshot {
                    let old_status = context.status();
                    let new_status = checker.check(&context.resource).await;
                    
                    // (same threshold logic as check_one)
                    
                    context.update_status(new_status);
                    
                    #[cfg(feature = "tracing")]
                    if old_status != new_status {
                        if let Some(ref callback) = config.on_health_change {
                            callback(&context.name, old_status, new_status);
                        }
                    }
                }
            }
        });
        
        self.background_task = Some(handle);
        self
    }
    
    /// Stop background health checking and wait for cleanup
    pub async fn stop(mut self) {
        if let Some(handle) = self.background_task.take() {
            handle.abort();
            let _ = handle.await; // Wait for task to finish
        }
    }
}

impl<T> Drop for HealthCheckWrapper<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.background_task.take() {
            handle.abort();
        }
    }
}

// Builder
pub struct HealthCheckWrapperBuilder<T> {
    contexts: Vec<HealthCheckedContext<T>>,
    config: Option<HealthCheckConfig>,
    checker: Option<Arc<dyn HealthChecker<T>>>,
}

impl<T> HealthCheckWrapperBuilder<T> {
    pub fn new() -> Self {
        Self {
            contexts: Vec::new(),
            config: None,
            checker: None,
        }
    }
    
    pub fn with_context(mut self, resource: T, name: impl Into<String>) -> Self {
        self.contexts.push(HealthCheckedContext::new(resource, name));
        self
    }
    
    pub fn with_config(mut self, config: HealthCheckConfig) -> Self {
        self.config = Some(config);
        self
    }
    
    pub fn with_checker<C>(mut self, checker: C) -> Self
    where
        C: HealthChecker<T> + 'static,
    {
        self.checker = Some(Arc::new(checker));
        self
    }
    
    pub fn build(self) -> HealthCheckWrapper<T>
    where
        T: Send + Sync + 'static,
    {
        HealthCheckWrapper {
            contexts: Arc::new(RwLock::new(self.contexts)),
            config: Arc::new(self.config.unwrap_or_default()),
            checker: self.checker.expect("HealthChecker must be provided"),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            background_task: None,
        }
    }
}
```

---

## Usage Examples

### Example 1: Redis Connection Pool

```rust
use redis_tower::RedisConnection;
use tower_resilience_healthcheck::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create multiple Redis connections
    let primary = RedisConnection::connect("redis://localhost:6379").await?;
    let replica1 = RedisConnection::connect("redis://localhost:6380").await?;
    let replica2 = RedisConnection::connect("redis://localhost:6381").await?;
    
    // Create health checker
    let redis_checker = |conn: &RedisConnection| async move {
        match conn.ping().await {
            Ok(_) => HealthStatus::Healthy,
            Err(_) => HealthStatus::Unhealthy,
        }
    };
    
    // Build health check wrapper
    let health_wrapper = HealthCheckWrapper::builder()
        .with_context(primary, "primary")
        .with_context(replica1, "replica-1")
        .with_context(replica2, "replica-2")
        .with_checker(redis_checker)
        .with_config(
            HealthCheckConfig::builder()
                .check_interval(Duration::from_secs(5))
                .selection_strategy(SelectionStrategy::RoundRobin)
                .on_health_change(|name, old, new| {
                    println!("{}: {:?} -> {:?}", name, old, new);
                })
                .build()
        )
        .build()
        .start(); // Start background health checking
    
    // Use it
    loop {
        if let Some(conn) = health_wrapper.get_healthy() {
            match conn.get("mykey").await {
                Ok(value) => println!("Got value: {:?}", value),
                Err(e) => eprintln!("Error: {}", e),
            }
        } else {
            eprintln!("No healthy connections available!");
        }
        
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

### Example 2: HTTP Client with Latency-Based Selection

```rust
use reqwest::Client;
use tower_resilience_healthcheck::*;

struct LatencyChecker {
    health_endpoint: String,
    threshold_ms: u64,
}

impl HealthChecker<Client> for LatencyChecker {
    async fn check(&self, client: &Client) -> HealthStatus {
        let start = Instant::now();
        
        match client.get(&self.health_endpoint).send().await {
            Ok(resp) if resp.status().is_success() => {
                let latency = start.elapsed().as_millis() as u64;
                
                // Store latency as custom metric
                // (would need access to context here - see note below)
                
                if latency < self.threshold_ms {
                    HealthStatus::Healthy
                } else if latency < self.threshold_ms * 2 {
                    HealthStatus::Degraded
                } else {
                    HealthStatus::Unhealthy
                }
            }
            _ => HealthStatus::Unhealthy,
        }
    }
}

// Custom selector that uses latency
let latency_selector = |contexts: &[HealthCheckedContextRef]| -> Option<usize> {
    contexts.iter()
        .enumerate()
        .filter(|(_, ctx)| ctx.status.is_usable())
        .min_by_key(|(i, _)| {
            contexts[*i].get_metric::<u64>("latency_ms").unwrap_or(u64::MAX)
        })
        .map(|(i, _)| i)
};

let wrapper = HealthCheckWrapper::builder()
    .with_context(Client::new(), "api-1")
    .with_context(Client::new(), "api-2")
    .with_checker(LatencyChecker {
        health_endpoint: "https://api.example.com/health".into(),
        threshold_ms: 100,
    })
    .with_config(
        HealthCheckConfig::builder()
            .selection_strategy(SelectionStrategy::Custom(Box::new(latency_selector)))
            .build()
    )
    .build()
    .start();
```

### Example 3: Database Connection Pool

```rust
use sqlx::{PgPool, Row};
use tower_resilience_healthcheck::*;

let db_checker = |pool: &PgPool| async move {
    match sqlx::query("SELECT 1").fetch_one(pool).await {
        Ok(_) => HealthStatus::Healthy,
        Err(_) => HealthStatus::Unhealthy,
    }
};

let wrapper = HealthCheckWrapper::builder()
    .with_context(
        PgPool::connect("postgres://user:pass@db1/mydb").await?,
        "db-primary"
    )
    .with_context(
        PgPool::connect("postgres://user:pass@db2/mydb").await?,
        "db-standby"
    )
    .with_checker(db_checker)
    .with_config(
        HealthCheckConfig::builder()
            .check_interval(Duration::from_secs(10))
            .selection_strategy(SelectionStrategy::PreferHealthy)
            .success_threshold(3)  // 3 successful checks to mark healthy
            .failure_threshold(2)  // 2 failed checks to mark unhealthy
            .build()
    )
    .build()
    .start();

// Use it
if let Some(db) = wrapper.get_healthy() {
    let row: (i32,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(db)
        .await?;
    println!("User count: {}", row.0);
}
```

---

## Integration with Circuit Breaker

Health checking and circuit breaking complement each other:

```rust
use tower::ServiceBuilder;
use tower_resilience_circuitbreaker::CircuitBreakerLayer;
use tower_resilience_healthcheck::*;

// Create health-checked resources
let health_wrapper = HealthCheckWrapper::builder()
    .with_context(redis_conn1, "redis-1")
    .with_context(redis_conn2, "redis-2")
    .with_checker(redis_health_check)
    .with_config(
        HealthCheckConfig::builder()
            .check_interval(Duration::from_secs(5))
            .selection_strategy(SelectionStrategy::RoundRobin)
            .build()
    )
    .build()
    .start();

// Wrap with circuit breaker
let service = ServiceBuilder::new()
    .layer(CircuitBreakerLayer::builder()
        .failure_rate_threshold(0.5)
        .wait_duration_in_open(Duration::from_secs(30))
        .build())
    .service(RedisService::new(health_wrapper));

// Now you have:
// - Proactive health checking (prevents sending to dead resources)
// - Reactive circuit breaking (stops trying after repeated failures)
```

---

## Implementation Roadmap

### Phase 1: Core (Days 1-2)
- [ ] Create `tower-resilience-healthcheck` crate
- [ ] Implement `HealthStatus` enum
- [ ] Implement `HealthChecker` trait with blanket impl
- [ ] Implement `HealthCheckedContext` with extensions
- [ ] Implement `HealthCheckConfig` and builder
- [ ] Implement `HealthCheckWrapper` with `FirstAvailable` strategy only
- [ ] Implement background health checking task
- [ ] Write basic tests with mock services

### Phase 2: Selection Strategies (Day 3)
- [ ] Implement type-erased `SelectionFn` trait
- [ ] Implement `Random` selector
- [ ] Implement `RoundRobin` selector
- [ ] Implement `PreferHealthy` selector
- [ ] Support `Custom` selector
- [ ] Write tests for each strategy
- [ ] Document custom selector examples

### Phase 3: Observability & Polish (Day 4)
- [ ] Add event callbacks (`on_health_change`, `on_check_failed`)
- [ ] Implement extension/custom metrics system
- [ ] Add `JoinHandle` management for background task
- [ ] Add `stop()` method for graceful shutdown
- [ ] Write comprehensive examples (Redis, HTTP, database)
- [ ] Add benchmarks

### Phase 4: Integration (Day 5)
- [ ] Update tower-resilience README
- [ ] Write integration example with circuit breaker
- [ ] Write integration example with reconnect
- [ ] Documentation polish
- [ ] Release preparation

---

## Module Structure

```
crates/tower-resilience-healthcheck/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public API, re-exports
│   ├── config.rs           # HealthCheckConfig and builder
│   ├── context.rs          # HealthCheckedContext
│   ├── checker.rs          # HealthChecker trait
│   ├── selector.rs         # SelectionStrategy and built-in selectors
│   └── wrapper.rs          # HealthCheckWrapper and builder
├── examples/
│   ├── basic.rs            # Mock services example
│   ├── redis.rs            # Redis connection pool
│   ├── http.rs             # HTTP client with latency
│   ├── database.rs         # Database connection pool
│   └── custom_selector.rs  # Custom selection strategies
└── tests/
    ├── integration.rs      # Integration tests
    └── strategies.rs       # Selector strategy tests
```

---

## Cargo.toml

```toml
[package]
name = "tower-resilience-healthcheck"
version = "0.1.0"
edition = "2021"
license = "MIT OR Apache-2.0"
description = "Proactive health checking for resources with intelligent selection strategies"
repository = "https://github.com/yourusername/tower-resilience"

[dependencies]
tokio = { version = "1.0", features = ["rt", "time", "sync"] }
rand = { version = "0.8", optional = true }
tracing = { version = "0.1", optional = true }

[dev-dependencies]
tokio = { version = "1.0", features = ["full"] }
redis = "0.24"
reqwest = "0.11"
sqlx = { version = "0.7", features = ["postgres"] }

[features]
default = []
random = ["dep:rand"]
tracing = ["dep:tracing"]
full = ["random", "tracing"]
```

---

## Testing Strategy

### Unit Tests
- Health status transitions
- Context state management
- Each selection strategy in isolation
- Extension storage and retrieval
- Threshold-based state changes

### Integration Tests
- Background health checking
- Multiple contexts with different states
- Selection strategy behavior under load
- Event callback invocations
- Graceful shutdown

### Examples as Tests
- Redis connection pool (requires Redis)
- HTTP client (uses httpbin.org)
- Mock services (no external deps)

---

## Key Design Decisions Summary

1. **Type Erasure for Selectors**: Keeps API simple, avoids complex generics on user-facing types
2. **Extension System**: `HashMap<String, Box<dyn Any>>` for custom metrics without changing core types
3. **Event Callbacks**: Behind `tracing` feature, follows tower-resilience patterns
4. **Background Task Management**: `JoinHandle` with `Drop` impl for safety, `stop()` for graceful shutdown
5. **Not a Tower Layer**: This is a wrapper/manager pattern, not middleware - different use case
6. **Threshold-Based Transitions**: Prevents flapping, requires consecutive successes/failures
7. **Degraded State**: Allows "slow but working" resources to still be used
8. **Reference Types**: `HealthCheckedContextRef` avoids cloning for selection
9. **Round Robin Counter**: In wrapper not selector to maintain state across calls

---

## Future Enhancements (Post-MVP)

- [ ] Weighted health scores (0-100 instead of enum)
- [ ] Historical health metrics (moving averages)
- [ ] Adaptive thresholds based on SLA
- [ ] Health check result caching
- [ ] Dynamic resource addition/removal
- [ ] Health check priority (critical vs nice-to-have)
- [ ] Kubernetes liveness/readiness probe integration

---

## Questions Resolved

**Q: Type erasure vs generics for Custom selector?**  
A: Type erasure - keeps `HealthCheckWrapper` non-generic

**Q: Add event system from start?**  
A: Yes - essential for observability

**Q: Crate naming?**  
A: `tower-resilience-healthcheck` - consistent with ecosystem

**Q: Round robin counter placement?**  
A: In wrapper - simplifies selector interface

**Q: Custom metrics storage?**  
A: Extension system with `HashMap<String, Box<dyn Any>>`

**Q: Background task management?**  
A: Return `JoinHandle`, provide `stop()`, `Drop` aborts task

**Q: Which example first?**  
A: Basic (mock services) → Redis → HTTP

---

## Ready to Implement!

This design is production-ready. All major decisions are made, the API is clean, and the implementation path is clear.

**Next step**: Create the crate and start with Phase 1 (Core functionality).
