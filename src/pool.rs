//! Connection pool with health checking and lifecycle management
//!
//! Provides a production-ready connection pool inspired by deadpool and bb8,
//! tailored for Redis connection management. This implementation includes:
//!
//! - **Wait queue with timeout**: Graceful backpressure when pool is exhausted
//! - **Connection recycling**: Hooks to reset connection state before reuse
//! - **Background reaper**: Proactive cleanup of stale connections
//! - **Enhanced metrics**: Detailed pool statistics and utilization tracking
//! - **Validation strategies**: Multiple connection validation options
//!
//! # Design Note
//! Redis connections are Arc-wrapped internally, making cloning cheap. This pool
//! maintains a set of healthy connections and hands out clones, rather than
//! using a borrow/return model. This simplifies the API and works well with Tower.

use crate::client::RedisConnection;
use crate::commands::{Discard, Ping, Select, Unwatch};
use crate::tls::TlsConfig;
use crate::types::RedisError;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;

/// Type alias for connection recycling hooks
pub type RecycleHook = Arc<
    dyn Fn(&mut RedisConnection) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send>>
        + Send
        + Sync,
>;

/// Connection validation strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationStrategy {
    /// No validation
    None,
    /// Validate only when checking out from pool
    #[default]
    OnCheckout,
    /// Validate when creating new connections
    OnCreate,
    /// Validate periodically in background
    WhileIdle(Duration),
    /// Validate on all occasions
    All,
}

/// Configuration for connection pool behavior
#[derive(Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_size: usize,
    /// Minimum number of idle connections to maintain
    pub min_idle: usize,
    /// Maximum lifetime of a connection before recycling (None = infinite)
    pub max_lifetime: Option<Duration>,
    /// How long a connection can be idle before being closed (None = infinite)
    pub idle_timeout: Option<Duration>,
    /// Timeout when waiting for an available connection (None = no timeout)
    pub wait_timeout: Option<Duration>,
    /// Connection validation strategy
    pub validation: ValidationStrategy,
    /// Optional hook to recycle connection state before returning to pool
    pub recycle_hook: Option<RecycleHook>,
    /// Background reaper interval (None = disabled)
    pub reaper_interval: Option<Duration>,
    /// Enable dynamic scaling based on load (default: false)
    pub enable_dynamic_scaling: bool,
    /// Scale up when utilization exceeds this threshold (0.0-1.0, default: 0.8)
    pub scale_up_threshold: f32,
    /// Scale down when utilization falls below this threshold (0.0-1.0, default: 0.2)
    pub scale_down_threshold: f32,
    /// How many connections to add during scale-up (default: 1)
    pub scale_increment: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_idle: 0,
            max_lifetime: Some(Duration::from_secs(30 * 60)), // 30 minutes
            idle_timeout: Some(Duration::from_secs(10 * 60)), // 10 minutes
            wait_timeout: Some(Duration::from_secs(30)),      // 30 seconds
            validation: ValidationStrategy::OnCheckout,
            recycle_hook: None,
            reaper_interval: Some(Duration::from_secs(30)), // 30 seconds
            enable_dynamic_scaling: false,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.2,
            scale_increment: 1,
        }
    }
}

impl std::fmt::Debug for PoolConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConfig")
            .field("max_size", &self.max_size)
            .field("min_idle", &self.min_idle)
            .field("max_lifetime", &self.max_lifetime)
            .field("idle_timeout", &self.idle_timeout)
            .field("wait_timeout", &self.wait_timeout)
            .field("validation", &self.validation)
            .field("has_recycle_hook", &self.recycle_hook.is_some())
            .field("reaper_interval", &self.reaper_interval)
            .field("enable_dynamic_scaling", &self.enable_dynamic_scaling)
            .field("scale_up_threshold", &self.scale_up_threshold)
            .field("scale_down_threshold", &self.scale_down_threshold)
            .field("scale_increment", &self.scale_increment)
            .finish()
    }
}

impl PoolConfig {
    /// Create a simple pool configuration with just max_size
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            ..Default::default()
        }
    }

    /// Set minimum idle connections
    pub fn with_min_idle(mut self, min_idle: usize) -> Self {
        self.min_idle = min_idle.min(self.max_size);
        self
    }

    /// Set maximum connection lifetime
    pub fn with_max_lifetime(mut self, duration: Option<Duration>) -> Self {
        self.max_lifetime = duration;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, duration: Option<Duration>) -> Self {
        self.idle_timeout = duration;
        self
    }

    /// Set wait timeout for acquiring connections
    pub fn with_wait_timeout(mut self, duration: Option<Duration>) -> Self {
        self.wait_timeout = duration;
        self
    }

    /// Set validation strategy
    pub fn with_validation(mut self, strategy: ValidationStrategy) -> Self {
        self.validation = strategy;
        self
    }

    /// Set connection recycling hook
    pub fn with_recycle_hook(mut self, hook: RecycleHook) -> Self {
        self.recycle_hook = Some(hook);
        self
    }

    /// Set background reaper interval
    pub fn with_reaper_interval(mut self, duration: Option<Duration>) -> Self {
        self.reaper_interval = duration;
        self
    }

    /// Enable dynamic scaling based on load
    ///
    /// When enabled, the pool will automatically scale up/down based on utilization.
    /// Requires the background reaper to be enabled (reaper_interval must be Some).
    pub fn with_dynamic_scaling(mut self, enabled: bool) -> Self {
        self.enable_dynamic_scaling = enabled;
        self
    }

    /// Set scale-up threshold (0.0-1.0)
    ///
    /// When utilization exceeds this threshold, the pool will add connections.
    /// Default: 0.8 (80%)
    pub fn with_scale_up_threshold(mut self, threshold: f32) -> Self {
        self.scale_up_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set scale-down threshold (0.0-1.0)
    ///
    /// When utilization falls below this threshold, the pool will remove idle connections.
    /// Default: 0.2 (20%)
    pub fn with_scale_down_threshold(mut self, threshold: f32) -> Self {
        self.scale_down_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set how many connections to add during scale-up
    ///
    /// Default: 1
    pub fn with_scale_increment(mut self, increment: usize) -> Self {
        self.scale_increment = increment.max(1);
        self
    }

    /// Enable/disable health check on checkout (convenience method)
    pub fn with_test_on_checkout(mut self, enabled: bool) -> Self {
        self.validation = if enabled {
            ValidationStrategy::OnCheckout
        } else {
            ValidationStrategy::None
        };
        self
    }

    /// Create default Redis recycling hook that resets connection state
    pub fn default_redis_recycle_hook() -> RecycleHook {
        Arc::new(|conn: &mut RedisConnection| {
            // Clone the connection for the async block
            let conn = conn.clone();
            Box::pin(async move {
                // Reset to DB 0
                conn.execute(Select::new(0)).await.ok();

                // Clear any transaction state
                conn.execute(Discard).await.ok();

                // Unwatch any keys
                conn.execute(Unwatch).await.ok();

                Ok(())
            })
        })
    }
}

/// A pooled connection with lifecycle tracking
#[derive(Clone)]
struct PooledConnection {
    /// The actual Redis connection
    conn: RedisConnection,
    /// When this connection was created
    created_at: Instant,
    /// When this connection was last used
    last_used: Instant,
    /// Number of times this connection has been used
    use_count: u64,
}

impl PooledConnection {
    fn new(conn: RedisConnection) -> Self {
        let now = Instant::now();
        Self {
            conn,
            created_at: now,
            last_used: now,
            use_count: 0,
        }
    }

    fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    fn idle_time(&self) -> Duration {
        self.last_used.elapsed()
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
        self.use_count += 1;
    }

    /// Check if connection should be recycled based on age
    fn should_recycle(&self, max_lifetime: Option<Duration>) -> bool {
        if let Some(max) = max_lifetime {
            self.age() > max
        } else {
            false
        }
    }

    /// Check if connection has been idle too long
    fn is_idle_too_long(&self, idle_timeout: Option<Duration>, respect_min: bool) -> bool {
        if !respect_min {
            return false;
        }
        if let Some(timeout) = idle_timeout {
            self.idle_time() > timeout
        } else {
            false
        }
    }
}

/// Enhanced pool statistics for monitoring
#[derive(Default)]
pub struct PoolStats {
    // Creation/recycling stats
    /// Total connections created
    pub total_created: AtomicUsize,
    /// Total connections recycled
    pub total_recycled: AtomicUsize,
    /// Total health check failures
    pub health_check_failures: AtomicUsize,

    // Usage stats
    /// Total get operations
    pub total_gets: AtomicUsize,
    /// Total successful gets
    pub successful_gets: AtomicUsize,
    /// Total failed gets (timeout/error)
    pub failed_gets: AtomicUsize,

    // Timing stats
    /// Total wait time in milliseconds
    pub total_wait_time_ms: AtomicU64,
    /// Maximum wait time seen in milliseconds
    pub max_wait_time_ms: AtomicU64,

    // Current state
    /// Currently in-use connections
    pub in_use_count: AtomicUsize,

    // Scaling stats
    /// Total scale-up operations
    pub total_scale_ups: AtomicUsize,
    /// Total scale-down operations
    pub total_scale_downs: AtomicUsize,
    /// Total connections added via scaling
    pub scaled_up_connections: AtomicUsize,
    /// Total connections removed via scaling
    pub scaled_down_connections: AtomicUsize,
}

/// A round-robin connection pool with health checking and lifecycle management
#[derive(Clone)]
pub struct ConnectionPool {
    /// Pool of connections
    connections: Arc<RwLock<Vec<PooledConnection>>>,
    /// Next connection index for round-robin
    next_index: Arc<AtomicUsize>,
    /// Semaphore controlling max connections
    semaphore: Arc<Semaphore>,
    /// Pool configuration
    config: Arc<PoolConfig>,
    /// Node address
    addr: String,
    /// TLS configuration
    tls: TlsConfig,
    /// Pool statistics
    stats: Arc<PoolStats>,
    /// Background reaper task handle
    reaper_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl ConnectionPool {
    /// Create a new connection pool with default configuration
    pub fn new(addr: String, max_size: usize) -> Self {
        Self::with_config(addr, PoolConfig::new(max_size))
    }

    /// Create a new connection pool with custom configuration
    pub fn with_config(addr: String, config: PoolConfig) -> Self {
        Self::with_tls(addr, config, TlsConfig::None)
    }

    /// Create a new connection pool with TLS configuration
    pub fn with_tls(addr: String, config: PoolConfig, tls: TlsConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_size));

        Self {
            connections: Arc::new(RwLock::new(Vec::new())),
            next_index: Arc::new(AtomicUsize::new(0)),
            semaphore,
            config: Arc::new(config),
            addr,
            tls,
            stats: Arc::new(PoolStats::default()),
            reaper_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Start the background reaper task
    ///
    /// The reaper periodically:
    /// - Removes stale connections (expired or idle too long)
    /// - Maintains minimum idle connections
    /// - Validates idle connections (if configured)
    pub async fn start_reaper(&self) {
        let interval = match self.config.reaper_interval {
            Some(d) => d,
            None => return, // Reaper disabled
        };

        // Stop existing reaper if running
        self.stop_reaper().await;

        let pool = self.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                pool.reaper_cycle().await;
            }
        });

        *self.reaper_handle.write().await = Some(handle);
    }

    /// Stop the background reaper task
    pub async fn stop_reaper(&self) {
        if let Some(handle) = self.reaper_handle.write().await.take() {
            handle.abort();
        }
    }

    /// Run one reaper cycle
    async fn reaper_cycle(&self) {
        // Remove stale connections
        self.cleanup_stale().await;

        // Dynamic scaling if enabled
        if self.config.enable_dynamic_scaling {
            self.check_and_scale().await;
        }

        // Maintain minimum idle
        let _ = self.ensure_min_idle().await;

        // Validate idle connections if configured
        if matches!(
            self.config.validation,
            ValidationStrategy::WhileIdle(_) | ValidationStrategy::All
        ) {
            self.validate_idle_connections().await;
        }
    }

    /// Check utilization and scale the pool accordingly
    async fn check_and_scale(&self) {
        let current_size = self.size().await;
        if current_size == 0 {
            return; // Empty pool, let ensure_min_idle handle it
        }

        let in_use = self.stats.in_use_count.load(Ordering::Relaxed);
        let utilization = in_use as f32 / current_size as f32;

        if utilization > self.config.scale_up_threshold {
            // High utilization: scale up
            if current_size < self.config.max_size {
                tracing::debug!(
                    "Pool {} utilization {:.1}% > {:.1}% threshold, scaling up",
                    self.addr,
                    utilization * 100.0,
                    self.config.scale_up_threshold * 100.0
                );
                let _ = self.scale_up().await;
            }
        } else if utilization < self.config.scale_down_threshold {
            // Low utilization: scale down
            if current_size > self.config.min_idle {
                tracing::debug!(
                    "Pool {} utilization {:.1}% < {:.1}% threshold, scaling down",
                    self.addr,
                    utilization * 100.0,
                    self.config.scale_down_threshold * 100.0
                );
                self.scale_down().await;
            }
        }
    }

    /// Get a connection from the pool
    ///
    /// This method implements a wait queue with timeout. If the pool is exhausted,
    /// it will wait up to `wait_timeout` for a connection to become available.
    ///
    /// # Backpressure
    /// Uses a semaphore to provide natural backpressure when pool is at capacity.
    pub async fn get(&self) -> Result<RedisConnection, RedisError> {
        let start = Instant::now();
        self.stats.total_gets.fetch_add(1, Ordering::Relaxed);

        // Acquire semaphore permit (wait queue with timeout)
        let _permit = if let Some(timeout) = self.config.wait_timeout {
            match tokio::time::timeout(timeout, self.semaphore.acquire()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    self.stats.failed_gets.fetch_add(1, Ordering::Relaxed);
                    return Err(RedisError::Protocol("Pool closed".to_string()));
                }
                Err(_) => {
                    self.stats.failed_gets.fetch_add(1, Ordering::Relaxed);
                    return Err(RedisError::Protocol("Pool timeout".to_string()));
                }
            }
        } else {
            self.semaphore
                .acquire()
                .await
                .map_err(|_| RedisError::Protocol("Pool closed".to_string()))?
        };

        // Track wait time
        let wait_time = start.elapsed();
        let wait_ms = wait_time.as_millis() as u64;
        self.stats
            .total_wait_time_ms
            .fetch_add(wait_ms, Ordering::Relaxed);
        self.stats
            .max_wait_time_ms
            .fetch_max(wait_ms, Ordering::Relaxed);

        // Get or create connection
        let mut conn = self.get_or_create_internal().await?;

        // Apply recycling hook if configured
        if let Some(hook) = &self.config.recycle_hook {
            (hook)(&mut conn).await?;
        }

        // Validate connection based on strategy
        let should_validate = matches!(
            self.config.validation,
            ValidationStrategy::OnCheckout | ValidationStrategy::All
        );

        if should_validate && !self.health_check(&conn).await {
            self.stats
                .health_check_failures
                .fetch_add(1, Ordering::Relaxed);
            self.stats.failed_gets.fetch_add(1, Ordering::Relaxed);
            // Remove unhealthy connection and try creating a new one
            self.remove_unhealthy_connection().await;
            return self.create_connection().await;
        }

        // Track in-use
        self.stats.in_use_count.fetch_add(1, Ordering::Relaxed);
        self.stats.successful_gets.fetch_add(1, Ordering::Relaxed);

        Ok(conn)
        // Permit automatically released on drop, allowing next waiter
    }

    /// Internal get or create logic
    async fn get_or_create_internal(&self) -> Result<RedisConnection, RedisError> {
        // Try to get an existing connection
        let conn = {
            let mut connections = self.connections.write().await;

            if !connections.is_empty() {
                let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
                let pooled = &mut connections[idx];

                // Check if too old
                if pooled.should_recycle(self.config.max_lifetime) {
                    self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
                    connections.remove(idx);
                    None
                } else {
                    pooled.touch();
                    Some(pooled.conn.clone())
                }
            } else {
                None
            }
        };

        if let Some(conn) = conn {
            Ok(conn)
        } else {
            self.create_connection().await
        }
    }

    /// Create a new connection and add it to the pool
    async fn create_connection(&self) -> Result<RedisConnection, RedisError> {
        // Create new connection
        let conn = RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await?;

        // Validate on create if configured
        let should_validate = matches!(
            self.config.validation,
            ValidationStrategy::OnCreate | ValidationStrategy::All
        );

        if should_validate && !self.health_check(&conn).await {
            self.stats
                .health_check_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(RedisError::Protocol(
                "Connection failed health check".to_string(),
            ));
        }

        self.stats.total_created.fetch_add(1, Ordering::Relaxed);

        // Add to pool
        let pooled = PooledConnection::new(conn.clone());
        let mut connections = self.connections.write().await;

        // Check if we're still under max (race condition possible)
        if connections.len() < self.config.max_size {
            connections.push(pooled);
        }

        Ok(conn)
    }

    /// Remove the most recently checked unhealthy connection
    async fn remove_unhealthy_connection(&self) {
        let mut connections = self.connections.write().await;
        if !connections.is_empty() {
            let idx = (self.next_index.load(Ordering::Relaxed).wrapping_sub(1))
                % connections.len().max(1);
            if idx < connections.len() {
                connections.remove(idx);
                self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Perform health check on a connection using PING
    async fn health_check(&self, conn: &RedisConnection) -> bool {
        conn.execute(Ping::new()).await.is_ok()
    }

    /// Validate idle connections in the pool
    async fn validate_idle_connections(&self) {
        let mut connections = self.connections.write().await;
        let mut to_remove = Vec::new();

        for (idx, pooled) in connections.iter().enumerate() {
            if !self.health_check(&pooled.conn).await {
                to_remove.push(idx);
                self.stats
                    .health_check_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Remove in reverse order
        for idx in to_remove.into_iter().rev() {
            connections.remove(idx);
            self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Clean up stale and idle connections
    async fn cleanup_stale(&self) {
        let mut connections = self.connections.write().await;

        let before_count = connections.len();
        let min_idle = self.config.min_idle;

        // Collect indices to remove
        let mut to_remove = Vec::new();
        for (idx, pooled) in connections.iter().enumerate() {
            let can_remove_for_idle = connections.len() - to_remove.len() > min_idle;

            let should_remove = pooled.should_recycle(self.config.max_lifetime)
                || pooled.is_idle_too_long(self.config.idle_timeout, can_remove_for_idle);

            if should_remove {
                to_remove.push(idx);
            }
        }

        // Remove in reverse order to maintain indices
        for idx in to_remove.into_iter().rev() {
            connections.remove(idx);
            self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
        }

        let removed = before_count - connections.len();
        if removed > 0 {
            drop(connections);
            let _ = self.ensure_min_idle().await;
        }
    }

    /// Ensure minimum idle connections are maintained
    async fn ensure_min_idle(&self) -> Result<(), RedisError> {
        let current_size = self.size().await;
        if current_size < self.config.min_idle {
            let needed = self.config.min_idle - current_size;
            for _ in 0..needed {
                if self.size().await >= self.config.max_size {
                    break;
                }
                let conn =
                    RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await?;
                self.stats.total_created.fetch_add(1, Ordering::Relaxed);

                let pooled = PooledConnection::new(conn);
                let mut connections = self.connections.write().await;
                if connections.len() < self.config.max_size {
                    connections.push(pooled);
                }
            }
        }
        Ok(())
    }

    /// Scale up the pool by adding connections
    ///
    /// Called when utilization exceeds the scale_up_threshold.
    /// Adds `scale_increment` connections up to `max_size`.
    async fn scale_up(&self) -> Result<(), RedisError> {
        let current_size = self.size().await;
        let increment = self.config.scale_increment;
        let target_size = (current_size + increment).min(self.config.max_size);

        if current_size >= self.config.max_size {
            return Ok(()); // Already at max
        }

        let mut added = 0;
        for _ in 0..increment {
            if self.size().await >= target_size {
                break;
            }

            match RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await {
                Ok(conn) => {
                    self.stats.total_created.fetch_add(1, Ordering::Relaxed);

                    let pooled = PooledConnection::new(conn);
                    let mut connections = self.connections.write().await;
                    if connections.len() < self.config.max_size {
                        connections.push(pooled);
                        added += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to create connection during scale-up: {}", e);
                    break;
                }
            }
        }

        if added > 0 {
            self.stats.total_scale_ups.fetch_add(1, Ordering::Relaxed);
            self.stats
                .scaled_up_connections
                .fetch_add(added, Ordering::Relaxed);
            tracing::debug!(
                "Scaled up pool {} from {} to {} connections (+{})",
                self.addr,
                current_size,
                self.size().await,
                added
            );
        }

        Ok(())
    }

    /// Scale down the pool by removing idle connections
    ///
    /// Called when utilization falls below the scale_down_threshold.
    /// Removes idle connections while respecting `min_idle`.
    async fn scale_down(&self) {
        let mut connections = self.connections.write().await;
        let current_size = connections.len();

        if current_size <= self.config.min_idle {
            return; // Already at minimum
        }

        // Find idle connections to remove
        let mut to_remove = Vec::new();
        for (i, pooled) in connections.iter().enumerate() {
            if to_remove.len() >= self.config.scale_increment {
                break;
            }

            // Only remove idle connections (use_count can help identify rarely used ones)
            if pooled.idle_time() > Duration::from_secs(10)
                && connections.len() - to_remove.len() > self.config.min_idle
            {
                to_remove.push(i);
            }
        }

        if !to_remove.is_empty() {
            // Remove in reverse order to preserve indices
            for &idx in to_remove.iter().rev() {
                connections.remove(idx);
            }

            let removed = to_remove.len();
            self.stats.total_scale_downs.fetch_add(1, Ordering::Relaxed);
            self.stats
                .scaled_down_connections
                .fetch_add(removed, Ordering::Relaxed);
            tracing::debug!(
                "Scaled down pool {} from {} to {} connections (-{})",
                self.addr,
                current_size,
                connections.len(),
                removed
            );
        }
    }

    /// Get the current pool size
    pub async fn size(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Grow the pool to at least the target size
    pub async fn grow(&self, target_size: usize) -> Result<(), RedisError> {
        let target = target_size.min(self.config.max_size);

        while self.size().await < target {
            let conn = RedisConnection::connect_with_config(&self.addr, self.tls.clone()).await?;
            self.stats.total_created.fetch_add(1, Ordering::Relaxed);

            let pooled = PooledConnection::new(conn);
            let mut connections = self.connections.write().await;
            if connections.len() < self.config.max_size {
                connections.push(pooled);
            }
        }

        Ok(())
    }

    /// Get the node address
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Get the pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Get the maximum pool size
    pub fn max_size(&self) -> usize {
        self.config.max_size
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStatistics {
        let total_gets = self.stats.total_gets.load(Ordering::Relaxed);
        let total_wait_ms = self.stats.total_wait_time_ms.load(Ordering::Relaxed);

        PoolStatistics {
            total_created: self.stats.total_created.load(Ordering::Relaxed),
            total_recycled: self.stats.total_recycled.load(Ordering::Relaxed),
            health_check_failures: self.stats.health_check_failures.load(Ordering::Relaxed),
            total_gets,
            successful_gets: self.stats.successful_gets.load(Ordering::Relaxed),
            failed_gets: self.stats.failed_gets.load(Ordering::Relaxed),
            in_use_count: self.stats.in_use_count.load(Ordering::Relaxed),
            total_wait_time_ms: total_wait_ms,
            max_wait_time_ms: self.stats.max_wait_time_ms.load(Ordering::Relaxed),
            avg_wait_time_ms: if total_gets > 0 {
                total_wait_ms as f64 / total_gets as f64
            } else {
                0.0
            },
            total_scale_ups: self.stats.total_scale_ups.load(Ordering::Relaxed),
            total_scale_downs: self.stats.total_scale_downs.load(Ordering::Relaxed),
            scaled_up_connections: self.stats.scaled_up_connections.load(Ordering::Relaxed),
            scaled_down_connections: self.stats.scaled_down_connections.load(Ordering::Relaxed),
        }
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        // Abort reaper task if running
        if let Some(handle) = self
            .reaper_handle
            .try_write()
            .ok()
            .and_then(|mut g| g.take())
        {
            handle.abort();
        }
    }
}

/// Snapshot of pool statistics
#[derive(Debug, Clone, Copy)]
pub struct PoolStatistics {
    /// Total connections created since pool initialization
    pub total_created: usize,
    /// Total connections recycled due to age/staleness
    pub total_recycled: usize,
    /// Total health check failures
    pub health_check_failures: usize,
    /// Total get operations attempted
    pub total_gets: usize,
    /// Total successful get operations
    pub successful_gets: usize,
    /// Total failed get operations (timeout/error)
    pub failed_gets: usize,
    /// Currently in-use connections
    pub in_use_count: usize,
    /// Total wait time in milliseconds
    pub total_wait_time_ms: u64,
    /// Maximum wait time seen in milliseconds
    pub max_wait_time_ms: u64,
    /// Average wait time in milliseconds
    pub avg_wait_time_ms: f64,
    /// Total scale-up operations
    pub total_scale_ups: usize,
    /// Total scale-down operations
    pub total_scale_downs: usize,
    /// Total connections added via scaling
    pub scaled_up_connections: usize,
    /// Total connections removed via scaling
    pub scaled_down_connections: usize,
}

impl PoolStatistics {
    /// Calculate pool utilization as a percentage
    pub fn utilization_percent(&self, max_size: usize) -> f64 {
        if max_size == 0 {
            0.0
        } else {
            (self.in_use_count as f64 / max_size as f64) * 100.0
        }
    }

    /// Calculate success rate as a percentage
    pub fn success_rate_percent(&self) -> f64 {
        if self.total_gets == 0 {
            0.0
        } else {
            (self.successful_gets as f64 / self.total_gets as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new(20)
            .with_min_idle(5)
            .with_max_lifetime(Some(Duration::from_secs(60)))
            .with_idle_timeout(Some(Duration::from_secs(30)))
            .with_wait_timeout(Some(Duration::from_secs(10)))
            .with_validation(ValidationStrategy::All)
            .with_test_on_checkout(false);

        assert_eq!(config.max_size, 20);
        assert_eq!(config.min_idle, 5);
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(60)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.wait_timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.validation, ValidationStrategy::None); // test_on_checkout overrides
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_size, 10);
        assert_eq!(config.min_idle, 0);
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(30 * 60)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(10 * 60)));
        assert_eq!(config.wait_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.validation, ValidationStrategy::OnCheckout);
    }

    #[test]
    fn test_validation_strategies() {
        let none = ValidationStrategy::None;
        let checkout = ValidationStrategy::OnCheckout;
        let create = ValidationStrategy::OnCreate;
        let idle = ValidationStrategy::WhileIdle(Duration::from_secs(60));
        let all = ValidationStrategy::All;

        assert_eq!(none, ValidationStrategy::None);
        assert_eq!(checkout, ValidationStrategy::OnCheckout);
        assert_eq!(create, ValidationStrategy::OnCreate);
        assert!(matches!(idle, ValidationStrategy::WhileIdle(_)));
        assert_eq!(all, ValidationStrategy::All);
    }

    #[test]
    fn test_pool_config_min_idle_clamping() {
        let config = PoolConfig::new(5).with_min_idle(10);
        assert_eq!(config.min_idle, 5); // Clamped to max_size
    }

    #[test]
    fn test_pool_creation() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);
        assert_eq!(pool.max_size(), 5);
        assert_eq!(pool.addr(), "127.0.0.1:6379");
    }

    #[tokio::test]
    async fn test_pool_initial_size_zero() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);
        assert_eq!(pool.size().await, 0);
    }

    #[test]
    fn test_pool_statistics_initial() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);
        let stats = pool.stats();

        assert_eq!(stats.total_created, 0);
        assert_eq!(stats.total_recycled, 0);
        assert_eq!(stats.health_check_failures, 0);
        assert_eq!(stats.total_gets, 0);
        assert_eq!(stats.successful_gets, 0);
        assert_eq!(stats.failed_gets, 0);
        assert_eq!(stats.in_use_count, 0);
        assert_eq!(stats.total_wait_time_ms, 0);
        assert_eq!(stats.max_wait_time_ms, 0);
        assert_eq!(stats.avg_wait_time_ms, 0.0);
    }

    #[test]
    fn test_pool_statistics_utilization() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 10);
        let stats = pool.stats();

        assert_eq!(stats.utilization_percent(10), 0.0);

        // Simulate 5 in-use connections
        pool.stats.in_use_count.store(5, Ordering::Relaxed);
        let stats = pool.stats();
        assert_eq!(stats.utilization_percent(10), 50.0);
    }

    #[test]
    fn test_pool_statistics_success_rate() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);

        // Simulate 10 total, 8 successful
        pool.stats.total_gets.store(10, Ordering::Relaxed);
        pool.stats.successful_gets.store(8, Ordering::Relaxed);
        pool.stats.failed_gets.store(2, Ordering::Relaxed);

        let stats = pool.stats();
        assert_eq!(stats.success_rate_percent(), 80.0);
    }

    #[test]
    fn test_pool_config_debug() {
        let config = PoolConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("max_size"));
        assert!(debug_str.contains("validation"));
    }

    #[test]
    fn test_default_recycle_hook() {
        let _hook = PoolConfig::default_redis_recycle_hook();
        // Just verify it compiles and can be created
    }

    #[test]
    fn test_pool_config_dynamic_scaling() {
        let config = PoolConfig::new(20)
            .with_dynamic_scaling(true)
            .with_scale_up_threshold(0.75)
            .with_scale_down_threshold(0.25)
            .with_scale_increment(2);

        assert!(config.enable_dynamic_scaling);
        assert_eq!(config.scale_up_threshold, 0.75);
        assert_eq!(config.scale_down_threshold, 0.25);
        assert_eq!(config.scale_increment, 2);
    }

    #[test]
    fn test_pool_config_scaling_threshold_clamping() {
        // Thresholds should be clamped to 0.0-1.0 range
        let config = PoolConfig::new(10)
            .with_scale_up_threshold(1.5) // Should clamp to 1.0
            .with_scale_down_threshold(-0.5); // Should clamp to 0.0

        assert_eq!(config.scale_up_threshold, 1.0);
        assert_eq!(config.scale_down_threshold, 0.0);
    }

    #[test]
    fn test_pool_config_scale_increment_minimum() {
        // Scale increment should be at least 1
        let config = PoolConfig::new(10).with_scale_increment(0);

        assert_eq!(config.scale_increment, 1);
    }

    #[test]
    fn test_pool_statistics_scaling_metrics() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 10);

        // Simulate some scaling operations
        pool.stats.total_scale_ups.store(3, Ordering::Relaxed);
        pool.stats.total_scale_downs.store(2, Ordering::Relaxed);
        pool.stats.scaled_up_connections.store(6, Ordering::Relaxed);
        pool.stats
            .scaled_down_connections
            .store(4, Ordering::Relaxed);

        let stats = pool.stats();
        assert_eq!(stats.total_scale_ups, 3);
        assert_eq!(stats.total_scale_downs, 2);
        assert_eq!(stats.scaled_up_connections, 6);
        assert_eq!(stats.scaled_down_connections, 4);
    }

    #[test]
    fn test_pool_scaling_enabled_requires_reaper() {
        // Dynamic scaling requires reaper to be enabled
        let config = PoolConfig::new(10)
            .with_dynamic_scaling(true)
            .with_reaper_interval(Some(Duration::from_secs(30)));

        assert!(config.enable_dynamic_scaling);
        assert!(config.reaper_interval.is_some());
    }

    #[test]
    fn test_pool_config_debug_includes_scaling() {
        let config = PoolConfig::new(10).with_dynamic_scaling(true);
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("enable_dynamic_scaling"));
        assert!(debug_str.contains("scale_up_threshold"));
        assert!(debug_str.contains("scale_down_threshold"));
        assert!(debug_str.contains("scale_increment"));
    }
}
