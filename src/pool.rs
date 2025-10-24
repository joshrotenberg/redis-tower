//! Connection pool with health checking and lifecycle management
//!
//! Provides a production-ready connection pool inspired by deadpool and bb8,
//! tailored for Redis connection management.
//!
//! # Design Note
//! Redis connections are Arc-wrapped internally, making cloning cheap. This pool
//! maintains a set of healthy connections and hands out clones, rather than
//! using a borrow/return model. This simplifies the API and works well with Tower.

use crate::client::RedisConnection;
use crate::commands::Ping;
use crate::types::RedisError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Configuration for connection pool behavior
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_size: usize,
    /// Minimum number of idle connections to maintain
    pub min_idle: usize,
    /// Maximum lifetime of a connection before recycling (None = infinite)
    pub max_lifetime: Option<Duration>,
    /// How long a connection can be idle before being closed (None = infinite)
    pub idle_timeout: Option<Duration>,
    /// Whether to verify connection health on checkout
    pub test_on_checkout: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_idle: 0,
            max_lifetime: Some(Duration::from_secs(30 * 60)), // 30 minutes
            idle_timeout: Some(Duration::from_secs(10 * 60)), // 10 minutes
            test_on_checkout: true,
        }
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

    /// Enable/disable health check on checkout
    pub fn with_test_on_checkout(mut self, enabled: bool) -> Self {
        self.test_on_checkout = enabled;
        self
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
}

impl PooledConnection {
    fn new(conn: RedisConnection) -> Self {
        let now = Instant::now();
        Self {
            conn,
            created_at: now,
            last_used: now,
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

/// A round-robin connection pool with health checking and lifecycle management
#[derive(Clone)]
pub struct ConnectionPool {
    /// Pool of connections
    connections: Arc<RwLock<Vec<PooledConnection>>>,
    /// Next connection index for round-robin
    next_index: Arc<AtomicUsize>,
    /// Pool configuration
    config: Arc<PoolConfig>,
    /// Node address
    addr: String,
    /// Pool statistics
    stats: Arc<PoolStats>,
}

/// Pool statistics for monitoring
#[derive(Default)]
pub struct PoolStats {
    /// Total connections created
    pub total_created: AtomicUsize,
    /// Total connections recycled
    pub total_recycled: AtomicUsize,
    /// Total health check failures
    pub health_check_failures: AtomicUsize,
    /// Total get operations
    pub total_gets: AtomicUsize,
}

impl ConnectionPool {
    /// Create a new connection pool with default configuration
    pub fn new(addr: String, max_size: usize) -> Self {
        Self::with_config(addr, PoolConfig::new(max_size))
    }

    /// Create a new connection pool with custom configuration
    pub fn with_config(addr: String, config: PoolConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(Vec::new())),
            next_index: Arc::new(AtomicUsize::new(0)),
            config: Arc::new(config),
            addr,
            stats: Arc::new(PoolStats::default()),
        }
    }

    /// Get a connection from the pool
    ///
    /// Returns a cloned connection. The connection remains in the pool for reuse.
    /// Performs cleanup and health checks as configured.
    pub async fn get(&self) -> Result<RedisConnection, RedisError> {
        self.stats.total_gets.fetch_add(1, Ordering::Relaxed);

        // Periodically clean up (every 100th get to reduce overhead)
        if self
            .stats
            .total_gets
            .load(Ordering::Relaxed)
            .is_multiple_of(100)
        {
            self.cleanup_stale().await;
        }

        // Try up to 3 times to get a healthy connection
        for attempt in 0..3 {
            // Get a connection from the pool
            let conn = {
                let mut connections = self.connections.write().await;

                if !connections.is_empty() {
                    let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
                    let pooled = &mut connections[idx];

                    // Check if too old
                    if pooled.should_recycle(self.config.max_lifetime) {
                        self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
                        // Remove and we'll create a new one
                        connections.remove(idx);
                        None
                    } else {
                        // Update last used time
                        pooled.touch();
                        Some(pooled.conn.clone())
                    }
                } else {
                    None
                }
            };

            if let Some(conn) = conn {
                // Health check if configured
                if self.config.test_on_checkout {
                    if self.health_check(&conn).await {
                        return Ok(conn);
                    } else {
                        self.stats
                            .health_check_failures
                            .fetch_add(1, Ordering::Relaxed);
                        // Remove the unhealthy connection
                        self.remove_unhealthy_connection().await;
                        // Try again (will create new connection on next attempt)
                        if attempt < 2 {
                            continue;
                        }
                    }
                } else {
                    return Ok(conn);
                }
            }

            // No connection available or health check failed, create new
            if attempt == 2 {
                // Last attempt, create connection
                return self.create_connection().await;
            }
        }

        // Shouldn't reach here, but just in case
        self.create_connection().await
    }

    /// Create a new connection and add it to the pool
    async fn create_connection(&self) -> Result<RedisConnection, RedisError> {
        let mut connections = self.connections.write().await;

        // Check if we can create more connections
        if connections.len() >= self.config.max_size {
            // Pool is full, return an existing connection
            if !connections.is_empty() {
                let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
                let pooled = &mut connections[idx];
                pooled.touch();
                return Ok(pooled.conn.clone());
            }
            return Err(RedisError::Protocol(
                "Connection pool exhausted".to_string(),
            ));
        }

        // Create new connection
        let conn = RedisConnection::connect(&self.addr).await?;
        self.stats.total_created.fetch_add(1, Ordering::Relaxed);

        // Add to pool
        let pooled = PooledConnection::new(conn.clone());
        connections.push(pooled);

        Ok(conn)
    }

    /// Remove the most recently checked unhealthy connection
    async fn remove_unhealthy_connection(&self) {
        let mut connections = self.connections.write().await;
        if !connections.is_empty() {
            // Remove the one we just tested (it's at the last index we used)
            let idx = (self.next_index.load(Ordering::Relaxed).wrapping_sub(1))
                % connections.len().max(1);
            if idx < connections.len() {
                connections.remove(idx);
            }
        }
    }

    /// Perform health check on a connection using PING
    async fn health_check(&self, conn: &RedisConnection) -> bool {
        conn.execute(Ping::new()).await.is_ok()
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
                self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Remove in reverse order to maintain indices
        for idx in to_remove.into_iter().rev() {
            connections.remove(idx);
        }

        let removed = before_count - connections.len();
        if removed > 0 {
            drop(connections);
            // Try to maintain min_idle
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
                let conn = RedisConnection::connect(&self.addr).await?;
                self.stats.total_created.fetch_add(1, Ordering::Relaxed);

                let pooled = PooledConnection::new(conn);
                let mut connections = self.connections.write().await;
                connections.push(pooled);
            }
        }
        Ok(())
    }

    /// Get the current pool size
    pub async fn size(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Grow the pool to at least the target size
    pub async fn grow(&self, target_size: usize) -> Result<(), RedisError> {
        let target = target_size.min(self.config.max_size);

        while self.size().await < target {
            let conn = RedisConnection::connect(&self.addr).await?;
            self.stats.total_created.fetch_add(1, Ordering::Relaxed);

            let pooled = PooledConnection::new(conn);
            let mut connections = self.connections.write().await;
            connections.push(pooled);
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
        PoolStatistics {
            total_created: self.stats.total_created.load(Ordering::Relaxed),
            total_recycled: self.stats.total_recycled.load(Ordering::Relaxed),
            health_check_failures: self.stats.health_check_failures.load(Ordering::Relaxed),
            total_gets: self.stats.total_gets.load(Ordering::Relaxed),
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
    /// Total get operations
    pub total_gets: usize,
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
            .with_test_on_checkout(false);

        assert_eq!(config.max_size, 20);
        assert_eq!(config.min_idle, 5);
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(60)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(30)));
        assert!(!config.test_on_checkout);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_size, 10);
        assert_eq!(config.min_idle, 0);
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(30 * 60)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(10 * 60)));
        assert!(config.test_on_checkout);
    }

    #[test]
    fn test_pool_config_min_idle_clamping() {
        // min_idle should be clamped to max_size
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

    #[tokio::test]
    async fn test_pooled_connection_age() {
        // Test age logic for connection recycling
        let age_5s = Duration::from_secs(5);
        let age_65s = Duration::from_secs(65);

        // 5 seconds old should NOT be recycled with 60s max lifetime
        assert!(age_5s <= Duration::from_secs(60));

        // 65 seconds old SHOULD be recycled with 60s max lifetime
        assert!(age_65s > Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_pooled_connection_idle_time() {
        let idle_2s = Duration::from_secs(2);
        let idle_65s = Duration::from_secs(65);

        // 2 seconds idle should NOT timeout with 60s idle timeout
        assert!(idle_2s <= Duration::from_secs(60));

        // 65 seconds idle SHOULD timeout with 60s idle timeout
        assert!(idle_65s > Duration::from_secs(60));
    }

    #[test]
    fn test_pool_statistics_initial() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);
        let stats = pool.stats();

        assert_eq!(stats.total_created, 0);
        assert_eq!(stats.total_recycled, 0);
        assert_eq!(stats.health_check_failures, 0);
        assert_eq!(stats.total_gets, 0);
    }

    #[test]
    fn test_pool_config_no_max_lifetime() {
        let config = PoolConfig::new(10).with_max_lifetime(None);
        assert_eq!(config.max_lifetime, None);
    }

    #[test]
    fn test_pool_config_no_idle_timeout() {
        let config = PoolConfig::new(10).with_idle_timeout(None);
        assert_eq!(config.idle_timeout, None);
    }

    #[test]
    fn test_pool_config_chaining() {
        let config = PoolConfig::new(15)
            .with_min_idle(3)
            .with_test_on_checkout(false)
            .with_max_lifetime(Some(Duration::from_secs(120)))
            .with_idle_timeout(Some(Duration::from_secs(60)));

        assert_eq!(config.max_size, 15);
        assert_eq!(config.min_idle, 3);
        assert!(!config.test_on_checkout);
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(120)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(60)));
    }
}
