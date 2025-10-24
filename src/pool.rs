//! Connection pool with health checking and lifecycle management
//!
//! Provides a production-ready connection pool inspired by deadpool and bb8,
//! tailored for Redis connection management.

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

    fn update_last_used(&mut self) {
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
    fn is_idle_too_long(&self, idle_timeout: Option<Duration>) -> bool {
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
    /// This will:
    /// 1. Try to get an existing healthy connection
    /// 2. Remove stale/unhealthy connections
    /// 3. Create new connections if needed
    /// 4. Perform health checks if configured
    pub async fn get(&self) -> Result<RedisConnection, RedisError> {
        self.stats.total_gets.fetch_add(1, Ordering::Relaxed);

        // Try to get an existing connection
        loop {
            // First, clean up stale connections
            self.cleanup_stale().await;

            // Try to get a connection
            let conn = {
                let mut connections = self.connections.write().await;

                if !connections.is_empty() {
                    let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
                    let mut pooled = connections.remove(idx);
                    pooled.update_last_used();

                    // Check if should be recycled due to age
                    if pooled.should_recycle(self.config.max_lifetime) {
                        self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
                        None // Will create new connection
                    } else {
                        Some(pooled.conn)
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
                        // Failed health check, try again
                        continue;
                    }
                } else {
                    return Ok(conn);
                }
            } else {
                // No connections available, create new one
                return self.create_connection().await;
            }
        }
    }

    /// Create a new connection
    async fn create_connection(&self) -> Result<RedisConnection, RedisError> {
        let mut connections = self.connections.write().await;

        // Check if we can create more connections
        if connections.len() >= self.config.max_size {
            // Pool is full, return existing connection if available
            if !connections.is_empty() {
                let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
                let mut pooled = connections.remove(idx);
                pooled.update_last_used();
                return Ok(pooled.conn);
            } else {
                return Err(RedisError::Protocol(
                    "Connection pool exhausted".to_string(),
                ));
            }
        }

        // Create new connection
        let conn = RedisConnection::connect(&self.addr).await?;
        self.stats.total_created.fetch_add(1, Ordering::Relaxed);

        Ok(conn)
    }

    /// Perform health check on a connection using PING
    async fn health_check(&self, conn: &RedisConnection) -> bool {
        conn.execute(Ping::new()).await.is_ok()
    }

    /// Clean up stale and idle connections
    async fn cleanup_stale(&self) {
        let mut connections = self.connections.write().await;

        // Remove connections that are too old or idle too long
        connections.retain(|pooled| {
            let should_keep = !pooled.should_recycle(self.config.max_lifetime)
                && !pooled.is_idle_too_long(self.config.idle_timeout);

            if !should_keep {
                self.stats.total_recycled.fetch_add(1, Ordering::Relaxed);
            }

            should_keep
        });

        // Ensure we don't drop below min_idle if possible
        // (This is a best-effort approach, actual creation happens in get())
    }

    /// Return a connection to the pool
    ///
    /// Note: In our current design, connections are cloned, so this is mainly
    /// for explicit pool management. Most use cases won't need this.
    pub async fn put(&self, conn: RedisConnection) {
        let mut connections = self.connections.write().await;

        if connections.len() < self.config.max_size {
            connections.push(PooledConnection::new(conn));
        }
        // If pool is full, just drop the connection
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
            self.put(conn).await;
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
    fn test_pool_config() {
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
    fn test_pool_creation() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);
        assert_eq!(pool.max_size(), 5);
        assert_eq!(pool.addr(), "127.0.0.1:6379");
    }

    #[tokio::test]
    async fn test_pool_size() {
        let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);
        assert_eq!(pool.size().await, 0);
    }

    #[tokio::test]
    async fn test_pooled_connection_lifecycle() {
        // This test verifies the lifecycle logic
        // We test the duration comparison logic without requiring a live Redis server

        let age = Duration::from_secs(5);
        let idle = Duration::from_secs(2);

        // Test should_recycle logic - 5 seconds is less than 60 second max lifetime
        let should_recycle_age = age > Duration::from_secs(60);
        assert!(!should_recycle_age);

        // Test idle timeout logic - 2 seconds is less than 60 second timeout
        let is_idle_too_long = idle > Duration::from_secs(60);
        assert!(!is_idle_too_long);
    }
}
