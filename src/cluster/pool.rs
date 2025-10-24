//! Connection pool for cluster nodes

use crate::client::RedisConnection;
use crate::types::RedisError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;

/// A simple round-robin connection pool for a single node
#[derive(Clone)]
pub struct ConnectionPool {
    /// Pool of connections to this node
    connections: Arc<RwLock<Vec<RedisConnection>>>,
    /// Next connection index for round-robin
    next_index: Arc<AtomicUsize>,
    /// Maximum pool size
    max_size: usize,
    /// Node address
    addr: String,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(addr: String, max_size: usize) -> Self {
        Self {
            connections: Arc::new(RwLock::new(Vec::new())),
            next_index: Arc::new(AtomicUsize::new(0)),
            max_size,
            addr,
        }
    }

    /// Get a connection from the pool, creating new connections if needed
    pub async fn get(&self) -> Result<RedisConnection, RedisError> {
        // Try to get existing connection first
        {
            let connections = self.connections.read().await;
            if !connections.is_empty() {
                let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
                return Ok(connections[idx].clone());
            }
        }

        // No connections yet, create initial connection
        self.add_connection().await
    }

    /// Add a new connection to the pool if under max_size
    async fn add_connection(&self) -> Result<RedisConnection, RedisError> {
        let mut connections = self.connections.write().await;

        // Check again under write lock (double-check pattern)
        if !connections.is_empty() {
            let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
            return Ok(connections[idx].clone());
        }

        // Create new connection if under max size
        if connections.len() < self.max_size {
            let conn = RedisConnection::connect(&self.addr).await?;
            connections.push(conn.clone());
            Ok(conn)
        } else {
            // Pool is full, return existing connection
            let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % connections.len();
            Ok(connections[idx].clone())
        }
    }

    /// Get the current pool size
    pub async fn size(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Grow the pool to target size
    pub async fn grow(&self, target_size: usize) -> Result<(), RedisError> {
        let target = target_size.min(self.max_size);
        let mut connections = self.connections.write().await;

        while connections.len() < target {
            let conn = RedisConnection::connect(&self.addr).await?;
            connections.push(conn);
        }

        Ok(())
    }

    /// Get the node address
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Get the maximum pool size
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
