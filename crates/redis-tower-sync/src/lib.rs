//! Synchronous (blocking) wrapper for redis-tower.
//!
//! Provides a [`SyncClient`] that wraps the async [`RedisClient`] with an
//! internal tokio runtime. Useful for CLI tools, migration scripts, and
//! contexts where async isn't needed.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower_sync::SyncClient;
//! use redis_tower::commands::*;
//! use redis_tower::RedisValueExt;
//!
//! let client = SyncClient::connect("127.0.0.1:6379")?;
//! client.execute(Set::new("key", "hello"))?;
//! let val: String = client.execute(Get::new("key"))?.parse_into()?;
//! println!("{val}");
//! ```

#![forbid(unsafe_code)]

use redis_tower::RedisClient;
use redis_tower_core::{Command, RedisError};

/// A synchronous Redis client wrapping [`RedisClient`] with an internal
/// tokio runtime.
///
/// Each method blocks the current thread until the Redis operation
/// completes. The internal runtime is created once on construction
/// and reused for all operations.
///
/// For concurrent workloads, use the async [`RedisClient`] directly.
///
/// # Thread Safety
///
/// `SyncClient` is `Send` but NOT `Sync`. It must not be used from an async
/// context — calling any method will panic if invoked from within a running
/// Tokio runtime. It is designed for synchronous CLI tools, migration scripts,
/// and integration tests where async is not needed.
pub struct SyncClient {
    rt: tokio::runtime::Runtime,
    inner: RedisClient,
}

impl SyncClient {
    /// Connect to a Redis server by address (e.g., `"127.0.0.1:6379"`).
    pub fn connect(addr: &str) -> Result<Self, RedisError> {
        let rt = tokio::runtime::Runtime::new().map_err(RedisError::from)?;
        let inner = rt.block_on(RedisClient::connect(addr))?;
        Ok(Self { rt, inner })
    }

    /// Connect using a Redis URL (e.g., `"redis://localhost:6379/0"`).
    pub fn connect_url(url: &str) -> Result<Self, RedisError> {
        let rt = tokio::runtime::Runtime::new().map_err(RedisError::from)?;
        let inner = rt.block_on(RedisClient::connect_url(url))?;
        Ok(Self { rt, inner })
    }

    /// Execute a Redis command synchronously.
    ///
    /// Blocks the current thread until the command completes and returns
    /// the typed response.
    pub fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        self.rt.block_on(self.inner.execute(cmd))
    }
}

// Re-export commonly used types so users don't need to depend on
// redis-tower directly for basic usage.
pub use redis_tower::commands;
pub use redis_tower_core::{Frame, RedisError as Error};
pub use redis_tower_core::{FromRedisBytes, RedisConvert, RedisValueExt};

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower::commands::Ping;

    #[test]
    fn sync_client_compiles() {
        // Verify the type signatures are correct -- can't actually connect
        // without a Redis server, but we can check the API compiles.
        fn _assert_send<T: Send>() {}
        _assert_send::<SyncClient>();
    }

    #[test]
    fn sync_client_execute_signature() {
        // Verify execute() accepts Command types.
        fn _check(client: &SyncClient) {
            let _ = client.execute(Ping::new());
        }
    }
}
