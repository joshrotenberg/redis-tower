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
//!
//! # Parity with the async clients
//!
//! `SyncClient` wraps [`RedisClient`] (the simplest shared async client) and
//! exposes the same single-connection command surface synchronously:
//!
//! - [`execute`](SyncClient::execute) mirrors `RedisClient::execute`.
//! - [`health_check`](SyncClient::health_check) mirrors `RedisClient::health_check`.
//! - [`pipeline`](SyncClient::pipeline) runs a [`Pipeline`] (batched roundtrip).
//! - [`transaction`](SyncClient::transaction) runs a [`Transaction`] (MULTI/EXEC).
//!
//! The pool, reconnection, and timeout middleware exposed by the async crate
//! (`ConnectionPool`, `ResilientRedisClient`, the Tower layers) are
//! deliberately not surfaced here: they exist to manage concurrency and
//! background reconnection, neither of which applies to a single-threaded
//! blocking caller that serializes one command at a time. Applications that
//! need that behavior should drive the async clients directly. The blocking
//! surface intentionally tracks `RedisClient`, not `ResilientRedisClient`.

#![forbid(unsafe_code)]

use redis_tower::RedisClient;
use redis_tower_core::{Command, RedisError};

// `Pipeline`, `PipelineResults`, `Transaction`, and `TransactionResult` are
// brought into scope (and re-exported) by the `pub use` near the bottom.

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

    /// Send a PING to verify the connection is alive.
    ///
    /// Returns `Ok(())` on success. The blocking analog of
    /// [`RedisClient::health_check`], useful for readiness checks in
    /// synchronous startup paths.
    pub fn health_check(&self) -> Result<(), RedisError> {
        self.rt.block_on(self.inner.health_check())
    }

    /// Execute a [`Pipeline`] synchronously, batching the queued commands into
    /// a single roundtrip and returning the per-command [`PipelineResults`].
    ///
    /// ```ignore
    /// use redis_tower_sync::SyncClient;
    /// use redis_tower_sync::{Pipeline, commands::*};
    ///
    /// let client = SyncClient::connect("127.0.0.1:6379")?;
    /// let results = client.pipeline(
    ///     Pipeline::new()
    ///         .push(Set::new("a", "1"))
    ///         .push(Get::new("a")),
    /// )?;
    /// let val: &Option<bytes::Bytes> = results.get(1)?;
    /// ```
    pub fn pipeline(&self, pipeline: Pipeline) -> Result<PipelineResults, RedisError> {
        // `RedisClient` is a cheap `Arc<Mutex<_>>` handle; clone it to satisfy
        // the `&mut PipelineExecutor` contract without holding `&mut self`.
        let mut inner = self.inner.clone();
        self.rt.block_on(pipeline.execute(&mut inner))
    }

    /// Execute a [`Transaction`] (MULTI/EXEC, optionally with WATCH)
    /// synchronously and return the [`TransactionResult`].
    ///
    /// A `WATCH` conflict aborts the transaction; inspect the result with
    /// [`TransactionResult::is_aborted`] / [`TransactionResult::is_committed`].
    pub fn transaction(&self, transaction: Transaction) -> Result<TransactionResult, RedisError> {
        let mut inner = self.inner.clone();
        self.rt.block_on(transaction.execute(&mut inner))
    }
}

// Re-export commonly used types so users don't need to depend on
// redis-tower directly for basic usage.
pub use redis_tower::commands;
pub use redis_tower::{Pipeline, PipelineResults, Transaction, TransactionResult};
pub use redis_tower_core::{Frame, RedisError as Error};
pub use redis_tower_core::{FromRedisBytes, RedisConvert, RedisValueExt};

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower::commands::{Get, Ping, Set};

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

    #[test]
    fn sync_client_health_check_signature() {
        fn _check(client: &SyncClient) {
            let _: Result<(), Error> = client.health_check();
        }
    }

    #[test]
    fn sync_client_pipeline_signature() {
        fn _check(client: &SyncClient) {
            let _ = client.pipeline(Pipeline::new().push(Set::new("a", "1")).push(Get::new("a")));
        }
    }

    #[test]
    fn sync_client_transaction_signature() {
        fn _check(client: &SyncClient) {
            let _ = client.transaction(Transaction::new().push(Set::new("a", "1")));
        }
    }
}
