//! Error and reconnection hooks
//!
//! Provides callback mechanisms for error handling and reconnection events.
//! Useful for monitoring connection health, implementing custom recovery logic,
//! or tracking metrics.
//!
//! # Example
//!
//! ```no_run
//! use redis_tower::hooks::{ErrorHook, ReconnectHook};
//! use redis_tower::types::RedisError;
//! use redis_tower::config::ClientConfig;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::builder()
//!         .on_error(|error: RedisError| {
//!             eprintln!("Redis error: {:?}", error);
//!         })
//!         .on_reconnect(|attempt: usize| {
//!             println!("Reconnected after {} attempts", attempt);
//!         })
//!         .build();
//!     # Ok(())
//! }
//! ```

use crate::types::RedisError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A callback invoked when a Redis error occurs
///
/// This can be used to log errors, update metrics, trigger alerts, or
/// implement custom error recovery logic.
///
/// The callback receives a `RedisError` and can perform async operations.
pub type ErrorCallback =
    Arc<dyn Fn(RedisError) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// A callback invoked when a connection is established
///
/// This is called both for initial connections and reconnections.
/// The callback receives the attempt number (1 for first connection,
/// 2+ for reconnections).
pub type ConnectCallback =
    Arc<dyn Fn(usize) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// A callback invoked when a reconnection attempt starts
///
/// This is called before each reconnection attempt (not for the initial connection).
/// The callback receives the attempt number (1, 2, 3, ...).
pub type ReconnectAttemptCallback =
    Arc<dyn Fn(usize) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Hook configuration for error and connection events
///
/// Provides callbacks for various events during the connection lifecycle.
#[derive(Clone)]
pub struct Hooks {
    /// Called when any Redis error occurs
    pub(crate) on_error: Option<ErrorCallback>,

    /// Called when a connection is successfully established
    pub(crate) on_connect: Option<ConnectCallback>,

    /// Called before a reconnection attempt
    pub(crate) on_reconnect_attempt: Option<ReconnectAttemptCallback>,
}

impl Default for Hooks {
    fn default() -> Self {
        Self::new()
    }
}

impl Hooks {
    /// Create a new empty hooks configuration
    pub fn new() -> Self {
        Self {
            on_error: None,
            on_connect: None,
            on_reconnect_attempt: None,
        }
    }

    /// Set the error callback
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower::hooks::Hooks;
    ///
    /// let hooks = Hooks::new()
    ///     .with_error_callback(|error| {
    ///         Box::pin(async move {
    ///             eprintln!("Redis error: {:?}", error);
    ///         })
    ///     });
    /// ```
    pub fn with_error_callback<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(RedisError) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_error = Some(Arc::new(move |error| Box::pin(callback(error))));
        self
    }

    /// Set the connect callback
    ///
    /// Called when a connection is successfully established (initial or reconnect).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower::hooks::Hooks;
    ///
    /// let hooks = Hooks::new()
    ///     .with_connect_callback(|attempt| {
    ///         Box::pin(async move {
    ///             println!("Connected on attempt {}", attempt);
    ///         })
    ///     });
    /// ```
    pub fn with_connect_callback<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_connect = Some(Arc::new(move |attempt| Box::pin(callback(attempt))));
        self
    }

    /// Set the reconnect attempt callback
    ///
    /// Called before each reconnection attempt (not for the initial connection).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower::hooks::Hooks;
    ///
    /// let hooks = Hooks::new()
    ///     .with_reconnect_attempt_callback(|attempt| {
    ///         Box::pin(async move {
    ///             println!("Attempting reconnect #{}", attempt);
    ///         })
    ///     });
    /// ```
    pub fn with_reconnect_attempt_callback<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_reconnect_attempt = Some(Arc::new(move |attempt| Box::pin(callback(attempt))));
        self
    }

    /// Invoke the error callback if set
    pub(crate) async fn notify_error(&self, error: RedisError) {
        if let Some(callback) = &self.on_error {
            callback(error).await;
        }
    }

    /// Invoke the connect callback if set
    pub(crate) async fn notify_connect(&self, attempt: usize) {
        if let Some(callback) = &self.on_connect {
            callback(attempt).await;
        }
    }

    /// Invoke the reconnect attempt callback if set
    pub(crate) async fn notify_reconnect_attempt(&self, attempt: usize) {
        if let Some(callback) = &self.on_reconnect_attempt {
            callback(attempt).await;
        }
    }
}

impl fmt::Debug for Hooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks")
            .field("on_error", &self.on_error.as_ref().map(|_| "<callback>"))
            .field(
                "on_connect",
                &self.on_connect.as_ref().map(|_| "<callback>"),
            )
            .field(
                "on_reconnect_attempt",
                &self.on_reconnect_attempt.as_ref().map(|_| "<callback>"),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_error_callback() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let hooks = Hooks::new().with_error_callback(move |_error| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        hooks
            .notify_error(RedisError::Connection("test".to_string()))
            .await;
        hooks
            .notify_error(RedisError::Connection("test2".to_string()))
            .await;

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_connect_callback() {
        let attempts = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let attempts_clone = attempts.clone();

        let hooks = Hooks::new().with_connect_callback(move |attempt| {
            let attempts = attempts_clone.clone();
            async move {
                attempts.lock().await.push(attempt);
            }
        });

        hooks.notify_connect(1).await;
        hooks.notify_connect(2).await;

        let recorded = attempts.lock().await;
        assert_eq!(*recorded, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_reconnect_attempt_callback() {
        let attempts = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let attempts_clone = attempts.clone();

        let hooks = Hooks::new().with_reconnect_attempt_callback(move |attempt| {
            let attempts = attempts_clone.clone();
            async move {
                attempts.lock().await.push(attempt);
            }
        });

        hooks.notify_reconnect_attempt(1).await;
        hooks.notify_reconnect_attempt(2).await;
        hooks.notify_reconnect_attempt(3).await;

        let recorded = attempts.lock().await;
        assert_eq!(*recorded, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_no_callback_doesnt_panic() {
        let hooks = Hooks::new();

        // Should not panic when callbacks are not set
        hooks
            .notify_error(RedisError::Connection("test".to_string()))
            .await;
        hooks.notify_connect(1).await;
        hooks.notify_reconnect_attempt(1).await;
    }

    #[test]
    fn test_hooks_debug() {
        let hooks = Hooks::new()
            .with_error_callback(|_| async {})
            .with_connect_callback(|_| async {});

        let debug_str = format!("{:?}", hooks);
        assert!(debug_str.contains("Hooks"));
        assert!(debug_str.contains("<callback>"));
    }
}
