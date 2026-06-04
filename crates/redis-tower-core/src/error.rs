use redis_tower_protocol::ProtocolError;

/// Errors returned by redis-tower operations.
///
/// Covers the full error surface: transport I/O, protocol framing, Redis
/// server errors, type conversion mismatches, and reconnection failures.
/// Use [`is_retryable`](RedisError::is_retryable) and
/// [`is_connection_error`](RedisError::is_connection_error) to classify
/// errors for retry and reconnection logic.
#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    /// Connection-level I/O error.
    #[error("connection error: {0}")]
    Connection(#[from] std::io::Error),

    /// RESP protocol error (parsing or serialization).
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Redis server returned an error response.
    #[error("redis error: {0}")]
    Redis(String),

    /// Response frame did not match the expected type.
    #[error("unexpected response: expected {expected}, got {actual}")]
    UnexpectedResponse {
        /// The expected frame type name.
        expected: &'static str,
        /// A debug representation of the actual frame received.
        actual: String,
    },

    /// Connection is closed.
    #[error("connection closed")]
    ConnectionClosed,

    /// URL parsing error.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    /// Transaction was aborted due to a WATCH violation.
    #[error("transaction aborted (WATCH condition violated)")]
    TransactionAborted,

    /// Failed to extract the connection because it has other references.
    #[error("connection is shared and cannot be exclusively owned")]
    ConnectionInUse,

    /// Type mismatch when extracting a pipeline/transaction result.
    #[error("type mismatch: expected {expected}")]
    TypeMismatch {
        /// The expected Rust type name.
        expected: &'static str,
    },

    /// Index out of bounds when accessing a pipeline or transaction result.
    #[error("index {index} out of bounds (len {len})")]
    IndexOutOfBounds {
        /// The index that was out of bounds.
        index: usize,
        /// The length of the collection.
        len: usize,
    },

    /// Reconnection failed after exhausting all retries.
    #[error("reconnect failed after {attempts} attempts: {last_error}")]
    ReconnectFailed {
        /// The number of reconnection attempts made.
        attempts: usize,
        /// The last error that caused the reconnection to fail.
        last_error: Box<RedisError>,
    },

    /// The circuit breaker is open; request rejected without touching the service.
    #[error("circuit open: too many recent failures")]
    CircuitOpen,

    /// Timed out waiting to acquire a connection from the pool.
    #[error("pool acquisition timeout after {waited:?} waiting for 1 of {pool_size} connections")]
    PoolAcquisitionTimeout {
        /// How long the caller waited before the timeout fired.
        waited: std::time::Duration,
        /// Number of connections in the pool.
        pool_size: usize,
    },

    /// The internal command queue is full; the caller should shed load.
    #[error("queue full: the auto-pipeline channel is at capacity")]
    QueueFull,

    /// TCP connection attempt timed out before the OS completed the handshake.
    ///
    /// Returned by [`crate::RedisConnection::connect_with_timeout`] and by
    /// [`ResilientConnection`](https://docs.rs/redis-tower) when a
    /// `connect_timeout` is configured on [`ReconnectConfig`](https://docs.rs/redis-tower).
    #[error("connect timeout")]
    ConnectTimeout,

    /// A command exceeded its configured per-command deadline.
    ///
    /// Returned by [`CommandTimeoutService`](https://docs.rs/redis-tower) when
    /// the inner service does not complete within the configured
    /// [`CommandTimeoutLayer`](https://docs.rs/redis-tower) duration.
    #[error("command timeout")]
    CommandTimeout,
}

impl RedisError {
    /// Returns true if this error is transient and the operation is worth retrying.
    ///
    /// Connection and protocol errors are retryable. Redis command errors
    /// (WRONGTYPE, NOSCRIPT, etc.) are not -- they will fail the same way
    /// on retry.
    ///
    /// Note: `TransactionAborted` is retryable at the transaction level
    /// (rebuild and re-execute) but not at the command level.
    ///
    /// # Safety Warning
    ///
    /// For connection errors ([`RedisError::Connection`] and
    /// [`RedisError::ConnectionClosed`]), the command may have been received
    /// and executed by Redis before the connection dropped. Retrying a
    /// non-idempotent write (INCR, LPUSH, ZADD, etc.) can silently duplicate
    /// data. Always check [`Command::idempotent`] before retrying on a
    /// connection error.
    ///
    /// [`Command::idempotent`]: crate::Command::idempotent
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RedisError::Connection(_) | RedisError::ConnectionClosed | RedisError::Protocol(_)
        )
    }

    /// Returns true if this is a WRONGTYPE error.
    pub fn is_wrongtype(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("WRONGTYPE"))
    }

    /// Returns true if this is a NOSCRIPT error.
    pub fn is_noscript(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("NOSCRIPT"))
    }

    /// Returns true if this is a MOVED redirect (cluster).
    pub fn is_moved(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("MOVED"))
    }

    /// Returns true if this is an ASK redirect (cluster).
    pub fn is_ask(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("ASK"))
    }

    /// Returns true if this is a BUSY error (script in progress).
    pub fn is_busy(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("BUSY"))
    }

    /// Returns true if this is an OOM error.
    pub fn is_oom(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("OOM"))
    }

    /// Returns true if this is a READONLY error (writing to replica).
    pub fn is_readonly(&self) -> bool {
        matches!(self, RedisError::Redis(msg) if msg.starts_with("READONLY"))
    }

    /// Returns the Redis error prefix (e.g., "WRONGTYPE", "NOSCRIPT", "ERR").
    pub fn server_error_prefix(&self) -> Option<&str> {
        match self {
            RedisError::Redis(msg) => msg.split_whitespace().next(),
            _ => None,
        }
    }

    /// Returns true if the connection is broken and needs to be replaced.
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            RedisError::Connection(_) | RedisError::ConnectionClosed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- is_retryable tests --

    #[test]
    fn connection_error_is_retryable() {
        let err = RedisError::Connection(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(err.is_retryable());
    }

    #[test]
    fn connection_closed_is_retryable() {
        assert!(RedisError::ConnectionClosed.is_retryable());
    }

    #[test]
    fn protocol_error_is_retryable() {
        let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof");
        let err = RedisError::Protocol(ProtocolError::Io(io_err));
        assert!(err.is_retryable());
    }

    #[test]
    fn redis_error_not_retryable() {
        let err = RedisError::Redis("WRONGTYPE".into());
        assert!(!err.is_retryable());
    }

    #[test]
    fn transaction_aborted_not_retryable() {
        assert!(!RedisError::TransactionAborted.is_retryable());
    }

    #[test]
    fn type_mismatch_not_retryable() {
        let err = RedisError::TypeMismatch { expected: "string" };
        assert!(!err.is_retryable());
    }

    #[test]
    fn invalid_url_not_retryable() {
        let err = RedisError::InvalidUrl("bad".into());
        assert!(!err.is_retryable());
    }

    #[test]
    fn unexpected_response_not_retryable() {
        let err = RedisError::UnexpectedResponse {
            expected: "array",
            actual: "string".into(),
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn connection_in_use_not_retryable() {
        assert!(!RedisError::ConnectionInUse.is_retryable());
    }

    #[test]
    fn reconnect_failed_not_retryable() {
        let err = RedisError::ReconnectFailed {
            attempts: 3,
            last_error: Box::new(RedisError::ConnectionClosed),
        };
        assert!(!err.is_retryable());
    }

    // -- is_connection_error tests --

    #[test]
    fn connection_io_error_is_connection_error() {
        let err = RedisError::Connection(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(err.is_connection_error());
    }

    #[test]
    fn connection_closed_is_connection_error() {
        assert!(RedisError::ConnectionClosed.is_connection_error());
    }

    #[test]
    fn protocol_error_not_connection_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof");
        let err = RedisError::Protocol(ProtocolError::Io(io_err));
        assert!(!err.is_connection_error());
    }

    #[test]
    fn redis_error_not_connection_error() {
        let err = RedisError::Redis("ERR unknown command".into());
        assert!(!err.is_connection_error());
    }

    #[test]
    fn reconnect_failed_not_connection_error() {
        let err = RedisError::ReconnectFailed {
            attempts: 5,
            last_error: Box::new(RedisError::ConnectionClosed),
        };
        assert!(!err.is_connection_error());
    }

    // -- Display tests --

    #[test]
    fn display_connection_error() {
        let err = RedisError::Connection(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert!(err.to_string().contains("connection error"));
    }

    #[test]
    fn display_reconnect_failed_includes_attempts() {
        let err = RedisError::ReconnectFailed {
            attempts: 3,
            last_error: Box::new(RedisError::ConnectionClosed),
        };
        let msg = err.to_string();
        assert!(msg.contains("3"));
        assert!(msg.contains("reconnect failed"));
    }

    #[test]
    fn display_unexpected_response() {
        let err = RedisError::UnexpectedResponse {
            expected: "array",
            actual: "string".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("array"));
        assert!(msg.contains("string"));
    }

    #[test]
    fn display_type_mismatch() {
        let err = RedisError::TypeMismatch {
            expected: "integer",
        };
        assert!(err.to_string().contains("integer"));
    }

    // -- server error classification tests --

    #[test]
    fn is_wrongtype_matches() {
        let err = RedisError::Redis(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        );
        assert!(err.is_wrongtype());
    }

    #[test]
    fn is_wrongtype_rejects_other() {
        let err = RedisError::Redis("ERR unknown command".into());
        assert!(!err.is_wrongtype());
    }

    #[test]
    fn is_noscript_matches() {
        let err = RedisError::Redis("NOSCRIPT No matching script".into());
        assert!(err.is_noscript());
    }

    #[test]
    fn is_moved_matches() {
        let err = RedisError::Redis("MOVED 3999 127.0.0.1:6381".into());
        assert!(err.is_moved());
    }

    #[test]
    fn is_ask_matches() {
        let err = RedisError::Redis("ASK 3999 127.0.0.1:6381".into());
        assert!(err.is_ask());
    }

    #[test]
    fn is_busy_matches() {
        let err =
            RedisError::Redis("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.".into());
        assert!(err.is_busy());
    }

    #[test]
    fn is_oom_matches() {
        let err =
            RedisError::Redis("OOM command not allowed when used memory > 'maxmemory'".into());
        assert!(err.is_oom());
    }

    #[test]
    fn is_readonly_matches() {
        let err = RedisError::Redis("READONLY You can't write against a read only replica.".into());
        assert!(err.is_readonly());
    }

    #[test]
    fn server_error_prefix_extracts_prefix() {
        let err = RedisError::Redis("WRONGTYPE Operation against a key".into());
        assert_eq!(err.server_error_prefix(), Some("WRONGTYPE"));
    }

    #[test]
    fn server_error_prefix_returns_err() {
        let err = RedisError::Redis("ERR unknown command 'foo'".into());
        assert_eq!(err.server_error_prefix(), Some("ERR"));
    }

    #[test]
    fn server_error_prefix_none_for_non_redis() {
        let err = RedisError::ConnectionClosed;
        assert_eq!(err.server_error_prefix(), None);
    }

    #[test]
    fn classification_methods_reject_non_redis_errors() {
        let err = RedisError::ConnectionClosed;
        assert!(!err.is_wrongtype());
        assert!(!err.is_noscript());
        assert!(!err.is_moved());
        assert!(!err.is_ask());
        assert!(!err.is_busy());
        assert!(!err.is_oom());
        assert!(!err.is_readonly());
    }
}
