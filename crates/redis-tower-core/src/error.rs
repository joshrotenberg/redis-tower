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
        expected: &'static str,
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
    TypeMismatch { expected: &'static str },

    /// Reconnection failed after exhausting all retries.
    #[error("reconnect failed after {attempts} attempts: {last_error}")]
    ReconnectFailed {
        attempts: usize,
        last_error: Box<RedisError>,
    },
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
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RedisError::Connection(_) | RedisError::ConnectionClosed | RedisError::Protocol(_)
        )
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
}
