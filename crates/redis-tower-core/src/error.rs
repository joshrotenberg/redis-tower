use redis_tower_protocol::ProtocolError;

/// Errors returned by redis-tower-core operations.
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
}
