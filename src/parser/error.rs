//! Error types for RESP protocol operations

/// Result type for RESP operations
pub type Result<T> = std::result::Result<T, RespError>;

/// Error types that can occur during RESP parsing or serialization
#[derive(Debug, thiserror::Error)]
pub enum RespError {
    /// Invalid RESP frame type
    #[error("Invalid RESP type byte: {0}")]
    InvalidType(char),

    /// Invalid UTF-8 in string data
    #[error("Invalid UTF-8 in RESP string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// Invalid integer format
    #[error("Invalid integer format: {0}")]
    InvalidInteger(#[from] std::num::ParseIntError),

    /// Invalid bulk string length
    #[error("Invalid bulk string length: {0}")]
    InvalidBulkStringLength(i32),

    /// Invalid array count
    #[error("Invalid array count: {0}")]
    InvalidArrayCount(i32),

    /// Incomplete data - need more bytes
    #[error("Incomplete RESP frame - need more data")]
    IncompleteData,

    /// Protocol error with custom message
    #[error("Protocol error: {0}")]
    Protocol(String),
}

impl RespError {
    /// Create a protocol error with a custom message
    pub fn protocol<S: Into<String>>(msg: S) -> Self {
        Self::Protocol(msg.into())
    }
}
