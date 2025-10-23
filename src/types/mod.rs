//! Type definitions and error handling

use thiserror::Error;

pub mod response;
pub mod value;

pub use response::RedisResponse;
pub use value::RedisValue;

/// Redis client errors
#[derive(Debug, Error)]
pub enum RedisError {
    /// Connection error
    #[error("Connection error: {0}")]
    Connection(#[from] std::io::Error),

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Type mismatch
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// Expected type
        expected: &'static str,
        /// Actual type
        actual: String,
    },

    /// Redis server error
    #[error("Redis error: {0}")]
    Redis(String),

    /// Unexpected response
    #[error("Unexpected response")]
    UnexpectedResponse,

    /// Cluster error: MOVED
    #[error("Cluster error: MOVED {slot} {addr}")]
    Moved {
        /// Slot number
        slot: u16,
        /// Target address
        addr: String,
    },

    /// Cluster error: ASK
    #[error("Cluster error: ASK {slot} {addr}")]
    Ask {
        /// Slot number
        slot: u16,
        /// Target address
        addr: String,
    },
}
