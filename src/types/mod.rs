//! Type definitions and error handling

use thiserror::Error;

pub mod value;

pub use value::RedisValue;

/// Redis client errors
#[derive(Debug, Error, Clone)]
pub enum RedisError {
    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

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

impl From<std::io::Error> for RedisError {
    fn from(err: std::io::Error) -> Self {
        RedisError::Connection(err.to_string())
    }
}

impl RedisError {
    /// Parse a Redis error string and convert to appropriate RedisError variant.
    ///
    /// Handles special cluster errors:
    /// - MOVED slot addr -> RedisError::Moved
    /// - ASK slot addr -> RedisError::Ask
    /// - Everything else -> RedisError::Redis
    ///
    /// # Examples
    /// ```
    /// use redis_tower::types::RedisError;
    ///
    /// let err = RedisError::from_redis_error("MOVED 7431 127.0.0.1:7001");
    /// match err {
    ///     RedisError::Moved { slot, addr } => {
    ///         assert_eq!(slot, 7431);
    ///         assert_eq!(addr, "127.0.0.1:7001");
    ///     }
    ///     _ => panic!("Expected Moved error"),
    /// }
    /// ```
    pub fn from_redis_error(error_msg: &str) -> Self {
        // Check for MOVED error: "MOVED slot addr"
        if error_msg.starts_with("MOVED ") {
            let parts: Vec<&str> = error_msg.split_whitespace().collect();
            if parts.len() >= 3 {
                if let Ok(slot) = parts[1].parse::<u16>() {
                    return RedisError::Moved {
                        slot,
                        addr: parts[2].to_string(),
                    };
                }
            }
        }

        // Check for ASK error: "ASK slot addr"
        if error_msg.starts_with("ASK ") {
            let parts: Vec<&str> = error_msg.split_whitespace().collect();
            if parts.len() >= 3 {
                if let Ok(slot) = parts[1].parse::<u16>() {
                    return RedisError::Ask {
                        slot,
                        addr: parts[2].to_string(),
                    };
                }
            }
        }

        // Default to generic Redis error
        RedisError::Redis(error_msg.to_string())
    }
}
