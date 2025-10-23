//! Redis commands with strong typing

use crate::codec::Frame;
use crate::types::RedisError;

/// Trait for Redis commands
pub trait Command {
    /// Response type for this command
    type Response;

    /// Convert command to RESP frame
    fn to_frame(&self) -> Frame;

    /// Parse response from RESP frame
    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError>;
}

pub mod strings;

pub use strings::{Get, Set, Del};
