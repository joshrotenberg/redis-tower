use redis_tower_protocol::Frame;

use crate::error::RedisError;

/// A typed Redis command.
///
/// Each Redis command is represented as a struct that implements this trait.
/// The associated `Response` type ensures compile-time type safety for
/// command results.
///
/// # Example
///
/// ```ignore
/// use redis_tower_core::{Command, Frame, RedisError};
/// use bytes::Bytes;
///
/// pub struct Ping;
///
/// impl Command for Ping {
///     type Response = String;
///
///     fn to_frame(&self) -> Frame {
///         Frame::array(vec![Frame::bulk("PING")])
///     }
///
///     fn parse_response(&self, frame: Frame) -> Result<String, RedisError> {
///         match frame {
///             Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
///             _ => Err(RedisError::UnexpectedResponse {
///                 expected: "simple string",
///                 actual: format!("{frame:?}"),
///             }),
///         }
///     }
///
///     fn name(&self) -> &str { "PING" }
/// }
/// ```
pub trait Command: Send + 'static {
    /// The typed response this command produces.
    type Response: Send + 'static;

    /// Serialize this command into a RESP frame for the wire.
    fn to_frame(&self) -> Frame;

    /// Parse a RESP response frame into the typed response.
    ///
    /// Takes `&self` so that parsing can depend on command configuration
    /// (e.g., optional flags that change the response shape).
    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError>;

    /// The Redis command name, for observability (metrics, tracing spans).
    fn name(&self) -> &str;

    /// Whether this command is safe to retry on connection errors.
    ///
    /// Returns `true` for read-only commands (GET, HGET, LRANGE, etc.) and
    /// commands where re-execution produces the same result (e.g. SET without
    /// side-effect sub-commands). Returns `false` (the default) for all other
    /// write commands where retrying may cause silent data duplication.
    ///
    /// Override this method in command implementations to declare idempotency.
    fn idempotent(&self) -> bool {
        false
    }
}
