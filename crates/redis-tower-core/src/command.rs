use redis_tower_protocol::Frame;
use tokio::time::Instant;

use crate::error::RedisError;

/// A typed Redis command.
///
/// Each Redis command is represented as a struct that implements this trait.
/// The associated `Response` type ensures compile-time type safety for
/// command results.
///
/// # Example
///
/// ```no_run
/// use redis_tower_core::{Command, Frame, RedisError};
/// use redis_tower_protocol::helpers::{array, bulk};
///
/// pub struct Ping;
///
/// impl Command for Ping {
///     type Response = String;
///
///     fn to_frame(&self) -> Frame {
///         array(vec![bulk("PING")])
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

    /// Whether this command blocks the connection until data is available or a
    /// timeout elapses -- `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`/`BZPOPMAX`, and
    /// `XREAD`/`XREADGROUP` with `BLOCK`.
    ///
    /// Returns `false` by default. This matters for multiplexed clients: a
    /// blocking command holds the single shared pipeline worker for its entire
    /// duration, stalling every other concurrent caller on that connection. Run
    /// blocking commands on a dedicated `RedisConnection` or a pooled connection
    /// instead. The flag lets callers and middleware detect such commands.
    fn is_blocking(&self) -> bool {
        false
    }

    /// Return the absolute deadline attached to this command, if any.
    ///
    /// Most commands have no deadline. [`WithDeadline`] overrides this method
    /// so deadline-aware middleware and resource acquisition can share one
    /// end-to-end budget instead of starting a fresh timeout at each stage.
    /// Implementations that wrap another command should propagate the wrapped
    /// command's deadline.
    fn deadline(&self) -> Option<Instant> {
        None
    }
}

/// A typed command carrying an absolute deadline.
///
/// The deadline is an absolute [`tokio::time::Instant`], so cloning or retrying
/// the command does not reset its time budget. Deadline-aware middleware and
/// pools can inspect it through [`Command::deadline`].
///
/// When envelopes are nested, the earliest deadline wins; wrapping a command
/// can therefore shorten, but never extend, an existing budget.
#[derive(Debug, Clone)]
pub struct WithDeadline<C> {
    command: C,
    deadline: Instant,
}

impl<C> WithDeadline<C> {
    /// Wrap `command` with an absolute `deadline`.
    pub fn new(command: C, deadline: Instant) -> Self {
        Self { command, deadline }
    }

    /// Wrap `command` with a deadline relative to now.
    pub fn after(command: C, timeout: std::time::Duration) -> Self {
        Self::new(command, Instant::now() + timeout)
    }

    /// Return the envelope's absolute deadline.
    ///
    /// For nested envelopes, [`Command::deadline`] returns the effective
    /// earliest deadline while this accessor returns the deadline stored on
    /// this envelope itself.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Borrow the wrapped command.
    pub fn get_ref(&self) -> &C {
        &self.command
    }

    /// Mutably borrow the wrapped command.
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.command
    }

    /// Consume the envelope and return the wrapped command.
    pub fn into_inner(self) -> C {
        self.command
    }
}

impl<C: Command> Command for WithDeadline<C> {
    type Response = C::Response;

    fn to_frame(&self) -> Frame {
        self.command.to_frame()
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        self.command.parse_response(frame)
    }

    fn name(&self) -> &str {
        self.command.name()
    }

    fn idempotent(&self) -> bool {
        self.command.idempotent()
    }

    fn is_blocking(&self) -> bool {
        self.command.is_blocking()
    }

    fn deadline(&self) -> Option<Instant> {
        Some(match self.command.deadline() {
            Some(inner) => self.deadline.min(inner),
            None => self.deadline,
        })
    }
}

/// Deadline metadata understood by the deadline-aware mode of
/// [`CommandTimeoutLayer`](https://docs.rs/redis-tower/latest/redis_tower/struct.CommandTimeoutLayer.html).
///
/// Typed [`Command`] requests implement this trait automatically. Raw RESP
/// [`Frame`] requests are also supported and report no per-request deadline,
/// preserving the static-timeout behavior of frame-level middleware stacks.
/// Custom request types can implement this trait to use
/// `CommandTimeoutLayer::with_request_deadlines`.
pub trait RequestDeadline {
    /// Return the request's absolute deadline, if it carries one.
    fn request_deadline(&self) -> Option<Instant>;
}

impl<C: Command> RequestDeadline for C {
    fn request_deadline(&self) -> Option<Instant> {
        Command::deadline(self)
    }
}

impl RequestDeadline for Frame {
    fn request_deadline(&self) -> Option<Instant> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestCommand;

    impl Command for TestCommand {
        type Response = Frame;

        fn to_frame(&self) -> Frame {
            Frame::Null
        }

        fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
            Ok(frame)
        }

        fn name(&self) -> &str {
            "TEST"
        }

        fn idempotent(&self) -> bool {
            true
        }

        fn is_blocking(&self) -> bool {
            true
        }
    }

    #[test]
    fn with_deadline_delegates_command_metadata() {
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let command = WithDeadline::new(TestCommand, deadline);

        assert_eq!(command.name(), "TEST");
        assert!(command.idempotent());
        assert!(command.is_blocking());
        assert_eq!(Command::deadline(&command), Some(deadline));
        assert!(matches!(command.to_frame(), Frame::Null));
        assert!(matches!(
            command.parse_response(Frame::Null),
            Ok(Frame::Null)
        ));
    }

    #[test]
    fn nested_envelopes_keep_the_earliest_deadline() {
        let early = Instant::now() + std::time::Duration::from_secs(1);
        let late = early + std::time::Duration::from_secs(1);

        let shortened = WithDeadline::new(WithDeadline::new(TestCommand, late), early);
        let not_extended = WithDeadline::new(WithDeadline::new(TestCommand, early), late);

        assert_eq!(Command::deadline(&shortened), Some(early));
        assert_eq!(Command::deadline(&not_extended), Some(early));
    }

    #[test]
    fn raw_frames_have_no_request_deadline() {
        assert_eq!(Frame::Null.request_deadline(), None);
    }
}
