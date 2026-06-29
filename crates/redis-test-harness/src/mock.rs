//! Mock Redis connection for unit testing.
//!
//! Returns pre-configured frames without connecting to a server.
//! Useful for testing `parse_response` error branches that can't
//! be triggered against a real Redis.
//!
//! # Example
//!
//! ```
//! use redis_tower_test::mock::MockConnection;
//! use redis_tower_protocol::Frame;
//! use bytes::Bytes;
//!
//! let mut mock = MockConnection::new();
//! mock.enqueue(Frame::Integer(42));
//! mock.enqueue(Frame::BulkString(Some(Bytes::from("hello"))));
//!
//! // First call returns Integer(42), second returns BulkString("hello").
//! let frame = mock.next_response().unwrap();
//! assert!(matches!(frame, Frame::Integer(42)));
//! ```

use std::collections::VecDeque;
use std::io;

use redis_tower_core::{Command, Frame, RedisError};

/// A mock Redis connection that returns pre-configured frames.
///
/// Frames are returned in FIFO order. If the queue is empty when a
/// response is requested, an error is returned.
pub struct MockConnection {
    responses: VecDeque<Frame>,
}

impl MockConnection {
    /// Create a new empty mock connection.
    pub fn new() -> Self {
        Self {
            responses: VecDeque::new(),
        }
    }

    /// Create a mock with pre-loaded responses.
    pub fn with_responses(responses: Vec<Frame>) -> Self {
        Self {
            responses: responses.into(),
        }
    }

    /// Enqueue a response frame.
    pub fn enqueue(&mut self, frame: Frame) {
        self.responses.push_back(frame);
    }

    /// Enqueue multiple response frames.
    pub fn enqueue_all(&mut self, frames: impl IntoIterator<Item = Frame>) {
        self.responses.extend(frames);
    }

    /// Enqueue an OK simple string response.
    pub fn enqueue_ok(&mut self) {
        self.responses
            .push_back(Frame::SimpleString(bytes::Bytes::from("OK")));
    }

    /// Enqueue a Redis error response.
    pub fn enqueue_error(&mut self, msg: &str) {
        self.responses
            .push_back(Frame::Error(bytes::Bytes::from(msg.to_string())));
    }

    /// Enqueue a null response.
    pub fn enqueue_null(&mut self) {
        self.responses.push_back(Frame::Null);
    }

    /// Get the next response frame from the queue.
    pub fn next_response(&mut self) -> io::Result<Frame> {
        self.responses
            .pop_front()
            .ok_or_else(|| io::Error::other("MockConnection: no more responses in queue"))
    }

    /// Execute a command against the mock, returning the parsed response.
    ///
    /// Pops the next frame from the queue and passes it through the
    /// command's `parse_response`. This lets you test response parsing
    /// without a real server.
    pub fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let frame = self
            .responses
            .pop_front()
            .ok_or(RedisError::ConnectionClosed)?;

        // Check for Redis error frames (same as RedisConnection).
        if let Frame::Error(ref e) = frame {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }

        cmd.parse_response(frame)
    }

    /// Returns the number of remaining responses in the queue.
    pub fn remaining(&self) -> usize {
        self.responses.len()
    }

    /// Returns true if the response queue is empty.
    pub fn is_empty(&self) -> bool {
        self.responses.is_empty()
    }
}

impl Default for MockConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn fifo_order() {
        let mut mock = MockConnection::new();
        mock.enqueue(Frame::Integer(1));
        mock.enqueue(Frame::Integer(2));
        mock.enqueue(Frame::Integer(3));

        assert!(matches!(mock.next_response().unwrap(), Frame::Integer(1)));
        assert!(matches!(mock.next_response().unwrap(), Frame::Integer(2)));
        assert!(matches!(mock.next_response().unwrap(), Frame::Integer(3)));
    }

    #[test]
    fn empty_queue_error() {
        let mut mock = MockConnection::new();
        assert!(mock.next_response().is_err());
    }

    #[test]
    fn with_responses() {
        let mock = MockConnection::with_responses(vec![
            Frame::SimpleString(Bytes::from("OK")),
            Frame::Integer(42),
        ]);
        assert_eq!(mock.remaining(), 2);
    }

    #[test]
    fn enqueue_helpers() {
        let mut mock = MockConnection::new();
        mock.enqueue_ok();
        mock.enqueue_error("ERR test");
        mock.enqueue_null();
        assert_eq!(mock.remaining(), 3);
    }

    #[test]
    fn remaining_and_empty() {
        let mut mock = MockConnection::new();
        assert!(mock.is_empty());
        mock.enqueue(Frame::Null);
        assert!(!mock.is_empty());
        assert_eq!(mock.remaining(), 1);
    }
}
