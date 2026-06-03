use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// MULTI
///
/// Marks the start of a transaction block. Subsequent commands are queued for
/// atomic execution with EXEC.
pub struct Multi;

impl Multi {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Multi {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Multi {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MULTI")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MULTI"
    }
}

/// EXEC
///
/// Executes all commands queued in a transaction. Returns `None` if the
/// transaction was aborted (a watched key changed), or `Some(results)` with one
/// frame per queued command otherwise.
pub struct Exec;

impl Exec {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Exec {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Exec {
    type Response = Option<Vec<Frame>>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("EXEC")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(None) | Frame::Null => Ok(None),
            Frame::Array(Some(frames)) => Ok(Some(frames)),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "EXEC"
    }
}

/// DISCARD
///
/// Discards all commands queued in a transaction and exits the transaction
/// block.
pub struct Discard;

impl Discard {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Discard {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Discard {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("DISCARD")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "DISCARD"
    }
}

/// WATCH key [key ...]
///
/// Marks the given keys to be watched for conditional execution of a
/// transaction (optimistic locking).
pub struct Watch {
    keys: Vec<String>,
}

impl Watch {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Watch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("WATCH")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "WATCH"
    }
}

/// UNWATCH
///
/// Flushes all previously watched keys for a transaction.
pub struct Unwatch;

impl Unwatch {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Unwatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Unwatch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("UNWATCH")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "UNWATCH"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- Multi --

    #[test]
    fn multi_to_frame() {
        let cmd = Multi::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("MULTI")]));
    }

    #[test]
    fn multi_parse_ok() {
        let cmd = Multi::new();
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    #[test]
    fn multi_parse_error_on_integer() {
        let cmd = Multi::new();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- Exec --

    #[test]
    fn exec_to_frame() {
        let cmd = Exec::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("EXEC")]));
    }

    #[test]
    fn exec_parse_results() {
        let cmd = Exec::new();
        let frame = array(vec![Frame::Integer(1), Frame::Integer(2)]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, Some(vec![Frame::Integer(1), Frame::Integer(2)]));
    }

    #[test]
    fn exec_parse_aborted() {
        let cmd = Exec::new();
        assert_eq!(cmd.parse_response(Frame::Array(None)).unwrap(), None);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- Discard --

    #[test]
    fn discard_to_frame() {
        let cmd = Discard::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("DISCARD")]));
    }

    #[test]
    fn discard_parse_ok() {
        let cmd = Discard::new();
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- Watch --

    #[test]
    fn watch_single_to_frame() {
        let cmd = Watch::new("k1");
        assert_eq!(cmd.to_frame(), array(vec![bulk("WATCH"), bulk("k1")]));
    }

    #[test]
    fn watch_multiple_to_frame() {
        let cmd = Watch::keys(vec!["k1", "k2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("WATCH"), bulk("k1"), bulk("k2")])
        );
    }

    #[test]
    fn watch_parse_ok() {
        let cmd = Watch::new("k1");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- Unwatch --

    #[test]
    fn unwatch_to_frame() {
        let cmd = Unwatch::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("UNWATCH")]));
    }

    #[test]
    fn unwatch_parse_ok() {
        let cmd = Unwatch::new();
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }
}
