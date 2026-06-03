use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Execute an arbitrary Redis command by name.
///
/// Use this for commands not covered by typed structs, custom modules,
/// or commands added in newer Redis versions.
///
/// # Example
///
/// ```ignore
/// let result = conn.execute(RawCommand::new("CUSTOM.CMD").arg("key").arg("val")).await?;
/// ```
///
/// See: <https://redis.io/commands>
pub struct RawCommand {
    name_str: String,
    args: Vec<Vec<u8>>,
}

impl RawCommand {
    /// Create a new raw command with the given command name.
    pub fn new(name: impl Into<String>) -> Self {
        let name_str = name.into();
        Self {
            name_str,
            args: Vec::new(),
        }
    }

    /// Append an argument to this command.
    #[must_use]
    pub fn arg(mut self, arg: impl AsRef<[u8]>) -> Self {
        self.args.push(arg.as_ref().to_vec());
        self
    }
}

impl Command for RawCommand {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut frames = Vec::with_capacity(1 + self.args.len());
        frames.push(bulk(self.name_str.as_str()));
        for arg in &self.args {
            frames.push(bulk(arg.as_slice()));
        }
        array(frames)
    }

    fn parse_response(&self, frame: Frame) -> Result<Frame, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        &self.name_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn to_frame_single_command() {
        let cmd = RawCommand::new("PING");
        let frame = cmd.to_frame();
        let expected = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("PING")))]));
        assert_eq!(frame, expected);
    }

    #[test]
    fn to_frame_with_args() {
        let cmd = RawCommand::new("SET").arg("key").arg("value");
        let frame = cmd.to_frame();
        let expected = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        assert_eq!(frame, expected);
    }

    #[test]
    fn parse_response_passes_through() {
        let cmd = RawCommand::new("PING");
        let frame = Frame::SimpleString(Bytes::from("PONG"));
        let result = cmd.parse_response(frame.clone()).unwrap();
        assert_eq!(result, frame);
    }

    #[test]
    fn name_returns_command_name() {
        let cmd = RawCommand::new("CUSTOM.CMD");
        assert_eq!(cmd.name(), "CUSTOM.CMD");
    }

    #[test]
    fn multiple_args_binary() {
        let cmd = RawCommand::new("SET")
            .arg(b"bin\x00key".as_slice())
            .arg(b"\xff\xfe".as_slice());
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(frames)) => assert_eq!(frames.len(), 3),
            other => panic!("expected array, got {other:?}"),
        }
    }
}
