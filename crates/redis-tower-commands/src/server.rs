use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// PING [message]
///
/// Returns PONG, or echoes the message if provided.
pub struct Ping {
    message: Option<String>,
}

impl Ping {
    pub fn new() -> Self {
        Self { message: None }
    }

    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
        }
    }
}

impl Default for Ping {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Ping {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PING")];
        if let Some(ref msg) = self.message {
            args.push(bulk(msg.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            Frame::BulkString(Some(s)) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string or bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PING"
    }
}

/// FLUSHDB [ASYNC|SYNC]
///
/// Delete all keys in the current database.
pub struct FlushDb {
    mode: Option<FlushMode>,
}

pub enum FlushMode {
    Async,
    Sync,
}

impl FlushDb {
    pub fn new() -> Self {
        Self { mode: None }
    }

    pub fn async_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Async);
        self
    }

    pub fn sync_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Sync);
        self
    }
}

impl Default for FlushDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FlushDb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FLUSHDB")];
        match &self.mode {
            Some(FlushMode::Async) => args.push(bulk("ASYNC")),
            Some(FlushMode::Sync) => args.push(bulk("SYNC")),
            None => {}
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
        "FLUSHDB"
    }
}
