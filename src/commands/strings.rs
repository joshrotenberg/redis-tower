//! String commands (GET, SET, DEL, etc.)

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// GET command - retrieve a value
#[derive(Debug, Clone)]
pub struct Get {
    key: String,
}

impl Get {
    /// Create a new GET command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Get {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(b"GET".to_vec()),
            Frame::BulkString(self.key.as_bytes().to_vec()),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => Ok(Some(Bytes::from(data))),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::Redis(e)),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SET command - set a value
#[derive(Debug, Clone)]
pub struct Set {
    key: String,
    value: Bytes,
}

impl Set {
    /// Create a new SET command
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Command for Set {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(b"SET".to_vec()),
            Frame::BulkString(self.key.as_bytes().to_vec()),
            Frame::BulkString(self.value.to_vec()),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::Redis(e)),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// DEL command - delete one or more keys
#[derive(Debug, Clone)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    /// Create a new DEL command
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl Command for Del {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(b"DEL".to_vec())];
        for key in &self.keys {
            frames.push(Frame::BulkString(key.as_bytes().to_vec()));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::Redis(e)),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
