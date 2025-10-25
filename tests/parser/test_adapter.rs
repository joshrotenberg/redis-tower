//! Test adapter to bridge old resp-parser API to new redis-tower parser API
//!
//! The old resp-parser used `parse()` -> Result<Option<Frame>, Error>
//! The new parser uses `parse_frame()` which returns Result
//!
//! This adapter provides the old API for compatibility with migrated tests.

use bytes::Bytes;
use redis_tower::parser::RespFrame;
use redis_tower::parser::resp3::{Frame, ParseError, parse_frame};

/// Test adapter that provides the old resp-parser API
pub struct RespParser {
    buffer: Vec<u8>,
}

impl RespParser {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Parse data using the old API signature
    /// Returns Result<Option<RespFrame>, RespError>
    pub fn parse(&mut self, data: &[u8]) -> Result<Option<RespFrame>, RespError> {
        // Append to buffer
        self.buffer.extend_from_slice(data);

        // Try to parse a frame
        let bytes = Bytes::copy_from_slice(&self.buffer);
        match parse_frame(bytes) {
            Ok((frame, remaining)) => {
                // Consume parsed data from buffer
                let consumed = self.buffer.len() - remaining.len();
                self.buffer.drain(0..consumed);

                // Convert Frame to RespFrame
                let resp_frame = frame_to_resp_frame(frame)?;
                Ok(Some(resp_frame))
            }
            Err(ParseError::Incomplete) => {
                // Need more data
                Ok(None)
            }
            Err(e) => {
                // Clear buffer on error to match old behavior
                self.buffer.clear();
                Err(e.into())
            }
        }
    }

    /// Clear the parser buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Get buffer length (for testing)
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert RESP3 Frame to RespFrame
fn frame_to_resp_frame(frame: Frame) -> Result<RespFrame, RespError> {
    match frame {
        Frame::SimpleString(s) => {
            let s = String::from_utf8(s.to_vec()).map_err(|_| RespError::InvalidUtf8)?;
            Ok(RespFrame::SimpleString(s))
        }
        Frame::Error(e) => {
            let e = String::from_utf8(e.to_vec()).map_err(|_| RespError::InvalidUtf8)?;
            Ok(RespFrame::Error(e))
        }
        Frame::Integer(i) => Ok(RespFrame::Integer(i)),
        Frame::BulkString(Some(data)) => Ok(RespFrame::BulkString(data.to_vec())),
        Frame::BulkString(None) => Ok(RespFrame::NullBulkString),
        Frame::Array(Some(frames)) => {
            let mut resp_frames = Vec::new();
            for f in frames {
                resp_frames.push(frame_to_resp_frame(f)?);
            }
            Ok(RespFrame::Array(resp_frames))
        }
        Frame::Array(None) => Ok(RespFrame::NullArray),
        _ => Err(RespError::UnsupportedType),
    }
}

/// Error type for test adapter
#[derive(Debug, PartialEq)]
pub enum RespError {
    InvalidType(char),
    InvalidInteger(String),
    InvalidBulkStringLength(i64),
    InvalidUtf8,
    UnsupportedType,
    ParseError,
}

impl From<ParseError> for RespError {
    fn from(e: ParseError) -> Self {
        match e {
            ParseError::InvalidTag(c) => RespError::InvalidType(c as char),
            ParseError::InvalidFormat => RespError::InvalidInteger("bad format".to_string()),
            ParseError::BadLength => RespError::InvalidBulkStringLength(-2),
            _ => RespError::ParseError,
        }
    }
}
