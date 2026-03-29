use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// SPUBLISH shardchannel message
///
/// Posts a message to the given shard channel. Returns the number of
/// clients that received the message.
pub struct SPublish {
    channel: String,
    message: String,
}

impl SPublish {
    pub fn new(channel: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            message: message.into(),
        }
    }
}

impl Command for SPublish {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SPUBLISH"),
            bulk(self.channel.as_str()),
            bulk(self.message.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SPUBLISH"
    }
}
