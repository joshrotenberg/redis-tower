use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// PUBLISH channel message
///
/// Posts a message to the given channel. Returns the number of clients
/// that received the message.
pub struct Publish {
    channel: String,
    message: String,
}

impl Publish {
    pub fn new(channel: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            message: message.into(),
        }
    }
}

impl Command for Publish {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("PUBLISH"),
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
        "PUBLISH"
    }
}

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

/// PUBSUB CHANNELS [pattern]
///
/// Lists the currently active channels. An active channel is a Pub/Sub
/// channel with one or more subscribers (excluding clients subscribed
/// to patterns). If no pattern is specified, all active channels are
/// listed. Glob-style patterns are supported.
pub struct PubSubChannels {
    pattern: Option<String>,
}

impl PubSubChannels {
    pub fn new() -> Self {
        Self { pattern: None }
    }

    pub fn with_pattern(pattern: impl Into<String>) -> Self {
        Self {
            pattern: Some(pattern.into()),
        }
    }
}

impl Default for PubSubChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubSubChannels {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PUBSUB"), bulk("CHANNELS")];
        if let Some(ref pat) = self.pattern {
            args.push(bulk(pat.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => {
                let mut channels = Vec::with_capacity(frames.len());
                for f in frames {
                    match f {
                        Frame::BulkString(Some(data)) => channels.push(data),
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    }
                }
                Ok(channels)
            }
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PUBSUB CHANNELS"
    }
}

/// PUBSUB NUMSUB [channel ...]
///
/// Returns the number of subscribers (not counting clients subscribed
/// to patterns) for the specified channels. The response is a flat
/// array of channel/count pairs in RESP2, or a map in RESP3.
pub struct PubSubNumSub {
    channels: Vec<String>,
}

impl PubSubNumSub {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
        }
    }

    pub fn with_channels(channels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            channels: channels.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Default for PubSubNumSub {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubSubNumSub {
    type Response = Vec<(Bytes, i64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PUBSUB"), bulk("NUMSUB")];
        for ch in &self.channels {
            args.push(bulk(ch.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP2: flat array [channel, count, channel, count, ...]
            Frame::Array(Some(frames)) => {
                if frames.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "even number of elements",
                        actual: format!("array of length {}", frames.len()),
                    });
                }
                let mut result = Vec::with_capacity(frames.len() / 2);
                let mut iter = frames.into_iter();
                while let (Some(ch), Some(count)) = (iter.next(), iter.next()) {
                    let channel = match ch {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string (channel name)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let n = match count {
                        Frame::Integer(n) => n,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "integer (subscriber count)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    result.push((channel, n));
                }
                Ok(result)
            }
            Frame::Array(None) => Ok(Vec::new()),
            // RESP3: map of channel -> count
            Frame::Map(pairs) => {
                let mut result = Vec::with_capacity(pairs.len());
                for (k, v) in pairs {
                    let channel = match k {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string (channel name)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let n = match v {
                        Frame::Integer(n) => n,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "integer (subscriber count)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    result.push((channel, n));
                }
                Ok(result)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PUBSUB NUMSUB"
    }
}

/// PUBSUB NUMPAT
///
/// Returns the number of unique patterns that are subscribed to by
/// clients (via PSUBSCRIBE).
pub struct PubSubNumPat;

impl PubSubNumPat {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PubSubNumPat {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubSubNumPat {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PUBSUB"), bulk("NUMPAT")])
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
        "PUBSUB NUMPAT"
    }
}

/// PUBSUB SHARDCHANNELS [pattern]
///
/// Lists the currently active shard channels. An active shard channel
/// is a Pub/Sub shard channel with one or more subscribers. If no
/// pattern is specified, all active shard channels are listed.
/// Glob-style patterns are supported. (Redis 7.0+)
pub struct PubSubShardChannels {
    pattern: Option<String>,
}

impl PubSubShardChannels {
    pub fn new() -> Self {
        Self { pattern: None }
    }

    pub fn with_pattern(pattern: impl Into<String>) -> Self {
        Self {
            pattern: Some(pattern.into()),
        }
    }
}

impl Default for PubSubShardChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubSubShardChannels {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PUBSUB"), bulk("SHARDCHANNELS")];
        if let Some(ref pat) = self.pattern {
            args.push(bulk(pat.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => {
                let mut channels = Vec::with_capacity(frames.len());
                for f in frames {
                    match f {
                        Frame::BulkString(Some(data)) => channels.push(data),
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    }
                }
                Ok(channels)
            }
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PUBSUB SHARDCHANNELS"
    }
}

/// PUBSUB SHARDNUMSUB [channel ...]
///
/// Returns the number of subscribers for the specified shard channels.
/// The response is a flat array of channel/count pairs in RESP2, or a
/// map in RESP3. (Redis 7.0+)
pub struct PubSubShardNumSub {
    channels: Vec<String>,
}

impl PubSubShardNumSub {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
        }
    }

    pub fn with_channels(channels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            channels: channels.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Default for PubSubShardNumSub {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubSubShardNumSub {
    type Response = Vec<(Bytes, i64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PUBSUB"), bulk("SHARDNUMSUB")];
        for ch in &self.channels {
            args.push(bulk(ch.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP2: flat array [channel, count, channel, count, ...]
            Frame::Array(Some(frames)) => {
                if frames.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "even number of elements",
                        actual: format!("array of length {}", frames.len()),
                    });
                }
                let mut result = Vec::with_capacity(frames.len() / 2);
                let mut iter = frames.into_iter();
                while let (Some(ch), Some(count)) = (iter.next(), iter.next()) {
                    let channel = match ch {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string (channel name)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let n = match count {
                        Frame::Integer(n) => n,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "integer (subscriber count)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    result.push((channel, n));
                }
                Ok(result)
            }
            Frame::Array(None) => Ok(Vec::new()),
            // RESP3: map of channel -> count
            Frame::Map(pairs) => {
                let mut result = Vec::with_capacity(pairs.len());
                for (k, v) in pairs {
                    let channel = match k {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string (channel name)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let n = match v {
                        Frame::Integer(n) => n,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "integer (subscriber count)",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    result.push((channel, n));
                }
                Ok(result)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PUBSUB SHARDNUMSUB"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_frame() {
        let cmd = Publish::new("news", "hello");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBLISH"), bulk("news"), bulk("hello")])
        );
        assert_eq!(cmd.name(), "PUBLISH");
    }

    #[test]
    fn publish_parse_response() {
        let cmd = Publish::new("news", "hello");
        let frame = Frame::Integer(3);
        assert_eq!(cmd.parse_response(frame).unwrap(), 3);
    }

    #[test]
    fn spublish_frame() {
        let cmd = SPublish::new("news", "hello");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("SPUBLISH"), bulk("news"), bulk("hello")])
        );
        assert_eq!(cmd.name(), "SPUBLISH");
    }

    #[test]
    fn pubsub_channels_no_pattern() {
        let cmd = PubSubChannels::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBSUB"), bulk("CHANNELS")])
        );
        assert_eq!(cmd.name(), "PUBSUB CHANNELS");
    }

    #[test]
    fn pubsub_channels_with_pattern() {
        let cmd = PubSubChannels::with_pattern("news.*");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBSUB"), bulk("CHANNELS"), bulk("news.*")])
        );
    }

    #[test]
    fn pubsub_channels_parse_response() {
        let cmd = PubSubChannels::new();
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("ch1"))),
            Frame::BulkString(Some(Bytes::from("ch2"))),
        ]));
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, vec![Bytes::from("ch1"), Bytes::from("ch2")]);
    }

    #[test]
    fn pubsub_channels_parse_empty() {
        let cmd = PubSubChannels::new();
        let frame = Frame::Array(Some(vec![]));
        assert!(cmd.parse_response(frame).unwrap().is_empty());
    }

    #[test]
    fn pubsub_numsub_no_channels() {
        let cmd = PubSubNumSub::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBSUB"), bulk("NUMSUB")])
        );
        assert_eq!(cmd.name(), "PUBSUB NUMSUB");
    }

    #[test]
    fn pubsub_numsub_with_channels() {
        let cmd = PubSubNumSub::with_channels(["ch1", "ch2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("PUBSUB"),
                bulk("NUMSUB"),
                bulk("ch1"),
                bulk("ch2"),
            ])
        );
    }

    #[test]
    fn pubsub_numsub_parse_resp2() {
        let cmd = PubSubNumSub::with_channels(["ch1", "ch2"]);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("ch1"))),
            Frame::Integer(5),
            Frame::BulkString(Some(Bytes::from("ch2"))),
            Frame::Integer(0),
        ]));
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(
            resp,
            vec![(Bytes::from("ch1"), 5), (Bytes::from("ch2"), 0)]
        );
    }

    #[test]
    fn pubsub_numsub_parse_resp3_map() {
        let cmd = PubSubNumSub::with_channels(["ch1"]);
        let frame = Frame::Map(vec![(
            Frame::BulkString(Some(Bytes::from("ch1"))),
            Frame::Integer(3),
        )]);
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, vec![(Bytes::from("ch1"), 3)]);
    }

    #[test]
    fn pubsub_numsub_parse_odd_length_error() {
        let cmd = PubSubNumSub::new();
        let frame = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("ch1")))]));
        assert!(cmd.parse_response(frame).is_err());
    }

    #[test]
    fn pubsub_numpat_frame() {
        let cmd = PubSubNumPat::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBSUB"), bulk("NUMPAT")])
        );
        assert_eq!(cmd.name(), "PUBSUB NUMPAT");
    }

    #[test]
    fn pubsub_numpat_parse_response() {
        let cmd = PubSubNumPat::new();
        let frame = Frame::Integer(7);
        assert_eq!(cmd.parse_response(frame).unwrap(), 7);
    }

    #[test]
    fn pubsub_shard_channels_no_pattern() {
        let cmd = PubSubShardChannels::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBSUB"), bulk("SHARDCHANNELS")])
        );
        assert_eq!(cmd.name(), "PUBSUB SHARDCHANNELS");
    }

    #[test]
    fn pubsub_shard_channels_with_pattern() {
        let cmd = PubSubShardChannels::with_pattern("shard.*");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("PUBSUB"),
                bulk("SHARDCHANNELS"),
                bulk("shard.*"),
            ])
        );
    }

    #[test]
    fn pubsub_shard_channels_parse_response() {
        let cmd = PubSubShardChannels::new();
        let frame = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("sc1")))]));
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, vec![Bytes::from("sc1")]);
    }

    #[test]
    fn pubsub_shard_numsub_no_channels() {
        let cmd = PubSubShardNumSub::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PUBSUB"), bulk("SHARDNUMSUB")])
        );
        assert_eq!(cmd.name(), "PUBSUB SHARDNUMSUB");
    }

    #[test]
    fn pubsub_shard_numsub_with_channels() {
        let cmd = PubSubShardNumSub::with_channels(["sc1", "sc2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("PUBSUB"),
                bulk("SHARDNUMSUB"),
                bulk("sc1"),
                bulk("sc2"),
            ])
        );
    }

    #[test]
    fn pubsub_shard_numsub_parse_resp2() {
        let cmd = PubSubShardNumSub::with_channels(["sc1"]);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("sc1"))),
            Frame::Integer(2),
        ]));
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, vec![(Bytes::from("sc1"), 2)]);
    }

    #[test]
    fn pubsub_shard_numsub_parse_resp3_map() {
        let cmd = PubSubShardNumSub::with_channels(["sc1"]);
        let frame = Frame::Map(vec![(
            Frame::BulkString(Some(Bytes::from("sc1"))),
            Frame::Integer(4),
        )]);
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, vec![(Bytes::from("sc1"), 4)]);
    }
}
