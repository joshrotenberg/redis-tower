//! Pub/Sub commands for publishing messages.
//!
//! Note: SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE are handled by
//! `PubSubConnection` in the `pubsub` module, as they require a dedicated
//! connection in Pub/Sub mode.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// Publish a message to a channel.
///
/// Returns the number of clients that received the message.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::Publish;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Publish a message
/// let subscribers = client.execute(
///     Publish::new("news", "Breaking: Rust 2.0 released!")
/// ).await?;
///
/// println!("Message delivered to {} subscribers", subscribers);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Publish {
    pub(crate) channel: String,
    pub(crate) message: Bytes,
}

impl Publish {
    /// Create a new PUBLISH command.
    pub fn new(channel: impl Into<String>, message: impl Into<Bytes>) -> Self {
        Self {
            channel: channel.into(),
            message: message.into(),
        }
    }
}

impl Command for Publish {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBLISH"))),
            Frame::BulkString(Some(Bytes::from(self.channel.clone()))),
            Frame::BulkString(Some(self.message.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the number of subscribers for channels.
///
/// Returns the number of subscribers (not counting clients subscribed to patterns)
/// for the specified channels.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubNumsub;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let counts = client.execute(
///     PubsubNumsub::new(&["news", "updates"])
/// ).await?;
///
/// for (channel, count) in &counts {
///     println!("{}: {} subscribers", channel, count);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubNumsub {
    pub(crate) channels: Vec<String>,
}

impl PubsubNumsub {
    /// Create a new PUBSUB NUMSUB command.
    pub fn new(channels: &[&str]) -> Self {
        Self {
            channels: channels.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Command for PubsubNumsub {
    type Response = Vec<(String, i64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"NUMSUB"))),
        ];

        for channel in &self.channels {
            args.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::new();
                let mut i = 0;

                while i < items.len() {
                    if i + 1 >= items.len() {
                        break;
                    }

                    let channel = match &items[i] {
                        Frame::BulkString(Some(bytes)) => {
                            String::from_utf8_lossy(bytes).to_string()
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected bulk string for channel".to_string(),
                            ));
                        }
                    };

                    let count = match &items[i + 1] {
                        Frame::Integer(n) => *n,
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected integer for count".to_string(),
                            ));
                        }
                    };

                    results.push((channel, count));
                    i += 2;
                }

                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the number of patterns subscribed to.
///
/// Returns the number of pattern subscriptions (those created with PSUBSCRIBE).
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubNumpat;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let count = client.execute(PubsubNumpat::new()).await?;
/// println!("Active pattern subscriptions: {}", count);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubNumpat;

impl PubsubNumpat {
    /// Create a new PUBSUB NUMPAT command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for PubsubNumpat {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubsubNumpat {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"NUMPAT"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get active channels.
///
/// Lists the currently active channels. An active channel is a channel with one or more subscribers.
/// If pattern is provided, only channels matching the pattern are returned.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubChannels;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Get all active channels
/// let channels = client.execute(PubsubChannels::all()).await?;
///
/// // Get channels matching a pattern
/// let news_channels = client.execute(PubsubChannels::pattern("news:*")).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubChannels {
    pub(crate) pattern: Option<String>,
}

impl PubsubChannels {
    /// List all active channels.
    pub fn all() -> Self {
        Self { pattern: None }
    }

    /// List active channels matching the given pattern.
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self {
            pattern: Some(pattern.into()),
        }
    }
}

impl Command for PubsubChannels {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"CHANNELS"))),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut channels = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(bytes)) => {
                            channels.push(String::from_utf8_lossy(&bytes).to_string());
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected bulk string for channel".to_string(),
                            ));
                        }
                    }
                }
                Ok(channels)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get active sharded pub/sub channels.
///
/// Lists the currently active sharded pub/sub channels. Available in Redis 7.0+.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubShardchannels;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let channels = client.execute(PubsubShardchannels::all()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubShardchannels {
    pub(crate) pattern: Option<String>,
}

impl PubsubShardchannels {
    /// List all active sharded channels.
    pub fn all() -> Self {
        Self { pattern: None }
    }

    /// List active sharded channels matching the given pattern.
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self {
            pattern: Some(pattern.into()),
        }
    }
}

impl Command for PubsubShardchannels {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"SHARDCHANNELS"))),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut channels = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(bytes)) => {
                            channels.push(String::from_utf8_lossy(&bytes).to_string());
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected bulk string for channel".to_string(),
                            ));
                        }
                    }
                }
                Ok(channels)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the number of subscribers for sharded channels.
///
/// Returns the number of subscribers for the specified sharded pub/sub channels.
/// Available in Redis 7.0+.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubShardnumsub;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let counts = client.execute(
///     PubsubShardnumsub::new(&["shard:1", "shard:2"])
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubShardnumsub {
    pub(crate) channels: Vec<String>,
}

impl PubsubShardnumsub {
    /// Create a new PUBSUB SHARDNUMSUB command.
    pub fn new(channels: &[&str]) -> Self {
        Self {
            channels: channels.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Command for PubsubShardnumsub {
    type Response = Vec<(String, i64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"SHARDNUMSUB"))),
        ];

        for channel in &self.channels {
            args.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::new();
                let mut i = 0;

                while i < items.len() {
                    if i + 1 >= items.len() {
                        break;
                    }

                    let channel = match &items[i] {
                        Frame::BulkString(Some(bytes)) => {
                            String::from_utf8_lossy(bytes).to_string()
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected bulk string for channel".to_string(),
                            ));
                        }
                    };

                    let count = match &items[i + 1] {
                        Frame::Integer(n) => *n,
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected integer for count".to_string(),
                            ));
                        }
                    };

                    results.push((channel, count));
                    i += 2;
                }

                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Publish a message to a sharded channel.
///
/// Posts a message to the given sharded channel. Available in Redis 7.0+.
/// Returns the number of clients that received the message.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::Spublish;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let subscribers = client.execute(
///     Spublish::new("shard:news", "Breaking news!")
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Spublish {
    pub(crate) channel: String,
    pub(crate) message: Bytes,
}

impl Spublish {
    /// Create a new SPUBLISH command.
    pub fn new(channel: impl Into<String>, message: impl Into<Bytes>) -> Self {
        Self {
            channel: channel.into(),
            message: message.into(),
        }
    }
}

impl Command for Spublish {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"SPUBLISH"))),
            Frame::BulkString(Some(Bytes::from(self.channel.clone()))),
            Frame::BulkString(Some(self.message.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_frame() {
        let cmd = Publish::new("news", "hello");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_publish_response() {
        let frame = Frame::Integer(5);
        let result = Publish::parse_response(frame).unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn test_pubsub_numsub_frame() {
        let cmd = PubsubNumsub::new(&["news", "updates"]);
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 4); // PUBSUB NUMSUB news updates
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_numsub_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("news"))),
            Frame::Integer(5),
            Frame::BulkString(Some(Bytes::from("updates"))),
            Frame::Integer(3),
        ]);

        let result = PubsubNumsub::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("news".to_string(), 5));
        assert_eq!(result[1], ("updates".to_string(), 3));
    }

    #[test]
    fn test_pubsub_numpat_frame() {
        let cmd = PubsubNumpat::new();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2); // PUBSUB NUMPAT
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_numpat_response() {
        let frame = Frame::Integer(10);
        let result = PubsubNumpat::parse_response(frame).unwrap();
        assert_eq!(result, 10);
    }

    #[test]
    fn test_pubsub_channels_all_frame() {
        let cmd = PubsubChannels::all();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2); // PUBSUB CHANNELS
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_channels_pattern_frame() {
        let cmd = PubsubChannels::pattern("news:*");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3); // PUBSUB CHANNELS news:*
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_channels_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("news"))),
            Frame::BulkString(Some(Bytes::from("updates"))),
            Frame::BulkString(Some(Bytes::from("events"))),
        ]);

        let result = PubsubChannels::parse_response(frame).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "news");
        assert_eq!(result[1], "updates");
        assert_eq!(result[2], "events");
    }

    #[test]
    fn test_pubsub_shardchannels_all_frame() {
        let cmd = PubsubShardchannels::all();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2); // PUBSUB SHARDCHANNELS
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_shardchannels_pattern_frame() {
        let cmd = PubsubShardchannels::pattern("shard:*");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3); // PUBSUB SHARDCHANNELS shard:*
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_shardchannels_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("shard:1"))),
            Frame::BulkString(Some(Bytes::from("shard:2"))),
        ]);

        let result = PubsubShardchannels::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "shard:1");
        assert_eq!(result[1], "shard:2");
    }

    #[test]
    fn test_pubsub_shardnumsub_frame() {
        let cmd = PubsubShardnumsub::new(&["shard:1", "shard:2"]);
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 4); // PUBSUB SHARDNUMSUB shard:1 shard:2
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_shardnumsub_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("shard:1"))),
            Frame::Integer(3),
            Frame::BulkString(Some(Bytes::from("shard:2"))),
            Frame::Integer(5),
        ]);

        let result = PubsubShardnumsub::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("shard:1".to_string(), 3));
        assert_eq!(result[1], ("shard:2".to_string(), 5));
    }

    #[test]
    fn test_spublish_frame() {
        let cmd = Spublish::new("shard:news", "hello");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_spublish_response() {
        let frame = Frame::Integer(7);
        let result = Spublish::parse_response(frame).unwrap();
        assert_eq!(result, 7);
    }

    #[test]
    fn test_pubsub_channels_empty_response() {
        let frame = Frame::Array(vec![]);
        let result = PubsubChannels::parse_response(frame).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_pubsub_numsub_empty_response() {
        let frame = Frame::Array(vec![]);
        let result = PubsubNumsub::parse_response(frame).unwrap();
        assert_eq!(result.len(), 0);
    }
}

/// SUBSCRIBE command - Subscribe to one or more channels
///
/// Subscribes the client to the specified channels. Once subscribed, the client
/// enters pub/sub mode and can only use pub/sub related commands.
///
/// Available since Redis 2.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Subscribe;
///
/// let cmd = Subscribe::new(vec!["news", "sports"]);
/// ```
#[derive(Debug, Clone)]
pub struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    /// Create a new SUBSCRIBE command
    pub fn new(channels: Vec<impl Into<String>>) -> Self {
        Self {
            channels: channels.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Command for Subscribe {
    type Response = (); // Enters subscription mode, responses are streamed

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("SUBSCRIBE")))];
        for channel in &self.channels {
            frames.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(_frame: Frame) -> Result<Self::Response, RedisError> {
        // SUBSCRIBE returns subscription confirmation messages, not a simple response
        Ok(())
    }
}

/// UNSUBSCRIBE command - Unsubscribe from channels
///
/// Unsubscribes the client from the given channels, or from all if none specified.
///
/// Available since Redis 2.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Unsubscribe;
///
/// // Unsubscribe from specific channels
/// let cmd = Unsubscribe::new(vec!["news", "sports"]);
///
/// // Unsubscribe from all channels
/// let cmd = Unsubscribe::all();
/// ```
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Unsubscribe {
    /// Create a new UNSUBSCRIBE command for specific channels
    pub fn new(channels: Vec<impl Into<String>>) -> Self {
        Self {
            channels: channels.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Unsubscribe from all channels
    pub fn all() -> Self {
        Self {
            channels: Vec::new(),
        }
    }
}

impl Command for Unsubscribe {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("UNSUBSCRIBE")))];
        for channel in &self.channels {
            frames.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(_frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(())
    }
}

/// PSUBSCRIBE command - Subscribe to channels matching patterns
///
/// Subscribes the client to the given patterns. Patterns use glob-style matching.
///
/// Available since Redis 2.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Psubscribe;
///
/// let cmd = Psubscribe::new(vec!["news.*", "sport.*"]);
/// ```
#[derive(Debug, Clone)]
pub struct Psubscribe {
    patterns: Vec<String>,
}

impl Psubscribe {
    /// Create a new PSUBSCRIBE command
    pub fn new(patterns: Vec<impl Into<String>>) -> Self {
        Self {
            patterns: patterns.into_iter().map(|p| p.into()).collect(),
        }
    }
}

impl Command for Psubscribe {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("PSUBSCRIBE")))];
        for pattern in &self.patterns {
            frames.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(_frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(())
    }
}

/// PUNSUBSCRIBE command - Unsubscribe from patterns
///
/// Unsubscribes the client from the given patterns, or from all if none specified.
///
/// Available since Redis 2.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Punsubscribe;
///
/// // Unsubscribe from specific patterns
/// let cmd = Punsubscribe::new(vec!["news.*"]);
///
/// // Unsubscribe from all patterns
/// let cmd = Punsubscribe::all();
/// ```
#[derive(Debug, Clone)]
pub struct Punsubscribe {
    patterns: Vec<String>,
}

impl Punsubscribe {
    /// Create a new PUNSUBSCRIBE command for specific patterns
    pub fn new(patterns: Vec<impl Into<String>>) -> Self {
        Self {
            patterns: patterns.into_iter().map(|p| p.into()).collect(),
        }
    }

    /// Unsubscribe from all patterns
    pub fn all() -> Self {
        Self {
            patterns: Vec::new(),
        }
    }
}

impl Command for Punsubscribe {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("PUNSUBSCRIBE")))];
        for pattern in &self.patterns {
            frames.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(_frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(())
    }
}

/// SSUBSCRIBE command - Subscribe to sharded channels (Redis 7.0+)
///
/// Subscribes the client to the specified sharded channels.
///
/// Available since Redis 7.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Ssubscribe;
///
/// let cmd = Ssubscribe::new(vec!["shard:news", "shard:sports"]);
/// ```
#[derive(Debug, Clone)]
pub struct Ssubscribe {
    channels: Vec<String>,
}

impl Ssubscribe {
    /// Create a new SSUBSCRIBE command
    pub fn new(channels: Vec<impl Into<String>>) -> Self {
        Self {
            channels: channels.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Command for Ssubscribe {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("SSUBSCRIBE")))];
        for channel in &self.channels {
            frames.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(_frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(())
    }
}

/// SUNSUBSCRIBE command - Unsubscribe from sharded channels (Redis 7.0+)
///
/// Unsubscribes the client from the given sharded channels, or from all if none specified.
///
/// Available since Redis 7.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Sunsubscribe;
///
/// // Unsubscribe from specific channels
/// let cmd = Sunsubscribe::new(vec!["shard:news"]);
///
/// // Unsubscribe from all sharded channels
/// let cmd = Sunsubscribe::all();
/// ```
#[derive(Debug, Clone)]
pub struct Sunsubscribe {
    channels: Vec<String>,
}

impl Sunsubscribe {
    /// Create a new SUNSUBSCRIBE command for specific channels
    pub fn new(channels: Vec<impl Into<String>>) -> Self {
        Self {
            channels: channels.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Unsubscribe from all sharded channels
    pub fn all() -> Self {
        Self {
            channels: Vec::new(),
        }
    }
}

impl Command for Sunsubscribe {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("SUNSUBSCRIBE")))];
        for channel in &self.channels {
            frames.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(_frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(())
    }
}
