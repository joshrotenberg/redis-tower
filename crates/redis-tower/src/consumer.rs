//! Managed stream consumer for Redis Streams.
//!
//! Wraps XREADGROUP into a Rust [`Stream`](futures::Stream) with automatic
//! acknowledgement and consumer group management.
//!
//! # Acknowledgement semantics
//!
//! With [`ConsumerConfig::auto_ack`] enabled, an item is acknowledged when the
//! returned stream is polled again after yielding it. Dropping the stream
//! before that next poll leaves the item pending so another consumer can
//! recover it. Advancing the stream is only a delivery acknowledgement; it
//! cannot prove that arbitrary application-side processing succeeded. Use
//! `auto_ack: false` and send [`XAck`] explicitly when the application must
//! decide whether processing was successful.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{RedisConnection, consumer::{StreamConsumer, ConsumerConfig}};
//! use tokio_stream::StreamExt;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let consumer = StreamConsumer::new("my-group", "worker-1", ["my-stream"])
//!     .config(ConsumerConfig {
//!         batch_size: 20,
//!         auto_ack: true,
//!         ..Default::default()
//!     });
//!
//! // The stream is not `Unpin`, so pin it before polling.
//! let mut stream = Box::pin(consumer.into_stream(conn));
//! while let Some(msg) = stream.next().await {
//!     let msg = msg?;
//!     println!("{}: {} fields", msg.id, msg.fields.len());
//! }
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use redis_tower_commands::{CommandArg, XAck, XAutoClaim, XGroupCreate, XReadGroup};
use redis_tower_core::{RedisConnection, RedisError};

/// A message received from a Redis stream.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    /// The stream key this message came from.
    pub stream: Bytes,
    /// The message ID (e.g., "1234567890-0").
    pub id: String,
    /// Field-value pairs in the message.
    pub fields: Vec<(Bytes, Bytes)>,
}

/// Configuration for the [`StreamConsumer`].
///
/// # Defaults
///
/// - `batch_size`: 10
/// - `block_ms`: `Some(5000)` (5 seconds)
/// - `auto_ack`: `true`
/// - `claim_idle_ms`: `None`
/// - `create_group`: `true`
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Max messages per XREADGROUP call (COUNT). Default: 10.
    pub batch_size: u64,
    /// Block timeout in milliseconds. `None` for non-blocking. Default: `Some(5000)`.
    pub block_ms: Option<u64>,
    /// Automatically XACK each message when the stream is polled again after
    /// yielding it. Default: true.
    ///
    /// This acknowledges delivery through the pull stream, not successful
    /// completion of arbitrary application-side processing. Set this to
    /// `false` and issue [`XAck`] explicitly for application-controlled
    /// acknowledgement.
    pub auto_ack: bool,
    /// If set, claim idle messages older than this many ms via XAUTOCLAIM on startup.
    pub claim_idle_ms: Option<u64>,
    /// Create the consumer group if it does not exist. Default: true.
    pub create_group: bool,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            block_ms: Some(5000),
            auto_ack: true,
            claim_idle_ms: None,
            create_group: true,
        }
    }
}

/// A managed Redis Streams consumer that yields messages as a Rust [`Stream`](futures::Stream).
///
/// On startup the consumer optionally creates the consumer group, drains
/// pending entries (id `"0"`), and then enters a loop reading new entries
/// (id `">"`). When `auto_ack` is enabled each message is acknowledged when
/// the caller polls the stream again after receiving it. Dropping the stream
/// before that next poll leaves the message pending.
pub struct StreamConsumer {
    group: CommandArg,
    consumer: CommandArg,
    streams: Vec<CommandArg>,
    config: ConsumerConfig,
}

impl StreamConsumer {
    /// Create a new consumer for the given group, consumer name, and stream keys.
    pub fn new(
        group: impl Into<CommandArg>,
        consumer: impl Into<CommandArg>,
        streams: impl IntoIterator<Item = impl Into<CommandArg>>,
    ) -> Self {
        Self {
            group: group.into(),
            consumer: consumer.into(),
            streams: streams.into_iter().map(Into::into).collect(),
            config: ConsumerConfig::default(),
        }
    }

    /// Replace the default configuration.
    pub fn config(mut self, config: ConsumerConfig) -> Self {
        self.config = config;
        self
    }

    /// Start consuming. Returns a [`Stream`](futures::Stream) of [`StreamMessage`] results.
    ///
    /// The returned stream takes exclusive ownership of the connection. On
    /// startup it:
    ///
    /// 1. Creates the consumer group (if `create_group` is true), ignoring
    ///    "BUSYGROUP" errors when the group already exists.
    /// 2. Optionally runs XAUTOCLAIM to reclaim idle messages.
    /// 3. Drains all pending messages (id `"0"`) until none remain.
    /// 4. Enters a loop reading new messages (id `">"`) with the configured
    ///    COUNT and BLOCK settings.
    /// 5. If `auto_ack` is true, sends XACK when the caller advances the
    ///    stream after a yielded message.
    pub fn into_stream(
        self,
        mut conn: RedisConnection,
    ) -> impl futures::Stream<Item = Result<StreamMessage, RedisError>> {
        let group = self.group;
        let consumer = self.consumer;
        let streams = self.streams;
        let config = self.config;

        async_stream::try_stream! {
            // 1. Create consumer groups if requested.
            if config.create_group {
                for stream_key in &streams {
                    let cmd = XGroupCreate::new(stream_key, &group, "0").mkstream();
                    match conn.execute(cmd).await {
                        Ok(()) => {}
                        Err(RedisError::Redis(ref msg)) if msg.contains("BUSYGROUP") => {
                            // Group already exists -- not an error.
                        }
                        Err(e) => Err(e)?,
                    }
                }
            }

            // 2. Claim idle messages if configured.
            if let Some(idle_ms) = config.claim_idle_ms {
                for stream_key in &streams {
                    let mut start = "0-0".to_string();
                    loop {
                        let cmd = XAutoClaim::new(
                            stream_key,
                            &group,
                            &consumer,
                            idle_ms,
                            &start,
                        )
                        .count(config.batch_size);
                        let result = conn.execute(cmd).await?;

                        for entry in result.entries {
                            let msg = StreamMessage {
                                stream: stream_key.clone().into_bytes(),
                                id: entry.id.clone(),
                                fields: entry.fields,
                            };
                            let ack_stream = stream_key.clone();
                            let ack_id = msg.id.clone();
                            yield msg;
                            if config.auto_ack {
                                let ack = XAck::new(&ack_stream, &group, &ack_id);
                                conn.execute(ack).await?;
                            }
                        }

                        // "0-0" means we have scanned everything.
                        if result.next_start_id == "0-0" {
                            break;
                        }
                        start = result.next_start_id;
                    }
                }
            }

            // 3. Drain pending messages (id "0").
            loop {
                let cmd = build_xreadgroup(&group, &consumer, &streams, "0", config.batch_size, None);
                let response = conn.execute(cmd).await?;

                let mut got_any = false;
                for (stream_key, entries) in response {
                    for entry in entries {
                        got_any = true;
                        let msg = StreamMessage {
                            stream: stream_key.clone(),
                            id: entry.id.clone(),
                            fields: entry.fields,
                        };
                        let ack_stream = stream_key.clone();
                        let ack_id = msg.id.clone();
                        yield msg;
                        if config.auto_ack {
                            let ack = XAck::new(&ack_stream, &group, &ack_id);
                            conn.execute(ack).await?;
                        }
                    }
                }

                if !got_any {
                    break;
                }
            }

            // 4. Main loop: read new messages (id ">").
            loop {
                let cmd = build_xreadgroup(
                    &group,
                    &consumer,
                    &streams,
                    ">",
                    config.batch_size,
                    config.block_ms,
                );
                let response = conn.execute(cmd).await?;

                for (stream_key, entries) in response {
                    for entry in entries {
                        let msg = StreamMessage {
                            stream: stream_key.clone(),
                            id: entry.id.clone(),
                            fields: entry.fields,
                        };
                        let ack_stream = stream_key.clone();
                        let ack_id = msg.id.clone();
                        yield msg;
                        if config.auto_ack {
                            let ack = XAck::new(&ack_stream, &group, &ack_id);
                            conn.execute(ack).await?;
                        }
                    }
                }
            }
        }
    }
}

/// Build an XREADGROUP command for one or more streams with the given ID.
fn build_xreadgroup(
    group: &CommandArg,
    consumer: &CommandArg,
    streams: &[CommandArg],
    id: &str,
    count: u64,
    block_ms: Option<u64>,
) -> XReadGroup {
    assert!(!streams.is_empty(), "at least one stream key is required");
    let mut cmd = XReadGroup::new(group, consumer, &streams[0]);
    for key in &streams[1..] {
        cmd = cmd.stream(key, ">");
    }
    // Override all stream IDs to the desired value.
    cmd = cmd.with_id(id).count(count);
    if let Some(ms) = block_ms {
        cmd = cmd.block(ms);
    }
    cmd
}
