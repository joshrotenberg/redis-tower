//! Streaming wrappers for SCAN commands
//!
//! Provides async iterators that automatically handle cursor-based iteration
//! for SCAN, HSCAN, SSCAN, and ZSCAN commands.
//!
//! # Examples
//!
//! ```rust,no_run
//! use redis_tower::{RedisClient, streaming::ScanStream};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//! let mut stream = ScanStream::new(client).pattern("user:*").count(100);
//!
//! while let Some(keys) = stream.next().await? {
//!     for key in keys {
//!         println!("Key: {:?}", key);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::RedisError;
use crate::client::RedisClient;
use crate::commands::scan::{HScan, SScan, Scan, ZScan};
use crate::commands::streams::{StreamEntry, XAck, XRead, XReadGroup};
use bytes::Bytes;

/// Streaming iterator for SCAN command
///
/// Automatically handles cursor-based iteration over all keys in the database.
pub struct ScanStream {
    client: RedisClient,
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    done: bool,
}

impl ScanStream {
    /// Create a new SCAN stream
    pub fn new(client: RedisClient) -> Self {
        Self {
            client,
            cursor: 0,
            pattern: None,
            count: None,
            done: false,
        }
    }

    /// Set the MATCH pattern for filtering keys
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of keys to return per iteration
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Get the next batch of keys
    ///
    /// Returns `Ok(None)` when iteration is complete.
    pub async fn next(&mut self) -> Result<Option<Vec<Bytes>>, RedisError> {
        if self.done {
            return Ok(None);
        }

        let mut scan = Scan::new(self.cursor);
        if let Some(pattern) = &self.pattern {
            scan = scan.pattern(pattern.clone());
        }
        if let Some(count) = self.count {
            scan = scan.count(count);
        }

        let result = self.client.call(scan).await?;
        self.cursor = result.cursor;

        if result.cursor == 0 {
            self.done = true;
        }

        if result.keys.is_empty() && !self.done {
            // Empty batch but not done - get next batch recursively
            Box::pin(self.next()).await
        } else if result.keys.is_empty() {
            // Empty batch and done
            Ok(None)
        } else {
            Ok(Some(result.keys))
        }
    }

    /// Reset the stream to start from the beginning
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.done = false;
    }
}

/// Streaming iterator for Redis Streams (XREAD)
///
/// Continuously polls a Redis Stream for new entries, automatically tracking
/// the last seen entry ID. Supports blocking mode for efficient real-time streaming.
///
/// # Examples
///
/// ```rust,no_run
/// use redis_tower::{RedisClient, streaming::XReadStream};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Stream new entries as they arrive
/// let mut stream = XReadStream::new(client, "events")
///     .start_from("$")  // $ = only new entries
///     .count(10)
///     .block(5000);  // Block up to 5 seconds waiting for entries
///
/// while let Some(entries) = stream.next().await? {
///     for entry in entries {
///         println!("Event {}: {:?}", entry.id, entry.fields);
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct XReadStream {
    client: RedisClient,
    stream_key: String,
    last_id: String,
    count: Option<i64>,
    block: Option<i64>,
}

impl XReadStream {
    /// Create a new XREAD stream
    ///
    /// # Arguments
    ///
    /// * `client` - Redis client to use for commands
    /// * `stream_key` - The stream key to read from
    pub fn new(client: RedisClient, stream_key: impl Into<String>) -> Self {
        Self {
            client,
            stream_key: stream_key.into(),
            last_id: "$".to_string(), // Default to only new entries
            count: None,
            block: None,
        }
    }

    /// Set the starting ID for reading entries
    ///
    /// * `"$"` - Only new entries (default)
    /// * `"0-0"` - All entries from the beginning
    /// * `"<id>"` - Start from specific entry ID
    pub fn start_from(mut self, id: impl Into<String>) -> Self {
        self.last_id = id.into();
        self
    }

    /// Set the maximum number of entries to return per call
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Set the blocking timeout in milliseconds
    ///
    /// * `0` - Block forever until entries arrive
    /// * `n > 0` - Block for up to n milliseconds
    /// * `None` - Non-blocking (default)
    pub fn block(mut self, milliseconds: i64) -> Self {
        self.block = Some(milliseconds);
        self
    }

    /// Get the next batch of stream entries
    ///
    /// Returns `Ok(None)` when blocking timeout expires with no new entries.
    pub async fn next(&mut self) -> Result<Option<Vec<StreamEntry>>, RedisError> {
        let mut xread = XRead::new().stream(&self.stream_key, &self.last_id);

        if let Some(count) = self.count {
            xread = xread.count(count);
        }

        if let Some(block) = self.block {
            xread = xread.block(block);
        }

        let results = self.client.call(xread).await?;

        // XREAD returns Vec<StreamEntries>, we expect one for our single stream
        if let Some(stream_entries) = results.first() {
            if stream_entries.entries.is_empty() {
                return Ok(None);
            }

            // Update last_id to the last entry we received
            if let Some(last_entry) = stream_entries.entries.last() {
                self.last_id = last_entry.id.clone();
            }

            Ok(Some(stream_entries.entries.clone()))
        } else {
            Ok(None)
        }
    }

    /// Reset the stream to start from a specific ID
    pub fn reset(&mut self, id: impl Into<String>) {
        self.last_id = id.into();
    }

    /// Get the current last seen entry ID
    pub fn last_id(&self) -> &str {
        &self.last_id
    }
}

/// Streaming iterator for Redis Streams with Consumer Groups (XREADGROUP)
///
/// Continuously polls a Redis Stream using consumer groups for distributed
/// processing. Supports automatic acknowledgment of processed messages.
///
/// # Examples
///
/// ```rust,no_run
/// use redis_tower::{RedisClient, streaming::XReadGroupStream};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Create consumer group first (one-time setup)
/// // XGROUP CREATE events mygroup $ MKSTREAM
///
/// // Stream entries with automatic acknowledgment
/// let mut stream = XReadGroupStream::new(
///     client,
///     "events",
///     "mygroup",
///     "consumer1"
/// )
/// .count(10)
/// .block(5000)
/// .auto_ack(true);  // Automatically ACK after receiving
///
/// while let Some(entries) = stream.next().await? {
///     for entry in entries {
///         println!("Processing event {}: {:?}", entry.id, entry.fields);
///         // Entry is automatically ACKed if auto_ack is true
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct XReadGroupStream {
    client: RedisClient,
    stream_key: String,
    group: String,
    consumer: String,
    count: Option<i64>,
    block: Option<i64>,
    noack: bool,
    auto_ack: bool,
}

impl XReadGroupStream {
    /// Create a new XREADGROUP stream
    ///
    /// # Arguments
    ///
    /// * `client` - Redis client to use for commands
    /// * `stream_key` - The stream key to read from
    /// * `group` - Consumer group name
    /// * `consumer` - Consumer name within the group
    pub fn new(
        client: RedisClient,
        stream_key: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Self {
        Self {
            client,
            stream_key: stream_key.into(),
            group: group.into(),
            consumer: consumer.into(),
            count: None,
            block: None,
            noack: false,
            auto_ack: false,
        }
    }

    /// Set the maximum number of entries to return per call
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Set the blocking timeout in milliseconds
    ///
    /// * `0` - Block forever until entries arrive
    /// * `n > 0` - Block for up to n milliseconds
    /// * `None` - Non-blocking (default)
    pub fn block(mut self, milliseconds: i64) -> Self {
        self.block = Some(milliseconds);
        self
    }

    /// Don't automatically add entries to the consumer's pending entries list
    ///
    /// Use this when you don't need delivery guarantees and want better performance.
    pub fn noack(mut self) -> Self {
        self.noack = true;
        self
    }

    /// Automatically acknowledge (ACK) entries after receiving them
    ///
    /// When enabled, entries are acknowledged immediately after being returned
    /// from `next()`. This is convenient but means you can't retry failed processing.
    pub fn auto_ack(mut self, enabled: bool) -> Self {
        self.auto_ack = enabled;
        self
    }

    /// Get the next batch of stream entries
    ///
    /// Returns `Ok(None)` when blocking timeout expires with no new entries.
    pub async fn next(&mut self) -> Result<Option<Vec<StreamEntry>>, RedisError> {
        let mut xreadgroup =
            XReadGroup::new(&self.group, &self.consumer).stream(&self.stream_key, ">"); // > = new messages not yet delivered

        if let Some(count) = self.count {
            xreadgroup = xreadgroup.count(count);
        }

        if let Some(block) = self.block {
            xreadgroup = xreadgroup.block(block);
        }

        if self.noack {
            xreadgroup = xreadgroup.noack();
        }

        let results = self.client.call(xreadgroup).await?;

        // XREADGROUP returns Vec<StreamEntries>, we expect one for our single stream
        if let Some(stream_entries) = results.first() {
            if stream_entries.entries.is_empty() {
                return Ok(None);
            }

            // Auto-acknowledge if enabled
            if self.auto_ack && !self.noack {
                let ids: Vec<String> = stream_entries
                    .entries
                    .iter()
                    .map(|e| e.id.clone())
                    .collect();

                let mut xack = XAck::new(&self.stream_key, &self.group);
                for id in ids {
                    xack = xack.id(id);
                }

                // Fire and forget - we don't care about the ACK result
                let _ = self.client.call(xack).await;
            }

            Ok(Some(stream_entries.entries.clone()))
        } else {
            Ok(None)
        }
    }

    /// Get the consumer group name
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Get the consumer name
    pub fn consumer(&self) -> &str {
        &self.consumer
    }
}

/// Streaming iterator for HSCAN command
///
/// Automatically handles cursor-based iteration over hash fields.
pub struct HScanStream {
    client: RedisClient,
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    done: bool,
}

impl HScanStream {
    /// Create a new HSCAN stream
    pub fn new(client: RedisClient, key: impl Into<String>) -> Self {
        Self {
            client,
            key: key.into(),
            cursor: 0,
            pattern: None,
            count: None,
            done: false,
        }
    }

    /// Set the MATCH pattern for filtering fields
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of fields to return per iteration
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Get the next batch of field-value pairs
    ///
    /// Returns `Ok(None)` when iteration is complete.
    pub async fn next(&mut self) -> Result<Option<Vec<(Bytes, Bytes)>>, RedisError> {
        if self.done {
            return Ok(None);
        }

        let mut hscan = HScan::new(&self.key, self.cursor);
        if let Some(pattern) = &self.pattern {
            hscan = hscan.pattern(pattern.clone());
        }
        if let Some(count) = self.count {
            hscan = hscan.count(count);
        }

        let result = self.client.call(hscan).await?;
        self.cursor = result.cursor;

        if result.cursor == 0 {
            self.done = true;
        }

        if result.fields.is_empty() && !self.done {
            Box::pin(self.next()).await
        } else if result.fields.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result.fields))
        }
    }

    /// Reset the stream to start from the beginning
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.done = false;
    }
}

/// Streaming iterator for SSCAN command
///
/// Automatically handles cursor-based iteration over set members.
pub struct SScanStream {
    client: RedisClient,
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    done: bool,
}

impl SScanStream {
    /// Create a new SSCAN stream
    pub fn new(client: RedisClient, key: impl Into<String>) -> Self {
        Self {
            client,
            key: key.into(),
            cursor: 0,
            pattern: None,
            count: None,
            done: false,
        }
    }

    /// Set the MATCH pattern for filtering members
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of members to return per iteration
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Get the next batch of members
    ///
    /// Returns `Ok(None)` when iteration is complete.
    pub async fn next(&mut self) -> Result<Option<Vec<Bytes>>, RedisError> {
        if self.done {
            return Ok(None);
        }

        let mut sscan = SScan::new(&self.key, self.cursor);
        if let Some(pattern) = &self.pattern {
            sscan = sscan.pattern(pattern.clone());
        }
        if let Some(count) = self.count {
            sscan = sscan.count(count);
        }

        let result = self.client.call(sscan).await?;
        self.cursor = result.cursor;

        if result.cursor == 0 {
            self.done = true;
        }

        if result.members.is_empty() && !self.done {
            Box::pin(self.next()).await
        } else if result.members.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result.members))
        }
    }

    /// Reset the stream to start from the beginning
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.done = false;
    }
}

/// Streaming iterator for ZSCAN command
///
/// Automatically handles cursor-based iteration over sorted set members and scores.
pub struct ZScanStream {
    client: RedisClient,
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    done: bool,
}

impl ZScanStream {
    /// Create a new ZSCAN stream
    pub fn new(client: RedisClient, key: impl Into<String>) -> Self {
        Self {
            client,
            key: key.into(),
            cursor: 0,
            pattern: None,
            count: None,
            done: false,
        }
    }

    /// Set the MATCH pattern for filtering members
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of members to return per iteration
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Get the next batch of member-score pairs
    ///
    /// Returns `Ok(None)` when iteration is complete.
    pub async fn next(&mut self) -> Result<Option<Vec<(Bytes, f64)>>, RedisError> {
        if self.done {
            return Ok(None);
        }

        let mut zscan = ZScan::new(&self.key, self.cursor);
        if let Some(pattern) = &self.pattern {
            zscan = zscan.pattern(pattern.clone());
        }
        if let Some(count) = self.count {
            zscan = zscan.count(count);
        }

        let result = self.client.call(zscan).await?;
        self.cursor = result.cursor;

        if result.cursor == 0 {
            self.done = true;
        }

        if result.members.is_empty() && !self.done {
            Box::pin(self.next()).await
        } else if result.members.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result.members))
        }
    }

    /// Reset the stream to start from the beginning
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.done = false;
    }
}
