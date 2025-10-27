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
