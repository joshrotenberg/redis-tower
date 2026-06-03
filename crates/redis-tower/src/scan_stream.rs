//! Async stream wrappers for SCAN-family cursor iteration.
//!
//! [`ScanStream`] handles cursor pagination automatically -- just consume
//! items from the returned stream until it ends.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::ScanStream;
//! use tokio_stream::StreamExt;
//!
//! let mut stream = ScanStream::scan(&mut conn, "user:*");
//! while let Some(key) = stream.next().await {
//!     let key = key?;
//!     println!("Found: {}", String::from_utf8_lossy(&key));
//! }
//! ```

use bytes::Bytes;
use futures::Stream;
use redis_tower_commands::{HScan, SScan, Scan, ZScan};
use redis_tower_core::{RedisConnection, RedisError};

/// Async stream wrappers for SCAN-family cursor iteration.
///
/// Each method returns an `impl Stream` that drives cursor pagination
/// internally. The stream borrows `&mut RedisConnection`, so it cannot
/// be `'static` -- this is intentional, since each iteration requires
/// exclusive access to the connection.
pub struct ScanStream;

impl ScanStream {
    /// Iterate over all keys matching a pattern via SCAN.
    ///
    /// Yields one `Bytes` key per item. The stream ends when Redis
    /// returns cursor "0".
    ///
    /// # Example
    ///
    /// ```ignore
    /// use redis_tower::ScanStream;
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = ScanStream::scan(&mut conn, "user:*");
    /// while let Some(key) = stream.next().await {
    ///     let key = key?;
    ///     println!("{}", String::from_utf8_lossy(&key));
    /// }
    /// ```
    pub fn scan<'a>(
        conn: &'a mut RedisConnection,
        pattern: impl Into<String>,
    ) -> impl Stream<Item = Result<Bytes, RedisError>> + 'a {
        let pattern = pattern.into();
        async_stream::try_stream! {
            let mut cursor = "0".to_string();
            loop {
                let result = conn.execute(
                    Scan::new().match_pattern(&pattern).cursor(&cursor)
                ).await?;
                cursor = result.cursor;
                for key in result.results {
                    let item: Bytes = key;
                    yield item;
                }
                if cursor == "0" {
                    break;
                }
            }
        }
    }

    /// Iterate over all keys matching a pattern via SCAN with a count hint.
    ///
    /// The `count` parameter hints to Redis how many elements to return
    /// per iteration. Redis may return more or fewer.
    pub fn scan_with_count<'a>(
        conn: &'a mut RedisConnection,
        pattern: impl Into<String>,
        count: u64,
    ) -> impl Stream<Item = Result<Bytes, RedisError>> + 'a {
        let pattern = pattern.into();
        async_stream::try_stream! {
            let mut cursor = "0".to_string();
            loop {
                let result = conn.execute(
                    Scan::new()
                        .match_pattern(&pattern)
                        .cursor(&cursor)
                        .count(count)
                ).await?;
                cursor = result.cursor;
                for key in result.results {
                    let item: Bytes = key;
                    yield item;
                }
                if cursor == "0" {
                    break;
                }
            }
        }
    }

    /// Iterate over hash fields via HSCAN.
    ///
    /// Yields `(field, value)` pairs as `(Bytes, Bytes)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use redis_tower::ScanStream;
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = ScanStream::hscan(&mut conn, "myhash", "*");
    /// while let Some(pair) = stream.next().await {
    ///     let (field, value) = pair?;
    ///     println!("{}: {}", String::from_utf8_lossy(&field), String::from_utf8_lossy(&value));
    /// }
    /// ```
    pub fn hscan<'a>(
        conn: &'a mut RedisConnection,
        key: impl Into<String>,
        pattern: impl Into<String>,
    ) -> impl Stream<Item = Result<(Bytes, Bytes), RedisError>> + 'a {
        let key = key.into();
        let pattern = pattern.into();
        async_stream::try_stream! {
            let mut cursor = "0".to_string();
            loop {
                let result = conn.execute(
                    HScan::new(&key).match_pattern(&pattern).cursor(&cursor)
                ).await?;
                cursor = result.cursor;
                for pair in result.results {
                    let item: (Bytes, Bytes) = pair;
                    yield item;
                }
                if cursor == "0" {
                    break;
                }
            }
        }
    }

    /// Iterate over set members via SSCAN.
    ///
    /// Yields one `Bytes` member per item.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use redis_tower::ScanStream;
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = ScanStream::sscan(&mut conn, "myset", "*");
    /// while let Some(member) = stream.next().await {
    ///     let member = member?;
    ///     println!("{}", String::from_utf8_lossy(&member));
    /// }
    /// ```
    pub fn sscan<'a>(
        conn: &'a mut RedisConnection,
        key: impl Into<String>,
        pattern: impl Into<String>,
    ) -> impl Stream<Item = Result<Bytes, RedisError>> + 'a {
        let key = key.into();
        let pattern = pattern.into();
        async_stream::try_stream! {
            let mut cursor = "0".to_string();
            loop {
                let result = conn.execute(
                    SScan::new(&key).match_pattern(&pattern).cursor(&cursor)
                ).await?;
                cursor = result.cursor;
                for member in result.results {
                    let item: Bytes = member;
                    yield item;
                }
                if cursor == "0" {
                    break;
                }
            }
        }
    }

    /// Iterate over sorted set members via ZSCAN.
    ///
    /// Yields `(member, score)` pairs as `(Bytes, f64)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use redis_tower::ScanStream;
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = ScanStream::zscan(&mut conn, "myzset", "*");
    /// while let Some(entry) = stream.next().await {
    ///     let (member, score) = entry?;
    ///     println!("{}: {}", String::from_utf8_lossy(&member), score);
    /// }
    /// ```
    pub fn zscan<'a>(
        conn: &'a mut RedisConnection,
        key: impl Into<String>,
        pattern: impl Into<String>,
    ) -> impl Stream<Item = Result<(Bytes, f64), RedisError>> + 'a {
        let key = key.into();
        let pattern = pattern.into();
        async_stream::try_stream! {
            let mut cursor = "0".to_string();
            loop {
                let result = conn.execute(
                    ZScan::new(&key).match_pattern(&pattern).cursor(&cursor)
                ).await?;
                cursor = result.cursor;
                for entry in result.results {
                    let item: (Bytes, f64) = entry;
                    yield item;
                }
                if cursor == "0" {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time check that the returned streams satisfy the Stream trait
    /// with the expected Item types. This function is never called -- it only
    /// needs to compile.
    #[allow(dead_code, unused_variables)]
    fn assert_stream_types(conn: &mut RedisConnection) {
        fn assert_stream<S: Stream>(_s: S) {}

        let s = ScanStream::scan(conn, "*");
        assert_stream::<_>(s);
        let owned = String::from("user:*");
        let s2 = ScanStream::scan(conn, owned);
        assert_stream::<_>(s2);
    }
}
