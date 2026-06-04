//! # TimeSeries Client
//!
//! Ergonomic client over RedisTimeSeries. Samples are exchanged as
//! `(timestamp, value)` pairs rather than raw [`Frame`](redis_tower::Frame)
//! replies.
#![allow(unused_variables, dead_code)]

use redis_tower_core::RedisError;

/// High-level client for RedisTimeSeries operations.
///
/// Wraps any underlying executor `C` and exposes typed sample append and
/// range-query operations.
#[derive(Debug, Clone)]
pub struct TimeSeriesClient<C> {
    client: C,
}

impl<C> TimeSeriesClient<C> {
    /// Create a new [`TimeSeriesClient`] wrapping the given executor.
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Append a sample to a time series.
    pub async fn add(
        &mut self,
        key: impl AsRef<str>,
        timestamp: i64,
        value: f64,
    ) -> Result<i64, RedisError> {
        todo!()
    }

    /// Get the last sample in a time series.
    pub async fn get(&mut self, key: impl AsRef<str>) -> Result<Option<(i64, f64)>, RedisError> {
        todo!()
    }

    /// Query a range of samples from a time series.
    pub async fn range(
        &mut self,
        key: impl AsRef<str>,
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, f64)>, RedisError> {
        todo!()
    }

    /// Create a new time series key.
    pub async fn create(&mut self, key: impl AsRef<str>) -> Result<(), RedisError> {
        todo!()
    }
}
