use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

// ---------------------------------------------------------------------------
// MEMORY subcommands
// ---------------------------------------------------------------------------

/// MEMORY USAGE key [SAMPLES count]
///
/// Returns the number of bytes that a key and its value require to be stored
/// in RAM. Returns `None` if the key does not exist.
///
/// See: <https://redis.io/commands/memory-usage>
pub struct MemoryUsage {
    key: String,
    samples: Option<u64>,
}

impl MemoryUsage {
    /// Creates a new [`MemoryUsage`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            samples: None,
        }
    }

    /// Set the number of nested values to sample (default 5).
    #[must_use]
    pub fn samples(mut self, count: u64) -> Self {
        self.samples = Some(count);
        self
    }
}

impl Command for MemoryUsage {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MEMORY"), bulk("USAGE"), bulk(self.key.as_str())];
        if let Some(samples) = self.samples {
            args.push(bulk("SAMPLES"));
            args.push(bulk(samples.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(Some(n)),
            Frame::Null => Ok(None),
            Frame::BulkString(None) => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MEMORY USAGE"
    }
}

/// MEMORY DOCTOR
///
/// Returns a diagnostic report about memory issues the server may have.
///
/// See: <https://redis.io/commands/memory-doctor>
pub struct MemoryDoctor;

impl MemoryDoctor {
    /// Creates a new [`MemoryDoctor`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryDoctor {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryDoctor {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MEMORY"), bulk("DOCTOR")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(s)) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MEMORY DOCTOR"
    }
}

/// MEMORY STATS
///
/// Returns detailed memory consumption statistics as a complex nested
/// key-value response.
///
/// See: <https://redis.io/commands/memory-stats>
pub struct MemoryStats;

impl MemoryStats {
    /// Creates a new [`MemoryStats`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryStats {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MEMORY"), bulk("STATS")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "MEMORY STATS"
    }
}

// ---------------------------------------------------------------------------
// SLOWLOG subcommands
// ---------------------------------------------------------------------------

/// SLOWLOG GET \[count\]
///
/// Returns entries from the slow log. Each entry is an array containing
/// the log id, timestamp, execution time, command array, client info, etc.
///
/// See: <https://redis.io/commands/slowlog-get>
pub struct SlowlogGet {
    count: Option<u64>,
}

impl SlowlogGet {
    /// Creates a new [`SlowlogGet`] command returning all slow log entries.
    pub fn new() -> Self {
        Self { count: None }
    }

    /// Return at most `count` slow log entries.
    pub fn count(count: u64) -> Self {
        Self { count: Some(count) }
    }
}

impl Default for SlowlogGet {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogGet {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SLOWLOG"), bulk("GET")];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "SLOWLOG GET"
    }
}

/// SLOWLOG LEN
///
/// Returns the number of entries in the slow log.
///
/// See: <https://redis.io/commands/slowlog-len>
pub struct SlowlogLen;

impl SlowlogLen {
    /// Creates a new [`SlowlogLen`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlowlogLen {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SLOWLOG"), bulk("LEN")])
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
        "SLOWLOG LEN"
    }
}

/// SLOWLOG RESET
///
/// Clears all entries from the slow log.
///
/// See: <https://redis.io/commands/slowlog-reset>
pub struct SlowlogReset;

impl SlowlogReset {
    /// Creates a new [`SlowlogReset`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlowlogReset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SLOWLOG"), bulk("RESET")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SLOWLOG RESET"
    }
}

// ---------------------------------------------------------------------------
// LATENCY subcommands
// ---------------------------------------------------------------------------

/// LATENCY LATEST
///
/// Returns the latest latency samples for all monitored events. Each entry
/// is an array of [event-name, timestamp, latest-latency-ms, max-latency-ms].
///
/// See: <https://redis.io/commands/latency-latest>
pub struct LatencyLatest;

impl LatencyLatest {
    /// Creates a new [`LatencyLatest`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for LatencyLatest {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LatencyLatest {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LATENCY"), bulk("LATEST")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "LATENCY LATEST"
    }
}

/// LATENCY HISTORY event
///
/// Returns latency time-series data for the specified event. Each entry
/// is an array of [timestamp, latency-ms].
///
/// See: <https://redis.io/commands/latency-history>
pub struct LatencyHistory {
    event: String,
}

impl LatencyHistory {
    /// Creates a new [`LatencyHistory`] command.
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
        }
    }
}

impl Command for LatencyHistory {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LATENCY"),
            bulk("HISTORY"),
            bulk(self.event.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "LATENCY HISTORY"
    }
}

/// LATENCY RESET [event ...]
///
/// Resets latency data for the specified events, or all events if none given.
/// Returns the number of events that were reset.
///
/// See: <https://redis.io/commands/latency-reset>
pub struct LatencyReset {
    events: Vec<String>,
}

impl LatencyReset {
    /// Creates a new [`LatencyReset`] command resetting all latency events.
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Reset a specific latency event.
    #[must_use]
    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.events.push(event.into());
        self
    }

    /// Reset multiple latency events.
    #[must_use]
    pub fn events(mut self, events: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.events.extend(events.into_iter().map(Into::into));
        self
    }
}

impl Default for LatencyReset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LatencyReset {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("LATENCY"), bulk("RESET")];
        for event in &self.events {
            args.push(bulk(event.as_str()));
        }
        array(args)
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
        "LATENCY RESET"
    }
}
