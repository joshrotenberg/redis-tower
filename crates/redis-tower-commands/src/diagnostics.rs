use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

// ---------------------------------------------------------------------------
// MEMORY subcommands
// ---------------------------------------------------------------------------

/// MEMORY USAGE key [SAMPLES count]
///
/// Returns the number of bytes that a key and its value require to be stored
/// in RAM. Returns `None` if the key does not exist.
#[derive(Clone)]
pub struct MemoryUsage {
    key: String,
    samples: Option<u64>,
}

impl MemoryUsage {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            samples: None,
        }
    }

    /// Set the number of nested values to sample (default 5).
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
#[derive(Clone)]
pub struct MemoryDoctor;

impl MemoryDoctor {
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
            // RESP3 returns MEMORY DOCTOR as a verbatim string.
            Frame::BulkString(Some(s)) | Frame::VerbatimString(_, s) => {
                Ok(String::from_utf8_lossy(&s).into_owned())
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk or verbatim string",
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
#[derive(Clone)]
pub struct MemoryStats;

impl MemoryStats {
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
#[derive(Clone)]
pub struct SlowlogGet {
    count: Option<u64>,
}

impl SlowlogGet {
    /// Return all slow log entries.
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
#[derive(Clone)]
pub struct SlowlogLen;

impl SlowlogLen {
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
#[derive(Clone)]
pub struct SlowlogReset;

impl SlowlogReset {
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
#[derive(Clone)]
pub struct LatencyLatest;

impl LatencyLatest {
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
#[derive(Clone)]
pub struct LatencyHistory {
    event: String,
}

impl LatencyHistory {
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
#[derive(Clone)]
pub struct LatencyReset {
    events: Vec<String>,
}

impl LatencyReset {
    /// Reset all latency events.
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Reset a specific latency event.
    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.events.push(event.into());
        self
    }

    /// Reset multiple latency events.
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

/// LATENCY GRAPH event
///
/// Returns a latency graph for the given event, rendered as ASCII art text.
#[derive(Clone)]
pub struct LatencyGraph {
    event: String,
}

impl LatencyGraph {
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
        }
    }
}

impl Command for LatencyGraph {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LATENCY"),
            bulk("GRAPH"),
            bulk(self.event.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP3 may return the graph as a verbatim string.
            Frame::BulkString(Some(s)) | Frame::VerbatimString(_, s) => {
                Ok(String::from_utf8_lossy(&s).into_owned())
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk or verbatim string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LATENCY GRAPH"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// HELP subcommands
// ---------------------------------------------------------------------------

/// MEMORY HELP
///
/// Returns helpful text describing the MEMORY subcommands.
#[derive(Clone)]
pub struct MemoryHelp;

impl MemoryHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MEMORY"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "MEMORY HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// SLOWLOG HELP
///
/// Returns helpful text describing the SLOWLOG subcommands.
#[derive(Clone)]
pub struct SlowlogHelp;

impl SlowlogHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlowlogHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SLOWLOG"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "SLOWLOG HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LATENCY HELP
///
/// Returns helpful text describing the LATENCY subcommands.
#[derive(Clone)]
pub struct LatencyHelp;

impl LatencyHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LatencyHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LatencyHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LATENCY"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "LATENCY HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// DEBUG HELP
///
/// Returns helpful text describing the DEBUG subcommands.
#[derive(Clone)]
pub struct DebugHelp;

impl DebugHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DebugHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for DebugHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("DEBUG"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "DEBUG HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn memory_doctor_parse_bulk_string() {
        let cmd = MemoryDoctor::new();
        let frame = Frame::BulkString(Some(Bytes::from("Sam, I detected a few issues")));
        let out = cmd.parse_response(frame).unwrap();
        assert!(out.contains("issues"));
    }

    #[test]
    fn memory_doctor_parse_verbatim_string_resp3() {
        // Under RESP3 MEMORY DOCTOR comes back as a verbatim string.
        let cmd = MemoryDoctor::new();
        let frame = Frame::VerbatimString(
            Bytes::from("txt"),
            Bytes::from("Sam, I detected a few issues"),
        );
        let out = cmd.parse_response(frame).unwrap();
        assert!(out.contains("issues"));
    }

    #[test]
    fn latency_graph_to_frame() {
        let cmd = LatencyGraph::new("command");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LATENCY"), bulk("GRAPH"), bulk("command")])
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn latency_graph_parse_string() {
        let cmd = LatencyGraph::new("command");
        let out = cmd
            .parse_response(Frame::BulkString(Some(Bytes::from("command - high . low"))))
            .unwrap();
        assert!(out.contains("command"));
    }

    #[test]
    fn help_subcommands_to_frame() {
        assert_eq!(
            MemoryHelp::new().to_frame(),
            array(vec![bulk("MEMORY"), bulk("HELP")])
        );
        assert_eq!(
            SlowlogHelp::new().to_frame(),
            array(vec![bulk("SLOWLOG"), bulk("HELP")])
        );
        assert_eq!(
            LatencyHelp::new().to_frame(),
            array(vec![bulk("LATENCY"), bulk("HELP")])
        );
        assert_eq!(
            DebugHelp::new().to_frame(),
            array(vec![bulk("DEBUG"), bulk("HELP")])
        );
        assert!(MemoryHelp::new().idempotent());
    }

    #[test]
    fn memory_help_parse_lines() {
        let cmd = MemoryHelp::new();
        let reply = array(vec![bulk("MEMORY <subcommand>"), bulk("USAGE <key>")]);
        let lines = cmd.parse_response(reply).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(&lines[1][..], b"USAGE <key>");
    }
}
