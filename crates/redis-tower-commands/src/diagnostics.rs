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

/// MEMORY PURGE
///
/// Asks the configured allocator to release memory that it can return to the
/// operating system.
#[derive(Debug, Clone)]
pub struct MemoryPurge;

impl MemoryPurge {
    /// Create a `MEMORY PURGE` request.
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryPurge {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryPurge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MEMORY"), bulk("PURGE")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(value) if value.eq_ignore_ascii_case(b"OK") => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MEMORY PURGE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// MEMORY MALLOC-STATS
///
/// Returns the allocator's implementation-specific statistics as raw bytes.
#[derive(Debug, Clone)]
pub struct MemoryMallocStats;

impl MemoryMallocStats {
    /// Create a `MEMORY MALLOC-STATS` request.
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryMallocStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryMallocStats {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MEMORY"), bulk("MALLOC-STATS")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(value))
            | Frame::SimpleString(value)
            | Frame::VerbatimString(_, value) => Ok(value),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk, simple, or verbatim string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MEMORY MALLOC-STATS"
    }

    fn idempotent(&self) -> bool {
        true
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

/// LATENCY HISTOGRAM \[command-name \[command-name ...\]\]
///
/// Returns cumulative command-latency histograms. The nested response is kept
/// as a raw frame because RESP2 uses alternating arrays while RESP3 uses maps.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    commands: Vec<Bytes>,
}

impl LatencyHistogram {
    /// Request histograms for every command that has latency data.
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
        }
    }

    /// Add one binary-safe command name to the filter.
    pub fn command(mut self, command: impl AsRef<[u8]>) -> Self {
        self.commands.push(Bytes::copy_from_slice(command.as_ref()));
        self
    }

    /// Add multiple binary-safe command names to the filter.
    pub fn commands<C, I>(mut self, commands: I) -> Self
    where
        C: AsRef<[u8]>,
        I: IntoIterator<Item = C>,
    {
        self.commands.extend(
            commands
                .into_iter()
                .map(|command| Bytes::copy_from_slice(command.as_ref())),
        );
        self
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LatencyHistogram {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("LATENCY"), bulk("HISTOGRAM")];
        args.extend(self.commands.iter().map(bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            frame @ (Frame::Array(Some(_)) | Frame::Map(_)) => Ok(frame),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LATENCY HISTOGRAM"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LATENCY DOCTOR
///
/// Returns Redis's human-readable latency analysis report.
#[derive(Debug, Clone)]
pub struct LatencyDoctor;

impl LatencyDoctor {
    /// Create a `LATENCY DOCTOR` request.
    pub fn new() -> Self {
        Self
    }
}

impl Default for LatencyDoctor {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LatencyDoctor {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LATENCY"), bulk("DOCTOR")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(value))
            | Frame::SimpleString(value)
            | Frame::VerbatimString(_, value) => Ok(String::from_utf8_lossy(&value).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk, simple, or verbatim string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LATENCY DOCTOR"
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
    fn memory_purge_to_frame_and_parse_ok() {
        let cmd = MemoryPurge::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("MEMORY"), bulk("PURGE")]));
        assert_eq!(cmd.name(), "MEMORY PURGE");
        assert!(cmd.idempotent());
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn memory_malloc_stats_accepts_resp2_and_resp3_text() {
        let cmd = MemoryMallocStats::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("MEMORY"), bulk("MALLOC-STATS")])
        );
        assert_eq!(cmd.name(), "MEMORY MALLOC-STATS");
        assert!(cmd.idempotent());
        assert_eq!(
            cmd.parse_response(Frame::BulkString(Some(Bytes::from("allocator stats"))))
                .unwrap(),
            Bytes::from("allocator stats")
        );
        assert_eq!(
            cmd.parse_response(Frame::VerbatimString(
                Bytes::from("txt"),
                Bytes::from("resp3 stats")
            ))
            .unwrap(),
            Bytes::from("resp3 stats")
        );
        assert!(cmd.parse_response(Frame::Null).is_err());
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
    fn latency_histogram_serializes_binary_safe_filters() {
        let cmd = LatencyHistogram::new()
            .command(b"GET")
            .commands([b"SET".as_slice(), b"custom\0command".as_slice()]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LATENCY"),
                bulk("HISTOGRAM"),
                bulk("GET"),
                bulk("SET"),
                bulk(b"custom\0command"),
            ])
        );
        assert_eq!(cmd.name(), "LATENCY HISTOGRAM");
        assert!(cmd.idempotent());
    }

    #[test]
    fn latency_histogram_preserves_resp2_and_resp3_shapes() {
        let cmd = LatencyHistogram::new();
        let resp2 = array(vec![bulk("get"), array(vec![])]);
        assert_eq!(cmd.parse_response(resp2.clone()).unwrap(), resp2);
        let resp3 = Frame::Map(vec![(bulk("get"), Frame::Map(vec![]))]);
        assert_eq!(cmd.parse_response(resp3.clone()).unwrap(), resp3);
        assert!(cmd.parse_response(Frame::Null).is_err());
    }

    #[test]
    fn latency_doctor_accepts_resp2_and_resp3_text() {
        let cmd = LatencyDoctor::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("LATENCY"), bulk("DOCTOR")]));
        assert_eq!(cmd.name(), "LATENCY DOCTOR");
        assert!(cmd.idempotent());
        assert_eq!(
            cmd.parse_response(Frame::BulkString(Some(Bytes::from("diagnosis"))))
                .unwrap(),
            "diagnosis"
        );
        assert_eq!(
            cmd.parse_response(Frame::VerbatimString(
                Bytes::from("txt"),
                Bytes::from("healthy")
            ))
            .unwrap(),
            "healthy"
        );
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
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
