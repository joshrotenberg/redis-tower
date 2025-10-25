//! Latency monitoring commands
//!
//! Commands for monitoring and analyzing Redis latency events.
//!
//! Available since Redis 2.8.13

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// LATENCY DOCTOR - Return a latency analysis report
///
/// Analyzes the latency spikes and provides human-readable analysis
/// and advice about possible causes and solutions.
///
/// Available since Redis 2.8.13
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyDoctor;
///
/// let cmd = LatencyDoctor;
/// // Returns detailed latency analysis report
/// ```
#[derive(Debug, Clone, Copy)]
pub struct LatencyDoctor;

impl Command for LatencyDoctor {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("DOCTOR"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::SimpleString(data) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LATENCY GRAPH - Return an ASCII-art style graph of a latency event
///
/// Produces an ASCII-art style graph for the specified event.
///
/// Available since Redis 2.8.13
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyGraph;
///
/// let cmd = LatencyGraph::new("command");
/// // Returns ASCII graph of latency spikes for the command event
/// ```
#[derive(Debug, Clone)]
pub struct LatencyGraph {
    event: String,
}

impl LatencyGraph {
    /// Create a new LATENCY GRAPH command
    ///
    /// # Arguments
    /// * `event` - The latency event name (e.g., "command", "fast-command", "fork")
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
        }
    }
}

impl Command for LatencyGraph {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("GRAPH"))),
            Frame::BulkString(Some(Bytes::from(self.event.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::SimpleString(data) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LATENCY HISTOGRAM - Return latency histogram distribution
///
/// Returns the cumulative distribution of latencies for specified commands.
///
/// Available since Redis 7.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyHistogram;
///
/// // Get histogram for all commands
/// let cmd = LatencyHistogram::all();
///
/// // Get histogram for specific commands
/// let cmd = LatencyHistogram::new(vec!["GET", "SET"]);
/// ```
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    commands: Vec<String>,
}

impl LatencyHistogram {
    /// Create a new LATENCY HISTOGRAM command for specific commands
    pub fn new(commands: Vec<impl Into<String>>) -> Self {
        Self {
            commands: commands.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Get histogram for all commands
    pub fn all() -> Self {
        Self {
            commands: Vec::new(),
        }
    }
}

impl Command for LatencyHistogram {
    type Response = String; // Complex nested structure, simplified for now

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("HISTOGRAM"))),
        ];

        for cmd in &self.commands {
            frames.push(Frame::BulkString(Some(Bytes::from(cmd.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // Returns complex nested array with histogram data
        Ok(format!("{:?}", frame))
    }
}

/// LATENCY HISTORY - Return time series of latency events
///
/// Returns the raw data of the latest latency spikes for the specified event.
///
/// Available since Redis 2.8.13
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyHistory;
///
/// let cmd = LatencyHistory::new("command");
/// // Returns array of [timestamp, latency_ms] pairs
/// ```
#[derive(Debug, Clone)]
pub struct LatencyHistory {
    event: String,
}

impl LatencyHistory {
    /// Create a new LATENCY HISTORY command
    ///
    /// # Arguments
    /// * `event` - The latency event name (e.g., "command", "fast-command", "fork")
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
        }
    }
}

impl Command for LatencyHistory {
    type Response = Vec<(i64, i64)>; // Vec of (timestamp, latency_ms) pairs

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("HISTORY"))),
            Frame::BulkString(Some(Bytes::from(self.event.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Array(pair) if pair.len() == 2 => {
                            let timestamp = match &pair[0] {
                                Frame::Integer(n) => *n,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };
                            let latency = match &pair[1] {
                                Frame::Integer(n) => *n,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };
                            result.push((timestamp, latency));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LATENCY LATEST - Return the latest latency samples for all events
///
/// Returns the latest latency spike for each event type.
///
/// Available since Redis 2.8.13
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyLatest;
///
/// let cmd = LatencyLatest;
/// // Returns latest latency samples for all tracked events
/// ```
#[derive(Debug, Clone, Copy)]
pub struct LatencyLatest;

impl Command for LatencyLatest {
    type Response = String; // Complex nested structure, simplified for now

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("LATEST"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// LATENCY RESET - Reset latency data for specified events
///
/// Resets the latency spikes time series of all, or only some, events.
///
/// Available since Redis 2.8.13
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyReset;
///
/// // Reset all events
/// let cmd = LatencyReset::all();
///
/// // Reset specific events
/// let cmd = LatencyReset::new(vec!["command", "fast-command"]);
/// ```
#[derive(Debug, Clone)]
pub struct LatencyReset {
    events: Vec<String>,
}

impl LatencyReset {
    /// Create a new LATENCY RESET command for specific events
    pub fn new(events: Vec<impl Into<String>>) -> Self {
        Self {
            events: events.into_iter().map(|e| e.into()).collect(),
        }
    }

    /// Reset all latency events
    pub fn all() -> Self {
        Self { events: Vec::new() }
    }
}

impl Command for LatencyReset {
    type Response = i64; // Number of event time series that were reset

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("RESET"))),
        ];

        for event in &self.events {
            frames.push(Frame::BulkString(Some(Bytes::from(event.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LATENCY HELP - Return help text about the LATENCY command
///
/// Returns help text about the different subcommands.
///
/// Available since Redis 2.8.13
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::LatencyHelp;
///
/// let cmd = LatencyHelp;
/// // Returns help text for LATENCY commands
/// ```
#[derive(Debug, Clone, Copy)]
pub struct LatencyHelp;

impl Command for LatencyHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LATENCY"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            result.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        Frame::SimpleString(data) => {
                            result.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_doctor_frame() {
        let cmd = LatencyDoctor;
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("DOCTOR"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_graph_frame() {
        let cmd = LatencyGraph::new("command");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GRAPH"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("command"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_histogram_all_frame() {
        let cmd = LatencyHistogram::all();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("HISTOGRAM"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_histogram_commands_frame() {
        let cmd = LatencyHistogram::new(vec!["GET", "SET"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("HISTOGRAM"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("SET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_history_frame() {
        let cmd = LatencyHistory::new("command");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("HISTORY"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("command"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_history_response() {
        let frame = Frame::Array(vec![
            Frame::Array(vec![Frame::Integer(1609459200), Frame::Integer(42)]),
            Frame::Array(vec![Frame::Integer(1609459300), Frame::Integer(38)]),
        ]);

        let result = LatencyHistory::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (1609459200, 42));
        assert_eq!(result[1], (1609459300, 38));
    }

    #[test]
    fn test_latency_latest_frame() {
        let cmd = LatencyLatest;
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LATEST"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_reset_all_frame() {
        let cmd = LatencyReset::all();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("RESET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_reset_events_frame() {
        let cmd = LatencyReset::new(vec!["command", "fast-command"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("RESET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("command"))));
                assert_eq!(
                    parts[3],
                    Frame::BulkString(Some(Bytes::from("fast-command")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_reset_response() {
        let frame = Frame::Integer(2);
        let result = LatencyReset::parse_response(frame).unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn test_latency_help_frame() {
        let cmd = LatencyHelp;
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LATENCY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("HELP"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_latency_help_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LATENCY DOCTOR"))),
            Frame::BulkString(Some(Bytes::from("LATENCY GRAPH <event>"))),
        ]);

        let result = LatencyHelp::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "LATENCY DOCTOR");
        assert_eq!(result[1], "LATENCY GRAPH <event>");
    }
}
