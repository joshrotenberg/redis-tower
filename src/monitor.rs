//! MONITOR command streaming support
//!
//! Provides a streaming interface for Redis MONITOR command, which logs all commands
//! processed by the Redis server in real-time.
//!
//! **Warning**: MONITOR has significant performance impact and should only be used
//! for debugging and development. Never use in production.

use crate::client::RedisConnection;
use crate::commands::connection::Monitor;
use crate::types::RedisError;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// A parsed MONITOR event from Redis
///
/// Format: `timestamp [db client_address] "command" "arg1" "arg2" ...`
#[derive(Debug, Clone, PartialEq)]
pub struct MonitorEvent {
    /// Unix timestamp with microseconds (e.g., 1339518083.107412)
    pub timestamp: f64,
    /// Database number
    pub database: u32,
    /// Client address (e.g., 127.0.0.1:60866)
    pub client_address: String,
    /// Command name
    pub command: String,
    /// Command arguments
    pub args: Vec<String>,
    /// Raw event line for debugging
    pub raw: String,
}

impl MonitorEvent {
    /// Parse a MONITOR event line
    ///
    /// Example line: `1339518083.107412 [0 127.0.0.1:60866] "keys" "*"`
    pub fn parse(line: &str) -> Result<Self, RedisError> {
        let raw = line.to_string();
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() < 3 {
            return Err(RedisError::Protocol(format!(
                "Invalid MONITOR event format: {}",
                line
            )));
        }

        // Parse timestamp
        let timestamp = parts[0]
            .parse::<f64>()
            .map_err(|_| RedisError::Protocol(format!("Invalid timestamp: {}", parts[0])))?;

        // Parse database and client address from [0 127.0.0.1:60866]
        let bracket_part = parts[1..]
            .iter()
            .take_while(|s| !s.starts_with('"'))
            .copied()
            .collect::<Vec<_>>();
        let bracket_str = bracket_part.join(" ");

        let database_client = bracket_str.trim_start_matches('[').trim_end_matches(']');

        let db_client_parts: Vec<&str> = database_client.split_whitespace().collect();
        if db_client_parts.len() < 2 {
            return Err(RedisError::Protocol(format!(
                "Invalid database/client format: {}",
                bracket_str
            )));
        }

        let database = db_client_parts[0].parse::<u32>().map_err(|_| {
            RedisError::Protocol(format!("Invalid database: {}", db_client_parts[0]))
        })?;

        let client_address = db_client_parts[1].to_string();

        // Parse command and args (everything in quotes)
        let rest = line.split(']').nth(1).unwrap_or("").trim();
        let command_parts = Self::parse_quoted_strings(rest);

        let command = command_parts
            .first()
            .ok_or_else(|| RedisError::Protocol("No command found".to_string()))?
            .clone();

        let args = command_parts.into_iter().skip(1).collect();

        Ok(MonitorEvent {
            timestamp,
            database,
            client_address,
            command,
            args,
            raw,
        })
    }

    /// Parse quoted strings from a line
    fn parse_quoted_strings(s: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = s.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        result.push(current.clone());
                        current.clear();
                        in_quotes = false;
                    } else {
                        in_quotes = true;
                    }
                }
                '\\' if in_quotes => {
                    // Handle escape sequences
                    if let Some(next_ch) = chars.next() {
                        current.push(next_ch);
                    }
                }
                _ if in_quotes => {
                    current.push(ch);
                }
                _ => {
                    // Skip whitespace outside quotes
                }
            }
        }

        result
    }

    /// Get the event timestamp as SystemTime
    pub fn timestamp_as_system_time(&self) -> SystemTime {
        let secs = self.timestamp.trunc() as u64;
        let nanos = (self.timestamp.fract() * 1_000_000_000.0) as u32;
        UNIX_EPOCH + std::time::Duration::new(secs, nanos)
    }
}

/// A stream of MONITOR events from Redis
///
/// This struct provides an async stream interface for consuming MONITOR events.
/// Each event represents a command processed by the Redis server.
pub struct MonitorStream {
    receiver: mpsc::UnboundedReceiver<Result<MonitorEvent, RedisError>>,
}

impl MonitorStream {
    /// Create a new MONITOR stream from a Redis connection
    ///
    /// This will consume the connection and start streaming MONITOR events.
    /// The connection cannot be used for any other commands after this.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower::{RedisClient, monitor::MonitorStream};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = RedisClient::connect("localhost:6379").await?;
    /// let connection = client.get_connection().await?;
    ///
    /// let mut stream = MonitorStream::new(connection).await?;
    ///
    /// while let Some(event) = stream.next().await {
    ///     match event {
    ///         Ok(ev) => println!("{} - {} {:?}", ev.timestamp, ev.command, ev.args),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(connection: RedisConnection) -> Result<Self, RedisError> {
        // Send MONITOR command
        connection.call(Monitor::new()).await?;

        let (tx, rx) = mpsc::unbounded_channel();

        // Take ownership of the framed connection
        let framed = connection.framed;

        // Spawn task to read MONITOR events
        tokio::spawn(async move {
            use futures::StreamExt as _;
            let mut framed = framed.lock().await;

            loop {
                // Read frames from connection
                match framed.next().await {
                    Some(Ok(frame)) => {
                        use crate::codec::Frame;
                        match frame {
                            Frame::SimpleString(line) => {
                                let line_str = String::from_utf8_lossy(&line).to_string();
                                let event = MonitorEvent::parse(&line_str);
                                if tx.send(event).is_err() {
                                    // Receiver dropped, stop streaming
                                    break;
                                }
                            }
                            Frame::Error(e) => {
                                let err =
                                    RedisError::Protocol(String::from_utf8_lossy(&e).to_string());
                                let _ = tx.send(Err(err));
                                break;
                            }
                            _ => {
                                // Unexpected frame type
                                let err = RedisError::Protocol(
                                    "Unexpected frame type in MONITOR stream".to_string(),
                                );
                                let _ = tx.send(Err(err));
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(Err(RedisError::Connection(e.to_string())));
                        break;
                    }
                    None => {
                        // Connection closed
                        break;
                    }
                }
            }
        });

        Ok(MonitorStream { receiver: rx })
    }

    /// Receive the next MONITOR event
    ///
    /// Returns `None` when the stream is closed (connection dropped or error occurred).
    pub async fn next(&mut self) -> Option<Result<MonitorEvent, RedisError>> {
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_monitor_event() {
        let line = r#"1339518083.107412 [0 127.0.0.1:60866] "keys" "*""#;
        let event = MonitorEvent::parse(line).unwrap();

        assert_eq!(event.timestamp, 1339518083.107412);
        assert_eq!(event.database, 0);
        assert_eq!(event.client_address, "127.0.0.1:60866");
        assert_eq!(event.command, "keys");
        assert_eq!(event.args, vec!["*"]);
    }

    #[test]
    fn test_parse_monitor_event_with_multiple_args() {
        let line = r#"1339518083.107412 [0 127.0.0.1:60866] "set" "mykey" "myvalue""#;
        let event = MonitorEvent::parse(line).unwrap();

        assert_eq!(event.command, "set");
        assert_eq!(event.args, vec!["mykey", "myvalue"]);
    }

    #[test]
    fn test_parse_monitor_event_different_database() {
        let line = r#"1339518083.107412 [5 127.0.0.1:60866] "get" "foo""#;
        let event = MonitorEvent::parse(line).unwrap();

        assert_eq!(event.database, 5);
        assert_eq!(event.command, "get");
    }

    #[test]
    fn test_parse_quoted_strings() {
        let s = r#""set" "key" "value with spaces""#;
        let parts = MonitorEvent::parse_quoted_strings(s);

        assert_eq!(parts, vec!["set", "key", "value with spaces"]);
    }

    #[test]
    fn test_parse_quoted_strings_with_escapes() {
        let s = r#""set" "key" "value\"with\"quotes""#;
        let parts = MonitorEvent::parse_quoted_strings(s);

        assert_eq!(parts, vec!["set", "key", r#"value"with"quotes"#]);
    }

    #[test]
    fn test_timestamp_as_system_time() {
        let line = r#"1339518083.107412 [0 127.0.0.1:60866] "keys" "*""#;
        let event = MonitorEvent::parse(line).unwrap();

        let system_time = event.timestamp_as_system_time();
        let duration = system_time.duration_since(UNIX_EPOCH).unwrap();

        assert_eq!(duration.as_secs(), 1339518083);
        // Check microseconds are approximately correct (within 1ms)
        let micros = duration.as_micros() - (1339518083 * 1_000_000);
        assert!((micros as i64 - 107412).abs() < 1000);
    }
}
