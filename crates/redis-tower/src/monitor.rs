//! Streaming support for Redis's `MONITOR` command.
//!
//! [`MonitorStream`] consumes a dedicated [`RedisConnection`], switches it to
//! monitor mode, and yields a parsed [`MonitorEvent`] for every command the
//! server reports. It reads directly from the owned connection; no background
//! task or intermediate channel is created.
//!
//! # Production impact
//!
//! `MONITOR` is intended for short-lived debugging. Redis must format and send
//! every eligible command to every connected monitor, which can reduce server
//! throughput substantially. [Redis's own illustrative benchmark] shows a
//! single monitor cutting throughput by more than 50 percent. Prefer metrics,
//! tracing, or slow-log tooling for continuous production observability.
//!
//! [Redis's own illustrative benchmark]: https://redis.io/docs/latest/commands/monitor/#cost-of-running-monitor
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{MonitorStream, RedisConnection};
//! use tokio_stream::StreamExt;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut events = MonitorStream::new(conn).await?;
//!
//! while let Some(event) = events.next().await {
//!     let event = event?;
//!     println!("db {}: {:?} {:?}", event.database, event.command, event.arguments);
//! }
//! # Ok(())
//! # }
//! ```

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use redis_tower_commands::Monitor;
use redis_tower_core::{Frame, RedisConnection, RedisError, RedisStream};
use redis_tower_protocol::RespCodec;
use tokio_stream::Stream;
use tokio_util::codec::Framed;

/// One command observed by Redis `MONITOR`.
///
/// Redis renders every command argument with its binary-safe quoted-string
/// representation. The parser reverses those escapes, including `\\xHH`, so
/// [`command`](Self::command) and [`arguments`](Self::arguments) contain the
/// original bytes even when they are not UTF-8. [`raw`](Self::raw) retains the
/// server's complete line (without the RESP `+` marker and CRLF).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorEvent {
    /// Time since the Unix epoch reported by Redis, with microsecond precision.
    pub timestamp: Duration,
    /// Logical Redis database number in which the command ran.
    pub database: i64,
    /// Client source reported by Redis, such as `127.0.0.1:54321`, `lua`, or a
    /// Unix-socket identifier.
    pub source: Bytes,
    /// Command name exactly as represented in the monitor event.
    pub command: Bytes,
    /// Command arguments after the command name, with Redis quoting reversed.
    pub arguments: Vec<Bytes>,
    /// Complete monitor line exactly as received from Redis.
    pub raw: Bytes,
}

impl MonitorEvent {
    /// Parse one raw `MONITOR` line.
    ///
    /// `raw` is the simple-string payload, without the RESP `+` marker or its
    /// trailing CRLF. Malformed timestamps, headers, quotes, and escapes are
    /// rejected instead of being decoded lossily.
    pub fn parse(raw: Bytes) -> Result<Self, RedisError> {
        let timestamp_end = find_byte(&raw, b' ')
            .ok_or_else(|| malformed("MONITOR line has no timestamp delimiter"))?;
        let timestamp = parse_timestamp(&raw[..timestamp_end])?;

        let header = &raw[timestamp_end + 1..];
        if !header.starts_with(b"[") {
            return Err(malformed("MONITOR line has no opening header bracket"));
        }

        let database_end = find_byte(&header[1..], b' ')
            .map(|position| position + 1)
            .ok_or_else(|| malformed("MONITOR header has no database delimiter"))?;
        let database = parse_i64(&header[1..database_end], "invalid MONITOR database number")?;

        let source_start = database_end + 1;
        let header_end = rfind_subslice(&header[source_start..], b"] \"")
            .map(|position| source_start + position)
            .ok_or_else(|| malformed("MONITOR header has no closing bracket"))?;
        let source = Bytes::copy_from_slice(&header[source_start..header_end]);

        // Keep the opening quote: the quoted-argument parser validates it and
        // all remaining separators rather than relying on whitespace splitting.
        let encoded_arguments = &header[header_end + 2..];
        let mut values = parse_quoted_arguments(encoded_arguments)?;
        if values.is_empty() {
            return Err(malformed("MONITOR line contains no command"));
        }
        let command = values.remove(0);

        Ok(Self {
            timestamp,
            database,
            source,
            command,
            arguments: values,
            raw,
        })
    }
}

/// A direct stream of commands observed through Redis `MONITOR`.
///
/// The stream owns the connection and polls its framed transport directly. It
/// must therefore be constructed from a fresh, exclusively owned connection.
/// Dropping the stream closes that dedicated connection and stops monitoring.
///
/// `MONITOR` is an expensive debugging facility: one monitor can materially
/// reduce server throughput, and each additional monitor adds more work. Do not
/// leave a monitor stream attached as a general-purpose production telemetry
/// mechanism.
pub struct MonitorStream {
    framed: Framed<RedisStream, RespCodec>,
}

impl MonitorStream {
    /// Enter monitor mode on `connection` and return its event stream.
    ///
    /// This waits for Redis's initial `OK` response before taking ownership of
    /// the framed transport. Any RESP limits configured on `connection` remain
    /// active for streamed monitor events. The connection must be fresh and
    /// exclusively owned; an outstanding `Service::call` causes
    /// [`RedisError::ConnectionInUse`].
    pub async fn new(mut connection: RedisConnection) -> Result<Self, RedisError> {
        connection.execute(Monitor::new()).await?;
        let framed = connection.into_framed()?;
        Ok(Self { framed })
    }
}

impl Stream for MonitorStream {
    type Item = Result<MonitorEvent, RedisError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.framed).poll_next(cx) {
            Poll::Ready(Some(Ok(Frame::SimpleString(raw)))) => {
                Poll::Ready(Some(MonitorEvent::parse(raw)))
            }
            Poll::Ready(Some(Ok(Frame::Error(error)))) => Poll::Ready(Some(Err(
                RedisError::Redis(String::from_utf8_lossy(&error).into_owned()),
            ))),
            Poll::Ready(Some(Ok(other))) => {
                Poll::Ready(Some(Err(RedisError::UnexpectedResponse {
                    expected: "MONITOR simple-string event",
                    actual: format!("{other:?}"),
                })))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(RedisError::from(error)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn parse_timestamp(bytes: &[u8]) -> Result<Duration, RedisError> {
    let decimal = find_byte(bytes, b'.')
        .ok_or_else(|| malformed("MONITOR timestamp has no decimal point"))?;
    let seconds = parse_u64(&bytes[..decimal], "invalid MONITOR timestamp seconds")?;
    let fraction = &bytes[decimal + 1..];
    if fraction.is_empty() || fraction.len() > 6 {
        return Err(malformed(
            "MONITOR timestamp must have one to six fractional digits",
        ));
    }

    let fractional = parse_u64(fraction, "invalid MONITOR timestamp fraction")?;
    let scale = 10u64.pow(6 - fraction.len() as u32);
    let micros = fractional
        .checked_mul(scale)
        .ok_or_else(|| malformed("MONITOR timestamp fraction overflow"))?;
    Ok(Duration::new(seconds, (micros as u32) * 1_000))
}

fn parse_u64(bytes: &[u8], error: &'static str) -> Result<u64, RedisError> {
    let text = std::str::from_utf8(bytes).map_err(|_| malformed(error))?;
    text.parse().map_err(|_| malformed(error))
}

fn parse_i64(bytes: &[u8], error: &'static str) -> Result<i64, RedisError> {
    let text = std::str::from_utf8(bytes).map_err(|_| malformed(error))?;
    text.parse().map_err(|_| malformed(error))
}

/// Reverse the output of Redis's `sdscatrepr` for a sequence of arguments.
fn parse_quoted_arguments(mut input: &[u8]) -> Result<Vec<Bytes>, RedisError> {
    let mut arguments = Vec::new();

    while !input.is_empty() {
        if input[0] != b'"' {
            return Err(malformed("MONITOR argument does not start with a quote"));
        }
        input = &input[1..];

        let mut decoded = BytesMut::new();
        let mut closed = false;
        while let Some((&byte, rest)) = input.split_first() {
            input = rest;
            match byte {
                b'"' => {
                    closed = true;
                    break;
                }
                b'\\' => decode_escape(&mut input, &mut decoded)?,
                byte => decoded.extend_from_slice(&[byte]),
            }
        }
        if !closed {
            return Err(malformed("unterminated MONITOR argument"));
        }
        arguments.push(decoded.freeze());

        if input.is_empty() {
            break;
        }
        if input[0] != b' ' {
            return Err(malformed("MONITOR arguments are not space-separated"));
        }
        input = &input[1..];
        if input.is_empty() {
            return Err(malformed("MONITOR line has a trailing argument separator"));
        }
    }

    Ok(arguments)
}

fn decode_escape(input: &mut &[u8], output: &mut BytesMut) -> Result<(), RedisError> {
    let (&escape, rest) = input
        .split_first()
        .ok_or_else(|| malformed("trailing backslash in MONITOR argument"))?;
    *input = rest;

    let decoded = match escape {
        b'\\' => b'\\',
        b'"' => b'"',
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'a' => 0x07,
        b'b' => 0x08,
        b'x' => {
            if input.len() < 2 {
                return Err(malformed("short hexadecimal escape in MONITOR argument"));
            }
            let high = hex_value(input[0])
                .ok_or_else(|| malformed("invalid hexadecimal escape in MONITOR argument"))?;
            let low = hex_value(input[1])
                .ok_or_else(|| malformed("invalid hexadecimal escape in MONITOR argument"))?;
            *input = &input[2..];
            (high << 4) | low
        }
        _ => return Err(malformed("unknown escape in MONITOR argument")),
    };
    output.extend_from_slice(&[decoded]);
    Ok(())
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn find_byte(bytes: &[u8], needle: u8) -> Option<usize> {
    bytes.iter().position(|byte| *byte == needle)
}

fn rfind_subslice(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes
        .windows(needle.len())
        .rposition(|window| window == needle)
}

fn malformed(actual: &'static str) -> RedisError {
    RedisError::UnexpectedResponse {
        expected: "timestamp [database source] followed by quoted MONITOR arguments",
        actual: actual.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &[u8]) -> MonitorEvent {
        MonitorEvent::parse(Bytes::copy_from_slice(line)).unwrap()
    }

    #[test]
    fn parses_tcp_event() {
        let raw = b"1339518090.420270 [0 127.0.0.1:60866] \"set\" \"x\" \"6\"";
        let event = parse(raw);

        assert_eq!(event.timestamp, Duration::new(1_339_518_090, 420_270_000));
        assert_eq!(event.database, 0);
        assert_eq!(event.source, Bytes::from_static(b"127.0.0.1:60866"));
        assert_eq!(event.command, Bytes::from_static(b"set"));
        assert_eq!(
            event.arguments,
            vec![Bytes::from_static(b"x"), Bytes::from_static(b"6")]
        );
        assert_eq!(event.raw, Bytes::copy_from_slice(raw));
    }

    #[test]
    fn parses_lua_event_without_arguments() {
        let event = parse(b"1339518100.363799 [2 lua] \"dbsize\"");

        assert_eq!(event.database, 2);
        assert_eq!(event.source, Bytes::from_static(b"lua"));
        assert_eq!(event.command, Bytes::from_static(b"dbsize"));
        assert!(event.arguments.is_empty());
    }

    #[test]
    fn parses_unix_socket_path_containing_the_header_delimiter() {
        let event = parse(b"1.000001 [0 unix:/tmp/redis-monitor-] \"edge.sock] \"ECHO\" \"edge\"");

        assert_eq!(
            event.source,
            Bytes::from_static(b"unix:/tmp/redis-monitor-] \"edge.sock")
        );
        assert_eq!(event.command, Bytes::from_static(b"ECHO"));
        assert_eq!(event.arguments, vec![Bytes::from_static(b"edge")]);
    }

    #[test]
    fn reverses_all_standard_redis_escapes() {
        let event = parse(
            br#"1.000001 [0 unix:/tmp/redis.sock] "set" "quote:\" slash:\\ newline:\n return:\r tab:\t bell:\a backspace:\b""#,
        );

        assert_eq!(
            event.arguments,
            vec![Bytes::from_static(
                b"quote:\" slash:\\ newline:\n return:\r tab:\t bell:\x07 backspace:\x08"
            )]
        );
    }

    #[test]
    fn hexadecimal_escapes_restore_arbitrary_bytes() {
        let event = parse(br#"1.5 [0 client] "set" "\x00\x7f\x80\xFf""#);
        assert_eq!(
            event.arguments,
            vec![Bytes::from_static(&[0x00, 0x7f, 0x80, 0xff])]
        );
    }

    #[test]
    fn fractional_timestamp_is_scaled_to_microseconds() {
        let event = parse(br#"10.25 [0 client] "ping""#);
        assert_eq!(event.timestamp, Duration::new(10, 250_000_000));
    }

    #[test]
    fn rejects_malformed_lines_and_escapes() {
        for line in [
            &b"not-a-timestamp [0 client] \"get\""[..],
            &b"1.000000 0 client] \"get\""[..],
            &b"1.000000 [db client] \"get\""[..],
            &b"1.000000 [0 client] get"[..],
            &b"1.000000 [0 client] \"get"[..],
            &b"1.000000 [0 client] \"get\"  \"key\""[..],
            &b"1.000000 [0 client] \"get\" \"\\q\""[..],
            &b"1.000000 [0 client] \"get\" \"\\x0\""[..],
            &b"1.000000 [0 client] \"get\" \"\\xgg\""[..],
        ] {
            assert!(
                MonitorEvent::parse(Bytes::copy_from_slice(line)).is_err(),
                "unexpectedly parsed {}",
                String::from_utf8_lossy(line)
            );
        }
    }
}
