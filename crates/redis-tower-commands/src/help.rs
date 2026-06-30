//! Shared parsing for `<CONTAINER> HELP` replies.
//!
//! Redis container commands (`ACL`, `CLIENT`, `CLUSTER`, ...) all expose a
//! `HELP` subcommand that returns an array of human-readable lines describing
//! the available subcommands. The wire shape is uniform across them, so the
//! HELP command structs in each command-group module share this parser.

use bytes::Bytes;
use redis_tower_core::{Frame, RedisError};

/// Parse a `<CONTAINER> HELP` reply into its individual lines.
///
/// The reply is an array whose elements are the help lines. Redis normally
/// returns each line as a bulk string, but some servers/subcommands reply with
/// simple strings, and under RESP3 verbatim strings are possible, so all three
/// scalar shapes are accepted.
pub(crate) fn parse_help_lines(frame: Frame) -> Result<Vec<Bytes>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::BulkString(Some(data)) => Ok(data),
                Frame::SimpleString(data) => Ok(data),
                Frame::VerbatimString(_, data) => Ok(data),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "bulk string or simple string",
                    actual: format!("{other:?}"),
                }),
            })
            .collect(),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mixed_scalar_shapes() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("ACL HELP"))),
            Frame::SimpleString(Bytes::from("CAT [<category>]")),
            Frame::VerbatimString(Bytes::from("txt"), Bytes::from("WHOAMI")),
        ]));
        let lines = parse_help_lines(frame).unwrap();
        assert_eq!(lines.len(), 3);
        assert_eq!(&lines[0][..], b"ACL HELP");
        assert_eq!(&lines[1][..], b"CAT [<category>]");
        assert_eq!(&lines[2][..], b"WHOAMI");
    }

    #[test]
    fn rejects_non_array() {
        let err = parse_help_lines(Frame::Integer(1)).unwrap_err();
        assert!(matches!(err, RedisError::UnexpectedResponse { .. }));
    }

    #[test]
    fn rejects_non_scalar_element() {
        let frame = Frame::Array(Some(vec![Frame::Integer(1)]));
        let err = parse_help_lines(frame).unwrap_err();
        assert!(matches!(err, RedisError::UnexpectedResponse { .. }));
    }
}
