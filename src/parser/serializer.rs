//! RESP protocol serializer implementation

use super::frame::RespFrame;

/// RESP protocol serializer
///
/// Provides methods to serialize RESP frames into byte sequences
/// that can be transmitted over the network.
#[derive(Debug, Clone, Default)]
pub struct RespSerializer;

impl RespSerializer {
    /// Create a new RESP serializer
    pub fn new() -> Self {
        Self
    }

    /// Serialize a single frame to bytes
    pub fn serialize(&self, frame: &RespFrame) -> Vec<u8> {
        frame.serialize()
    }

    /// Serialize multiple frames to bytes
    pub fn serialize_multiple(&self, frames: &[RespFrame]) -> Vec<u8> {
        let mut buf = Vec::new();
        for frame in frames {
            frame.write_to(&mut buf);
        }
        buf
    }

    /// Serialize a frame and write it to an existing buffer
    pub fn serialize_into(&self, frame: &RespFrame, buf: &mut Vec<u8>) {
        frame.write_to(buf);
    }

    /// Get the estimated size needed to serialize a frame
    pub fn estimate_size(&self, frame: &RespFrame) -> usize {
        frame.size_hint()
    }

    /// Get the estimated size needed to serialize multiple frames
    pub fn estimate_size_multiple(&self, frames: &[RespFrame]) -> usize {
        frames.iter().map(|f| f.size_hint()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_simple_string() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::SimpleString("OK".to_string());
        assert_eq!(serializer.serialize(&frame), b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::Error("ERR something went wrong".to_string());
        assert_eq!(
            serializer.serialize(&frame),
            b"-ERR something went wrong\r\n"
        );
    }

    #[test]
    fn test_serialize_integer() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::Integer(42);
        assert_eq!(serializer.serialize(&frame), b":42\r\n");

        let frame = RespFrame::Integer(-123);
        assert_eq!(serializer.serialize(&frame), b":-123\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::BulkString(b"hello world".to_vec());
        assert_eq!(serializer.serialize(&frame), b"$11\r\nhello world\r\n");

        // Empty bulk string
        let frame = RespFrame::BulkString(vec![]);
        assert_eq!(serializer.serialize(&frame), b"$0\r\n\r\n");
    }

    #[test]
    fn test_serialize_null_types() {
        let serializer = RespSerializer::new();

        let frame = RespFrame::NullBulkString;
        assert_eq!(serializer.serialize(&frame), b"$-1\r\n");

        let frame = RespFrame::NullArray;
        assert_eq!(serializer.serialize(&frame), b"*-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(b"foo".to_vec()),
            RespFrame::BulkString(b"bar".to_vec()),
        ]);
        assert_eq!(
            serializer.serialize(&frame),
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );

        // Empty array
        let frame = RespFrame::Array(vec![]);
        assert_eq!(serializer.serialize(&frame), b"*0\r\n");
    }

    #[test]
    fn test_serialize_nested_array() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::Array(vec![
            RespFrame::Array(vec![
                RespFrame::BulkString(b"foo".to_vec()),
                RespFrame::BulkString(b"bar".to_vec()),
            ]),
            RespFrame::Integer(42),
        ]);
        assert_eq!(
            serializer.serialize(&frame),
            b"*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n"
        );
    }

    #[test]
    fn test_serialize_multiple() {
        let serializer = RespSerializer::new();
        let frames = vec![
            RespFrame::SimpleString("OK".to_string()),
            RespFrame::Integer(42),
            RespFrame::BulkString(b"test".to_vec()),
        ];
        let result = serializer.serialize_multiple(&frames);
        assert_eq!(result, b"+OK\r\n:42\r\n$4\r\ntest\r\n");
    }

    #[test]
    fn test_serialize_into() {
        let serializer = RespSerializer::new();
        let mut buf = Vec::new();

        let frame = RespFrame::SimpleString("Hello".to_string());
        serializer.serialize_into(&frame, &mut buf);

        let frame = RespFrame::Integer(123);
        serializer.serialize_into(&frame, &mut buf);

        assert_eq!(buf, b"+Hello\r\n:123\r\n");
    }

    #[test]
    fn test_size_estimation() {
        let serializer = RespSerializer::new();

        let frame = RespFrame::SimpleString("OK".to_string());
        let estimated = serializer.estimate_size(&frame);
        let actual = serializer.serialize(&frame);
        assert_eq!(estimated, actual.len());

        let frame = RespFrame::BulkString(b"hello".to_vec());
        let estimated = serializer.estimate_size(&frame);
        let actual = serializer.serialize(&frame);
        // Size hint is approximate, so it should be close
        assert!((estimated as i32 - actual.len() as i32).abs() <= 5);
    }

    #[test]
    fn test_size_estimation_multiple() {
        let serializer = RespSerializer::new();
        let frames = vec![
            RespFrame::SimpleString("OK".to_string()),
            RespFrame::Integer(42),
        ];

        let estimated = serializer.estimate_size_multiple(&frames);
        let actual = serializer.serialize_multiple(&frames);
        // Size hint is approximate, especially for integers, so allow larger variance
        assert!((estimated as i32 - actual.len() as i32).abs() <= 20);
    }

    #[test]
    fn test_complex_mixed_types() {
        let serializer = RespSerializer::new();
        let frame = RespFrame::Array(vec![
            RespFrame::SimpleString("SET".to_string()),
            RespFrame::BulkString(b"key".to_vec()),
            RespFrame::BulkString(b"value".to_vec()),
            RespFrame::Array(vec![
                RespFrame::BulkString(b"EX".to_vec()),
                RespFrame::Integer(3600),
            ]),
        ]);

        let result = serializer.serialize(&frame);
        let expected = b"*4\r\n+SET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$2\r\nEX\r\n:3600\r\n";
        assert_eq!(result, expected);
    }
}
