//! RESP3 specification compliance validation
//!
//! This test validates our RESP3 implementation against the official specification.
//! Tests are organized by RESP3 feature areas with clear pass/fail criteria.

use bytes::Bytes;
use redis_tower::parser::resp3::{Frame, ParseError, parse_frame};

#[cfg(test)]
mod spec_compliance {
    use super::*;

    /// Test basic RESP3 simple types
    #[test]
    fn test_simple_types_compliance() {
        // Simple string: +hello world\r\n
        let data = Bytes::from("+hello world\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from("hello world")));
        assert!(rest.is_empty());

        // Simple error: -ERR this is the error description\r\n
        let data = Bytes::from("-ERR this is the error description\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Error(Bytes::from("ERR this is the error description"))
        );
        assert!(rest.is_empty());

        // Number: :1234\r\n
        let data = Bytes::from(":1234\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Integer(1234));
        assert!(rest.is_empty());

        // Null: _\r\n
        let data = Bytes::from("_\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Null);
        assert!(rest.is_empty());

        // Boolean true: #t\r\n
        let data = Bytes::from("#t\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Boolean(true));
        assert!(rest.is_empty());

        // Boolean false: #f\r\n
        let data = Bytes::from("#f\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Boolean(false));
        assert!(rest.is_empty());

        // Double: ,1.23\r\n
        let data = Bytes::from(",1.23\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Double(1.23));
        assert!(rest.is_empty());

        // Big number: (3492890328409238509324850943850943825024385\r\n
        let data = Bytes::from("(3492890328409238509324850943850943825024385\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::BigNumber(Bytes::from("3492890328409238509324850943850943825024385"))
        );
        assert!(rest.is_empty());
    }

    /// Test blob string and related types
    #[test]
    fn test_blob_types_compliance() {
        // Blob string: $11\r\nhello world\r\n
        let data = Bytes::from("$11\r\nhello world\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from("hello world"))));
        assert!(rest.is_empty());

        // Empty blob string: $0\r\n\r\n
        let data = Bytes::from("$0\r\n\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from(""))));
        assert!(rest.is_empty());

        // Null blob string: $-1\r\n
        let data = Bytes::from("$-1\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::BulkString(None));
        assert!(rest.is_empty());

        // Blob error: !21\r\nSYNTAX invalid syntax\r\n
        let data = Bytes::from("!21\r\nSYNTAX invalid syntax\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::BlobError(Bytes::from("SYNTAX invalid syntax"))
        );
        assert!(rest.is_empty());

        // Verbatim string: =15\r\ntxt:Some string\r\n
        let data = Bytes::from("=15\r\ntxt:Some string\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::VerbatimString(Bytes::from("txt"), Bytes::from("Some string"))
        );
        assert!(rest.is_empty());
    }

    /// Test aggregate types (Array, Map, Set, etc.)
    #[test]
    fn test_aggregate_types_compliance() {
        // Array: *3\r\n:1\r\n:2\r\n:3\r\n
        let data = Bytes::from("*3\r\n:1\r\n:2\r\n:3\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
            ]))
        );
        assert!(rest.is_empty());

        // Map: %2\r\n+first\r\n:1\r\n+second\r\n:2\r\n
        let data = Bytes::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Map(vec![
                (Frame::SimpleString(Bytes::from("first")), Frame::Integer(1)),
                (
                    Frame::SimpleString(Bytes::from("second")),
                    Frame::Integer(2)
                ),
            ])
        );
        assert!(rest.is_empty());

        // Set: ~3\r\n+a\r\n+b\r\n+c\r\n
        let data = Bytes::from("~3\r\n+a\r\n+b\r\n+c\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Set(vec![
                Frame::SimpleString(Bytes::from("a")),
                Frame::SimpleString(Bytes::from("b")),
                Frame::SimpleString(Bytes::from("c")),
            ])
        );
        assert!(rest.is_empty());

        // Push: >2\r\n+pubsub\r\n+message\r\n
        let data = Bytes::from(">2\r\n+pubsub\r\n+message\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Push(vec![
                Frame::SimpleString(Bytes::from("pubsub")),
                Frame::SimpleString(Bytes::from("message")),
            ])
        );
        assert!(rest.is_empty());
    }

    /// Test special float values
    #[test]
    fn test_special_floats_compliance() {
        // Positive infinity: ,inf\r\n
        let data = Bytes::from(",inf\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::SpecialFloat(Bytes::from("inf")));
        assert!(rest.is_empty());

        // Negative infinity: ,-inf\r\n
        let data = Bytes::from(",-inf\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::SpecialFloat(Bytes::from("-inf")));
        assert!(rest.is_empty());

        // NaN: ,nan\r\n
        let data = Bytes::from(",nan\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::SpecialFloat(Bytes::from("nan")));
        assert!(rest.is_empty());
    }

    /// Test streaming headers (basic support)
    #[test]
    fn test_streaming_headers_compliance() {
        // Streaming string header: $?\r\n
        let data = Bytes::from("$?\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::StreamedStringHeader);
        assert!(rest.is_empty());

        // Streaming array header: *?\r\n
        let data = Bytes::from("*?\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::StreamedArrayHeader);
        assert!(rest.is_empty());

        // Stream terminator: .\r\n
        let data = Bytes::from(".\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::StreamTerminator);
        assert!(rest.is_empty());
    }

    /// Test UTF-8 support with correct byte lengths
    #[test]
    fn test_utf8_support_compliance() {
        // UTF-8 in simple strings
        let data = Bytes::from("+Hello, 世界!\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from("Hello, 世界!")));
        assert!(rest.is_empty());

        // UTF-8 in blob strings (14 bytes for "Hello, 世界!")
        let data = Bytes::from("$14\r\nHello, 世界!\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from("Hello, 世界!"))));
        assert!(rest.is_empty());

        // Various UTF-8 strings
        let test_cases = vec![
            ("🦀 Rust", 9),     // Crab emoji + space + "Rust"
            ("αβγδε", 10),      // Greek letters
            ("مرحبا", 10),      // Arabic
            ("こんにちは", 15), // Japanese
        ];

        for (text, byte_len) in test_cases {
            let data_str = format!("${byte_len}\r\n{text}\r\n");
            let data = Bytes::from(data_str);
            let (frame, rest) = parse_frame(data).unwrap();
            assert_eq!(frame, Frame::BulkString(Some(Bytes::from(text))));
            assert!(rest.is_empty());
        }
    }

    /// Test binary safety
    #[test]
    fn test_binary_safety_compliance() {
        // Blob string with null bytes and non-UTF8 data
        let mut data = Vec::new();
        data.extend_from_slice(b"$6\r\n");
        data.extend_from_slice(&[0, 1, 255, 254, 0x80, 0x81]);
        data.extend_from_slice(b"\r\n");

        let bytes = Bytes::from(data);
        let (frame, rest) = parse_frame(bytes).unwrap();

        if let Frame::BulkString(Some(content)) = frame {
            assert_eq!(content.len(), 6);
            assert_eq!(content[0], 0);
            assert_eq!(content[1], 1);
            assert_eq!(content[2], 255);
            assert_eq!(content[3], 254);
            assert_eq!(content[4], 0x80);
            assert_eq!(content[5], 0x81);
        } else {
            panic!("Expected BulkString");
        }
        assert!(rest.is_empty());
    }

    /// Test error conditions and edge cases
    #[test]
    fn test_error_conditions_compliance() {
        // Invalid type byte
        let data = Bytes::from("@invalid\r\n");
        let result = parse_frame(data);
        assert!(matches!(result, Err(ParseError::InvalidTag(_))));

        // Invalid boolean
        let data = Bytes::from("#x\r\n");
        let result = parse_frame(data);
        assert!(matches!(result, Err(ParseError::InvalidBoolean)));

        // Incomplete frames
        let incomplete_cases = vec![
            "+hello",       // Missing CRLF
            "$5\r\nhel",    // Incomplete blob string
            ":123",         // Incomplete number
            "*2\r\n:1\r\n", // Incomplete array
        ];

        for case in incomplete_cases {
            let data = Bytes::from(case);
            let result = parse_frame(data);
            assert_eq!(result, Err(ParseError::Incomplete));
        }
    }

    /// Test RESP2 backward compatibility
    #[test]
    fn test_resp2_compatibility() {
        // All RESP2 types should work in RESP3

        // Simple string
        let data = Bytes::from("+OK\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from("OK")));

        // Error
        let data = Bytes::from("-ERR\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Error(Bytes::from("ERR")));

        // Integer
        let data = Bytes::from(":1000\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Integer(1000));

        // Bulk string
        let data = Bytes::from("$6\r\nfoobar\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from("foobar"))));

        // Array
        let data = Bytes::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("foo"))),
                Frame::BulkString(Some(Bytes::from("bar"))),
            ]))
        );

        // Null representations
        let data = Bytes::from("$-1\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::BulkString(None));

        let data = Bytes::from("*-1\r\n");
        let (frame, _) = parse_frame(data).unwrap();
        assert_eq!(frame, Frame::Array(None));
    }

    /// Test number boundary values
    #[test]
    fn test_number_boundaries_compliance() {
        let test_cases = vec![
            (":9223372036854775807\r\n", i64::MAX),
            (":-9223372036854775808\r\n", i64::MIN),
            (":0\r\n", 0),
            (":1\r\n", 1),
            (":-1\r\n", -1),
        ];

        for (input, expected) in test_cases {
            let data = Bytes::from(input);
            let (frame, rest) = parse_frame(data).unwrap();
            assert_eq!(frame, Frame::Integer(expected));
            assert!(rest.is_empty());
        }
    }

    /// Test complex nested structures
    #[test]
    fn test_nested_structures_compliance() {
        // Nested array: *2\r\n*3\r\n:1\r\n$5\r\nhello\r\n:2\r\n#f\r\n
        let data = Bytes::from("*2\r\n*3\r\n:1\r\n$5\r\nhello\r\n:2\r\n#f\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::Array(Some(vec![
                    Frame::Integer(1),
                    Frame::BulkString(Some(Bytes::from("hello"))),
                    Frame::Integer(2),
                ])),
                Frame::Boolean(false),
            ]))
        );
        assert!(rest.is_empty());

        // Map with complex types: %1\r\n*2\r\n:1\r\n:2\r\n+value\r\n
        let data = Bytes::from("%1\r\n*2\r\n:1\r\n:2\r\n+value\r\n");
        let (frame, rest) = parse_frame(data).unwrap();
        assert_eq!(
            frame,
            Frame::Map(vec![(
                Frame::Array(Some(vec![Frame::Integer(1), Frame::Integer(2)])),
                Frame::SimpleString(Bytes::from("value"))
            ),])
        );
        assert!(rest.is_empty());
    }

    /// Test that multiple frames can be parsed in sequence
    #[test]
    fn test_multiple_frames_compliance() {
        // Multiple frames: +OK\r\n:42\r\n$5\r\nhello\r\n
        let data = Bytes::from("+OK\r\n:42\r\n$5\r\nhello\r\n");

        // Parse first frame
        let (frame1, rest) = parse_frame(data).unwrap();
        assert_eq!(frame1, Frame::SimpleString(Bytes::from("OK")));

        // Parse second frame
        let (frame2, rest) = parse_frame(rest).unwrap();
        assert_eq!(frame2, Frame::Integer(42));

        // Parse third frame
        let (frame3, rest) = parse_frame(rest).unwrap();
        assert_eq!(frame3, Frame::BulkString(Some(Bytes::from("hello"))));

        assert!(rest.is_empty());
    }
}

/// Known limitations and missing features
#[cfg(test)]
mod known_limitations {
    use super::*;

    /// Test streaming string chunks (;) implementation
    #[test]
    fn test_streaming_string_chunks() {
        // Streaming string chunk: ;4\r\nHell\r\n
        let data = Bytes::from(";4\r\nHell\r\n");
        let result = parse_frame(data);
        assert!(result.is_ok());
        let (frame, rest) = result.unwrap();
        assert!(matches!(frame, Frame::StreamedStringChunk(_)));
        if let Frame::StreamedStringChunk(chunk) = frame {
            assert_eq!(chunk, Bytes::from("Hell"));
        }
        assert!(rest.is_empty());

        // Test zero-length chunk (end marker)
        let data = Bytes::from(";0\r\n");
        let result = parse_frame(data);
        assert!(result.is_ok());
        let (frame, _) = result.unwrap();
        if let Frame::StreamedStringChunk(chunk) = frame {
            assert!(chunk.is_empty());
        }

        // Test longer chunk
        let data = Bytes::from(";11\r\nHello World\r\n");
        let result = parse_frame(data);
        assert!(result.is_ok());
        let (frame, _) = result.unwrap();
        if let Frame::StreamedStringChunk(chunk) = frame {
            assert_eq!(chunk, Bytes::from("Hello World"));
        }
    }

    #[test]
    fn test_streaming_protocol() {
        use redis_tower::parser::resp3::parse_streaming_sequence;

        // Complete streaming string example from spec:
        // $?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;1\r\nd\r\n;0\r\n
        // This would represent "Hello world" sent in chunks
        let data = Bytes::from("$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n");
        let result = parse_streaming_sequence(data);
        assert!(result.is_ok());
        let (frame, rest) = result.unwrap();

        if let Frame::StreamedString(chunks) = frame {
            assert_eq!(chunks.len(), 3);
            assert_eq!(chunks[0], Bytes::from("Hell"));
            assert_eq!(chunks[1], Bytes::from("o worl"));
            assert_eq!(chunks[2], Bytes::from("d"));
        } else {
            panic!("Expected StreamedString frame");
        }
        assert!(rest.is_empty());

        // Test streaming array
        let data = Bytes::from("*?\r\n+hello\r\n:42\r\n.\r\n");
        let result = parse_streaming_sequence(data);
        assert!(result.is_ok());
        let (frame, _) = result.unwrap();

        if let Frame::StreamedArray(items) = frame {
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], Frame::SimpleString(_)));
            assert!(matches!(items[1], Frame::Integer(42)));
        } else {
            panic!("Expected StreamedArray frame");
        }

        // Test streaming map
        let data = Bytes::from("%?\r\n+key1\r\n+val1\r\n+key2\r\n:123\r\n.\r\n");
        let result = parse_streaming_sequence(data);
        assert!(result.is_ok());
        let (frame, _) = result.unwrap();

        if let Frame::StreamedMap(pairs) = frame {
            assert_eq!(pairs.len(), 2);
        } else {
            panic!("Expected StreamedMap frame");
        }
    }

    #[test]
    #[ignore = "HELLO command not implemented in parser"]
    fn test_hello_command() {
        // The HELLO command is part of the protocol but not implemented
        // in the frame parser (it would typically be handled at a higher level)
    }
}

/// Performance and stress tests
#[cfg(test)]
mod performance_validation {
    use super::*;

    #[test]
    fn test_large_bulk_string() {
        // 1MB bulk string
        let size = 1024 * 1024;
        let content = vec![b'x'; size];

        let mut data = Vec::new();
        data.extend_from_slice(format!("${size}\r\n").as_bytes());
        data.extend_from_slice(&content);
        data.extend_from_slice(b"\r\n");

        let bytes = Bytes::from(data);
        let (frame, rest) = parse_frame(bytes).unwrap();

        if let Frame::BulkString(Some(content)) = frame {
            assert_eq!(content.len(), size);
            assert!(content.iter().all(|&b| b == b'x'));
        } else {
            panic!("Expected BulkString");
        }
        assert!(rest.is_empty());
    }

    #[test]
    fn test_large_array() {
        // Array with 1000 elements
        let size = 1000;
        let mut data = format!("*{size}\r\n");
        for i in 0..size {
            data.push_str(&format!(":{i}\r\n"));
        }

        let bytes = Bytes::from(data);
        let (frame, rest) = parse_frame(bytes).unwrap();

        if let Frame::Array(Some(elements)) = frame {
            assert_eq!(elements.len(), size);
            for (i, element) in elements.iter().enumerate() {
                assert_eq!(*element, Frame::Integer(i as i64));
            }
        } else {
            panic!("Expected Array");
        }
        assert!(rest.is_empty());
    }
}
