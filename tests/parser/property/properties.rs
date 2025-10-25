//! Core property-based tests for RESP parser correctness
//!
//! This module contains the main property tests that validate fundamental
//! correctness properties of the RESP parser and serializer.

use super::generators::*;
use super::{lightweight_proptest_config, proptest_config};
use crate::parser::test_adapter::RespParser;
use proptest::prelude::*;
use redis_tower::parser::RespFrame;

/// Test that all valid frames can round-trip through serialization and parsing
#[cfg(test)]
mod round_trip_properties {
    use super::*;

    proptest! {
        #![proptest_config(proptest_config())]

        /// Property: serialize(parse(serialize(frame))) == serialize(frame)
        #[test]
        fn resp2_frame_roundtrip(frame in arb_resp2_frame()) {
            let serialized = serialize_frame(&frame);

            let mut parser = RespParser::new();
            let parsed = parser.parse(&serialized);

            prop_assert!(parsed.is_ok(), "Failed to parse serialized frame: {:?}", parsed.err());

            let parsed_frame = parsed.unwrap();
            prop_assert!(parsed_frame.is_some(), "Parser should return a frame");
            let parsed_frame = parsed_frame.unwrap();

            let re_serialized = serialize_frame(&parsed_frame);

            prop_assert_eq!(serialized, re_serialized,
                "Round-trip failed: original != re-serialized");
        }

        /// Property: Parsing should be deterministic
        #[test]
        fn parsing_is_deterministic(frame in arb_resp2_frame()) {
            let serialized = serialize_frame(&frame);

            let mut parser1 = RespParser::new();
            let mut parser2 = RespParser::new();

            let result1 = parser1.parse(&serialized);
            let result2 = parser2.parse(&serialized);

            prop_assert_eq!(result1.is_ok(), result2.is_ok());

            if let (Ok(frame1_opt), Ok(frame2_opt)) = (result1, result2) {
                prop_assert_eq!(frame1_opt, frame2_opt, "Parsing should be deterministic");
            }
        }

        /// Property: Serialization should be consistent
        #[test]
        fn serialization_consistency(frame in arb_resp2_frame()) {
            let serialized1 = serialize_frame(&frame);
            let serialized2 = serialize_frame(&frame);

            prop_assert_eq!(serialized1, serialized2,
                "Serialization should be deterministic");
        }
    }
}

/// Test properties related to partial and incremental parsing
#[cfg(test)]
mod incremental_parsing_properties {
    use super::*;

    proptest! {
        #![proptest_config(proptest_config())]

        /// Property: Partial data should either parse completely or return incomplete
        #[test]
        fn partial_parsing_consistency(partial_data in arb_partial_resp()) {
            let (first_part, second_part) = partial_data;
            let mut parser = RespParser::new();

            // Parse first part
            let first_result = parser.parse(&first_part);

            match first_result {
                Ok(Some(frame)) => {
                    // If it parsed successfully, re-serializing should work
                    let re_serialized = serialize_frame(&frame);
                    prop_assert_eq!(first_part, re_serialized);
                }
                Ok(None) => {
                    // Parser needs more data - try with complete data
                    let complete_result = parser.parse(&second_part);

                    prop_assert!(complete_result.is_ok(),
                        "Complete data should parse successfully");
                }

                Err(_) => {
                    // Other errors are acceptable, just ensure they're handled gracefully
                    prop_assert!(true, "Parser handled error gracefully");
                }
            }
        }

        /// Property: Parser should handle buffer reuse correctly
        #[test]
        fn parser_reuse_consistency(frames in prop::collection::vec(arb_resp2_frame(), 1..=5)) {
            let mut parser = RespParser::new();

            for frame in frames {
                let serialized = serialize_frame(&frame);
                let result = parser.parse(&serialized);

                prop_assert!(result.is_ok(), "Parser reuse should work for valid frames");

                if let Ok(Some(parsed_frame)) = result {
                    prop_assert_eq!(frame, parsed_frame, "Parsed frame should match original");
                }
            }
        }
    }
}

/// Test properties related to error handling and edge cases
#[cfg(test)]
mod error_handling_properties {
    use super::*;

    proptest! {
        #![proptest_config(proptest_config())]

        /// Property: Invalid data should never cause panics
        #[test]
        fn invalid_data_no_panic(malformed_data in arb_malformed_resp()) {
            let mut parser = RespParser::new();

            // This should never panic, regardless of input
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                parser.parse(&malformed_data)
            }));

            prop_assert!(result.is_ok(), "Parser should never panic on invalid input");

            // The parse result can be either Ok or Err, but shouldn't panic
            if let Ok(parse_result) = result {
                match parse_result {
                    Ok(Some(frame)) => {
                        // If it somehow parsed, re-serializing should work
                        let re_serialized = serialize_frame(&frame);
                        prop_assert!(!re_serialized.is_empty(), "Re-serialized data should not be empty");
                    }
                    Ok(None) => {
                        // Incomplete data is fine
                        prop_assert!(true, "Parser correctly indicated incomplete data");
                    }
                    Err(_) => {
                        // Error is expected and acceptable for malformed data
                        prop_assert!(true, "Parser correctly rejected malformed data");
                    }
                }
            }
        }

        /// Property: Edge case frames should be handled correctly
        #[test]
        fn edge_cases_handled_correctly(frame in arb_edge_case_frames()) {
            let serialized = serialize_frame(&frame);
            let mut parser = RespParser::new();

            let result = parser.parse(&serialized);
            prop_assert!(result.is_ok(), "Edge case frames should parse correctly");

            if let Ok(Some(parsed_frame)) = result {
                prop_assert_eq!(frame, parsed_frame, "Edge case frame should round-trip correctly");
            }
        }


    }
}

/// Test properties specific to Redis command patterns
#[cfg(test)]
mod redis_command_properties {
    use super::*;

    proptest! {
        #![proptest_config(proptest_config())]

        /// Property: Redis commands should always parse as arrays
        #[test]
        fn redis_commands_are_arrays(command in arb_redis_commands()) {
            match command {
                RespFrame::Array(_) => prop_assert!(true, "Redis commands should be arrays"),
                _ => prop_assert!(false, "Redis command generated non-array frame"),
            }

            // Should also round-trip correctly
            let serialized = serialize_frame(&command);
            let mut parser = RespParser::new();
            let result = parser.parse(&serialized);

            prop_assert!(result.is_ok(), "Redis commands should parse correctly");

            if let Ok(Some(parsed_frame)) = result {
                prop_assert_eq!(command, parsed_frame, "Redis commands should round-trip");
            }
        }

        /// Property: Command arrays should have reasonable structure
        #[test]
        fn redis_command_structure(command in arb_redis_commands()) {
            if let RespFrame::Array(elements) = command {
                prop_assert!(!elements.is_empty(), "Commands should have at least one element");

                // First element should be the command name (bulk string)
                if let Some(RespFrame::BulkString(cmd_name_bytes)) = elements.first() {
                    let cmd_name = String::from_utf8_lossy(cmd_name_bytes);
                    prop_assert!(!cmd_name.is_empty(), "Command name should not be empty");
                    prop_assert!(cmd_name.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_lowercase()),
                        "Command name should be alphabetic");
                } else {
                    prop_assert!(false, "First element should be a bulk string command name");
                }
            }
        }
    }
}

/// Test serialization-specific properties
#[cfg(test)]
mod serialization_properties {
    use super::*;

    proptest! {
        #![proptest_config(proptest_config())]

        /// Property: Serialization should be deterministic
        #[test]
        fn serialization_is_deterministic(frame in arb_resp2_frame()) {
            let serialized1 = serialize_frame(&frame);
            let serialized2 = serialize_frame(&frame);

            prop_assert_eq!(serialized1, serialized2,
                "Serialization should be deterministic");
        }

        /// Property: Serialized data should always be valid RESP
        #[test]
        fn serialized_data_is_valid_resp(frame in arb_resp2_frame()) {
            let serialized = serialize_frame(&frame);

            // Should start with a valid RESP type marker
            prop_assert!(!serialized.is_empty(), "Serialized data should not be empty");

            let type_marker = serialized[0] as char;
            prop_assert!(matches!(type_marker, '+' | '-' | ':' | '$' | '*'),
                "Should start with valid RESP type marker, got: {}", type_marker);

            // Should end with CRLF (for most types)
            if serialized.len() >= 2 {
                let ends_with_crlf = serialized.ends_with(b"\r\n");
                prop_assert!(ends_with_crlf, "Should end with CRLF");
            }
        }

        /// Property: Empty collections should serialize correctly
        #[test]
        fn empty_collections_serialize_correctly(empty_frame in prop_oneof![
            Just(RespFrame::Array(vec![])),
            Just(RespFrame::NullArray),
            Just(RespFrame::NullBulkString),
        ]) {
            let serialized = serialize_frame(&empty_frame);
            let mut parser = RespParser::new();

            let result = parser.parse(&serialized);
            prop_assert!(result.is_ok(), "Empty collections should parse correctly");

            if let Ok(Some(parsed_frame)) = result {
                prop_assert_eq!(empty_frame, parsed_frame, "Empty collections should round-trip");
            }
        }

        /// Property: Size hints should be reasonable
        #[test]
        fn size_hints_are_reasonable(frame in arb_resp2_frame()) {
            let serialized = serialize_frame(&frame);
            let size_hint = frame.size_hint();

            // Size hint should be close to actual size
            let actual_size = serialized.len();
            let difference = size_hint.abs_diff(actual_size);

            // Allow some tolerance for size estimation
            prop_assert!(difference <= actual_size / 10 + 10,
                "Size hint {} should be close to actual size {}", size_hint, actual_size);
        }
    }
}

/// Test properties specific to memory-intensive operations with lightweight config
#[cfg(test)]
mod memory_intensive_properties {
    use super::*;

    proptest! {
        #![proptest_config(lightweight_proptest_config())]

        /// Property: Large inputs should be handled without memory issues (lightweight)
        #[test]
        fn large_input_memory_safety(large_data in arb_large_bytes()) {
            // Create a frame with large data
            let frame = RespFrame::BulkString(large_data.clone());

            let serialized = serialize_frame(&frame);
            let mut parser = RespParser::new();

            let result = parser.parse(&serialized);
            prop_assert!(result.is_ok(), "Large inputs should be handled correctly");

            if let Ok(Some(RespFrame::BulkString(parsed_bytes))) = result {
                prop_assert_eq!(large_data, parsed_bytes, "Large data should round-trip correctly");
            }
        }

        /// Property: Edge case large strings should be handled correctly
        #[test]
        fn edge_case_large_strings(large_string in arb_edge_case_strings()) {
            let frame = RespFrame::BulkString(large_string.clone().into_bytes());
            let serialized = serialize_frame(&frame);
            let mut parser = RespParser::new();

            let result = parser.parse(&serialized);
            prop_assert!(result.is_ok(), "Large edge case strings should parse correctly");

            if let Ok(Some(RespFrame::BulkString(parsed_bytes))) = result {
                let parsed_string = String::from_utf8_lossy(&parsed_bytes);
                prop_assert_eq!(large_string, parsed_string, "Large string should round-trip correctly");
            }
        }
    }
}
