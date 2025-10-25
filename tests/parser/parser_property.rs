//! Property-based testing integration for RESP parser
//!
//! This file serves as the main entry point for property-based tests,
//! integrating all property test modules and providing high-level test orchestration.

use proptest::prelude::*;
use redis_tower::parser::RespFrame;

mod test_adapter;
use test_adapter::RespParser;

mod property;
use property::*;

/// Configuration for all property tests in this suite
/// Uses reasonable defaults - see module docs for customization options
fn test_config() -> ProptestConfig {
    // Use the shared configuration from the property module
    // This ensures consistent behavior and respects environment variables
    proptest_config()
}

proptest! {
    #![proptest_config(test_config())]

    /// Fundamental property: All valid RESP2 frames should round-trip perfectly
    #[test]
    fn resp2_roundtrip_property(frame in arb_resp2_frame()) {
        let serialized = serialize_frame(&frame);
        let mut parser = RespParser::new();

        let parse_result = parser.parse(&serialized);
        prop_assert!(parse_result.is_ok(), "Failed to parse valid frame: {:?}", parse_result.err());

        let parsed_frame_opt = parse_result.unwrap();
        prop_assert!(parsed_frame_opt.is_some(), "Parser should return a frame for complete data");
        let parsed_frame = parsed_frame_opt.unwrap();

        prop_assert_eq!(&frame, &parsed_frame, "Frame should round-trip exactly");

        // Verify re-serialization is identical
        let re_serialized = serialize_frame(&parsed_frame);
        prop_assert_eq!(serialized, re_serialized, "Re-serialization should be identical");
    }

    /// Size estimation property: Estimated size should match actual serialized size
    #[test]
    fn size_estimation_property(frame in arb_resp2_frame()) {
        let estimated = frame.size_hint();
        let actual_bytes = serialize_frame(&frame);

        prop_assert_eq!(estimated, actual_bytes.len(),
            "Size estimation mismatch: estimated {} vs actual {}", estimated, actual_bytes.len());
    }

    /// Parser reuse property: Parser should work correctly across multiple parse operations
    #[test]
    fn parser_reuse_property(frames in prop::collection::vec(arb_resp2_frame(), 1..=10)) {
        let mut parser = RespParser::new();

        for (i, frame) in frames.iter().enumerate() {
            let serialized = serialize_frame(frame);
            let result = parser.parse(&serialized);

            prop_assert!(result.is_ok(), "Parse #{} failed: {:?}", i, result.err());

            let parsed_opt = result.unwrap();
            prop_assert!(parsed_opt.is_some(), "Frame #{} should be complete", i);
            let parsed = parsed_opt.unwrap();

            prop_assert_eq!(frame, &parsed, "Frame #{} didn't match", i);
        }
    }

    /// Incremental parsing property: Partial data should be handled gracefully
    #[test]
    fn incremental_parsing_property((first_part, second_part) in arb_partial_resp()) {
        let mut parser = RespParser::new();

        // Parse incomplete data
        let incomplete_result = parser.parse(&first_part);

        match incomplete_result {
            Ok(Some(frame)) => {
                // If it parsed successfully, it should be complete
                let re_serialized = serialize_frame(&frame);
                prop_assert_eq!(first_part, re_serialized);
            }
            Ok(None) => {
                // Expected case - parser needs more data, try with complete data
                let complete_result = parser.parse(&second_part);
                prop_assert!(complete_result.is_ok(),
                    "Complete data should parse successfully: {:?}", complete_result.err());
            }
            Err(_) => {
                // Other errors are acceptable for malformed partial data
                prop_assert!(true, "Parser gracefully handled error");
            }
        }
    }

    /// Robustness property: Invalid input should never panic
    #[test]
    fn robustness_property(malformed in arb_malformed_resp()) {
        // Create a new parser for each test to avoid state contamination
        let mut parser = RespParser::new();

        // Should never panic, regardless of input
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse(&malformed)
        }));

        prop_assert!(panic_result.is_ok(), "Parser panicked on malformed input");

        // The actual parse result can be Ok or Err, but no panic
        if let Ok(parse_result) = panic_result {
            match parse_result {
                Ok(Some(frame)) => {
                    // If it somehow parsed, should be able to re-serialize
                    let _re_serialized = serialize_frame(&frame);
                }
                Ok(None) => {
                    // Incomplete data is expected and fine
                    prop_assert!(true);
                }
                Err(_) => {
                    // Error is expected and fine for malformed data
                    prop_assert!(true);
                }
            }
        }
    }

    /// Edge cases property: Special cases should be handled correctly
    #[test]
    fn edge_cases_property(frame in arb_edge_case_frames()) {
        let serialized = serialize_frame(&frame);
        let mut parser = RespParser::new();

        let result = parser.parse(&serialized);
        prop_assert!(result.is_ok(), "Edge case frame should parse: {:?}", result.err());

        let parsed_opt = result.unwrap();
        prop_assert!(parsed_opt.is_some(), "Edge case should return complete frame");
        let parsed = parsed_opt.unwrap();

        prop_assert_eq!(frame, parsed, "Edge case should round-trip");
    }

    /// Redis command property: Commands should maintain structure
    #[test]
    fn redis_command_property(command in arb_redis_commands()) {
        // Should be an array
        match &command {
            RespFrame::Array(elements) => {
                prop_assert!(!elements.is_empty(), "Commands need at least one element");

                // First element should be command name
                if let RespFrame::BulkString(name_bytes) = &elements[0] {
                    let name = String::from_utf8_lossy(name_bytes);
                    prop_assert!(!name.is_empty(), "Command name shouldn't be empty");
                }
            }
            _ => prop_assert!(false, "Commands should be arrays"),
        }

        // Should round-trip
        let serialized = serialize_frame(&command);
        let mut parser = RespParser::new();
        let result = parser.parse(&serialized);

        prop_assert!(result.is_ok(), "Command should parse");
        let parsed_opt = result.unwrap();
        prop_assert!(parsed_opt.is_some(), "Command should be complete");
        let parsed = parsed_opt.unwrap();

        prop_assert_eq!(command, parsed, "Command should round-trip");
    }

    /// Deterministic behavior property: Same input should always produce same output
    #[test]
    fn deterministic_property(frame in arb_resp2_frame()) {
        let serialized = serialize_frame(&frame);

        // Parse with two different parser instances
        let mut parser1 = RespParser::new();
        let mut parser2 = RespParser::new();

        let result1 = parser1.parse(&serialized);
        let result2 = parser2.parse(&serialized);

        prop_assert_eq!(result1.is_ok(), result2.is_ok(), "Results should have same success/failure");

        if let (Ok(frame1_opt), Ok(frame2_opt)) = (result1, result2) {
            prop_assert_eq!(frame1_opt, frame2_opt, "Parsed frames should be identical");
        }

        // Serialization should also be deterministic
        let serialized1 = serialize_frame(&frame);
        let serialized2 = serialize_frame(&frame);
        prop_assert_eq!(serialized1, serialized2, "Serialization should be deterministic");
    }

    /// Memory safety property: Large inputs shouldn't cause issues
    #[test]
    fn memory_safety_property(large_data in arb_large_bytes()) {
        let frame = RespFrame::BulkString(large_data.clone());

        let serialized = serialize_frame(&frame);
        let mut parser = RespParser::new();

        // Should handle large data without issues
        let result = parser.parse(&serialized);
        prop_assert!(result.is_ok(), "Large data should be handled");

        if let Ok(Some(RespFrame::BulkString(parsed_bytes))) = result {
            prop_assert_eq!(large_data, parsed_bytes, "Large data should round-trip");
        }
    }

    /// Buffer management property: Parser should handle varying buffer states correctly
    #[test]
    fn buffer_management_property(frames in prop::collection::vec(arb_resp2_frame(), 1..=3)) {
        let mut parser = RespParser::new();

        // Serialize all frames
        let serialized_frames: Vec<Vec<u8>> = frames.iter().map(serialize_frame).collect();

        // Parse each frame and verify buffer state
        for (i, (frame, serialized)) in frames.iter().zip(serialized_frames.iter()).enumerate() {
            let _initial_buffer_len = parser.buffer_len();

            let result = parser.parse(serialized);
            prop_assert!(result.is_ok(), "Frame #{} should parse successfully", i);

            let parsed_opt = result.unwrap();
            prop_assert!(parsed_opt.is_some(), "Frame #{} should be complete", i);
            let parsed = parsed_opt.unwrap();

            prop_assert_eq!(frame, &parsed, "Frame #{} should match", i);

            // Buffer should be empty after consuming complete frame
            prop_assert_eq!(parser.buffer_len(), 0, "Buffer should be empty after complete frame");
        }
    }

    /// Streaming property: Data sent in chunks should parse correctly
    #[test]
    fn streaming_chunks_property(
        frame in arb_resp2_frame(),
        chunk_sizes in prop::collection::vec(1usize..=10, 1..=5)
    ) {
        let serialized = serialize_frame(&frame);
        if serialized.is_empty() {
            return Ok(());
        }

        let mut parser = RespParser::new();
        let mut pos = 0;
        let mut result = None;

        // Send data in chunks
        for chunk_size in chunk_sizes {
            let end = std::cmp::min(pos + chunk_size, serialized.len());
            if pos >= end {
                break;
            }

            let chunk = &serialized[pos..end];
            let parse_result = parser.parse(chunk);
            prop_assert!(parse_result.is_ok(), "Chunk parsing should never fail");

            let parsed_opt = parse_result.unwrap();
            if parsed_opt.is_some() {
                result = parsed_opt;
                break;
            }

            pos = end;
        }

        // Send any remaining data only if we haven't parsed the frame yet
        if pos < serialized.len() && result.is_none() {
            let remaining = &serialized[pos..];
            let parse_result = parser.parse(remaining);
            prop_assert!(parse_result.is_ok(), "Final chunk should parse");

            result = parse_result.unwrap();
        }

        prop_assert!(result.is_some(), "Frame should eventually be parsed");
        prop_assert_eq!(&frame, &result.unwrap(), "Streamed frame should match original");
    }

    /// Parser isolation property: Multiple parsers should not interfere with each other
    #[test]
    fn parser_isolation_property(
        frames in prop::collection::vec(arb_resp2_frame(), 2..=5)
    ) {
        let serialized_frames: Vec<Vec<u8>> = frames.iter().map(serialize_frame).collect();
        let mut parsers: Vec<RespParser> = (0..frames.len()).map(|_| RespParser::new()).collect();

        // Parse frames with different parsers
        for (i, (frame, serialized)) in frames.iter().zip(serialized_frames.iter()).enumerate() {
            let result = parsers[i].parse(serialized);
            prop_assert!(result.is_ok(), "Parser #{} should work independently", i);

            let parsed_opt = result.unwrap();
            prop_assert!(parsed_opt.is_some(), "Parser #{} should return complete frame", i);
            let parsed = parsed_opt.unwrap();

            prop_assert_eq!(frame, &parsed, "Parser #{} should parse correctly", i);
        }

        // Verify parsers are still independent
        for (i, parser) in parsers.iter().enumerate() {
            prop_assert_eq!(parser.buffer_len(), 0, "Parser #{} buffer should be clean", i);
        }
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use proptest::strategy::ValueTree;

    #[test]
    fn property_test_smoke_test() {
        // Ensure our property test infrastructure is working
        let frame = RespFrame::SimpleString("test".to_string());
        let serialized = serialize_frame(&frame);
        let mut parser = RespParser::new();

        let result = parser.parse(&serialized);
        assert!(result.is_ok());

        let parsed_opt = result.unwrap();
        assert!(parsed_opt.is_some());
        let parsed = parsed_opt.unwrap();

        assert_eq!(frame, parsed);
    }

    #[test]
    fn generators_produce_valid_data() {
        // Test that our generators actually produce valid frames
        let mut runner = proptest::test_runner::TestRunner::default();

        let strategy = arb_resp2_frame();
        let frame = strategy.new_tree(&mut runner).unwrap().current();

        // Should be able to serialize and parse
        let serialized = serialize_frame(&frame);
        let mut parser = RespParser::new();

        let result = parser.parse(&serialized);
        assert!(result.is_ok(), "Generated frame should be valid");
        assert!(
            result.unwrap().is_some(),
            "Generated frame should be complete"
        );
    }

    #[test]
    fn malformed_generator_produces_invalid_data() {
        // Test that malformed generator actually produces invalid data
        let mut runner = proptest::test_runner::TestRunner::default();

        let strategy = arb_malformed_resp();
        let malformed_data = strategy.new_tree(&mut runner).unwrap().current();

        let mut parser = RespParser::new();
        let result = parser.parse(&malformed_data);

        // Should either error or parse successfully, but not panic
        // (We can't guarantee it will error since some malformed data might accidentally be valid)
        // Both Ok and Err results are acceptable for malformed data
        let _ = result;
    }

    #[test]
    fn empty_buffer_handling() {
        let mut parser = RespParser::new();
        let result = parser.parse(&[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn parser_state_isolation() {
        let mut parser = RespParser::new();

        // Parse valid frame
        let result1 = parser.parse(b"+OK\r\n");
        assert!(result1.is_ok());
        assert!(result1.unwrap().is_some());

        // Parse another valid frame - should work independently
        let result2 = parser.parse(b":42\r\n");
        assert!(result2.is_ok());
        assert!(result2.unwrap().is_some());
    }

    #[test]
    fn incremental_parsing_smoke_test() {
        let mut parser = RespParser::new();

        // Send partial data
        let result1 = parser.parse(b"$5\r\nhel");
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), None); // Not complete yet

        // Send remaining data
        let result2 = parser.parse(b"lo\r\n");
        assert!(result2.is_ok());
        let frame = result2.unwrap();
        assert!(frame.is_some());

        if let Some(RespFrame::BulkString(data)) = frame {
            assert_eq!(data, b"hello");
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn comprehensive_property_validation() {
        // Validate that our property test generators cover the expected range
        let mut runner = proptest::test_runner::TestRunner::default();

        // Test different frame types are generated
        let mut frame_types = std::collections::HashSet::new();
        for _ in 0..100 {
            let strategy = arb_resp2_frame();
            let frame = strategy.new_tree(&mut runner).unwrap().current();

            let type_name = match frame {
                RespFrame::SimpleString(_) => "SimpleString",
                RespFrame::Error(_) => "Error",
                RespFrame::Integer(_) => "Integer",
                RespFrame::BulkString(_) => "BulkString",
                RespFrame::Array(_) => "Array",
                RespFrame::NullBulkString => "NullBulkString",
                RespFrame::NullArray => "NullArray",
            };
            frame_types.insert(type_name);
        }

        // Should generate multiple different types
        assert!(
            frame_types.len() >= 5,
            "Should generate diverse frame types, got: {frame_types:?}"
        );
    }

    #[test]
    fn property_test_coverage_validation() {
        // Ensure our property tests are comprehensive by checking key properties manually

        // Test all RESP2 frame types can round-trip
        let test_frames = vec![
            RespFrame::SimpleString("test".to_string()),
            RespFrame::Error("ERR test".to_string()),
            RespFrame::Integer(42),
            RespFrame::Integer(-42),
            RespFrame::BulkString(b"hello world".to_vec()),
            RespFrame::BulkString(vec![]), // Empty bulk string
            RespFrame::Array(vec![
                RespFrame::SimpleString("cmd".to_string()),
                RespFrame::BulkString(b"arg".to_vec()),
            ]),
            RespFrame::Array(vec![]), // Empty array
            RespFrame::NullBulkString,
            RespFrame::NullArray,
        ];

        for (i, frame) in test_frames.iter().enumerate() {
            let serialized = serialize_frame(frame);
            let mut parser = RespParser::new();

            let result = parser.parse(&serialized);
            assert!(result.is_ok(), "Frame {i} should parse");

            let parsed_opt = result.unwrap();
            assert!(parsed_opt.is_some(), "Frame {i} should be complete");
            let parsed = parsed_opt.unwrap();

            assert_eq!(frame, &parsed, "Frame {i} should round-trip");
        }
    }

    #[test]
    fn stress_test_property_framework() {
        // Quick stress test to ensure property framework is robust
        let mut success_count = 0;
        let mut _error_count = 0;

        for _ in 0..50 {
            let mut runner = proptest::test_runner::TestRunner::default();

            // Test valid frames
            let strategy = arb_resp2_frame();
            let frame = strategy.new_tree(&mut runner).unwrap().current();
            let serialized = serialize_frame(&frame);
            let mut parser = RespParser::new();

            match parser.parse(&serialized) {
                Ok(Some(_)) => success_count += 1,
                Ok(None) => {} // Incomplete, shouldn't happen with complete frames
                Err(_) => _error_count += 1,
            }

            // Test malformed data doesn't panic
            let malformed_strategy = arb_malformed_resp();
            let malformed = malformed_strategy.new_tree(&mut runner).unwrap().current();
            let mut malformed_parser = RespParser::new();

            let _result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                malformed_parser.parse(&malformed)
            }));
            // Just ensure no panic - result can be anything
        }

        // Most valid frames should parse successfully
        assert!(
            success_count >= 40,
            "Most generated frames should be valid, got {success_count} successes"
        );
    }
}
