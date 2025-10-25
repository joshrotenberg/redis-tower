//! Property test generators for RESP frames and data structures
//!
//! This module provides generators for creating valid and invalid RESP frames
//! for comprehensive property-based testing.
//!
//! ## Memory Usage
//!
//! Generators use conservative memory limits by default to be good citizens.
//! See the module documentation for customization options.

use proptest::prelude::*;
use redis_tower::parser::RespFrame;

use super::{get_large_data_size, get_max_data_size};

/// Generate arbitrary valid strings for RESP testing
/// Uses configurable size limits (default: 2KB, CI: 1KB)
pub fn arb_string() -> impl Strategy<Value = String> {
    let max_size = get_max_data_size();
    prop::collection::vec(any::<u8>(), 0..max_size)
        .prop_map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

/// Generate ASCII-only strings (useful for simple strings)
pub fn arb_ascii_string() -> impl Strategy<Value = String> {
    "[\\x20-\\x7E]*"
}

/// Generate binary-safe byte vectors
/// Uses configurable size limits (default: 2KB, CI: 1KB)
pub fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    let max_size = get_max_data_size();
    prop::collection::vec(any::<u8>(), 0..max_size)
}

/// Generate large byte vectors for stress testing
/// Uses configurable large data size limits (default: 4KB, CI: 2KB)
/// Note: Set PROPTEST_LARGE_DATA_SIZE env var for more intensive testing
pub fn arb_large_bytes() -> impl Strategy<Value = Vec<u8>> {
    let max_size = get_large_data_size();
    let standard_size = get_max_data_size();
    // Ensure min_size is always less than max_size
    let min_size = std::cmp::min(standard_size / 2, max_size - 1);
    let actual_max = std::cmp::max(max_size, min_size + 1);
    prop::collection::vec(any::<u8>(), min_size..actual_max)
}

/// Generate valid integers in RESP range
pub fn arb_resp_integer() -> impl Strategy<Value = i64> {
    any::<i64>()
}

/// Generate RESP2 compatible frames
pub fn arb_resp2_frame() -> impl Strategy<Value = RespFrame> {
    let leaf = prop_oneof![
        arb_ascii_string().prop_map(RespFrame::SimpleString),
        arb_ascii_string().prop_map(RespFrame::Error),
        arb_resp_integer().prop_map(RespFrame::Integer),
        arb_bytes().prop_map(RespFrame::BulkString),
        Just(RespFrame::NullBulkString),
    ];

    leaf.prop_recursive(
        3,  // Max depth
        10, // Max elements per collection
        5,  // Items per collection
        |inner| {
            prop_oneof![
                // Arrays of other frames
                prop::collection::vec(inner.clone(), 0..=10).prop_map(RespFrame::Array),
                // Null arrays
                Just(RespFrame::NullArray),
            ]
        },
    )
}

/// Generate malformed RESP data for negative testing
pub fn arb_malformed_resp() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Missing CRLF
        arb_ascii_string().prop_map(|s| format!("+{s}").into_bytes()),
        // Invalid type markers
        arb_ascii_string().prop_map(|s| format!("@{s}\r\n").into_bytes()),
        // Malformed lengths
        Just(b"$-2\r\n".to_vec()),
        Just(b"*-2\r\n".to_vec()),
        // Length mismatch
        Just(b"$5\r\nhi\r\n".to_vec()),
        Just(b"*2\r\n$2\r\nhi\r\n".to_vec()),
        // Incomplete data
        arb_string().prop_map(|s| format!("${}\r\n{}", s.len() + 10, s).into_bytes()),
        // Invalid integers
        Just(b":not-a-number\r\n".to_vec()),
        // Deeply nested that might cause stack overflow
        (1..=100).prop_map(|depth| {
            let mut data = Vec::new();
            for _ in 0..depth {
                data.extend_from_slice(b"*1\r\n");
            }
            data.extend_from_slice(b"+OK\r\n");
            data
        }),
        // Random bytes that might accidentally be valid
        prop::collection::vec(any::<u8>(), 1..100),
        // Truncated frames
        Just(b"+OK\r".to_vec()),
        Just(b"$5\r\nhell".to_vec()),
        Just(b"*1\r\n+".to_vec()),
        // Oversized lengths (but reasonable for CI)
        Just(b"$100000\r\nshort\r\n".to_vec()),
        Just(b"*10000\r\n+OK\r\n".to_vec()),
    ]
}

/// Generate partially valid RESP data (for incremental parsing tests)
pub fn arb_partial_resp() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    arb_resp2_frame().prop_flat_map(|frame| {
        let serialized = serialize_frame(&frame);
        if serialized.len() <= 1 {
            return Just((serialized, vec![])).boxed();
        }

        (1..serialized.len())
            .prop_map(move |split_point| {
                let first_part = serialized[..split_point].to_vec();
                let second_part = serialized[split_point..].to_vec();
                (first_part, second_part)
            })
            .boxed()
    })
}

/// Generate edge case string sizes
/// Uses reasonable defaults with smaller buffer boundary tests
pub fn arb_edge_case_strings() -> impl Strategy<Value = String> {
    let max_data_size = get_max_data_size();
    let large_data_size = get_large_data_size();

    prop_oneof![
        // Empty string
        Just(String::new()),
        // Single character
        any::<char>().prop_map(|c| c.to_string()),
        // Small powers of 2 sizes (common buffer boundaries, but reasonable)
        Just("a".repeat(255)),
        Just("b".repeat(256)),
        Just("c".repeat(257)),
        Just("d".repeat(511)),
        Just("e".repeat(512)),
        Just("f".repeat(513)),
        // Medium sizes up to max_data_size
        Just("g".repeat(max_data_size / 2)),
        Just("h".repeat(max_data_size)),
        // Large strings for stress testing (configurable)
        {
            let min_large = std::cmp::max(max_data_size, 1024);
            let max_large = std::cmp::max(large_data_size, min_large + 1);
            (min_large..max_large).prop_map(|size| "x".repeat(size))
        },
    ]
}

/// Helper function to serialize frames for testing
pub fn serialize_frame(frame: &RespFrame) -> Vec<u8> {
    frame.serialize()
}

/// Generate frames that are specifically designed to test parser edge cases
pub fn arb_edge_case_frames() -> impl Strategy<Value = RespFrame> {
    prop_oneof![
        // Empty collections
        Just(RespFrame::Array(vec![])),
        Just(RespFrame::NullArray),
        Just(RespFrame::NullBulkString),
        // Single element collections
        arb_resp2_frame().prop_map(|f| RespFrame::Array(vec![f])),
        // Very large integers
        Just(RespFrame::Integer(i64::MAX)),
        Just(RespFrame::Integer(i64::MIN)),
        Just(RespFrame::Integer(0)),
        // Edge case strings
        arb_edge_case_strings().prop_map(|s| RespFrame::BulkString(s.into_bytes())),
        arb_edge_case_strings().prop_map(RespFrame::SimpleString),
        arb_edge_case_strings().prop_map(RespFrame::Error),
        // Deeply nested arrays (but not too deep to avoid stack overflow)
        (1..=10u32).prop_flat_map(|depth| {
            let mut current = RespFrame::SimpleString("deep".to_string());
            for _ in 0..depth {
                current = RespFrame::Array(vec![current]);
            }
            Just(current)
        }),
        // Arrays with mixed types
        prop::collection::vec(
            prop_oneof![
                arb_ascii_string().prop_map(RespFrame::SimpleString),
                arb_ascii_string().prop_map(RespFrame::Error),
                arb_resp_integer().prop_map(RespFrame::Integer),
                arb_bytes().prop_map(RespFrame::BulkString),
                Just(RespFrame::NullBulkString),
            ],
            0..=20
        )
        .prop_map(RespFrame::Array),
    ]
}

/// Strategy for generating realistic Redis command patterns
pub fn arb_redis_commands() -> impl Strategy<Value = RespFrame> {
    prop_oneof![
        // GET command
        (arb_string()).prop_map(|key| {
            RespFrame::Array(vec![
                RespFrame::BulkString("GET".to_string().into_bytes()),
                RespFrame::BulkString(key.into_bytes()),
            ])
        }),
        // SET command
        (arb_string(), arb_string()).prop_map(|(key, value)| {
            RespFrame::Array(vec![
                RespFrame::BulkString("SET".to_string().into_bytes()),
                RespFrame::BulkString(key.into_bytes()),
                RespFrame::BulkString(value.into_bytes()),
            ])
        }),
        // MGET command
        prop::collection::vec(arb_string(), 1..=10).prop_map(|keys| {
            let mut cmd = vec![RespFrame::BulkString("MGET".to_string().into_bytes())];
            cmd.extend(
                keys.into_iter()
                    .map(|k| RespFrame::BulkString(k.into_bytes())),
            );
            RespFrame::Array(cmd)
        }),
        // HSET command
        (arb_string(), arb_string(), arb_string()).prop_map(|(hash, field, value)| {
            RespFrame::Array(vec![
                RespFrame::BulkString("HSET".to_string().into_bytes()),
                RespFrame::BulkString(hash.into_bytes()),
                RespFrame::BulkString(field.into_bytes()),
                RespFrame::BulkString(value.into_bytes()),
            ])
        }),
        // LPUSH command
        (arb_string(), prop::collection::vec(arb_string(), 1..=5)).prop_map(|(key, values)| {
            let mut cmd = vec![
                RespFrame::BulkString("LPUSH".to_string().into_bytes()),
                RespFrame::BulkString(key.into_bytes()),
            ];
            cmd.extend(
                values
                    .into_iter()
                    .map(|v| RespFrame::BulkString(v.into_bytes())),
            );
            RespFrame::Array(cmd)
        }),
        // DEL command
        prop::collection::vec(arb_string(), 1..=5).prop_map(|keys| {
            let mut cmd = vec![RespFrame::BulkString("DEL".to_string().into_bytes())];
            cmd.extend(
                keys.into_iter()
                    .map(|k| RespFrame::BulkString(k.into_bytes())),
            );
            RespFrame::Array(cmd)
        }),
        // PING command (no args)
        Just(RespFrame::Array(vec![RespFrame::BulkString(
            "PING".to_string().into_bytes()
        )])),
        // PING command (with message)
        arb_string().prop_map(|message| {
            RespFrame::Array(vec![
                RespFrame::BulkString("PING".to_string().into_bytes()),
                RespFrame::BulkString(message.into_bytes()),
            ])
        }),
    ]
}
