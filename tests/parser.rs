//! Parser test suite entry point
//!
//! This file makes all parser tests in tests/parser/ discoverable by cargo test.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(clippy::all)]

// Test modules
mod parser {
    // Test adapter for compatibility with migrated tests
    pub mod test_adapter;

    // Property-based testing modules
    pub mod property;

    // Test utilities
    pub mod test_utils;
}

// Include individual test files
#[path = "parser/parser_integration.rs"]
mod parser_integration;

#[path = "parser/parser_large_payload.rs"]
mod parser_large_payload;

#[path = "parser/parser_property.rs"]
mod parser_property;

#[path = "parser/parser_resp3_compliance.rs"]
mod parser_resp3_compliance;
