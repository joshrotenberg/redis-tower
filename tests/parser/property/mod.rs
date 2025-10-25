//! Property-based testing for RESP parser
//!
//! This module contains property tests using proptest to validate parser correctness
//! across a wide range of inputs and edge cases.
//!
//! ## Memory Usage Configuration
//!
//! By default, property tests use conservative memory limits to be good citizens:
//! - Standard tests: 200 cases, 2KB max data size
//! - Memory-intensive tests: 50 cases, 1KB max data size
//! - CI environments: Further reduced to minimize resource usage
//!
//! ### Customizing Test Intensity
//!
//! For more intensive testing, set environment variables:
//!
//! ```bash
//! # Increase test cases and data sizes
//! export PROPTEST_CASES=1000
//! export PROPTEST_MAX_DATA_SIZE=10000
//!
//! # Run with intensive settings
//! cargo test property_tests
//! ```
//!
//! Available environment variables:
//! - `PROPTEST_CASES`: Number of test cases (default: 200, CI: 100)
//! - `PROPTEST_MAX_SHRINK_ITERS`: Shrinking iterations (default: 2000, CI: 1000)
//! - `PROPTEST_MAX_DATA_SIZE`: Maximum data size in bytes (default: 2048, CI: 1024)
//! - `PROPTEST_LARGE_DATA_SIZE`: Large data size for stress tests (default: 4096, CI: 2048)

use proptest::prelude::*;

pub mod generators;
pub mod properties;

// Re-export key testing utilities
pub use generators::*;

/// Get the number of test cases to run
fn get_proptest_cases() -> u32 {
    if let Ok(cases_str) = std::env::var("PROPTEST_CASES") {
        cases_str.parse().unwrap_or_else(|_| {
            eprintln!("Warning: Invalid PROPTEST_CASES value '{cases_str}', using default");
            get_default_cases()
        })
    } else {
        get_default_cases()
    }
}

/// Get default test case count based on environment
fn get_default_cases() -> u32 {
    if std::env::var("CI").is_ok() {
        100 // Conservative for CI
    } else {
        200 // Reasonable default for local development
    }
}

/// Get the maximum number of shrink iterations
fn get_proptest_max_shrink_iters() -> u32 {
    if let Ok(iters_str) = std::env::var("PROPTEST_MAX_SHRINK_ITERS") {
        iters_str.parse().unwrap_or_else(|_| {
            eprintln!(
                "Warning: Invalid PROPTEST_MAX_SHRINK_ITERS value '{iters_str}', using default"
            );
            get_default_shrink_iters()
        })
    } else {
        get_default_shrink_iters()
    }
}

/// Get default shrink iterations based on environment
fn get_default_shrink_iters() -> u32 {
    if std::env::var("CI").is_ok() {
        1000 // Conservative for CI
    } else {
        2000 // Reasonable default for local development
    }
}

/// Get maximum data size for standard tests
pub fn get_max_data_size() -> usize {
    if let Ok(size_str) = std::env::var("PROPTEST_MAX_DATA_SIZE") {
        size_str.parse().unwrap_or_else(|_| {
            eprintln!("Warning: Invalid PROPTEST_MAX_DATA_SIZE value '{size_str}', using default");
            get_default_data_size()
        })
    } else {
        get_default_data_size()
    }
}

/// Get default data size based on environment
fn get_default_data_size() -> usize {
    if std::env::var("CI").is_ok() {
        1024 // 1KB - very conservative for CI
    } else {
        2048 // 2KB - reasonable for local development
    }
}

/// Get maximum data size for large data stress tests
pub fn get_large_data_size() -> usize {
    if let Ok(size_str) = std::env::var("PROPTEST_LARGE_DATA_SIZE") {
        size_str.parse().unwrap_or_else(|_| {
            eprintln!(
                "Warning: Invalid PROPTEST_LARGE_DATA_SIZE value '{size_str}', using default"
            );
            get_default_large_data_size()
        })
    } else {
        get_default_large_data_size()
    }
}

/// Get default large data size based on environment
fn get_default_large_data_size() -> usize {
    if std::env::var("CI").is_ok() {
        2048 // 2KB - conservative for CI
    } else {
        4096 // 4KB - reasonable for local development
    }
}

/// Property test configuration optimized for RESP testing
/// Uses conservative defaults to be a good citizen
pub fn proptest_config() -> ProptestConfig {
    ProptestConfig {
        cases: get_proptest_cases(),
        max_shrink_iters: get_proptest_max_shrink_iters(),
        ..ProptestConfig::default()
    }
}

/// Lightweight configuration for memory-intensive tests
/// Uses even more conservative settings to minimize memory usage
pub fn lightweight_proptest_config() -> ProptestConfig {
    let base_cases = get_proptest_cases();
    let base_shrinks = get_proptest_max_shrink_iters();

    ProptestConfig {
        cases: base_cases / 4,              // 25% of standard cases
        max_shrink_iters: base_shrinks / 2, // 50% of standard shrink iterations
        ..ProptestConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #![proptest_config(proptest_config())]

        /// Basic smoke test - ensure proptest is working
        #[test]
        fn proptest_smoke_test(s in ".*") {
            // This should always pass, just ensuring proptest integration works
            prop_assert!(s.is_empty() || !s.is_empty());
        }
    }
}
