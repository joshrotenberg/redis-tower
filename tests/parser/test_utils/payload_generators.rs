//! Payload generation utilities for large payload testing
//!
//! This module provides utilities for generating test data of various sizes
//! and types for comprehensive performance testing.

use redis_tower::parser::RespFrame;
use std::collections::HashMap;

/// Pattern types for data generation
#[derive(Debug, Clone)]
pub enum DataPattern {
    /// Repeating alphanumeric pattern
    Alphanumeric,
    /// Random-like pattern (deterministic for reproducibility)
    Pseudorandom,
    /// Specific byte value repeated
    Repeated(u8),
    /// Binary pattern with all byte values
    Binary,
}

/// Generator for test payloads of various sizes and types
pub struct PayloadGenerator {
    seed: u64,
}

impl PayloadGenerator {
    /// Create a new payload generator with a seed for reproducible results
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Generate bulk string data of specified size with pattern
    pub fn generate_bulk_string_data(&self, size: usize, pattern: DataPattern) -> Vec<u8> {
        match pattern {
            DataPattern::Alphanumeric => self.generate_alphanumeric(size),
            DataPattern::Pseudorandom => self.generate_pseudorandom(size),
            DataPattern::Repeated(byte) => vec![byte; size],
            DataPattern::Binary => self.generate_binary_pattern(size),
        }
    }

    /// Generate RESP bulk string frame
    pub fn generate_bulk_string(&self, size: usize, pattern: DataPattern) -> RespFrame {
        let data = self.generate_bulk_string_data(size, pattern);
        RespFrame::BulkString(data)
    }

    /// Generate array with specified number of elements
    pub fn generate_array(
        &self,
        element_count: usize,
        element_type: ArrayElementType,
    ) -> RespFrame {
        let mut elements = Vec::with_capacity(element_count);

        for i in 0..element_count {
            match element_type {
                ArrayElementType::Integers => {
                    elements.push(RespFrame::Integer(i as i64));
                }
                ArrayElementType::Strings => {
                    elements.push(RespFrame::SimpleString(format!("item{}", i)));
                }
                ArrayElementType::BulkStrings => {
                    elements.push(RespFrame::BulkString(format!("bulk{}", i).into_bytes()));
                }
                ArrayElementType::Mixed => match i % 5 {
                    0 => elements.push(RespFrame::Integer(i as i64)),
                    1 => elements.push(RespFrame::SimpleString(format!("item{}", i))),
                    2 => elements.push(RespFrame::BulkString(format!("bulk{}", i).into_bytes())),
                    3 => elements.push(RespFrame::NullBulkString),
                    _ => elements.push(RespFrame::Error(format!("error{}", i))),
                },
                ArrayElementType::Nested => {
                    // Create nested arrays (2 levels deep)
                    let nested = RespFrame::Array(vec![
                        RespFrame::Integer(i as i64),
                        RespFrame::SimpleString(format!("nested{}", i)),
                    ]);
                    elements.push(nested);
                }
            }
        }

        RespFrame::Array(elements)
    }

    /// Generate deeply nested array structure
    pub fn generate_nested_array(&self, depth: usize, elements_per_level: usize) -> RespFrame {
        if depth == 0 {
            return RespFrame::Integer(42);
        }

        let mut elements = Vec::with_capacity(elements_per_level);
        for i in 0..elements_per_level {
            if i == 0 {
                // First element is nested
                elements.push(self.generate_nested_array(depth - 1, elements_per_level));
            } else {
                // Other elements are simple
                elements.push(RespFrame::SimpleString(format!("level{}_{}", depth, i)));
            }
        }

        RespFrame::Array(elements)
    }

    /// Generate complex structure with multiple data types
    pub fn generate_complex_structure(&self, complexity: ComplexityLevel) -> RespFrame {
        match complexity {
            ComplexityLevel::Simple => RespFrame::Array(vec![
                RespFrame::SimpleString("simple".to_string()),
                RespFrame::Integer(42),
                RespFrame::BulkString(b"data".to_vec()),
            ]),
            ComplexityLevel::Medium => RespFrame::Array(vec![
                self.generate_array(100, ArrayElementType::Mixed),
                self.generate_bulk_string(1024, DataPattern::Alphanumeric),
                RespFrame::Array(vec![
                    RespFrame::Integer(1),
                    RespFrame::Integer(2),
                    RespFrame::Integer(3),
                ]),
            ]),
            ComplexityLevel::High => RespFrame::Array(vec![
                self.generate_array(1000, ArrayElementType::Mixed),
                self.generate_bulk_string(10240, DataPattern::Pseudorandom),
                self.generate_nested_array(5, 3),
                RespFrame::Array((0..100).map(|i| RespFrame::Integer(i)).collect()),
            ]),
        }
    }

    /// Generate test data for streaming scenarios
    pub fn generate_streaming_chunks(&self, total_size: usize, chunk_size: usize) -> Vec<Vec<u8>> {
        let data = self.generate_alphanumeric(total_size);
        let mut chunks = Vec::new();

        for chunk in data.chunks(chunk_size) {
            chunks.push(chunk.to_vec());
        }

        chunks
    }

    /// Generate Redis command-like structures
    pub fn generate_redis_command(&self, command: &str, args: &[&str]) -> RespFrame {
        let mut elements = vec![RespFrame::BulkString(command.as_bytes().to_vec())];

        for arg in args {
            elements.push(RespFrame::BulkString(arg.as_bytes().to_vec()));
        }

        RespFrame::Array(elements)
    }

    /// Generate multiple frames for sequential parsing tests
    pub fn generate_frame_sequence(&self, count: usize) -> Vec<RespFrame> {
        let mut frames = Vec::with_capacity(count);

        for i in 0..count {
            match i % 6 {
                0 => frames.push(RespFrame::SimpleString(format!("OK{}", i))),
                1 => frames.push(RespFrame::Integer(i as i64)),
                2 => frames.push(RespFrame::BulkString(format!("data{}", i).into_bytes())),
                3 => frames.push(RespFrame::Error(format!("ERR{}", i))),
                4 => frames.push(RespFrame::NullBulkString),
                _ => frames.push(RespFrame::Array(vec![
                    RespFrame::Integer(i as i64),
                    RespFrame::SimpleString(format!("item{}", i)),
                ])),
            }
        }

        frames
    }

    // Private helper methods
    fn generate_alphanumeric(&self, size: usize) -> Vec<u8> {
        let pattern = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let mut data = Vec::with_capacity(size);

        for i in 0..size {
            data.push(pattern[i % pattern.len()]);
        }

        data
    }

    fn generate_pseudorandom(&self, size: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);
        let mut state = self.seed;

        for _ in 0..size {
            // Simple LCG for reproducible "random" data
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            data.push((state >> 16) as u8);
        }

        data
    }

    fn generate_binary_pattern(&self, size: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);

        for i in 0..size {
            data.push((i % 256) as u8);
        }

        data
    }
}

/// Types of array elements for generation
#[derive(Debug, Clone, Copy)]
pub enum ArrayElementType {
    Integers,
    Strings,
    BulkStrings,
    Mixed,
    Nested,
}

/// Complexity levels for structure generation
#[derive(Debug, Clone, Copy)]
pub enum ComplexityLevel {
    Simple,
    Medium,
    High,
}

/// Predefined test scenarios
pub struct TestScenarios;

impl TestScenarios {
    /// Get standard bulk string test sizes
    pub fn bulk_string_sizes() -> Vec<usize> {
        vec![
            1024,             // 1KB
            10 * 1024,        // 10KB
            100 * 1024,       // 100KB
            1024 * 1024,      // 1MB
            10 * 1024 * 1024, // 10MB
        ]
    }

    /// Get standard array element counts
    pub fn array_element_counts() -> Vec<usize> {
        vec![100, 1000, 10000, 100000]
    }

    /// Get standard chunk sizes for streaming tests
    pub fn streaming_chunk_sizes() -> Vec<usize> {
        vec![64, 256, 1024, 4096, 16384]
    }

    /// Get nested array depths for testing
    pub fn nested_depths() -> Vec<usize> {
        vec![2, 5, 10, 20]
    }
}

/// Performance test configuration
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub payload_size: usize,
    pub chunk_size: Option<usize>,
    pub element_count: Option<usize>,
    pub nesting_depth: Option<usize>,
    pub iterations: usize,
    pub pattern: DataPattern,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            payload_size: 1024,
            chunk_size: None,
            element_count: None,
            nesting_depth: None,
            iterations: 1,
            pattern: DataPattern::Alphanumeric,
        }
    }
}

impl TestConfig {
    /// Create config for bulk string testing
    pub fn bulk_string(size: usize) -> Self {
        Self {
            payload_size: size,
            ..Default::default()
        }
    }

    /// Create config for array testing
    pub fn array(element_count: usize) -> Self {
        Self {
            element_count: Some(element_count),
            ..Default::default()
        }
    }

    /// Create config for streaming testing
    pub fn streaming(payload_size: usize, chunk_size: usize) -> Self {
        Self {
            payload_size,
            chunk_size: Some(chunk_size),
            ..Default::default()
        }
    }

    /// Create config for nested structure testing
    pub fn nested(depth: usize) -> Self {
        Self {
            nesting_depth: Some(depth),
            ..Default::default()
        }
    }

    /// Set number of iterations
    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    /// Set data pattern
    pub fn with_pattern(mut self, pattern: DataPattern) -> Self {
        self.pattern = pattern;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_generator_basic() {
        let generator = PayloadGenerator::new(12345);

        // Test alphanumeric generation
        let data = generator.generate_bulk_string_data(100, DataPattern::Alphanumeric);
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b.is_ascii_alphanumeric()));

        // Test repeated pattern
        let data = generator.generate_bulk_string_data(50, DataPattern::Repeated(b'X'));
        assert_eq!(data.len(), 50);
        assert!(data.iter().all(|&b| b == b'X'));
    }

    #[test]
    fn test_array_generation() {
        let generator = PayloadGenerator::new(12345);

        let frame = generator.generate_array(10, ArrayElementType::Integers);
        if let RespFrame::Array(elements) = frame {
            assert_eq!(elements.len(), 10);
            assert!(elements.iter().all(|e| matches!(e, RespFrame::Integer(_))));
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_nested_array_generation() {
        let generator = PayloadGenerator::new(12345);

        let frame = generator.generate_nested_array(3, 2);
        if let RespFrame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
            // First element should be nested
            assert!(matches!(elements[0], RespFrame::Array(_)));
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_reproducible_generation() {
        let generator1 = PayloadGenerator::new(12345);
        let generator2 = PayloadGenerator::new(12345);

        let data1 = generator1.generate_bulk_string_data(100, DataPattern::Pseudorandom);
        let data2 = generator2.generate_bulk_string_data(100, DataPattern::Pseudorandom);

        assert_eq!(data1, data2);
    }

    #[test]
    fn test_redis_command_generation() {
        let generator = PayloadGenerator::new(12345);

        let frame = generator.generate_redis_command("SET", &["key", "value"]);
        if let RespFrame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], RespFrame::BulkString(_)));
            assert!(matches!(elements[1], RespFrame::BulkString(_)));
            assert!(matches!(elements[2], RespFrame::BulkString(_)));
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_streaming_chunks() {
        let generator = PayloadGenerator::new(12345);

        let chunks = generator.generate_streaming_chunks(100, 30);
        assert_eq!(chunks.len(), 4); // 100 / 30 = 3.33, so 4 chunks
        assert_eq!(chunks[0].len(), 30);
        assert_eq!(chunks[1].len(), 30);
        assert_eq!(chunks[2].len(), 30);
        assert_eq!(chunks[3].len(), 10); // remainder
    }

    #[test]
    fn test_config_builders() {
        let config = TestConfig::bulk_string(1024)
            .with_iterations(10)
            .with_pattern(DataPattern::Binary);

        assert_eq!(config.payload_size, 1024);
        assert_eq!(config.iterations, 10);
        assert!(matches!(config.pattern, DataPattern::Binary));
    }
}
