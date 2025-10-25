# Property-Based Testing Suite for RESP Parser

This directory contains a comprehensive property-based testing suite for the RESP (Redis Serialization Protocol) parser using the `proptest` crate. Property-based testing validates that the parser maintains correctness across a wide range of inputs by testing fundamental properties rather than specific test cases.

## Memory-Friendly Configuration

This test suite uses **conservative memory defaults** to be a good citizen:
- **Standard tests**: 200 cases, 2KB max data size
- **Memory-intensive tests**: 50 cases, 1KB max data size  
- **CI environments**: Further reduced limits (100 cases, 1KB data)

### Customizing Test Intensity

For more intensive testing, set environment variables:

```bash
# Increase test cases and data sizes for thorough testing
export PROPTEST_CASES=1000
export PROPTEST_MAX_DATA_SIZE=10000
export PROPTEST_LARGE_DATA_SIZE=50000

# Run with intensive settings
cargo test property_tests
```

**Available Environment Variables:**
- `PROPTEST_CASES`: Number of test cases (default: 200, CI: 100)
- `PROPTEST_MAX_SHRINK_ITERS`: Shrinking iterations (default: 2000, CI: 1000)
- `PROPTEST_MAX_DATA_SIZE`: Maximum data size in bytes (default: 2048, CI: 1024)
- `PROPTEST_LARGE_DATA_SIZE`: Large data size for stress tests (default: 4096, CI: 2048)

**Memory Usage Examples:**
```bash
# Conservative (good for laptops, CI)
cargo test property_tests  # Uses defaults

# Moderate (desktop development)
PROPTEST_CASES=500 PROPTEST_MAX_DATA_SIZE=8192 cargo test property_tests

# Intensive (powerful machines, before releases)
PROPTEST_CASES=2000 PROPTEST_MAX_DATA_SIZE=32768 PROPTEST_LARGE_DATA_SIZE=1048576 cargo test property_tests
```

## Overview

Property-based testing complements traditional unit tests by:
- **Generating thousands of test cases automatically** from property specifications
- **Finding edge cases** that manual tests might miss
- **Validating fundamental correctness properties** like round-trip consistency
- **Providing confidence** in parser robustness across diverse inputs

## Test Structure

### Core Files

- **`mod.rs`** - Module configuration and basic property test setup
- **`generators.rs`** - Input generators for creating valid and invalid RESP data
- **`properties.rs`** - Core property tests organized by testing category
- **`../property_tests.rs`** - Integration tests and high-level property validation

### Test Categories

#### 1. Round-Trip Properties (`round_trip_properties`)
- **Frame Round-Trip**: `serialize(parse(serialize(frame))) == serialize(frame)`
- **Parsing Determinism**: Same input always produces same output
- **Serialization Consistency**: Multiple serializations of same frame are identical

#### 2. Incremental Parsing Properties (`incremental_parsing_properties`)
- **Partial Data Handling**: Parser correctly handles incomplete frames
- **Parser Reuse**: Multiple parsing operations on same parser work correctly
- **Buffer Management**: Internal buffer state is managed properly

#### 3. Error Handling Properties (`error_handling_properties`)
- **No Panic Guarantee**: Invalid input never causes panics
- **Edge Case Handling**: Special cases (empty arrays, null values, etc.) work correctly
- **Memory Safety**: Large inputs don't cause memory issues
- **Problematic Pattern Handling**: Binary data, control characters, etc. are handled gracefully

#### 4. Redis Command Properties (`redis_command_properties`)
- **Command Structure**: Redis commands maintain proper array structure
- **Command Validation**: Generated commands have reasonable structure

#### 5. Serialization Properties (`serialization_properties`)
- **Deterministic Serialization**: Same frame always serializes identically
- **Valid RESP Output**: Serialized data conforms to RESP protocol
- **Size Estimation**: Size hints are reasonably accurate

## Generators

### Valid Data Generators
- **`arb_resp2_frame()`** - Generates all valid RESP2 frame types
- **`arb_redis_commands()`** - Generates realistic Redis command patterns
- **`arb_edge_case_frames()`** - Generates frames with edge case characteristics

### Invalid Data Generators
- **`arb_malformed_resp()`** - Generates invalid RESP data for robustness testing
- **`arb_partial_resp()`** - Generates incomplete frames for incremental parsing tests
- **`arb_problematic_patterns()`** - Generates data patterns that commonly cause issues

### Utility Generators
- **`arb_string()`**, **`arb_ascii_string()`** - String generators with different characteristics
- **`arb_bytes()`**, **`arb_large_bytes()`** - Binary data generators
- **`serialize_frame()`** - Helper function for consistent frame serialization

## Key Properties Tested

### 1. Correctness Properties
```rust
// Every valid frame should round-trip perfectly
forall frame: RespFrame. 
  parse(serialize(frame)) == Some(frame)

// Parsing should be deterministic
forall data: &[u8]. 
  parse(data) == parse(data)

// Serialization should be deterministic  
forall frame: RespFrame.
  serialize(frame) == serialize(frame)
```

### 2. Robustness Properties
```rust
// Parser should never panic on any input
forall data: &[u8].
  no_panic(parse(data))

// Partial data should be handled gracefully
forall (partial, complete): (&[u8], &[u8]).
  parse(partial) succeeds OR parse(partial + complete) succeeds
```

### 3. Performance Properties
```rust
// Large inputs should be handled efficiently
forall large_data: Vec<u8> where large_data.len() > 10MB.
  parse(serialize(BulkString(large_data))) succeeds in reasonable time

// Size estimates should be accurate
forall frame: RespFrame.
  |frame.size_hint() - serialize(frame).len()| <= tolerance
```

## Default Configuration

The property tests are configured with conservative defaults:
- **Test Cases**: 200 per property (100 in CI environments)
- **Data Size**: 2KB maximum (1KB in CI environments)
- **Large Data**: 4KB maximum for stress tests (2KB in CI)
- **Shrinking**: Up to 2000 iterations to find minimal failing cases (1000 in CI)

These defaults ensure reasonable memory usage while maintaining good test coverage. Use environment variables (see above) to increase intensity when needed.

## Running Property Tests

```bash
# Run all property tests (with conservative defaults)
cargo test --test property_tests

# Run specific property test category
cargo test --test property_tests round_trip

# Run with increased intensity
PROPTEST_CASES=1000 PROPTEST_MAX_DATA_SIZE=10000 cargo test --test property_tests

# Run with maximum intensity (for release validation)
PROPTEST_CASES=5000 PROPTEST_MAX_DATA_SIZE=100000 PROPTEST_LARGE_DATA_SIZE=1000000 cargo test --test property_tests

# Run with verbose output to see generated test cases
cargo test --test property_tests -- --nocapture

# Check current configuration
PROPTEST_CASES=0 cargo test --test property_tests -- --list
```

## Interpreting Results

### Successful Tests
When property tests pass, they provide high confidence that:
- The parser correctly handles the tested property across thousands of inputs
- Edge cases are properly handled
- No regressions have been introduced

### Test Failures
When a property test fails, proptest will:
1. **Show the minimal failing case** after shrinking
2. **Save regression cases** to prevent future regressions
3. **Provide the exact input** that caused the failure

Example failure output:
```
Test failed: Frame should round-trip exactly
minimal failing input: frame = Array([BulkString([255, 0, 128])])
```

### Regression Testing
Failed test cases are automatically saved to `.proptest-regressions` files and re-run in future test executions to prevent regressions.

## Coverage Analysis

The property test suite validates:
- ✅ **All RESP2 frame types** (SimpleString, Error, Integer, BulkString, Array, Null types)
- ✅ **Binary safety** (handling of non-UTF8 data)
- ✅ **Edge cases** (empty collections, very large data, boundary values)
- ✅ **Error conditions** (malformed input, invalid lengths, missing terminators)
- ✅ **Streaming scenarios** (partial data, incremental parsing, buffer management)
- ✅ **Real-world patterns** (Redis commands, mixed-type arrays)

## Best Practices

### When Adding New Properties
1. **Start simple** - Test one fundamental property at a time
2. **Use appropriate generators** - Match input complexity to what you're testing
3. **Validate assumptions** - Include sanity checks in property tests
4. **Consider shrinking** - Write properties that help proptest find minimal failing cases

### Generator Design
1. **Balanced distribution** - Ensure generators cover edge cases and common cases
2. **Reasonable bounds** - Avoid generating excessively large data that slows tests
3. **Realistic patterns** - Model real-world usage patterns when appropriate

### Property Selection
1. **Focus on invariants** - Test properties that should always hold
2. **Test boundaries** - Include edge cases and limit conditions
3. **Complement unit tests** - Cover different aspects than deterministic tests

## Integration with CI/CD

Property tests are integrated into the standard test suite and run automatically on:
- **Pull requests** - Ensuring new changes don't break fundamental properties
- **Merge to main** - Validating release candidates
- **Nightly builds** - Extended testing with higher case counts

The property test suite typically takes 5-15 seconds to run with conservative default settings, making it suitable for regular CI execution. With increased intensity settings, tests may take several minutes but provide more thorough validation.

## Memory Considerations

The default configuration is designed to:
- **Minimize memory usage** (< 50MB total allocation)
- **Run efficiently on CI** (GitHub Actions, etc.)
- **Provide good coverage** while being resource-friendly
- **Allow customization** for more intensive local testing

When increasing test intensity, be aware that memory usage scales with:
- Number of test cases × maximum data size
- Shrinking iterations can temporarily increase memory usage
- Large data tests can allocate significant memory during stress testing