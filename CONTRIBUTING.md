# Contributing to redis-tower

Thank you for your interest in contributing to redis-tower! This document provides guidelines and instructions for contributing.

## Code of Conduct

Be respectful, professional, and constructive in all interactions. We're here to build great software together.

## Development Setup

### Prerequisites

- Rust 1.75+ (2024 edition)
- Redis 6.2+ for integration tests
- Git

### Clone and Build

```bash
git clone https://github.com/yourusername/redis-tower.git
cd redis-tower
cargo build --all-features
```

### Running Tests

```bash
# Unit tests (no Redis required)
cargo test --lib --all-features

# Integration tests (requires Redis on localhost:6379)
cargo test --test '*' --all-features

# All tests
cargo test --all-features
```

### Code Quality

Before submitting a PR, ensure your code passes all checks:

```bash
# Format code
cargo fmt --all

# Check formatting
cargo fmt --all -- --check

# Run clippy with strict warnings
cargo clippy --all-targets --all-features -- -D warnings

# Run tests
cargo test --all-features
```

## Project Standards

### Code Style

- **No emojis** in code, commits, or documentation (except README badges)
- Use Rust 2024 edition idioms
- Follow standard Rust naming conventions
- Maximum line length: 100 characters
- Use meaningful variable names

### Documentation

- All public APIs must have doc comments
- Include examples in doc comments where helpful
- Document panics, errors, and safety considerations
- Keep CLAUDE.md updated with architectural decisions

### Error Handling

- Use `thiserror` for library errors
- Use `anyhow` for application/example errors
- Provide context in error messages
- Don't unwrap in library code (except tests)

### Testing

- Maintain minimum 70% test coverage
- Write unit tests for all public APIs
- Add integration tests for complex features
- Test edge cases and error conditions

## Git Workflow

### Branch Naming

- `feat/descriptive-name` - New features
- `fix/descriptive-name` - Bug fixes
- `docs/descriptive-name` - Documentation updates
- `refactor/descriptive-name` - Code refactoring
- `test/descriptive-name` - Test improvements

### Commit Messages

Use conventional commit format:

```
<type>: <description>

[optional body]

[optional footer]
```

Types:
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation changes
- `style` - Code style changes (formatting, no logic change)
- `refactor` - Code refactoring
- `test` - Adding or updating tests
- `chore` - Maintenance tasks

Examples:

```
feat: add ZMPOP and BZMPOP commands for Redis 7.0+

Implements sorted set pop operations that can pop from multiple
keys in a single operation. Includes builder pattern for count
option and MIN/MAX selection.

Commands: 200 total (189 core + 11 module), 50% coverage
```

```
fix: correct GeoUnit serialization in GEOSEARCHSTORE

Changed from to_string() to as_str() to match existing GeoUnit
implementation pattern.
```

### Pull Request Process

1. **Create a branch** from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make your changes** following the guidelines above

3. **Test thoroughly**:
   ```bash
   cargo test --all-features
   cargo clippy --all-targets --all-features -- -D warnings
   cargo fmt --all -- --check
   ```

4. **Commit with good messages**:
   ```bash
   git add .
   git commit -m "feat: add my feature"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feat/my-feature
   ```

6. **Open a Pull Request** on GitHub:
   - Provide clear title and description
   - Reference any related issues
   - Include test results if applicable
   - Wait for review before merging

### DO NOT

- ❌ Merge your own PRs without review
- ❌ Commit directly to `main` branch
- ❌ Include "Generated with Claude Code" or similar signatures
- ❌ Use emojis in code or commit messages
- ❌ Add `Co-Authored-By: Claude` to commits

## Adding New Commands

### Implementation Checklist

When adding a new Redis command:

- [ ] Create command struct in appropriate module (strings.rs, lists.rs, etc.)
- [ ] Implement `Command` trait with proper `to_frame()` and `parse_response()`
- [ ] Add builder methods for optional parameters
- [ ] Implement `ReadOnly` trait if applicable
- [ ] Add comprehensive doc comments with examples
- [ ] Export command in `src/commands/mod.rs`
- [ ] Add unit tests for frame building and response parsing
- [ ] Update `COMMANDS_TRACKING.md` with new command count
- [ ] Test with real Redis if possible

### Command Implementation Template

```rust
/// MYCOMMAND - Brief description
///
/// Longer description of what the command does.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::MyCommand;
///
/// // Basic usage
/// let cmd = MyCommand::new("key");
///
/// // With options
/// let cmd = MyCommand::new("key").option(value);
/// ```
#[derive(Debug, Clone)]
pub struct MyCommand {
    key: String,
    option: Option<String>,
}

impl MyCommand {
    /// Create a new MYCOMMAND
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            option: None,
        }
    }

    /// Optional parameter description
    pub fn option(mut self, value: impl Into<String>) -> Self {
        self.option = Some(value.into());
        self
    }
}

impl Command for MyCommand {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("MYCOMMAND"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref opt) = self.option {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(opt.as_bytes()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Implement ReadOnly if this is a read-only command
impl ReadOnly for MyCommand {
    fn is_read_only(&self) -> bool {
        true  // or false for write commands
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mycommand_frame() {
        let cmd = MyCommand::new("key");
        let frame = cmd.to_frame();
        
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MYCOMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("key"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_mycommand_response() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = MyCommand::parse_response(frame).unwrap();
        assert_eq!(result, "OK");
    }
}
```

## Feature Flags

When adding features that require external dependencies or are optional:

1. Add feature to `Cargo.toml`:
   ```toml
   [features]
   myfeature = ["dependency"]
   ```

2. Gate code with `#[cfg(feature = "myfeature")]`

3. Add `required-features` to examples/tests that use it

4. Document the feature in README.md

## Documentation

### API Documentation

- Run `cargo doc --all-features --open` to view docs
- Ensure all public items have documentation
- Include examples in doc comments
- Document edge cases and gotchas

### Project Documentation

- Update README.md for user-facing changes
- Update CLAUDE.md for architectural decisions
- Update COMMANDS_TRACKING.md when adding commands
- Keep examples up to date

## Testing Strategy

### Unit Tests

Located in each module file:
- Test frame building (`to_frame()`)
- Test response parsing (`parse_response()`)
- Test builder patterns
- Test edge cases

### Integration Tests

Located in `tests/` directory:
- Require running Redis server
- Test real Redis operations
- Test error conditions
- Test cluster/sentinel features

### Running Integration Tests

```bash
# Start Redis
redis-server --port 6379

# Run integration tests
cargo test --test integration --all-features

# Or run specific integration test
cargo test --test integration basic_operations
```

## Performance

- Profile before optimizing
- Benchmark significant changes
- Avoid allocations in hot paths
- Use zero-copy where possible
- Consider using `Bytes` instead of `Vec<u8>`

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas
- Tag maintainers for urgent issues

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT OR Apache-2.0).
