# redis-tower

An experimental Tower-based Redis client with strong typing and composable middleware.

## Status

**Experimental** - This is a learning project exploring Tower patterns for Redis clients.

## Features

- **Tower-native**: Built on Tower's Service trait
- **Type-safe**: Strongly typed commands and responses
- **Middleware-first**: Circuit breakers, retries, timeouts as layers
- **Composable**: Users can add their own Tower middleware

## Example

```rust
use redis_tower::{RedisClient, commands::Get};

let client = RedisClient::connect("localhost:6379").await?;

// Strongly typed commands
let value: Option<String> = client
    .call(Get::new("my_key"))
    .await?;
```

## Architecture

See [CLAUDE.md](CLAUDE.md) for detailed architecture and implementation plan.

## Development

```bash
# Build
cargo build

# Run tests
cargo test

# Run example
cargo run --example basic

# Run benchmarks
cargo bench
```

## License

MIT OR Apache-2.0
