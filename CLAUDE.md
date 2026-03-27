# Redis-Tower Client

## Project Status

**Version**: 0.1.0 (v2 rewrite)
**Phase**: Core scaffold -- workspace set up, minimal commands
**MSRV**: 1.87, Edition 2024

## Project Vision

A Tower-native Redis client with typed commands, composable middleware, and
feature-gated extensions. See DESIGN.md for the full design document.

## Architecture

Workspace with four crates:

- `redis-tower-protocol` -- RESP Frame, Codec, Parser (placeholder, will be replaced by external `resp-rs` crate)
- `redis-tower-core` -- Command trait, RedisConnection (Service impl), RedisStream, URL parsing, errors
- `redis-tower-commands` -- Typed command structs (GET, SET, DEL, EXISTS, EXPIRE, TTL, PING, INCR, MGET, FLUSHDB)
- `redis-tower` -- Facade crate with RedisClient, re-exports core + commands

## Key Design Decisions

- `RedisConnection` requires `&mut self` for `Service::call` (proper Tower contract)
- `Command::parse_response` takes `&self` (response parsing can depend on command config)
- `Command::name()` method for observability
- Protocol crate is a placeholder -- will swap in `resp-rs` when published

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
```

## Workspace Layout

```
crates/
  redis-tower-protocol/   # Frame, Codec (placeholder for resp-rs)
  redis-tower-core/       # Command trait, connection, Service impl
  redis-tower-commands/   # Typed command implementations
  redis-tower/            # Facade crate (what users depend on)
```
