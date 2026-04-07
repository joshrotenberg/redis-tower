# Job Queue Comparison

Distributed job queue using Redis Streams, implemented with three libraries to
compare ergonomics and boilerplate.

## What it does

1. Producer adds 100 jobs to a Redis Stream ("jobs")
2. Creates a consumer group ("workers")
3. Spawns 4 consumer tasks that XREADGROUP, process (sleep 1ms), and XACK
4. Waits for all jobs to be consumed
5. Prints total time, jobs/sec, and per-worker counts

## Running

Requires a local Redis server on 127.0.0.1:6379.

```bash
cargo run -p job-queue-tower
cargo run -p job-queue-redis-rs
cargo run -p job-queue-fred
```

## Library versions

- **redis-tower** -- path dependency (workspace crates)
- **redis** (redis-rs) -- 0.27
- **fred** -- 10

## Key differences

### redis-tower

Uses `StreamConsumer` which wraps XREADGROUP into a Rust `Stream`. The consumer
handles group creation, pending message draining, and auto-ack internally.
The worker loop is just `while let Some(msg) = stream.next().await`.

### redis-rs

Uses raw `xadd`, `xgroup_create`, `xreadgroup`, and `xack` via the Commands
trait. Requires manual group creation, explicit `StreamReadOptions` setup, and
navigating nested `StreamReadReply` types.

### fred

Uses fred's typed stream methods. Similar to redis-rs in that you call
individual commands, but with slightly more ergonomic types via `XReadResponse`
and `xreadgroup_map`.
