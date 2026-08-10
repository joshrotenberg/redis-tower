# standalone-bench

Standalone Redis throughput and latency comparison across:

- `redis_tower::RedisClient` (shared mutex baseline)
- `redis_tower::MultiplexedClient` (auto-pipelined production path)
- redis-rs blocking connection
- redis-rs async `MultiplexedConnection`
- redis-rs async `ConnectionManager`
- fred async `Client`

The runner starts and stops its own Redis server. Its default matrix covers
GET, SET, and 100-command batches with 64 B, 1 KiB, and 16 KiB values.

```bash
cargo run -p standalone-bench --release
cargo run -p standalone-bench --release -- --json
```

## Matrix configuration

| Environment variable | CLI option | Default | Description |
|---|---|---:|---|
| `BENCH_SECS` | `--secs` | `10` | Measured seconds per run |
| `BENCH_WARMUP` | `--warmup` | `2` | Unmeasured warmup seconds per run |
| `BENCH_RUNS` | `--runs` | `3` | Repeated runs per cell |
| `BENCH_CONCURRENCY` | `--concurrency` | `1,8,32,128` | GET/SET concurrency levels |
| `BENCH_PIPELINE_CONCURRENCY` | `--pipeline-concurrency` | `1` | Explicit batch concurrency levels |
| `BENCH_PAYLOAD_SIZES` | `--payload-sizes` | `64,1024,16384` | Value sizes; `K`/`KiB` and `M`/`MiB` suffixes are accepted |
| `BENCH_PIPELINE_COMMANDS` | `--pipeline-commands` | `100` | Commands in each explicit batch |
| `BENCH_WORKLOADS` | `--workloads` | `set,get,pipeline` | Workloads to include |
| `BENCH_CLIENTS` | `--clients` | all six | Client aliases to include |
| `BENCH_INCLUDE_SAMPLES` | `--include-samples` | `false` | Retain every bounded per-run sample in JSON output |
| `BENCH_PORT` | `--port` | `6480` | Throwaway Redis server port |

For example, this is a small three-client smoke matrix:

```bash
cargo run -p standalone-bench --release -- \
  --secs 1 --warmup 0 --runs 1 \
  --payload-sizes 64 --concurrency 1,8 \
  --clients redis-tower-mux,redis-rs-async,fred \
  --workloads set,get --json
```

## Reading the report

Every successful GET must return a value of the configured size. Missing or
wrong-sized values count as errors. Failed prepopulation writes, worker
connection/setup errors, and worker panics abort with a non-zero exit status.
JSON and human output report p50, p99, and p999 (plus p90/max in JSON).

Each JSON record declares `schema_version: 2` and adds a stable kebab-case
`client_id`. For compatibility, `client` keeps its original Rust variant name
and `total_ops` / `ops_per_sec_*` keep the original per-iteration (batch for
pipeline) meaning. Prefer the explicit batch and command fields in new tools.
Pass `--include-samples` for publication evidence: each schema-v2 cell then
contains the raw bounded run samples used to recompute its means and standard
deviations. The field is omitted by default for backward compatibility.

For GET and SET, one batch is one command, so batches/s and commands/s are the
same. For the pipeline workload, latency and batches/s describe the complete
batch while commands/s multiplies by `commands_per_batch`. Reporting both
prevents a 100-command batch from being mislabeled as a single command.

The redis-tower multiplexed pipeline row uses its production auto-pipelining
path by issuing the configured commands concurrently; the other pipeline rows
use each client's explicit pipeline API. Compare rows produced by the same
invocation and record the Redis/client versions and host configuration with any
published result.
