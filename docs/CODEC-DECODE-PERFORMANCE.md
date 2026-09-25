# RESP decode ownership and copy evidence

`RespCodec` deliberately copies one complete, bounded frame after preflight.
It does not copy the entire unread receive buffer, and it does not claim a
zero-copy decode path. This page records why that ownership model is the
current production choice and how to reproduce its evidence.

## Decision

The historical decoder passed `src.clone().freeze()` to `resp-rs` for every
reply. `BytesMut::clone()` copies the full unread slice, so a batch of equal
frames caused the shrinking remainder to be copied repeatedly. The current
decoder first determines the complete frame extent, copies only those bytes,
parses that immutable frame, and advances the receive buffer only after parse
success. Total parser-input copying is therefore linear in decoded wire bytes.

A `split_to(frame_len).freeze()` design removes that copy and is faster in the
focused harness, but it couples response lifetime to receive-buffer lifetime.
A small response retained after the unread tail is drained can pin the entire
original allocation. It also complicates the codec's promise that parse errors
leave input unchanged: restoring an already-split prefix requires rebuilding
the buffer on the exceptional path.

The bounded first-frame copy is the selected tradeoff because it:

- avoids the historical quadratic copied-byte mechanism;
- limits a retained response to frame-sized backing storage rather than the
  whole receive batch;
- preserves unchanged bytes on incomplete input and all errors; and
- requires no unsafe code in the production protocol crate.

The first-frame implementation arrived with the earlier bounded-preflight
work. The evidence here validates and regression-tests that choice; it is not
a claim that this evidence round alone produced an end-to-end client speedup.

## Retained evidence

The checked [raw evidence](../conformance/codec-copy-evidence.json) was recorded
on macOS arm64 at source `f9d07f10c742e4c3d7e53355068a2e6043fd67a3`
with Rust 1.98.1, Cargo 1.98.1, `bytes` 1.12.1, `resp-rs` 0.1.8,
`tokio-util` 0.7.19, and Criterion 0.5.1. The source tree was clean when the
run began. Allocation values are requested bytes observed through an
instrumented `System` allocator, not RSS. Durations are 30 raw within-process
samples of 20 iterations with allocation instrumentation active. They are
directional mechanism evidence, not Redis client throughput or a cross-host
performance claim.

The retained-batch rows below keep every decoded frame until the batch is
complete. “Copied wire” counts only bytes copied to create parser input;
“allocated” and “live” also include receive storage, `Frame` values, aggregate
vectors, and allocator overhead.

| Scenario and strategy | Copied wire | Allocated through decode | Live after decode | Median time |
|---|---:|---:|---:|---:|
| 512 five-byte replies; historical whole-buffer copy | 656,640 B | 708,352 B | 708,352 B | 53,549 ns |
| 512 five-byte replies; production first-frame copy | 2,560 B | 54,272 B | 54,272 B | 36,169 ns |
| 512 five-byte replies; split/shared comparison | 0 B | 39,464 B | 39,464 B | 11,323 ns |
| 128 mixed replies (71,296 wire bytes); historical whole-buffer copy | 4,630,640 B | 4,718,832 B | 3,576,560 B | 94,233 ns |
| 128 mixed replies; production first-frame copy | 71,296 B | 158,720 B | 158,544 B | 16,201 ns |
| 128 mixed replies; split/shared comparison | 0 B | 85,160 B | 85,160 B | 6,250 ns |

The lifetime counterexample is the reason the fastest microbenchmark strategy
is not the production default. The input contains a 23-byte first frame and a
262,155-byte unread tail. After the unread buffer is dropped while the head
frame remains alive:

| Strategy | Copied wire | Bytes pinned by the retained head | Median time |
|---|---:|---:|---:|
| Historical whole-buffer copy | 262,178 B | 262,202 B | 8,105 ns |
| Production first-frame copy | 23 B | 47 B | 2,920 ns |
| Split/shared comparison | 0 B | 262,218 B | 3,353 ns |

These numbers should not be extrapolated to a complete Redis workload. The
production path also performs the bounded preflight scan, while the comparison
strategies receive known fixture extents. Later parser or buffer changes should
rerun the same artifact rather than preserving these numbers as a permanent
marketing baseline.

## Regression and benchmark coverage

The protocol unit suite counts both completed materializations and their exact
wire bytes. A mixed pipeline containing binary bulk data, a push, scalars, and
an aggregate proves that production materializes the sum of frame sizes once,
not the sum of shrinking unread suffixes. A byte-at-a-time case proves that no
copy happens until a frame is complete. Existing boundary tests continue to
cover limits, malformed input, unchanged error bytes, binary payloads, pushes,
and response order.

Criterion now includes:

- dropped and batch-retained lifetimes for 100 simple replies;
- dropped and batch-retained mixed payloads from zero through 4 KiB; and
- the same mixed wire delivered in 1-, 7-, 64-, and 1,024-byte fragments.

The raw allocation artifact repeats those four fragment sizes over 112 mixed
frames, retains the decoded batch, and records receive-buffer reallocations and
requested bytes. Every fragmentation plan decodes frames equivalent to the
contiguous production path and reports exactly one frame's wire bytes per
completed materialization.

| Delivery chunk | Copied wire | Allocated through decode | Buffer reallocations | Median time |
|---:|---:|---:|---:|---:|
| 1 B | 5,616 B | 20,720 B | 6 | 75,838 ns |
| 7 B | 5,616 B | 20,720 B | 6 | 21,730 ns |
| 64 B | 5,616 B | 20,720 B | 3 | 12,513 ns |
| 1,024 B | 5,616 B | 22,256 B | 1 | 10,633 ns |

To reproduce the checked allocation/copy artifact from a clean checkout, use
an absolute output path because Cargo runs the custom benchmark from the
package directory:

```bash
evidence_output="$PWD/conformance/codec-copy-evidence.json"
cargo bench -p redis-tower-protocol --bench codec_allocation -- \
  --evidence --samples 30 --iterations 20 --check \
  --output "$evidence_output"
```

The `--check` mode verifies equivalent decoded frames, exact deterministic
copy counts, lower production allocation than the historical mechanism, and
the retained-tail tradeoff. `cargo test --benches` runs a short silent smoke;
an explicit `--evidence` invocation is required for retained timing samples.
Run the Criterion timing suite separately with:

```bash
cargo bench -p redis-tower-protocol --bench codec
```
