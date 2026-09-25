# RESP codec support and validation

The protocol crate decodes one bounded RESP reply at a time. Its public
`Frame` type comes from `resp-rs` and can represent more wire forms than the
client currently accepts. This page is the disposition contract used by the
property tests, fuzz oracle, and reviewed seed corpus.

## Frame disposition

| Wire form | Decode disposition | Generated and fixture coverage |
|---|---|---|
| Simple strings and errors | Supported; line contents cannot contain an unescaped CRLF | Generated leaves and independent pipeline fixtures |
| Integers | Supported across the full `i64` range; overflow is rejected | Generated leaves plus malformed/overflow boundaries |
| Bulk strings and RESP2 null bulk strings | Supported with byte-exact binary payloads | Generated binary leaves and framing-looking payload fixtures |
| Blob errors | Supported with byte-exact payloads | Generated leaves and mixed aggregate fixtures |
| Finite doubles | Supported | Generated finite values |
| `inf`, `-inf`, and `nan` doubles | Supported and normalized to canonical `SpecialFloat` frames | Generated canonical values and independent wire fixtures |
| Booleans and RESP3 null | Supported | Generated leaves |
| Big numbers | Supported as their wire bytes | Generated signed decimal values and independent fixtures |
| Verbatim strings | Supported when the payload has a three-byte format followed by `:` | Generated binary content under the `txt` format and malformed fixtures |
| Fixed arrays, null arrays, sets, maps, and pushes | Supported recursively within the configured size and depth limits | Generated mixed aggregates, raw fixtures, and exact boundary tests |
| Fixed attributes | Rejected without consuming input | Deterministic top-level and nested rejection fixtures |
| Streamed string/blob/verbatim headers, aggregate headers, chunks, and terminators | Rejected without consuming input | Every token at top level and in aggregates, across every split point |
| Malformed tags, scalars, lengths, terminators, or overflow | Rejected without consuming input | Deterministic regressions and arbitrary-byte fuzzing |
| Frames over `max_frame_size` or `max_depth` | Rejected before materialization and without consuming input | Exact limits, every split point, declared cardinality, and nested maps |

Encoding a `Frame` is intentionally less restrictive because outbound frames
are application-built. In particular, the re-exported enum can serialize
attributes and streamed variants that this decoder rejects. Do not infer
receive support from the variants exposed by `resp-rs`.

The declared-cardinality regression uses a test-only scan observer: an array
or map declaring ten million entries visits exactly its one available header
and never spills the inline nesting stack. This deterministically verifies
constant pre-materialization work without attempting a dangerous allocation.

## Fragmentation oracle

The `decode_chunked` fuzz target decodes each input twice under identical
limits: once with the entire wire available and once with a repeating sequence
of arbitrary one-to-64-byte chunks. It compares:

- every successful frame in order, represented by its canonical re-encoding;
- the bytes retained at the first incomplete or rejected frame; and
- the terminal disposition, including the exact parser error variant.

Canonical bytes avoid treating `NaN != NaN` as a protocol disagreement. A
frame-size error compares by disposition rather than the diagnostic `size`
field: a prefix can prove that the cap is exceeded before the complete extent
is known. If incremental decoding rejects a prefix, the harness appends the
not-yet-delivered bytes without retrying the decoder, matching a connection
that closes on a protocol error while preserving a whole-buffer-comparable
remainder.

The property suite separately builds up to sixteen valid replies, including
nested mixed aggregates, then checks arbitrary partitions and truncation
points. Raw wire fixtures are kept independent of the crate encoder so a
paired encoder/decoder defect cannot be the only evidence.

## Fuzz input and retained evidence

Both fuzz targets use this compact envelope:

```text
[max_depth, max_size_hi, max_size_lo, plan_length, plan..., RESP wire...]
```

The plan is capped at 16 bytes and each value maps to a non-zero chunk size.
Inputs shorter than four bytes are treated as raw wire with conservative
limits. Reviewed seeds live as named hexadecimal fixtures under
`fuzz/corpus-seeds/`; generated corpus entries and crash artifacts remain
ignored working state.

Pull requests run ten seconds per target. The weekly and manually dispatchable
`Scheduled Fuzzing` workflow accepts a one-to-3,600-second duration per target.
Each job retains its reviewed starting seeds, final corpus, crash artifacts,
log, toolchain versions, source SHA, dependency-file hashes, duration, exit
code, and terminal status for 90 days. The manifest is written as `running`
before cargo-fuzz starts and changes to `passed` only after a zero exit code;
an interruption therefore cannot masquerade as a completed pass.

For local reproduction:

```bash
python3 scripts/prepare_fuzz_corpus.py \
  fuzz/corpus-seeds/decode_chunked fuzz/corpus/decode_chunked
cargo +nightly fuzz run decode_chunked fuzz/corpus/decode_chunked -- \
  -max_total_time=300
```

## Historical mutation triage

The September 21 protocol report predated the current preflight limit scanner
and scored a library-only mutation scope at 58.5%. Its survivors are evidence
to classify, not a score that can be compared with a differently scoped run.

| Historical survivor family | Classification and current evidence |
|---|---|
| Delete or alter map depth traversal | Real assertion gap. `nested_maps_consume_the_same_depth_budget_as_other_aggregates` now rejects a two-level map at depth one and accepts it at depth two. Fixed attributes are separately unsupported. |
| Scan inside a blob instead of skipping its declared payload | Real framing and depth gap. Binary fixtures contain streaming and aggregate tokens, while unit and integration regressions prove those bytes remain opaque. |
| Alter CRLF scanning at a fragment boundary | Real fragmentation gap. Every strict prefix of independent frames is incomplete, exact split tests cover bounded frames, and the generated/fuzz oracles compare arbitrary partitions. |
| Change length/cardinality arithmetic or overflow handling | A real gap when the mutation changes rejection, consumption, or allocation bounds; deterministic tests cover oversized declarations, impossible cardinality, integer overflow, and malformed terminators. A mutation that changes only the early diagnostic lower-bound `size` while preserving its documented class is equivalent under this contract. |
| Timeout or mutation outside a reachable supported disposition | Tool limitation or excluded protocol feature, to be reported separately rather than counted as covered. Attributes and streaming remain intentional fail-closed exclusions. |

A future score comparison must record the exact source SHA, cargo-mutants
version, test command, package scope, and raw outcomes. The scheduled mutation
workflow already retains that evidence; this work does not relabel the old
number as a current baseline.
