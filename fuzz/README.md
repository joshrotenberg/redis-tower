# RESP codec fuzzing

These `cargo-fuzz` targets exercise the unified RESP2/RESP3 decoder:

- `decode` feeds arbitrary wire data through configurable frame-size and
  nesting limits.
- `decode_chunked` compares ordered frames, remaining bytes, and terminal error
  disposition between whole-buffer decoding and an arbitrary repeating network
  fragmentation plan.

Both targets accept `[depth, size_hi, size_lo, plan_length, plan..., wire...]`.
The plan is capped at 16 bytes; each value selects a one-to-64-byte chunk.
Short inputs remain raw wire inputs under conservative default limits.

Install `cargo-fuzz`, then run either target with a nightly toolchain:

```sh
python3 scripts/prepare_fuzz_corpus.py \
  fuzz/corpus-seeds/decode fuzz/corpus/decode
cargo +nightly fuzz run decode fuzz/corpus/decode

python3 scripts/prepare_fuzz_corpus.py \
  fuzz/corpus-seeds/decode_chunked fuzz/corpus/decode_chunked
cargo +nightly fuzz run decode_chunked fuzz/corpus/decode_chunked
```

Named hexadecimal fixtures under `fuzz/corpus-seeds/` are reviewed and tracked;
the preparation command materializes their exact bytes. Generated inputs under
`fuzz/corpus/` and crashes under `fuzz/artifacts/` remain ignored working state.

CI builds both targets, tests the shared oracle, and gives each target a
ten-second smoke campaign. The bounded weekly/manual workflow retains the final
corpus, crashes, log, source and dependency provenance, duration, and outcome
for 90 days. The full contract and mutation classification live in
[`docs/PROTOCOL-TESTING.md`](../docs/PROTOCOL-TESTING.md).
