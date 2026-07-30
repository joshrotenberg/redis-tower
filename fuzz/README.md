# RESP codec fuzzing

These `cargo-fuzz` targets exercise the unified RESP2/RESP3 decoder:

- `decode` feeds arbitrary wire data through configurable frame-size and
  nesting limits.
- `decode_chunked` delivers arbitrary wire data in small network-style chunks
  and drains every complete pipelined frame after each chunk.

Install `cargo-fuzz`, then run either target with a nightly toolchain:

```sh
cargo +nightly fuzz run decode
cargo +nightly fuzz run decode_chunked
```

CI builds both targets and gives each one a short smoke campaign. Longer local
or scheduled campaigns can reuse any interesting inputs written under
`fuzz/corpus/`; crashes and minimized reproducers are written under
`fuzz/artifacts/`.
