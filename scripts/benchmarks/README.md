# Publication benchmark runner

`run_publication.sh` produces the raw comparison and four-hour reliability
evidence used for a release. Run it on dedicated, otherwise-idle hardware with
at least 16 GiB of RAM, Redis CLI/server binaries, Rust, and Python 3 available.

Publication mode always runs the soak; there is no opt-out:

```bash
bash scripts/benchmarks/run_publication.sh ../redis-tower-publication-results
```

For development-only checks, spell out `--matrix-only`:

```bash
bash scripts/benchmarks/run_publication.sh --matrix-only ../redis-tower-matrix-check
```

That mode writes `summary.incomplete.json` and a manifest with
`publication_complete: false`. It cannot produce an ordinary publication
summary, and its directory cannot later be reused for publication mode.

## Explicit bounded sweeps

The runner performs three deliberately non-Cartesian sweeps so the workload is
practical on a 16 GiB host:

1. Standalone and cluster GET/SET: payloads 16 B, 64 B, 1 KiB, 16 KiB, and
   100 KiB, each at concurrency 1, 8, 32, and 128.
2. Standalone pipeline depth: 10, 100, and 1000 commands across all five
   payloads, always at concurrency 1.
3. Standalone pipeline concurrency: concurrency 1, 8, 32, and 128 at the fixed
   1 KiB payload and fixed depth 100.

Depth, payload, and concurrency are not multiplied into one Cartesian pipeline
matrix. The identical depth-100 / 1-KiB / concurrency-1 cell is shared by the
second and third sweeps rather than measured twice. Each cell runs three
measured 10-second samples after a two-second
warmup. Publication JSON retains those raw samples so the validator can
recompute every throughput mean and population standard deviation.

Publication mode then runs one standalone four-hour GET-validation soak with a
same-port Redis SIGKILL/restart halfway through. The validator requires exactly
240 approximately one-minute windows, complete operation/rate/latency/RSS
accounting, and exactly one ordered chaos, reconnect, and recovery lifecycle.

## Preflight, privacy, and resumption

The runner requires a clean tracked worktree and refuses untracked source input
outside its owned result directory. It rejects ambient `BENCH_*` and `SOAK_*`
variables, sets every benchmark input itself, uses an isolated release target
directory, and generates an ignored `Cargo.lock` before using `--locked` when
needed. On macOS it verifies AC power and an open lid when those states are
discoverable, then wraps the entire run in `caffeinate -ims`. On compatible
Linux systems it uses `systemd-inhibit`; otherwise it prints a sleep warning.

Environment evidence intentionally omits hostnames, hardware serials, raw
`uname -a`, local paths, and environment values. It retains hostname-free
OS/kernel details, `rustc -vV`, `cargo -vV`, Redis versions, the lockfile, and a
sanitized dependency graph containing only package name, version, normalized
source, and resolved features.

Every long matrix unit writes into a `.partial` checkpoint directory and is
atomically renamed only after success. Re-running the same command reuses
successful checkpoints when source SHA, lockfile hash, mode, and configuration
all match. A mismatch or an unowned non-empty result directory is refused. The
final manifest records that provenance, the complete configuration, completion
state, and SHA-256/size for every artifact.

The runner uses fixed ports 6480 (standalone matrix), 6481 (soak), and
17000-17002 (cluster matrix); keep those ports free.
