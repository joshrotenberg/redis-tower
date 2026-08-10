# Publication benchmark runner

`run_publication.sh` produces the raw comparison and four-hour reliability
evidence used for a release. Run it on dedicated, otherwise-idle hardware with
at least 16 GiB of RAM, Redis CLI/server binaries, Rust, and Python 3 available.

Publication mode always runs the soak; there is no opt-out. After this change
is merged, run the exact release command from a normal host terminal, outside
any filesystem or hardware sandbox, in a clean `main` checkout:

```bash
cd /absolute/path/to/redis-tower
git switch main
git pull --ff-only
test -z "$(git status --porcelain --untracked-files=all)"
bash "$PWD/scripts/benchmarks/run_publication.sh" \
  "$PWD/../redis-tower-publication-results"
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
source, and resolved features. CPU model/count and RAM size must be available
and numeric; if sandbox restrictions hide them, preflight fails with an
explicit instruction to rerun outside the sandbox.

Every long matrix unit writes into a `.partial` checkpoint directory and is
atomically renamed only after semantic validation and creation of a checkpoint
record containing command, configuration, provenance, and content hashes.
Re-running the same command reuses a checkpoint only after repeating those
checks. The canonical execution fingerprint binds CPU, RAM, OS/kernel,
Rust/Cargo/Python/Redis versions, source SHA, lockfile, mode, and the complete
configuration; any host or tool mismatch refuses resumption. Finalization
rejects missing, unexpected, partial, non-regular, or symlinked artifacts,
regenerates the rendered summary and charts from raw checkpoints byte for byte,
and only then writes the hashed exact-inventory manifest.

The runner uses fixed ports 6480 (standalone matrix), 6481 (soak), and
17000-17002 (cluster matrix); keep those ports free.

## Disk budget

The retained evidence is small even with raw samples: the matrix contains 548
aggregate rows and 1,644 per-run samples, so JSON, JSONL, logs, charts, and
manifests are conservatively below 100 MiB. The isolated release Cargo target
is budgeted at up to 6 GiB, while the largest temporary Redis dataset and its
replicas are budgeted below 1 GiB. Allowing another GiB for filesystem and
build variance keeps the expected peak below roughly 8 GiB.

Before creating state or compiling, a fresh run requires at least 10 GiB free
on the workspace/build filesystem, 2 GiB on the temporary filesystem, and 1
GiB on the result filesystem. A fresh run never receives credit for an old
target directory. After an existing run state passes exact provenance
validation, resume preflight requires a regular marker in the source-specific
isolated target that binds it to the exact source, lockfile, benchmark config,
and execution fingerprint. A fresh run creates that marker atomically only in a
new or empty target; it refuses to adopt a non-empty unmarked or mismatched
target. A valid resume may credit only the target's allocated size when it
shares the workspace filesystem, capped at the documented 6-GiB target budget.
A missing or mismatched marker aborts the resume, and result artifacts are never
credited. Thus workspace free space never falls below 4 GiB, while the 2-GiB
temporary and 1-GiB result free-space floors always remain. Symlinked target or
result roots are refused. A host with 16 GiB free therefore retains roughly 8
GiB of conservative headroom at peak without blocking a legitimate resume once
the isolated target has consumed part of that budget.
