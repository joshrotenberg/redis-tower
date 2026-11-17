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
measured 10-second samples after a two-second warmup. Publication JSON retains
those raw samples so the validator can recompute every throughput rate from
counts and measured wall time, then every mean and population standard
deviation. Samples outside the expected 9.5-to-15-second measured window are
rejected.

Publication mode then runs one standalone four-hour GET-validation soak with a
same-port Redis SIGKILL/restart halfway through. The validator requires exactly
240 approximately one-minute windows, complete operation/rate/latency/RSS
accounting, and exactly one ordered chaos, reconnect, and recovery lifecycle.
Every interval must continue completing successful work; errors are accepted
only in the bounded chaos/recovery interval. The mean RSS of the final ten
minutes may grow by at most the larger of 16 MiB or 50% relative to the first
ten minutes.

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
explicit instruction to rerun outside the sandbox. Publication mode also
enforces the documented 16-GiB minimum. Cargo configuration files in
`CARGO_HOME` or any workspace ancestor are refused so an unrecorded linker,
registry replacement, or rustflag cannot alter the build.

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
A mismatched marker aborts the resume. If initialization was interrupted before
an empty target was claimed, retrying safely creates the marker; an exact
interrupted marker write is promoted atomically. Result artifacts are never
credited. Thus workspace free space never falls below 4 GiB, while the 2-GiB
temporary and 1-GiB result free-space floors always remain. Symlinked target or
result roots are refused. A host with 16 GiB free therefore retains roughly 8
GiB of conservative headroom at peak without blocking a legitimate resume once
the isolated target has consumed part of that budget.

## Verify and reproduce archived evidence

### Weekly hosted-runner comparisons

The weekly standalone and Cluster comparison artifacts are retained for 90 days.
Their normal and Cluster replica JSON includes the bounded raw run samples:
counts, measured wall time, errors, rates and latency. These allow throughput
means and population standard deviations to be recomputed; aggregate latency
percentiles remain means of per-run percentiles, not pooled percentiles.

Each comparison artifact also contains its `Cargo.lock`, exact `source-sha.txt`,
Cargo/rustc/Redis versions, path-free `dependencies.json`, and stderr logs.
Before measurement, `configuration.json` records the package, repeated runs,
measured/warmup seconds, payload/concurrency lists, pipeline concurrency/depth
and fixed client/workload policies. The gate reads these same expected inputs,
not a matrix inferred from surviving rows. Configuration is allowlisted rather
than an environment dump, and remains available if measurement fails.
The lockfile is generated only when absent and is recorded before measurements;
metadata and comparison commands use `--locked`. The dependency metadata is a
sanitized **workspace-wide** resolution (names, versions, normalized sources and
resolved features), not a claim about the feature set of one selected binary.
Compare the lockfile and source SHA with those recorded in a reproduced run.
The workflow uploads available artifacts and diagnostics even when a run fails;
missing, partial or failed measurements are not complete comparison evidence.

Before upload, the weekly job validates the exact configured comparison matrix:
126 standalone cells (GET/SET plus depth-100 pipelines), 90 Cluster GET/SET
cells, and 18 master/strict-replica GET cells, with three raw samples per cell.
Missing, extra or duplicate cells, incorrect client identities, nonzero errors,
missing samples and inconsistent counts, rates or latency summaries fail the job.
The validator reuses the publication accounting and measured-window checks;
it does not seal the archive or validate complete host/configuration provenance.
Artifacts remain available on failure for investigation.

Recheck an extracted weekly comparison artifact from the source checkout named
in its `source-sha.txt`:

```bash
python3 scripts/benchmarks/validate_weekly_results.py \
  --recorded-config --result-dir /absolute/path/to/extracted-artifact \
  --package standalone-bench
```

Recorded mode rejects missing/unsupported configuration, non-canonical numeric
settings, policy drift and mixed explicit override flags. Fixed policies are
checked against that source checkout. The file records expected inputs; it is
not cryptographic evidence of execution or complete runtime/Redis/host provenance.

For older artifacts without a configuration record, explicit validation remains
available. Supply the original run's expected settings (current defaults shown):

```bash
python3 scripts/benchmarks/validate_weekly_results.py \
  --result-dir /absolute/path/to/extracted-artifact --package standalone-bench \
  --payload-sizes 64,1024,16384 --concurrency 1,32,128 --runs 3 --secs 3 \
  --pipeline-concurrency 1 --pipeline-commands 100
```

For the Cluster artifact, use `--package cluster-bench`; both normal and replica
JSON must be present. Use the original run's expected configuration, not a
matrix inferred from whichever result rows survived. These counts and the
depth-100 default describe validation inputs, not matched batching semantics
across clients or new performance conclusions.

To record expected inputs before a custom comparison, run the same explicit
command with `--record-config --warmup 1` after creating a new result directory.
It writes `configuration.json` and exits without measuring or validating results;
it refuses to overwrite an existing record. The caller must pass those same
settings to the benchmark binary. The weekly workflow does this using shared
environment inputs, and runs `--recorded-config` validation after measurement.

These are short hosted-runner saturation measurements, not fixed-load latency
tests, verified manifests, complete host/configuration provenance or the sealed
dedicated-host publication evidence described below. They do not establish an
absolute performance guarantee or universal client ranking. Full inventory
sealing and dedicated-host/fault/publication work remain separate follow-ups.

### Sealed publication artifacts

A completed result directory is self-verifying on any host; this does not
rerun benchmarks or require the original isolated Cargo target:

```bash
python3 scripts/benchmarks/artifact_manifest.py verify-recorded \
  --result-dir /absolute/path/to/completed-results
```

To reproduce the measurement inputs, first verify the archive, check out the
`source_sha` recorded in `manifest.json`, and restore the exact ignored
lockfile before invoking the runner into a new empty directory:

```bash
cd /absolute/path/to/completed-results
shasum -a 256 -c cargo-lock.sha256

cd /absolute/path/to/redis-tower-at-recorded-source
cp /absolute/path/to/completed-results/cargo-lock.txt Cargo.lock
bash scripts/benchmarks/run_publication.sh /absolute/path/to/new-results
```

The new run intentionally records its own execution-host fingerprint, so its
manifest and performance values will differ across hosts even when source,
lockfile, commands, and configuration match.


## Codec CI evidence

The codec gate retains separate first-pass and confirmation evidence artifacts,
including when the comparator reports a regression. Each subject has its raw
Criterion baselines, full resolved `Cargo.lock`, build output, saved codec
executable, command records, source/tree identity and before/after executable
SHA256. The harness revision and hashes of its helper, sanitizer and workflow
are recorded separately from the measured base/head revisions.

`dependencies.json` is explicitly labeled `workspace-resolved`: its sanitized
dependency versions and resolved features describe plain `cargo metadata`'s
workspace view. That view can differ from the units selected by the protocol
benchmark build and does not establish measured feature equivalence.
`build.stdout` retains every actual build unit's complete `compiler-artifact`
record, including its features. `compiler-artifact.json` selects the codec
executable's record; it is not the complete build-unit inventory. The verbose
build log records compiler commands and flags.

The helper builds the existing codec target with `--locked --no-run`, lists its
expected cases, and performs one measured invocation of the saved executable.
It supplies Cargo's `--bench` argument and runs from the protocol package root,
matching [Cargo's benchmark working directory](https://doc.rust-lang.org/cargo/commands/cargo-bench.html).
`CRITERION_HOME` explicitly selects the original workspace `target/criterion`
output directory. The measured argv and allowlisted environment are retained;
its settings remain one-second warmup, two-second measurement and 50 samples.
The executable's list mode performs no measurements. The direct invocation,
including the explicit environment overrides, is part of the recorded recipe.

Each subject first runs explicit `cargo metadata --format-version 1` resolution.
Cargo reuses compatible locked versions and updates the lock when the subject's
manifests require it, preserving the original `cargo bench` resolution behavior.
The artifact retains the full pre/post lockfiles, resolved `Cargo.lock`,
resolution argv/status, source-bound `reused`/`created`/`updated`/`failed`
classification and lock-diff digest. The diff is independently reconstructable
from the retained lockfiles; its diagnostic rendering redacts URLs and local
paths. Raw metadata JSON is omitted in favor of the sanitized dependency view,
and resolution stderr is redacted. The subsequent build uses `--locked` and
checks that the resolved lock stays unchanged through measurement. The separate
confirmation runner retains its own lock identity; no cross-runner lock or
executable equality is inferred from matching source.

Each artifact has an exact file inventory and hashes. Sealing checks expected
case/sample completeness, positive finite sample data, raw mean arithmetic,
build/lock/executable identity and comparison output/status. A failed comparator
can produce complete evidence; missing or malformed evidence remains explicitly
incomplete. The final upload uses `always()` even if measurement, comparison or
sealing fails. An interrupted job may have only partial diagnostic evidence.

To verify a downloaded complete artifact:

```bash
python3 scripts/benchmarks/codec_evidence.py verify path/to/codec-evidence
```

Run its synthetic and stub-process contracts without building Rust:

```bash
python3 scripts/benchmarks/test_codec_evidence.py
python3 scripts/test_ci_workflow.py
python3 scripts/test_check_criterion_regressions.py
python3 scripts/test_decide_codec_benchmark_gate.py
```

Host snapshots include the runner/image identifiers, CPU/kernel/affinity,
load/memory/process summaries and available frequency/governor data. Unavailable
fields are explicit. The environment is allowlisted, not dumped wholesale.
These hosted-runner observations do not establish a noise mechanism or
performance equivalence. The existing uniform +10% threshold, benchmark order,
separate-runner confirmation and fail-closed aggregator remain in force.
Threshold calibration and a fixed-budget diagnostic campaign remain future
work under #710; this retention slice launches no additional campaign.
