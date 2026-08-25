# Engineering hygiene

The `Engineering Hygiene` workflow keeps expensive diligence visible without
putting every slow measurement on the ordinary Linux CI critical path.

## Pull-request gates

Every pull request runs three classes of checks:

- `cargo-semver-checks` compares all publishable crate APIs with the pull
  request target revision under a patch-compatible policy. Intentional API
  breaks must therefore be explicit rather than arriving as collateral edits.
- Library tests run with every feature on macOS arm64, Windows x64, and Linux
  arm64. The all-feature builds exercise native TLS on each host; Unix-socket
  tests run on the Unix hosts.
- `check_release_hygiene.py` discovers every publishable `redis-tower*` crate
  from the workspace manifest. It requires `#![deny(missing_docs)]`, complete
  docs.rs metadata, standard package metadata, and expected package contents.

Run the release audit locally with:

```bash
python3 scripts/check_release_hygiene.py --check-package-contents
```

## Mutation score

The scheduled and manual workflow runs `cargo-mutants` against the library
tests of every publishable crate. Ordinary pull requests do not wait for this
suite. Each package retains its complete `mutants.out` directory for 90 days,
and the summary job emits per-package and combined JSON/Markdown reports.

The conservative headline score is:

```text
caught / (caught + missed + timeout)
```

Unviable mutants are reported but excluded from the denominator. Timeouts are
included because they need investigation rather than being silently treated as
caught. The initial 70% floor is a tracked target, not a merge gate; establish a
stable baseline before deciding whether to enforce it.

For a focused local run:

```bash
cargo mutants --package redis-tower-core --in-place --jobs 2
python3 scripts/mutation_score.py mutants.out --minimum-score 0.70
```

## CI wall clock and flake signal

The scheduled report reads the latest 50 completed, non-cancelled runs of the
main `CI` workflow. It records p50, p95, and maximum wall-clock duration plus:

- a rerun/flake signal: runs whose GitHub `run_attempt` is greater than one;
- a failure signal: completed runs whose conclusion is not success.

The enforced operating budgets are a 15-minute p95 and a 10% rerun rate.
Failure rate is tracked but not budgeted because a failing pull-request commit
usually represents a real code/configuration error, not a flaky test. Cancelled
runs are excluded because superseded jobs and GitHub queue incidents otherwise
distort elapsed time.

The reporter accepts captured GitHub API JSON for local reproduction:

```bash
python3 scripts/ci_health.py --input workflow-runs.json \
  --max-p95-minutes 15 --max-rerun-rate 0.10
```

## Resource and build footprint

The existing `Weekly Benchmarks` workflow remains the source of comparative
resource evidence. Its isolated probes compare redis-tower, redis-rs, and Fred
for RSS per connection and CPU at a fixed offered rate, then compare cold-build
time and stripped binary size. JSON results and the exact `Cargo.lock` are
retained for 90 days. Hosted-runner output is trend/smoke evidence; release
claims still require an otherwise-idle dedicated host as described in the
[benchmark publication protocol](../scripts/benchmarks/README.md).
