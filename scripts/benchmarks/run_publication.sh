#!/usr/bin/env bash
set -euo pipefail

# Run the complete issue #481 publication matrix. The four-hour soak is opt-in:
# RUN_FOUR_HOUR_SOAK=1 scripts/benchmarks/run_publication.sh <new-result-dir>

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <new-result-directory>" >&2
  exit 2
fi

workspace_root="$(git rev-parse --show-toplevel)"
result_dir="$1"
if [[ "$result_dir" != /* ]]; then
  result_dir="$workspace_root/$result_dir"
fi
if [[ -e "$result_dir" ]] && [[ -n "$(find "$result_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "refusing to overwrite non-empty result directory: $result_dir" >&2
  exit 2
fi

cd "$workspace_root"
if [[ -n "$(git status --porcelain)" ]]; then
  echo "publication benchmarks require a clean worktree" >&2
  exit 2
fi
mkdir -p "$result_dir"
if ! command -v redis-server >/dev/null || ! command -v redis-cli >/dev/null; then
  echo "redis-server and redis-cli must be available on PATH" >&2
  exit 2
fi

payloads="16,64,1024,16384,102400"
concurrencies="1,8,32,128"

{
  echo "captured_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_sha=$(git rev-parse HEAD)"
  echo "git_describe=$(git describe --always --dirty --tags)"
  echo "uname=$(uname -a)"
  echo "rustc=$(rustc --version)"
  echo "cargo=$(cargo --version)"
  echo "redis_server=$(redis-server --version)"
  echo "redis_cli=$(redis-cli --version)"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "cpu=$(sysctl -n machdep.cpu.brand_string)"
    echo "logical_cpu=$(sysctl -n hw.logicalcpu)"
    echo "physical_cpu=$(sysctl -n hw.physicalcpu)"
    echo "memory_bytes=$(sysctl -n hw.memsize)"
    sw_vers
  elif command -v lscpu >/dev/null; then
    lscpu
  fi
} > "$result_dir/environment.txt"

cargo metadata --format-version 1 --locked > "$result_dir/cargo-metadata.json"
cp Cargo.lock "$result_dir/cargo-lock.txt"
shasum -a 256 Cargo.lock > "$result_dir/cargo-lock.sha256"

{
  echo "cargo build --release -p standalone-bench -p cluster-bench -p soak-bench"
  echo "target/release/standalone-bench --secs 10 --warmup 2 --runs 3 --payload-sizes $payloads --concurrency $concurrencies --workloads set,get --json"
  for depth in 10 100 1000; do
    echo "target/release/standalone-bench --secs 10 --warmup 2 --runs 3 --payload-sizes $payloads --pipeline-concurrency 1 --pipeline-commands $depth --workloads pipeline --json"
  done
  echo "target/release/cluster-bench --secs 10 --warmup 2 --runs 3 --payload-sizes $payloads --concurrency $concurrencies --scenario throughput --json"
  echo "SOAK_MODE=standalone SOAK_CHAOS=standalone-sigkill SOAK_DURATION_SECS=14400 SOAK_WARMUP_SECS=60 SOAK_REPORT_INTERVAL_SECS=60 SOAK_CHAOS_AFTER_SECS=7200 SOAK_CONCURRENCY=32 target/release/soak-bench --jsonl"
} > "$result_dir/commands.txt"

# Compile before measurements so build work is outside all timed windows.
cargo build --release -p standalone-bench -p cluster-bench -p soak-bench

target/release/standalone-bench \
  --secs 10 --warmup 2 --runs 3 \
  --payload-sizes "$payloads" \
  --concurrency "$concurrencies" \
  --workloads set,get --json \
  > "$result_dir/standalone-throughput.json" \
  2> "$result_dir/standalone-throughput.stderr.log"

for depth in 10 100 1000; do
  target/release/standalone-bench \
    --secs 10 --warmup 2 --runs 3 \
    --payload-sizes "$payloads" \
    --pipeline-concurrency 1 \
    --pipeline-commands "$depth" \
    --workloads pipeline --json \
    > "$result_dir/standalone-pipeline-$depth.json" \
    2> "$result_dir/standalone-pipeline-$depth.stderr.log"
done

target/release/cluster-bench \
  --secs 10 --warmup 2 --runs 3 \
  --payload-sizes "$payloads" \
  --concurrency "$concurrencies" \
  --scenario throughput --json \
  > "$result_dir/cluster-throughput.json" \
  2> "$result_dir/cluster-throughput.stderr.log"

python3 scripts/benchmarks/render_results.py \
  --standalone "$result_dir/standalone-throughput.json" \
  --cluster "$result_dir/cluster-throughput.json" \
  --pipeline "10=$result_dir/standalone-pipeline-10.json" \
  --pipeline "100=$result_dir/standalone-pipeline-100.json" \
  --pipeline "1000=$result_dir/standalone-pipeline-1000.json" \
  --output-dir "$result_dir"

if [[ "${RUN_FOUR_HOUR_SOAK:-0}" == "1" ]]; then
  soak_command=(
    env
    SOAK_MODE=standalone
    SOAK_CHAOS=standalone-sigkill
    SOAK_DURATION_SECS=14400
    SOAK_WARMUP_SECS=60
    SOAK_REPORT_INTERVAL_SECS=60
    SOAK_CHAOS_AFTER_SECS=7200
    SOAK_CONCURRENCY=32
    target/release/soak-bench
    --jsonl
  )
  if command -v caffeinate >/dev/null; then
    caffeinate -dimsu "${soak_command[@]}" \
      > "$result_dir/standalone-soak-4h.jsonl" \
      2> "$result_dir/standalone-soak-4h.stderr.log"
  else
    "${soak_command[@]}" \
      > "$result_dir/standalone-soak-4h.jsonl" \
      2> "$result_dir/standalone-soak-4h.stderr.log"
  fi
  python3 scripts/benchmarks/render_results.py \
    --standalone "$result_dir/standalone-throughput.json" \
    --cluster "$result_dir/cluster-throughput.json" \
    --pipeline "10=$result_dir/standalone-pipeline-10.json" \
    --pipeline "100=$result_dir/standalone-pipeline-100.json" \
    --pipeline "1000=$result_dir/standalone-pipeline-1000.json" \
    --soak "$result_dir/standalone-soak-4h.jsonl" \
    --output-dir "$result_dir"
fi

echo "benchmark artifacts written to $result_dir"
