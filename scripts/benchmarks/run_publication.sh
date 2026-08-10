#!/usr/bin/env bash
set -euo pipefail

# Reproducible publication evidence runner. Publication mode always includes
# the four-hour soak. `--matrix-only` is an explicit development mode whose
# manifest and summary are permanently marked incomplete for publication.

usage() {
  echo "usage: bash scripts/benchmarks/run_publication.sh [--matrix-only] <result-directory>" >&2
}

runner_args=("$@")
run_mode="publication"
if [[ ${1:-} == "--matrix-only" ]]; then
  run_mode="matrix-only"
  shift
fi
if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
script_path="$script_dir/run_publication.sh"
workspace_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
result_dir="$1"
if [[ "$result_dir" != /* ]]; then
  result_dir="$workspace_root/$result_dir"
fi
if [[ -L "$result_dir" ]]; then
  echo "result directory root must not be a symlink" >&2
  exit 2
fi
resume_candidate=0
if [[ -f "$result_dir/.run-state.json" && ! -L "$result_dir/.run-state.json" ]]; then
  resume_candidate=1
fi

# Ambient benchmark variables make a recorded command differ from the command
# that actually ran. Refuse them by name without printing their possibly
# sensitive values; every benchmark control is set below.
ambient_controls=""
for variable_name in $(compgen -e); do
  case "$variable_name" in
    BENCH_*|SOAK_*) ambient_controls="$ambient_controls $variable_name" ;;
  esac
done
if [[ -n "$ambient_controls" ]]; then
  echo "refusing ambient benchmark controls:$ambient_controls" >&2
  echo "unset them; this runner supplies every BENCH_/SOAK_ parameter" >&2
  exit 2
fi

host_os="$(uname -s)"
if [[ "$host_os" == "Darwin" ]]; then
  if command -v pmset >/dev/null 2>&1; then
    power_status="$(pmset -g batt 2>/dev/null || true)"
    if [[ -n "$power_status" ]] && ! grep -q "AC Power" <<<"$power_status"; then
      echo "publication benchmarks require macOS AC power" >&2
      exit 2
    fi
  fi
  if command -v ioreg >/dev/null 2>&1; then
    lid_status="$(ioreg -r -k AppleClamshellState -d 1 2>/dev/null || true)"
    if grep -q 'AppleClamshellState.*Yes' <<<"$lid_status"; then
      echo "publication benchmarks require an open laptop lid" >&2
      exit 2
    fi
  fi
  if [[ ${REDIS_TOWER_RUN_INHIBITED:-0} != "1" ]]; then
    if ! command -v caffeinate >/dev/null 2>&1; then
      echo "caffeinate is required on macOS to protect the complete run from sleep" >&2
      exit 2
    fi
    exec env REDIS_TOWER_RUN_INHIBITED=1 caffeinate -ims \
      bash "$script_path" "${runner_args[@]}"
  fi
elif [[ ${REDIS_TOWER_RUN_INHIBITED:-0} != "1" ]] \
  && command -v systemd-inhibit >/dev/null 2>&1 \
  && systemd-inhibit --list >/dev/null 2>&1; then
  exec env REDIS_TOWER_RUN_INHIBITED=1 systemd-inhibit \
    --what=sleep:idle --mode=block --why="redis-tower publication benchmarks" \
    bash "$script_path" "${runner_args[@]}"
elif [[ ${REDIS_TOWER_RUN_INHIBITED:-0} != "1" ]]; then
  echo "warning: no supported sleep inhibitor found; keep this host awake for the run" >&2
fi

cd "$workspace_root"
if ! git diff --quiet --ignore-submodules -- || ! git diff --cached --quiet --ignore-submodules --; then
  echo "publication benchmarks require a clean tracked worktree" >&2
  exit 2
fi
allowed_result_prefix=""
if [[ "$result_dir" == "$workspace_root/"* ]]; then
  allowed_result_prefix="${result_dir#"$workspace_root/"}"
fi
reject_untracked_source_inputs() {
  local untracked_path
  while IFS= read -r -d '' untracked_path; do
    if [[ -n "$allowed_result_prefix" ]] \
      && { [[ "$untracked_path" == "$allowed_result_prefix" ]] \
        || [[ "$untracked_path" == "$allowed_result_prefix/"* ]]; }; then
      continue
    fi
    echo "publication benchmarks refuse untracked source input: $untracked_path" >&2
    exit 2
  done < <(git ls-files --others --exclude-standard -z)
}
reject_untracked_source_inputs
if ! command -v redis-server >/dev/null 2>&1 || ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-server and redis-cli must be available on PATH" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 must be available on PATH" >&2
  exit 2
fi

base_env=(
  env -i
  "PATH=$PATH"
  "HOME=${HOME:?HOME must be set}"
  "TMPDIR=${TMPDIR:-/tmp}"
  "LANG=C"
  "LC_ALL=C"
)
if [[ -n ${CARGO_HOME:-} ]]; then
  base_env+=("CARGO_HOME=$CARGO_HOME")
fi
if [[ -n ${RUSTUP_HOME:-} ]]; then
  base_env+=("RUSTUP_HOME=$RUSTUP_HOME")
fi
python_clean=("${base_env[@]}" python3)

source_sha="$(git rev-parse HEAD)"
target_dir="$workspace_root/target/publication-$source_sha"
disk_budget_tool="$script_dir/check_disk_budget.py"
initial_disk_mode="fresh"
if [[ $resume_candidate -eq 1 ]]; then
  initial_disk_mode="minima"
fi
# Fresh runs receive no credit for pre-existing files. A resume receives
# owned-target/checkpoint credit only after its state passes exact validation.
"${python_clean[@]}" "$disk_budget_tool" \
  --mode "$initial_disk_mode" \
  --workspace "$workspace_root" \
  --temporary "${TMPDIR:-/tmp}" \
  --result "$result_dir" \
  --target "$target_dir"

if [[ ! -f Cargo.lock ]]; then
  echo "Cargo.lock is absent; generating the ignored benchmark lockfile" >&2
  "${base_env[@]}" cargo generate-lockfile
fi

sha256_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

source_date_epoch="$(git show -s --format=%ct HEAD)"
lock_sha256="$(sha256_file Cargo.lock)"
manifest_tool="$script_dir/artifact_manifest.py"
renderer="$script_dir/render_results.py"
metadata_sanitizer="$script_dir/sanitize_metadata.py"
fingerprint_tool="$script_dir/execution_fingerprint.py"

fingerprint_tmp="$(mktemp "${TMPDIR:-/tmp}/redis-tower-fingerprint.XXXXXX")"
trap 'rm -f "$fingerprint_tmp"' EXIT
"${python_clean[@]}" "$fingerprint_tool" collect \
  --source-sha "$source_sha" \
  --lock-sha256 "$lock_sha256" \
  --mode "$run_mode" > "$fingerprint_tmp"
fingerprint_sha256="$("${python_clean[@]}" "$manifest_tool" fingerprint-digest \
  --fingerprint-file "$fingerprint_tmp")"

verify_execution_provenance() {
  local current_fingerprint
  current_fingerprint="$(mktemp "${TMPDIR:-/tmp}/redis-tower-fingerprint-check.XXXXXX")"
  if ! "${python_clean[@]}" "$fingerprint_tool" collect \
    --source-sha "$source_sha" \
    --lock-sha256 "$lock_sha256" \
    --mode "$run_mode" > "$current_fingerprint"; then
    rm -f "$current_fingerprint"
    return 1
  fi
  if ! cmp -s "$current_fingerprint" "$fingerprint_tmp"; then
    echo "execution host or tool versions changed during the benchmark run" >&2
    rm -f "$current_fingerprint"
    return 1
  fi
  rm -f "$current_fingerprint"
}

verify_source_provenance() {
  if [[ "$(git rev-parse HEAD)" != "$source_sha" ]]; then
    echo "source HEAD changed during the benchmark run" >&2
    exit 2
  fi
  if ! git diff --quiet --ignore-submodules -- \
    || ! git diff --cached --quiet --ignore-submodules --; then
    echo "tracked source changed during the benchmark run" >&2
    exit 2
  fi
  reject_untracked_source_inputs
  if [[ "$(sha256_file Cargo.lock)" != "$lock_sha256" ]]; then
    echo "Cargo.lock changed during the benchmark run" >&2
    exit 2
  fi
}

"${python_clean[@]}" "$manifest_tool" init \
  --result-dir "$result_dir" \
  --source-sha "$source_sha" \
  --lock-sha256 "$lock_sha256" \
  --mode "$run_mode" \
  --fingerprint-file "$fingerprint_tmp"
if [[ $resume_candidate -eq 1 ]]; then
  "${python_clean[@]}" "$disk_budget_tool" \
    --mode resume \
    --workspace "$workspace_root" \
    --temporary "${TMPDIR:-/tmp}" \
    --result "$result_dir" \
    --target "$target_dir"
fi

install_generated() {
  local final="$1"
  local candidate="$2"
  if [[ -L "$final" ]] || [[ -e "$final" && ! -f "$final" ]]; then
    echo "evidence path is not a regular file: $final" >&2
    exit 2
  fi
  if [[ -L "$candidate" || ! -f "$candidate" ]]; then
    echo "generated evidence is not a regular file: $candidate" >&2
    exit 2
  fi
  if [[ -f "$final" ]]; then
    if ! cmp -s "$candidate" "$final"; then
      echo "existing evidence differs from the current execution contract: $final" >&2
      exit 2
    fi
    rm -f "$candidate"
  else
    mv "$candidate" "$final"
  fi
}

if [[ -L "$result_dir/manifest.json" ]] \
  || [[ -e "$result_dir/manifest.json" && ! -f "$result_dir/manifest.json" ]]; then
  echo "completed manifest path is not a regular file" >&2
  exit 2
fi
if [[ -f "$result_dir/manifest.json" ]]; then
  "${python_clean[@]}" "$manifest_tool" verify \
    --result-dir "$result_dir" \
    --source-sha "$source_sha" \
    --lock-sha256 "$lock_sha256" \
    --mode "$run_mode" \
    --fingerprint-file "$fingerprint_tmp"
  echo "verified existing completed benchmark artifact set: $result_dir"
  exit 0
fi

fingerprint_candidate="$result_dir/execution-fingerprint.json.partial"
if [[ -e "$fingerprint_candidate" || -L "$fingerprint_candidate" ]]; then
  echo "incomplete execution fingerprint remains: $fingerprint_candidate" >&2
  exit 2
fi
cp "$fingerprint_tmp" "$fingerprint_candidate"
install_generated "$result_dir/execution-fingerprint.json" "$fingerprint_candidate"

build_env=(
  "${base_env[@]}"
  "CARGO_INCREMENTAL=0"
  "CARGO_PROFILE_RELEASE_DEBUG=false"
  "CARGO_TARGET_DIR=$target_dir"
  "CARGO_TERM_COLOR=never"
  "SOURCE_DATE_EPOCH=$source_date_epoch"
)
runtime_env=(
  "${base_env[@]}"
  "CARGO_INCREMENTAL=0"
  "CARGO_PROFILE_RELEASE_DEBUG=false"
  "CARGO_TARGET_DIR=$target_dir"
  "CARGO_TERM_COLOR=never"
  "SOURCE_DATE_EPOCH=$source_date_epoch"
)

write_environment() {
  local final="$result_dir/environment.txt"
  local partial="$final.partial"
  if [[ -e "$partial" || -L "$partial" ]]; then
    echo "incomplete environment evidence remains: $partial" >&2
    exit 2
  fi
  "${python_clean[@]}" "$fingerprint_tool" describe \
    --fingerprint-file "$fingerprint_tmp" > "$partial"
  install_generated "$final" "$partial"
}

write_build_environment() {
  local final="$result_dir/build-environment.json"
  local partial="$final.partial"
  if [[ -e "$partial" || -L "$partial" ]]; then
    echo "incomplete build environment evidence remains: $partial" >&2
    exit 2
  fi
  printf '%s\n' \
    '{' \
    '  "schema_version": 1,' \
    '  "execution_environment": "cleared_then_allowlisted",' \
    '  "inherited_names_without_values": ["PATH", "HOME", "TMPDIR", "CARGO_HOME", "RUSTUP_HOME"],' \
    '  "normalized": {' \
    '    "CARGO_INCREMENTAL": "0",' \
    '    "CARGO_PROFILE_RELEASE_DEBUG": "false",' \
    '    "CARGO_TARGET_DIR": "isolated",' \
    '    "CARGO_TERM_COLOR": "never",' \
    '    "LANG": "C",' \
    '    "LC_ALL": "C",' \
    '    "SOURCE_DATE_EPOCH": "git commit timestamp"' \
    '  }' \
    '}' > "$partial"
  install_generated "$final" "$partial"
}

write_lock_artifacts() {
  local lock_final="$result_dir/cargo-lock.txt"
  local lock_partial="$lock_final.partial"
  local digest_final="$result_dir/cargo-lock.sha256"
  local digest_partial="$digest_final.partial"
  if [[ -e "$lock_partial" || -L "$lock_partial" \
    || -e "$digest_partial" || -L "$digest_partial" ]]; then
    echo "incomplete Cargo.lock evidence remains" >&2
    exit 2
  fi
  cp Cargo.lock "$lock_partial"
  install_generated "$lock_final" "$lock_partial"
  printf '%s  %s\n' "$lock_sha256" "cargo-lock.txt" > "$digest_partial"
  install_generated "$digest_final" "$digest_partial"
  if [[ "$(sha256_file "$result_dir/cargo-lock.txt")" != "$lock_sha256" ]]; then
    echo "recorded Cargo.lock does not match the provenance state" >&2
    exit 2
  fi
  if [[ "$(awk '{print $1}' "$result_dir/cargo-lock.sha256")" != "$lock_sha256" ]]; then
    echo "recorded Cargo.lock digest does not match the provenance state" >&2
    exit 2
  fi
}

write_dependency_graph() {
  local final="$result_dir/dependency-graph.json"
  local partial="$final.partial"
  if [[ -e "$partial" || -L "$partial" ]]; then
    echo "incomplete dependency graph evidence remains: $partial" >&2
    exit 2
  fi
  "${build_env[@]}" cargo metadata --format-version 1 --locked \
    | "${python_clean[@]}" "$metadata_sanitizer" > "$partial"
  install_generated "$final" "$partial"
}

write_commands() {
  local final="$result_dir/commands.txt"
  local partial="$final.partial"
  if [[ -e "$partial" || -L "$partial" ]]; then
    echo "incomplete command evidence remains: $partial" >&2
    exit 2
  fi
  "${python_clean[@]}" "$manifest_tool" commands --mode "$run_mode" > "$partial"
  install_generated "$final" "$partial"
}

write_environment
write_build_environment
write_lock_artifacts
write_dependency_graph
write_commands

echo "fetching locked dependencies and building isolated release binaries" >&2
"${build_env[@]}" cargo fetch --locked
"${build_env[@]}" cargo build --profile release --locked \
  -p standalone-bench -p cluster-bench -p soak-bench

checkpoints="$result_dir/checkpoints"
mkdir -p "$checkpoints"

partial_has_unexpected_entries() {
  local directory="$1"
  shift
  local entry
  local basename
  local allowed
  local matched
  for entry in "$directory"/* "$directory"/.[!.]* "$directory"/..?*; do
    if [[ ! -e "$entry" && ! -L "$entry" ]]; then
      continue
    fi
    basename="${entry##*/}"
    matched=0
    for allowed in "$@"; do
      if [[ "$basename" == "$allowed" && -f "$entry" && ! -L "$entry" ]]; then
        matched=1
        break
      fi
    done
    if [[ $matched -ne 1 ]]; then
      return 0
    fi
  done
  return 1
}

run_json_checkpoint() {
  local name="$1"
  local binary="$2"
  shift 2
  local final="$checkpoints/$name"
  local partial="$final.partial"
  local actual_command=("$target_dir/release/$binary" "$@")
  local logical_command=("\$CARGO_TARGET_DIR/release/$binary" "$@")
  if [[ -d "$final" ]]; then
    "${python_clean[@]}" "$manifest_tool" checkpoint-verify \
      --checkpoint-dir "$final" \
      --name "$name" \
      --mode "$run_mode" \
      --fingerprint-sha256 "$fingerprint_sha256"
    echo "reusing completed checkpoint $name" >&2
    return
  fi
  if [[ -e "$final" || -L "$final" ]]; then
    echo "checkpoint path is not a directory: $name" >&2
    exit 2
  fi
  if [[ -e "$partial" && ! -d "$partial" || -L "$partial" ]]; then
    echo "checkpoint partial path is not a regular directory: $name" >&2
    exit 2
  fi
  mkdir -p "$partial"
  if partial_has_unexpected_entries "$partial" \
      result.json stderr.log checkpoint.json.partial; then
    echo "checkpoint partial contains an unexpected file: $name" >&2
    exit 2
  fi
  echo "running checkpoint $name" >&2
  "${runtime_env[@]}" "${actual_command[@]}" \
    > "$partial/result.json" 2> "$partial/stderr.log"
  "${python_clean[@]}" "$manifest_tool" checkpoint-finalize \
    --checkpoint-dir "$partial" \
    --name "$name" \
    --mode "$run_mode" \
    --fingerprint-sha256 "$fingerprint_sha256" \
    -- "${logical_command[@]}"
  mv "$partial" "$final"
}

standalone_clients="redis-tower,redis-tower-mux,redis-rs-sync,redis-rs-async,redis-rs-manager,fred"
cluster_clients="redis-tower,redis-tower-mux,redis-rs-sync,redis-rs-async,fred"
payloads=(16 64 1024 16384 102400)
concurrencies=(1 8 32 128)
depths=(10 100 1000)

standalone_paths=()
for payload in "${payloads[@]}"; do
  for concurrency in "${concurrencies[@]}"; do
    name="standalone-throughput-p$payload-c$concurrency"
    run_json_checkpoint "$name" standalone-bench \
      --secs 10 --warmup 2 --runs 3 \
      --payload-sizes "$payload" --concurrency "$concurrency" \
      --pipeline-concurrency 1 --pipeline-commands 100 \
      --clients "$standalone_clients" --workloads set,get --port 6480 \
      --include-samples --json
    standalone_paths+=("$checkpoints/$name/result.json")
  done
done

pipeline_depth_args=()
for depth in "${depths[@]}"; do
  for payload in "${payloads[@]}"; do
    name="standalone-pipeline-d$depth-p$payload-c1"
    run_json_checkpoint "$name" standalone-bench \
      --secs 10 --warmup 2 --runs 3 \
      --payload-sizes "$payload" --concurrency 1 \
      --pipeline-concurrency 1 --pipeline-commands "$depth" \
      --clients "$standalone_clients" --workloads pipeline --port 6480 \
      --include-samples --json
    pipeline_depth_args+=("$depth=$checkpoints/$name/result.json")
  done
done

pipeline_concurrency_paths=()
for concurrency in "${concurrencies[@]}"; do
  name="standalone-pipeline-d100-p1024-c$concurrency"
  if [[ $concurrency -eq 1 ]]; then
    pipeline_concurrency_paths+=("$checkpoints/$name/result.json")
    continue
  fi
  run_json_checkpoint "$name" standalone-bench \
    --secs 10 --warmup 2 --runs 3 \
    --payload-sizes 1024 --concurrency 1 \
    --pipeline-concurrency "$concurrency" --pipeline-commands 100 \
    --clients "$standalone_clients" --workloads pipeline --port 6480 \
    --include-samples --json
  pipeline_concurrency_paths+=("$checkpoints/$name/result.json")
done

cluster_paths=()
for payload in "${payloads[@]}"; do
  for concurrency in "${concurrencies[@]}"; do
    name="cluster-throughput-p$payload-c$concurrency"
    run_json_checkpoint "$name" cluster-bench \
      --secs 10 --warmup 2 --runs 3 \
      --payload-sizes "$payload" --concurrency "$concurrency" \
      --clients "$cluster_clients" --base-port 17000 --scenario throughput \
      --include-samples --json
    cluster_paths+=("$checkpoints/$name/result.json")
  done
done

soak_path=""
if [[ "$run_mode" == "publication" ]]; then
  name="standalone-soak-4h"
  final="$checkpoints/$name"
  partial="$final.partial"
  soak_env=(
    SOAK_MODE=standalone
    SOAK_CHAOS=standalone-sigkill
    SOAK_DURATION_SECS=14400
    SOAK_WARMUP_SECS=60
    SOAK_REPORT_INTERVAL_SECS=60
    SOAK_CHAOS_AFTER_SECS=7200
    SOAK_CONCURRENCY=32
    SOAK_OPERATION_TIMEOUT_MS=2000
    SOAK_ERROR_BACKOFF_MS=1
    SOAK_STARTUP_TIMEOUT_SECS=30
    SOAK_RECOVERY_TIMEOUT_SECS=30
    SOAK_PAYLOAD_BYTES=1024
    SOAK_CLUSTER_SLOT=42
    SOAK_CLUSTER_NODE_TIMEOUT_MS=1000
    SOAK_STANDALONE_PORT=6481
  )
  soak_logical_command=(
    "${soak_env[@]}"
    "\$CARGO_TARGET_DIR/release/soak-bench"
    --jsonl
  )
  if [[ -d "$final" ]]; then
    "${python_clean[@]}" "$manifest_tool" checkpoint-verify \
      --checkpoint-dir "$final" \
      --name "$name" \
      --mode "$run_mode" \
      --fingerprint-sha256 "$fingerprint_sha256"
    echo "reusing completed checkpoint $name" >&2
  else
    if [[ -e "$final" || -L "$final" ]]; then
      echo "four-hour soak checkpoint path is not a directory" >&2
      exit 2
    fi
    if [[ -e "$partial" && ! -d "$partial" || -L "$partial" ]]; then
      echo "soak checkpoint partial path is not a regular directory" >&2
      exit 2
    fi
    mkdir -p "$partial"
    if partial_has_unexpected_entries "$partial" \
        result.jsonl stderr.log checkpoint.json.partial; then
      echo "soak checkpoint partial contains an unexpected file" >&2
      exit 2
    fi
    echo "running mandatory four-hour standalone chaos soak" >&2
    "${runtime_env[@]}" "${soak_env[@]}" \
      "$target_dir/release/soak-bench" --jsonl \
      > "$partial/result.jsonl" 2> "$partial/stderr.log"
    "${python_clean[@]}" "$manifest_tool" checkpoint-finalize \
      --checkpoint-dir "$partial" \
      --name "$name" \
      --mode "$run_mode" \
      --fingerprint-sha256 "$fingerprint_sha256" \
      -- "${soak_logical_command[@]}"
    mv "$partial" "$final"
  fi
  soak_path="$final/result.jsonl"
fi

render_args=()
for path in "${standalone_paths[@]}"; do
  render_args+=(--standalone "$path")
done
for path in "${cluster_paths[@]}"; do
  render_args+=(--cluster "$path")
done
for value in "${pipeline_depth_args[@]}"; do
  render_args+=(--pipeline-depth "$value")
done
for path in "${pipeline_concurrency_paths[@]}"; do
  render_args+=(--pipeline-concurrency "$path")
done
if [[ "$run_mode" == "publication" ]]; then
  render_args+=(--soak "$soak_path")
else
  render_args+=(--matrix-only)
fi

rendered="$result_dir/rendered"
rendered_partial="$rendered.partial"
expected_summary="summary.json"
forbidden_summary="summary.incomplete.json"
if [[ "$run_mode" == "matrix-only" ]]; then
  expected_summary="summary.incomplete.json"
  forbidden_summary="summary.json"
fi
if [[ ! -d "$rendered" ]]; then
  if [[ -e "$rendered" || -L "$rendered" ]]; then
    echo "rendered artifact path is not a directory" >&2
    exit 2
  fi
  if [[ -e "$rendered_partial" && ! -d "$rendered_partial" || -L "$rendered_partial" ]]; then
    echo "rendered partial path is not a regular directory" >&2
    exit 2
  fi
  mkdir -p "$rendered_partial"
  if partial_has_unexpected_entries "$rendered_partial" \
      throughput-vs-concurrency.svg \
      p99-vs-concurrency.svg \
      "$expected_summary"; then
    echo "rendered partial contains an unexpected file" >&2
    exit 2
  fi
  "${python_clean[@]}" "$renderer" "${render_args[@]}" --output-dir "$rendered_partial"
  if [[ ! -f "$rendered_partial/$expected_summary" \
    || -e "$rendered_partial/$forbidden_summary" ]]; then
    echo "renderer produced a summary inconsistent with run mode $run_mode" >&2
    exit 2
  fi
  mv "$rendered_partial" "$rendered"
elif [[ ! -f "$rendered/$expected_summary" \
  || -e "$rendered/$forbidden_summary" ]] \
  || partial_has_unexpected_entries "$rendered" \
    throughput-vs-concurrency.svg p99-vs-concurrency.svg "$expected_summary"; then
  echo "rendered checkpoint is inconsistent with run mode $run_mode" >&2
  exit 2
fi

verify_source_provenance
verify_execution_provenance
"${python_clean[@]}" "$manifest_tool" finalize --result-dir "$result_dir"
"${python_clean[@]}" "$manifest_tool" verify \
  --result-dir "$result_dir" \
  --source-sha "$source_sha" \
  --lock-sha256 "$lock_sha256" \
  --mode "$run_mode" \
  --fingerprint-file "$fingerprint_tmp"

if [[ "$run_mode" == "publication" ]]; then
  echo "publication benchmark evidence completed and verified: $result_dir"
else
  echo "development matrices completed: $result_dir" >&2
  echo "manifest is intentionally INCOMPLETE FOR PUBLICATION (four-hour soak not run)" >&2
fi
