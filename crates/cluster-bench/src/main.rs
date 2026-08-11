//! Redis Cluster comparison across redis-tower, redis-rs, and fred.
//!
//! Spins up a 3-master Redis cluster via redis-server-wrapper, runs a fixed-duration
//! workload across several concurrency levels, and prints a comparison table.
//!
//! Stable-throughput clients (all five run in the default scenario):
//!
//! - `RedisTower` -- redis-tower-cluster `ClusterClient` baseline (one
//!   cluster-wide `Arc<Mutex<ClusterConnection>>`).
//! - `RedisTowerMux` -- redis-tower-cluster `MultiplexedClusterClient`
//!   (per-node factory-backed `AutoPipelineService` -- the production
//!   high-concurrency path).
//! - `RedisRsSync` -- redis-rs cluster blocking client.
//! - `RedisRsAsync` -- redis-rs cluster_async client.
//! - `Fred` -- fred's clustered async client.
//!
//! The opt-in reshard and failover scenarios compare the two concurrent
//! clients, `RedisTowerMux` and `RedisRsAsync`, under the same topology event.
//!
//! Env vars:
//! ```text
//! BENCH_SECS=8               measured window per run, in seconds (default: 10)
//! BENCH_WARMUP=2             warmup window discarded per run, in seconds (default: 2)
//! BENCH_RUNS=3               repeats per cell; results report mean +/- stddev (default: 3)
//! BENCH_CONCURRENCY=1,8,...  concurrency levels (default: 1,8,32,128)
//! BENCH_PAYLOAD_SIZES=64,1K,16K payload bytes (default: 64,1024,16384)
//! BENCH_CLIENTS=fred,...     client aliases to include (default: all five)
//! BENCH_INCLUDE_SAMPLES=true retain bounded per-run samples in JSON
//! BENCH_BASE_PORT=17000      starting port for the throwaway cluster
//! BENCH_SCENARIO=throughput  throughput (default), replica, reshard, or failover
//! ```
//!
//! Running:
//! ```bash
//! cargo run -p cluster-bench --release            # human-readable table
//! cargo run -p cluster-bench --release -- --json  # JSON array on stdout
//! ```

mod churn;
mod clients;
mod runner;

use std::collections::BTreeMap;
use std::time::Duration;

use redis_server_wrapper::{RedisCluster, chaos};
use redis_tower_test::cluster::{ClusterFixture, key_for_slot};

use crate::churn::{
    AggregatedChurnReport, ChurnClient, ChurnConfig, ChurnEventReport, ChurnReport, ChurnScenario,
    ChurnWorkload, aggregate_churn,
};
use crate::clients::{Client, ClientKind};
use crate::runner::{AggregatedReport, BenchConfig, BenchReport, Workload, aggregate};

const THROUGHPUT_SCHEMA_VERSION: u64 = 2;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let json = std::env::args().any(|a| a == "--json");
    let scenario = match selected_scenario() {
        Ok(scenario) => scenario,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let result = match scenario {
        SelectedScenario::Throughput => run_throughput(json).await,
        SelectedScenario::Replica => run_replica_reads(json).await,
        SelectedScenario::Churn(scenario) => run_topology_churn(scenario, json).await,
    };
    if let Err(error) = result {
        eprintln!("cluster benchmark failed: {error}");
        std::process::exit(1);
    }
    // Blocking comparison clients can retain runtime resources after output.
    std::process::exit(0);
}

#[derive(Clone)]
struct MatrixConfig {
    duration: Duration,
    warmup: Duration,
    runs: usize,
    concurrencies: Vec<usize>,
    payload_sizes: Vec<usize>,
    clients: Vec<ClientKind>,
    include_samples: bool,
}

impl MatrixConfig {
    fn from_env(default_clients: &[ClientKind]) -> Result<Self, String> {
        Ok(Self {
            duration: Duration::from_secs(env_or_arg_parse("BENCH_SECS", "--secs", 10_u64)?),
            warmup: Duration::from_secs(env_or_arg_parse("BENCH_WARMUP", "--warmup", 2_u64)?),
            runs: env_or_arg_parse("BENCH_RUNS", "--runs", 3_usize)?.max(1),
            concurrencies: parse_positive_list(
                &env_or_arg("BENCH_CONCURRENCY", "--concurrency", "1,8,32,128"),
                "concurrency",
            )?,
            payload_sizes: parse_size_list(&env_or_arg(
                "BENCH_PAYLOAD_SIZES",
                "--payload-sizes",
                "64,1024,16384",
            ))?,
            clients: parse_clients(
                env_or_arg_optional("BENCH_CLIENTS", "--clients").as_deref(),
                default_clients,
            )?,
            include_samples: flag_or_env("BENCH_INCLUDE_SAMPLES", "--include-samples")?,
        })
    }
}

async fn run_throughput(json: bool) -> Result<(), String> {
    let config = MatrixConfig::from_env(&ClientKind::THROUGHPUT_DEFAULTS)?;
    let base_port = env_or_arg_parse("BENCH_BASE_PORT", "--base-port", 17_000_u16)?;
    // Diagnostics go to stderr so `--json` keeps stdout machine-parseable.
    eprintln!(
        "starting 3-master redis cluster on ports {}..{}",
        base_port,
        base_port + 2
    );
    let cluster = RedisCluster::builder()
        // Match the executable fingerprinted by run_publication.sh and keep
        // every node free of Redis Stack modules.
        .redis_server_bin("redis-server")
        .with_node_config(|context| context.server.no_stack_modules())
        .masters(3)
        .replicas_per_master(0)
        .base_port(base_port)
        .start()
        .await
        .map_err(|error| format!("failed to start cluster: {error}"))?;
    eprintln!("cluster ready");

    let seed = cluster.addr();
    let seed_urls: Vec<String> = cluster
        .node_addrs()
        .into_iter()
        .take(3)
        .map(|a| format!("redis://{a}/"))
        .collect();

    let workloads = [Workload::Set, Workload::Get];
    let mut reports: Vec<AggregatedReport> = Vec::new();
    for &payload_bytes in &config.payload_sizes {
        let payload = "x".repeat(payload_bytes);
        eprintln!("pre-populating 1024 keys with {payload_bytes}-byte values...");
        clients::prepopulate(&seed, &payload).await?;
        run_matrix_cells(
            &config,
            &workloads,
            payload_bytes,
            &seed,
            &seed_urls,
            &mut reports,
        )
        .await?;
    }
    drop(cluster);
    print_reports(&reports, json, config.include_samples);
    Ok(())
}

async fn run_replica_reads(json: bool) -> Result<(), String> {
    let config = MatrixConfig::from_env(&ClientKind::REPLICA_DEFAULTS)?;
    let mut builder = ClusterFixture::builder();
    if let Some(port) = env_or_arg_optional("BENCH_REPLICA_BASE_PORT", "--replica-base-port") {
        builder = builder.base_port(
            port.parse()
                .map_err(|_| format!("invalid replica base port {port:?}"))?,
        );
    }
    eprintln!("starting managed 3-master + 3-replica cluster");
    let fixture = builder.start().await.map_err(|error| error.to_string())?;
    let seed = fixture.seed_addr();
    let seed_urls = fixture
        .node_addrs()
        .into_iter()
        .map(|address| format!("redis://{address}/"))
        .collect::<Vec<_>>();
    let mut reports = Vec::new();
    for &payload_bytes in &config.payload_sizes {
        let payload = "x".repeat(payload_bytes);
        eprintln!(
            "pre-populating and replica-verifying 1024 keys with {payload_bytes}-byte values..."
        );
        clients::prepopulate_and_verify_replicas(&fixture, &payload).await?;
        run_matrix_cells(
            &config,
            &[Workload::Get],
            payload_bytes,
            &seed,
            &seed_urls,
            &mut reports,
        )
        .await?;
    }
    drop(fixture);
    print_reports(&reports, json, config.include_samples);
    Ok(())
}

async fn run_matrix_cells(
    config: &MatrixConfig,
    workloads: &[Workload],
    payload_bytes: usize,
    seed: &str,
    seed_urls: &[String],
    reports: &mut Vec<AggregatedReport>,
) -> Result<(), String> {
    for &workload in workloads {
        for &concurrency in &config.concurrencies {
            for &kind in &config.clients {
                let bench = BenchConfig {
                    duration: config.duration,
                    warmup: config.warmup,
                    concurrency,
                    workload,
                    payload_bytes,
                };
                let mut cell = Vec::<BenchReport>::with_capacity(config.runs);
                for run_index in 0..config.runs {
                    eprintln!(
                        "running {} workload={workload:?} payload={payload_bytes} concurrency={concurrency} run={}/{}",
                        kind.as_str(),
                        run_index + 1,
                        config.runs
                    );
                    let client = Client::connect(kind, seed, seed_urls)
                        .await
                        .map_err(|error| format!("{} connect failed: {error}", kind.as_str()))?;
                    cell.push(
                        runner::run(client, bench)
                            .await
                            .map_err(|error| format!("{} worker failed: {error}", kind.as_str()))?,
                    );
                }
                reports.push(aggregate(&cell));
            }
        }
    }
    Ok(())
}

fn print_reports(reports: &[AggregatedReport], json: bool, include_samples: bool) {
    if json {
        println!("{}", to_json(reports, include_samples));
    } else {
        println!();
        print_table(reports);
    }
}

fn print_table(reports: &[AggregatedReport]) {
    println!(
        "{:<25} {:<6} {:>8} {:>6} {:>5} {:>11} {:>14} {:>10} {:>9} {:>9} {:>9} {:>8}",
        "client",
        "work",
        "bytes",
        "conc",
        "runs",
        "commands",
        "commands/s",
        "cmd/s sd",
        "p50 (us)",
        "p99 (us)",
        "p999 (us)",
        "errors",
    );
    println!("{}", "-".repeat(145));
    for r in reports {
        println!(
            "{:<25} {:<6} {:>8} {:>6} {:>5} {:>11} {:>14} {:>10} {:>9} {:>9} {:>9} {:>8}",
            r.client.as_str(),
            format!("{:?}", r.workload),
            r.payload_bytes,
            r.concurrency,
            r.runs,
            r.total_commands,
            format!("{:.0}", r.commands_per_sec_mean),
            format!("{:.0}", r.commands_per_sec_stddev),
            format!("{:.0}", r.p50_us),
            format!("{:.0}", r.p99_us),
            format!("{:.0}", r.p999_us),
            r.errors,
        );
    }
}

/// Serialize the aggregated reports to a JSON array for mechanical diffing.
fn to_json(reports: &[AggregatedReport], include_samples: bool) -> String {
    let arr: Vec<serde_json::Value> = reports
        .iter()
        .map(|r| {
            let mut value = serde_json::json!({
                "schema_version": THROUGHPUT_SCHEMA_VERSION,
                "client": format!("{:?}", r.client),
                "client_id": r.client.as_str(),
                "workload": format!("{:?}", r.workload),
                "payload_bytes": r.payload_bytes,
                "concurrency": r.concurrency,
                "runs": r.runs,
                "commands_per_batch": 1,
                // Compatibility aliases retain the v1 command/op semantics.
                "total_ops": r.total_commands,
                "ops_per_sec_mean": r.commands_per_sec_mean,
                "ops_per_sec_stddev": r.commands_per_sec_stddev,
                "total_batches": r.total_commands,
                "batches_per_sec_mean": r.commands_per_sec_mean,
                "batches_per_sec_stddev": r.commands_per_sec_stddev,
                "total_commands": r.total_commands,
                "errors": r.errors,
                "commands_per_sec_mean": r.commands_per_sec_mean,
                "commands_per_sec_stddev": r.commands_per_sec_stddev,
                "latency_unit": "command",
                "p50_us": r.p50_us,
                "p90_us": r.p90_us,
                "p99_us": r.p99_us,
                "p999_us": r.p999_us,
                "max_us": r.max_us,
            });
            if include_samples {
                value["samples"] = serde_json::Value::Array(
                    r.samples
                        .iter()
                        .enumerate()
                        .map(|(index, sample)| {
                            serde_json::json!({
                                "run": index + 1,
                                "total_batches": sample.total_commands,
                                "total_commands": sample.total_commands,
                                "errors": sample.errors,
                                "elapsed_secs": sample.elapsed_secs,
                                "batches_per_sec": sample.commands_per_sec,
                                "commands_per_sec": sample.commands_per_sec,
                                "p50_us": sample.p50_us,
                                "p90_us": sample.p90_us,
                                "p99_us": sample.p99_us,
                                "p999_us": sample.p999_us,
                                "max_us": sample.max_us,
                            })
                        })
                        .collect(),
                );
            }
            value
        })
        .collect();
    serde_json::to_string_pretty(&serde_json::Value::Array(arr))
        .unwrap_or_else(|_| "[]".to_string())
}

enum SelectedScenario {
    Throughput,
    Replica,
    Churn(ChurnScenario),
}

fn selected_scenario() -> Result<SelectedScenario, String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut selected = std::env::var("BENCH_SCENARIO").ok();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--scenario" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    "--scenario requires throughput, replica, reshard, or failover".to_string()
                })?;
                selected = Some(value.clone());
                index += 1;
            }
            "--reshard" => selected = Some("reshard".into()),
            "--failover" => selected = Some("failover".into()),
            "--replica-reads" => selected = Some("replica".into()),
            value if value.starts_with("--scenario=") => {
                selected = Some(value["--scenario=".len()..].to_string());
            }
            _ => {}
        }
        index += 1;
    }

    match selected
        .as_deref()
        .unwrap_or("throughput")
        .to_ascii_lowercase()
        .as_str()
    {
        "throughput" | "stable" => Ok(SelectedScenario::Throughput),
        "replica" | "replica-read" | "replica-reads" => Ok(SelectedScenario::Replica),
        "reshard" | "resharding" => Ok(SelectedScenario::Churn(ChurnScenario::Reshard)),
        "failover" => Ok(SelectedScenario::Churn(ChurnScenario::Failover)),
        other => Err(format!(
            "unknown BENCH_SCENARIO/--scenario value {other:?}; expected throughput, replica, reshard, or failover"
        )),
    }
}

fn arg_value(name: &str) -> Option<String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let prefix = format!("{name}=");
    for (index, value) in args.iter().enumerate() {
        if let Some(value) = value.strip_prefix(&prefix) {
            return Some(value.to_owned());
        }
        if value == name {
            return args.get(index + 1).cloned();
        }
    }
    None
}

fn env_or_arg_optional(env: &str, arg: &str) -> Option<String> {
    arg_value(arg).or_else(|| std::env::var(env).ok())
}

fn env_or_arg(env: &str, arg: &str, default: &str) -> String {
    env_or_arg_optional(env, arg).unwrap_or_else(|| default.to_owned())
}

fn env_or_arg_parse<T>(env: &str, arg: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr + Copy,
{
    let Some(value) = env_or_arg_optional(env, arg) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| format!("invalid value {value:?} for {arg}/{env}"))
}

fn flag_or_env(env: &str, arg: &str) -> Result<bool, String> {
    if std::env::args().any(|value| value == arg) {
        return Ok(true);
    }
    match std::env::var(env) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(format!(
                "invalid value {value:?} for {env}; expected true or false"
            )),
        },
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(format!("could not read {env}: {error}")),
    }
}

fn parse_positive_list(value: &str, name: &str) -> Result<Vec<usize>, String> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| format!("invalid {name} value {value:?}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err(format!("{name} list must not be empty"));
    }
    Ok(values)
}

fn parse_size_list(value: &str) -> Result<Vec<usize>, String> {
    let values = value
        .split(',')
        .map(parse_size)
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err("payload size list must not be empty".into());
    }
    Ok(values)
}

fn parse_size(value: &str) -> Result<usize, String> {
    let normalized = value.trim().to_ascii_lowercase();
    let (number, multiplier) = if let Some(number) = normalized.strip_suffix("kib") {
        (number, 1024usize)
    } else if let Some(number) = normalized.strip_suffix("kb") {
        (number, 1024)
    } else if let Some(number) = normalized.strip_suffix('k') {
        (number, 1024)
    } else if let Some(number) = normalized.strip_suffix("mib") {
        (number, 1024 * 1024)
    } else if let Some(number) = normalized.strip_suffix("mb") {
        (number, 1024 * 1024)
    } else if let Some(number) = normalized.strip_suffix('m') {
        (number, 1024 * 1024)
    } else if let Some(number) = normalized.strip_suffix('b') {
        (number, 1)
    } else {
        (normalized.as_str(), 1)
    };
    let number = number
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("invalid payload size {value:?}"))?;
    number
        .checked_mul(multiplier)
        .filter(|size| *size > 0)
        .ok_or_else(|| format!("invalid payload size {value:?}"))
}

fn parse_clients(value: Option<&str>, defaults: &[ClientKind]) -> Result<Vec<ClientKind>, String> {
    let Some(value) = value else {
        return Ok(defaults.to_vec());
    };
    let clients = value
        .split(',')
        .map(|value| {
            ClientKind::parse(value).ok_or_else(|| format!("unknown benchmark client {value:?}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if clients.is_empty() {
        return Err("client list must not be empty".into());
    }
    Ok(clients)
}

async fn run_topology_churn(scenario: ChurnScenario, json: bool) -> Result<(), String> {
    let warmup = Duration::from_secs(env_parse("BENCH_WARMUP", 2_u64));
    let baseline = Duration::from_secs(env_parse("BENCH_BASELINE_SECS", 3_u64));
    let recovery = Duration::from_secs(env_parse("BENCH_RECOVERY_SECS", 3_u64));
    let hold = Duration::from_millis(env_parse("BENCH_CHURN_HOLD_MS", 1_000_u64));
    let topology_timeout = Duration::from_secs(env_parse("BENCH_TOPOLOGY_TIMEOUT_SECS", 15_u64));
    let concurrency = env_parse("BENCH_CHURN_CONCURRENCY", 16_usize).max(1);
    let runs = env_parse("BENCH_CHURN_RUNS", 1_usize).max(1);
    let base_port = env_parse("BENCH_CHURN_BASE_PORT", 17_800_u16);
    let cluster_node_timeout = env_parse("BENCH_CLUSTER_NODE_TIMEOUT_MS", 1_000_u64);
    let slot = env_parse("BENCH_CHURN_SLOT", 42_u16);
    if slot >= 16_384 {
        return Err(format!("BENCH_CHURN_SLOT must be below 16384, got {slot}"));
    }
    let workload = match std::env::var("BENCH_CHURN_WORKLOAD")
        .unwrap_or_else(|_| "get".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "get" => ChurnWorkload::Get,
        "set" => ChurnWorkload::Set,
        value => {
            return Err(format!(
                "BENCH_CHURN_WORKLOAD must be get or set, got {value:?}"
            ));
        }
    };
    let config = ChurnConfig {
        warmup,
        baseline,
        recovery,
        concurrency,
        workload,
    };

    let mut by_client: BTreeMap<String, Vec<ChurnReport>> = BTreeMap::new();
    for run_index in 0..runs {
        eprintln!(
            "starting 3-master + 3-replica cluster for {} run {}/{} on ports {}..{}",
            scenario.as_str(),
            run_index + 1,
            runs,
            base_port,
            base_port.saturating_add(5)
        );
        let fixture = ClusterFixture::builder()
            .base_port(base_port)
            .cluster_node_timeout(cluster_node_timeout)
            .start()
            .await
            .map_err(|error| error.to_string())?;
        let seed = fixture.seed_addr();
        let seed_urls = fixture
            .node_addrs()
            .into_iter()
            .map(|address| format!("redis://{address}/"))
            .collect::<Vec<_>>();
        let key = key_for_slot(slot);
        let topology = fixture
            .topology()
            .await
            .map_err(|error| error.to_string())?;
        let old_owner = topology
            .owner_of_slot(slot)
            .cloned()
            .ok_or_else(|| format!("slot {slot} has no owner"))?;
        churn::seed_key(&old_owner.addr, &key).await?;

        let tower_client = ChurnClient::connect_tower_mux(&seed).await?;
        let redis_rs_client = match ChurnClient::connect_redis_rs(&seed_urls).await {
            Ok(client) => client,
            Err(error) => {
                tower_client.shutdown().await;
                return Err(error);
            }
        };
        let clients = vec![tower_client, redis_rs_client];

        eprintln!(
            "running {} workload={} slot={} concurrency={} warmup={:?} baseline={:?}",
            scenario.as_str(),
            workload.as_str(),
            slot,
            concurrency,
            warmup,
            baseline
        );
        let reports = match scenario {
            ChurnScenario::Reshard => {
                let fixture_ref = &fixture;
                let target = topology
                    .masters()
                    .find(|node| node.index != old_owner.index)
                    .cloned()
                    .ok_or_else(|| "reshard target master is unavailable".to_string())?;
                churn::run_churn(scenario, clients, key, config, |trigger| async move {
                    let guard = fixture_ref
                        .begin_reshard(slot, target.index)
                        .await
                        .map_err(|error| error.to_string())?;
                    // Conservatively begin churn immediately before MIGRATE so
                    // no redirect-affected completion can leak into baseline.
                    let event_started_at = std::time::Instant::now();
                    trigger.mark_churn_started();
                    let moved = guard
                        .migrate_keys()
                        .await
                        .map_err(|error| error.to_string())?;
                    // MIGRATE has now returned with the affected key on the
                    // importing node. Start recovery timing at that confirmed
                    // workload-visible boundary, not at injector setup.
                    trigger.mark_triggered();
                    // Hold IMPORTING/MIGRATING after moving the affected key so
                    // stale routers repeatedly exercise ASK + ASKING.
                    tokio::time::sleep(hold).await;
                    guard.complete().await.map_err(|error| error.to_string())?;
                    let owner = fixture_ref
                        .wait_for_slot_owner(slot, &target.id, topology_timeout)
                        .await
                        .map_err(|error| error.to_string())?;
                    let convergence = event_started_at.elapsed();
                    // Retain a post-handoff churn window for MOVED handling and
                    // topology-refresh samples; timing values are informational.
                    tokio::time::sleep(hold).await;
                    Ok(ChurnEventReport {
                        event_duration: event_started_at.elapsed(),
                        topology_convergence: Some(convergence),
                        topology_change: Some(format!(
                            "slot {slot}: {} -> {} (migrated {moved} keys)",
                            old_owner.addr, owner.addr
                        )),
                    })
                })
                .await?
            }
            ChurnScenario::Failover => {
                let fixture_ref = &fixture;
                churn::run_churn(scenario, clients, key, config, |trigger| async move {
                    let killed = old_owner.clone();
                    // The owner was resolved before workers entered the stable
                    // window. Begin churn before SIGKILL so an EOF observed
                    // while the kill subprocess returns cannot leak into it.
                    let event_started_at = std::time::Instant::now();
                    trigger.mark_churn_started();
                    chaos::kill_node(fixture_ref.node(killed.index));
                    // The kill subprocess has returned, so the old owner is
                    // now confirmed dead. Recovery timing starts here.
                    trigger.mark_triggered();
                    let owner = fixture_ref
                        .wait_for_slot_owner_change(slot, &killed.id, topology_timeout)
                        .await
                        .map_err(|error| error.to_string())?;
                    let convergence = event_started_at.elapsed();
                    // Continue the measured churn window briefly after election
                    // so refresh/reconnect success is represented in tail data.
                    tokio::time::sleep(hold).await;
                    Ok(ChurnEventReport {
                        event_duration: event_started_at.elapsed(),
                        topology_convergence: Some(convergence),
                        topology_change: Some(format!(
                            "slot {slot}: {} -> {} (killed fixture node {})",
                            killed.addr, owner.addr, killed.index
                        )),
                    })
                })
                .await?
            }
        };

        for report in reports {
            by_client
                .entry(report.client.clone())
                .or_default()
                .push(report);
        }
        drop(fixture);
    }

    let reports = by_client
        .values()
        .map(|runs| aggregate_churn(runs))
        .collect::<Vec<_>>();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&reports).map_err(|error| error.to_string())?
        );
    } else {
        println!();
        print_churn_table(&reports);
    }
    Ok(())
}

fn env_parse<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn print_churn_table(reports: &[AggregatedChurnReport]) {
    println!(
        "{:<10} {:<18} {:>4} {:>5} {:>9} {:>9} {:>9} {:>10} {:>10} {:>10} {:>9} {:>7} {:>8} {:>8} {:>9}",
        "scenario",
        "client",
        "runs",
        "conc",
        "base p99",
        "churn p99",
        "p99 delta",
        "base p999",
        "churn p999",
        "p999 delta",
        "dropped",
        "err %",
        "first ok",
        "err win",
        "topology",
    );
    println!("{}", "-".repeat(158));
    for report in reports {
        println!(
            "{:<10} {:<18} {:>4} {:>5} {:>9} {:>9} {:>9} {:>10} {:>10} {:>10} {:>9} {:>7.2} {:>8} {:>8} {:>9}",
            report.scenario.as_str(),
            report.client,
            report.runs,
            report.concurrency,
            display_us(report.stable_p99_us_mean),
            display_us(report.churn_p99_us_mean),
            display_delta_us(report.p99_delta_us_mean),
            display_us(report.stable_p999_us_mean),
            display_us(report.churn_p999_us_mean),
            display_delta_us(report.p999_delta_us_mean),
            report.dropped_ops,
            report.churn_error_rate_pct,
            display_ms(report.time_to_first_success_ms_mean),
            display_ms(report.error_window_ms_mean),
            display_ms(report.topology_convergence_ms_mean),
        );
        println!(
            "  samples (ok/error): stable={}/{} churn={}/{} recovery={}/{}; first-success-runs={}/{} recovery-runs={}/{} error-runs; recovery-after-error={}",
            report.stable_successes,
            report.stable_errors,
            report.churn_successes,
            report.churn_errors,
            report.recovery_successes,
            report.recovery_errors,
            report.first_success_runs,
            report.runs,
            report.recovered_after_error_runs,
            report.runs_with_errors,
            display_ms(report.recovery_after_error_ms_mean),
        );
        if let Some(redirects) = &report.redirects {
            println!(
                "  redis-tower events: ASK={} MOVED={} refresh_ok={} refresh_partial={} refresh_error={} refresh_mean={:.1}ms",
                redirects.ask,
                redirects.moved,
                report
                    .topology_refreshes
                    .as_ref()
                    .map_or(0, |value| value.success),
                report
                    .topology_refreshes
                    .as_ref()
                    .map_or(0, |value| value.partial),
                report
                    .topology_refreshes
                    .as_ref()
                    .map_or(0, |value| value.error),
                report
                    .topology_refreshes
                    .as_ref()
                    .map_or(0.0, |value| value.mean_duration_ms),
            );
        }
    }
    println!(
        "latencies are microseconds; first-ok, recovery-after-error, error-window, and topology are milliseconds"
    );
    println!("timings are informational and intentionally have no pass/fail thresholds");
}

fn display_ms(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}"))
        .unwrap_or_else(|| "n/a".into())
}

fn display_us(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.0}"))
        .unwrap_or_else(|| "n/a".into())
}

fn display_delta_us(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:+.0}"))
        .unwrap_or_else(|| "n/a".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binary_payload_sizes() {
        assert_eq!(parse_size("64B").unwrap(), 64);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("16KiB").unwrap(), 16 * 1024);
        assert!(parse_size("0").is_err());
    }

    #[test]
    fn client_selection_rejects_unknown_aliases() {
        assert!(parse_clients(Some("fred,wat"), &ClientKind::THROUGHPUT_DEFAULTS).is_err());
    }

    #[test]
    fn throughput_json_retains_v1_aliases_and_adds_stable_ids() {
        let report = AggregatedReport {
            client: ClientKind::RedisTowerMux,
            workload: Workload::Get,
            concurrency: 8,
            payload_bytes: 1024,
            runs: 3,
            total_commands: 42,
            errors: 1,
            commands_per_sec_mean: 123.0,
            commands_per_sec_stddev: 4.0,
            p50_us: 10.0,
            p90_us: 20.0,
            p99_us: 30.0,
            p999_us: 40.0,
            max_us: 50.0,
            samples: vec![BenchReport {
                client: ClientKind::RedisTowerMux,
                workload: Workload::Get,
                concurrency: 8,
                payload_bytes: 1024,
                total_commands: 42,
                errors: 1,
                elapsed_secs: 42.0 / 123.0,
                commands_per_sec: 123.0,
                p50_us: 10.0,
                p90_us: 20.0,
                p99_us: 30.0,
                p999_us: 40.0,
                max_us: 50.0,
            }],
        };
        let value: serde_json::Value = serde_json::from_str(&to_json(&[report], false)).unwrap();
        let record = &value[0];
        assert_eq!(record["schema_version"], 2);
        assert_eq!(record["client"], "RedisTowerMux");
        assert_eq!(record["client_id"], "redis-tower-mux");
        assert_eq!(record["total_ops"], record["total_commands"]);
        assert_eq!(record["ops_per_sec_mean"], record["commands_per_sec_mean"]);
        assert_eq!(record["total_batches"], record["total_commands"]);
        assert!(record.get("samples").is_none());
    }

    #[test]
    fn publication_json_can_retain_raw_run_samples() {
        let report = aggregate(&[
            BenchReport {
                client: ClientKind::RedisTowerMux,
                workload: Workload::Get,
                concurrency: 1,
                payload_bytes: 16,
                total_commands: 10,
                errors: 0,
                elapsed_secs: 0.1,
                commands_per_sec: 100.0,
                p50_us: 10.0,
                p90_us: 20.0,
                p99_us: 30.0,
                p999_us: 40.0,
                max_us: 50.0,
            },
            BenchReport {
                client: ClientKind::RedisTowerMux,
                workload: Workload::Get,
                concurrency: 1,
                payload_bytes: 16,
                total_commands: 20,
                errors: 0,
                elapsed_secs: 0.1,
                commands_per_sec: 200.0,
                p50_us: 11.0,
                p90_us: 21.0,
                p99_us: 31.0,
                p999_us: 41.0,
                max_us: 51.0,
            },
        ]);
        let value: serde_json::Value = serde_json::from_str(&to_json(&[report], true)).unwrap();
        assert_eq!(value[0]["samples"].as_array().unwrap().len(), 2);
        assert_eq!(value[0]["samples"][1]["commands_per_sec"], 200.0);
    }
}
