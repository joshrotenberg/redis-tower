//! Cluster throughput baseline: redis-tower-cluster vs redis-rs (sync & async).
//!
//! Spins up a 3-master Redis cluster via redis-server-wrapper, runs a fixed-duration
//! workload across several concurrency levels, and prints a comparison table.
//!
//! Stable-throughput clients (all four run in the default scenario):
//!
//! - `RedisTower` -- redis-tower-cluster `ClusterClient` baseline (one
//!   cluster-wide `Arc<Mutex<ClusterConnection>>`).
//! - `RedisTowerMux` -- redis-tower-cluster `MultiplexedClusterClient`
//!   (per-node factory-backed `AutoPipelineService` -- the production
//!   high-concurrency path).
//! - `RedisRsSync` -- redis 1.2 cluster blocking client.
//! - `RedisRsAsync` -- redis 1.2 cluster_async client.
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
//! BENCH_BASE_PORT=17000      starting port for the throwaway cluster
//! BENCH_SCENARIO=throughput  throughput (default), reshard, or failover
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

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let json = std::env::args().any(|a| a == "--json");
    let scenario = match selected_churn_scenario() {
        Ok(scenario) => scenario,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    if let Some(scenario) = scenario {
        if let Err(error) = run_topology_churn(scenario, json).await {
            eprintln!("cluster churn benchmark failed: {error}");
            std::process::exit(1);
        }
        std::process::exit(0);
    }

    let duration_secs: u64 = std::env::var("BENCH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let warmup_secs: u64 = std::env::var("BENCH_WARMUP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let runs: usize = std::env::var("BENCH_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(3);
    let concurrencies: Vec<usize> = std::env::var("BENCH_CONCURRENCY")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|x| x.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![1, 8, 32, 128]);

    let base_port: u16 = std::env::var("BENCH_BASE_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(redis_tower_test::ports::CLUSTER_BENCH_BASE_PORT);
    // Diagnostics go to stderr so `--json` keeps stdout machine-parseable.
    eprintln!(
        "starting 3-master redis cluster on ports {}..{}",
        base_port,
        base_port + 2
    );
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(base_port)
        .start()
        .await
        .expect("failed to start cluster");
    eprintln!("cluster ready");

    let seed = cluster.addr();
    let seed_urls: Vec<String> = cluster
        .node_addrs()
        .into_iter()
        .take(3)
        .map(|a| format!("redis://{a}/"))
        .collect();

    let kinds = [
        ClientKind::RedisTower,
        ClientKind::RedisRsSync,
        ClientKind::RedisRsAsync,
        ClientKind::RedisTowerMux,
    ];

    let workloads = [Workload::Set, Workload::Get];

    // Pre-populate keys for GET workload (same keyspace the runner uses).
    eprintln!("pre-populating 1024 keys...");
    clients::prepopulate(&seed, &seed_urls).await;
    eprintln!("pre-populate done");

    let mut reports: Vec<AggregatedReport> = Vec::new();

    for wl in workloads {
        for &concurrency in &concurrencies {
            for kind in &kinds {
                let cfg = BenchConfig {
                    duration: Duration::from_secs(duration_secs),
                    warmup: Duration::from_secs(warmup_secs),
                    concurrency,
                    workload: wl,
                };
                let mut cell: Vec<BenchReport> = Vec::with_capacity(runs);
                for run_idx in 0..runs {
                    eprintln!(
                        "running {kind:?} workload={wl:?} concurrency={concurrency} run={}/{runs}",
                        run_idx + 1
                    );
                    let client = match Client::connect(*kind, &seed, &seed_urls).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("  connect failed: {e}");
                            continue;
                        }
                    };
                    cell.push(runner::run(client, cfg).await);
                }
                if !cell.is_empty() {
                    reports.push(aggregate(&cell));
                }
            }
        }
    }

    if json {
        println!("{}", to_json(&reports));
    } else {
        println!();
        print_table(&reports);
    }

    drop(cluster);
    // Some sync client resources (blocking thread pool, etc) can keep the
    // tokio runtime alive after the bench completes. The results are in
    // stdout already, so exit hard.
    std::process::exit(0);
}

fn print_table(reports: &[AggregatedReport]) {
    println!(
        "{:<22} {:<10} {:>6} {:>5} {:>12} {:>14} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "client",
        "workload",
        "conc",
        "runs",
        "ops",
        "ops/s (mean)",
        "ops/s sd",
        "p50 (us)",
        "p90 (us)",
        "p99 (us)",
        "p999 (us)",
    );
    println!("{}", "-".repeat(132));
    for r in reports {
        println!(
            "{:<22} {:<10} {:>6} {:>5} {:>12} {:>14} {:>10} {:>10} {:>10} {:>10} {:>10}",
            format!("{:?}", r.client),
            format!("{:?}", r.workload),
            r.concurrency,
            r.runs,
            r.total_ops,
            format!("{:.0}", r.ops_per_sec_mean),
            format!("{:.0}", r.ops_per_sec_stddev),
            format!("{:.0}", r.p50_us),
            format!("{:.0}", r.p90_us),
            format!("{:.0}", r.p99_us),
            format!("{:.0}", r.p999_us),
        );
    }
}

/// Serialize the aggregated reports to a JSON array for mechanical diffing.
fn to_json(reports: &[AggregatedReport]) -> String {
    let arr: Vec<serde_json::Value> = reports
        .iter()
        .map(|r| {
            serde_json::json!({
                "client": format!("{:?}", r.client),
                "workload": format!("{:?}", r.workload),
                "concurrency": r.concurrency,
                "runs": r.runs,
                "total_ops": r.total_ops,
                "ops_per_sec_mean": r.ops_per_sec_mean,
                "ops_per_sec_stddev": r.ops_per_sec_stddev,
                "p50_us": r.p50_us,
                "p90_us": r.p90_us,
                "p99_us": r.p99_us,
                "p999_us": r.p999_us,
                "max_us": r.max_us,
            })
        })
        .collect();
    serde_json::to_string_pretty(&serde_json::Value::Array(arr))
        .unwrap_or_else(|_| "[]".to_string())
}

fn selected_churn_scenario() -> Result<Option<ChurnScenario>, String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut selected = std::env::var("BENCH_SCENARIO").ok();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--scenario" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    "--scenario requires throughput, reshard, or failover".to_string()
                })?;
                selected = Some(value.clone());
                index += 1;
            }
            "--reshard" => selected = Some("reshard".into()),
            "--failover" => selected = Some("failover".into()),
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
        "throughput" | "stable" => Ok(None),
        "reshard" | "resharding" => Ok(Some(ChurnScenario::Reshard)),
        "failover" => Ok(Some(ChurnScenario::Failover)),
        other => Err(format!(
            "unknown BENCH_SCENARIO/--scenario value {other:?}; expected throughput, reshard, or failover"
        )),
    }
}

async fn run_topology_churn(scenario: ChurnScenario, json: bool) -> Result<(), String> {
    let warmup = Duration::from_secs(env_parse("BENCH_WARMUP", 2_u64));
    let baseline = Duration::from_secs(env_parse("BENCH_BASELINE_SECS", 3_u64));
    let recovery = Duration::from_secs(env_parse("BENCH_RECOVERY_SECS", 3_u64));
    let hold = Duration::from_millis(env_parse("BENCH_CHURN_HOLD_MS", 1_000_u64));
    let topology_timeout = Duration::from_secs(env_parse("BENCH_TOPOLOGY_TIMEOUT_SECS", 15_u64));
    let concurrency = env_parse("BENCH_CHURN_CONCURRENCY", 16_usize).max(1);
    let runs = env_parse("BENCH_CHURN_RUNS", 1_usize).max(1);
    let base_port = env_parse(
        "BENCH_CHURN_BASE_PORT",
        redis_tower_test::ports::CLUSTER_BENCH_CHURN_BASE_PORT,
    );
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
