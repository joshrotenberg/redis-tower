//! Cluster throughput baseline: redis-tower-cluster vs redis-rs (sync & async).
//!
//! Spins up a 3-master Redis cluster via redis-test-harness, runs a fixed-duration
//! workload across several concurrency levels, and prints a comparison table.
//!
//! Clients under test (all four always run):
//!
//! - `RedisTower` -- redis-tower-cluster `ClusterClient` baseline (one
//!   cluster-wide `Arc<Mutex<ClusterConnection>>`).
//! - `RedisTowerMux` -- redis-tower-cluster `MultiplexedClusterClient`
//!   (per-node factory-backed `AutoPipelineService` -- the production
//!   high-concurrency path).
//! - `RedisRsSync` -- redis 1.2 cluster blocking client.
//! - `RedisRsAsync` -- redis 1.2 cluster_async client.
//!
//! Env vars:
//! ```text
//! BENCH_SECS=8               measured window per run, in seconds (default: 10)
//! BENCH_WARMUP=2             warmup window discarded per run, in seconds (default: 2)
//! BENCH_RUNS=3               repeats per cell; results report mean +/- stddev (default: 3)
//! BENCH_CONCURRENCY=1,8,...  concurrency levels (default: 1,8,32,128)
//! BENCH_BASE_PORT=17000      starting port for the throwaway cluster
//! ```
//!
//! Running:
//! ```bash
//! cargo run -p cluster-bench --release            # human-readable table
//! cargo run -p cluster-bench --release -- --json  # JSON array on stdout
//! ```

mod clients;
mod runner;

use std::time::Duration;

use redis_server_wrapper::RedisCluster;

use crate::clients::{Client, ClientKind};
use crate::runner::{AggregatedReport, BenchConfig, BenchReport, Workload, aggregate};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let json = std::env::args().any(|a| a == "--json");

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
        .unwrap_or(17000);
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
