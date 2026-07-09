//! Sentinel throughput comparison: sentinel discovery overhead vs a direct connection.
//!
//! Spins up a full Sentinel topology (1 master, 2 replicas, 3 sentinels) via
//! `redis-server-wrapper`, runs fixed-duration workloads across several
//! concurrency levels, and prints a comparison table.
//!
//! redis-rs ships no sentinel client benchmark, so there is no external client
//! to compare against. The interesting question is instead the overhead of the
//! sentinel-discovered path relative to a direct connection to the same master:
//!
//! - `SentinelClient`            -- `Arc<Mutex<SentinelConnection>>` (mutex baseline).
//! - `MultiplexedSentinelClient` -- factory-reconnect + AutoPipeline (production path).
//! - `DirectMux`                 -- `MultiplexedClient` straight to the master,
//!   no sentinel hop (the reference line).
//!
//! Compare `MultiplexedSentinelClient` against `DirectMux` at matched
//! concurrency to read the steady-state cost of the sentinel-discovered
//! connection. No failover is exercised.
//!
//! ## Env vars
//!
//! ```text
//! BENCH_SECS=8               measured window per run, in seconds (default: 10)
//! BENCH_WARMUP=2             warmup window discarded per run, in seconds (default: 2)
//! BENCH_RUNS=3               repeats per cell; results report mean +/- stddev (default: 3)
//! BENCH_CONCURRENCY=1,8,...  concurrency levels (default: 1,8,32,128)
//! BENCH_MASTER_PORT=6490     master port for the throwaway topology (default: 6490)
//! BENCH_REPLICA_BASE=6491    base port for the replicas (default: 6491)
//! BENCH_SENTINEL_BASE=26490  base port for the sentinels (default: 26490)
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo run -p sentinel-bench --release            # human-readable table
//! cargo run -p sentinel-bench --release -- --json  # JSON array on stdout
//! ```

mod clients;
mod runner;

use std::time::Duration;

use redis_server_wrapper::RedisSentinel;

use crate::clients::{Client, ClientKind, Targets};
use crate::runner::{AggregatedReport, BenchConfig, BenchReport, Workload, aggregate};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let json = std::env::args().any(|a| a == "--json");

    let duration_secs: u64 = env_parse("BENCH_SECS").unwrap_or(10);
    let warmup_secs: u64 = env_parse("BENCH_WARMUP").unwrap_or(2);
    let runs: usize = env_parse("BENCH_RUNS").filter(|&n| n >= 1).unwrap_or(3);
    let concurrencies: Vec<usize> = std::env::var("BENCH_CONCURRENCY")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|x| x.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![1, 8, 32, 128]);
    let master_port: u16 = env_parse("BENCH_MASTER_PORT").unwrap_or(6490);
    let replica_base: u16 = env_parse("BENCH_REPLICA_BASE").unwrap_or(6491);
    let sentinel_base: u16 = env_parse("BENCH_SENTINEL_BASE").unwrap_or(26490);

    // Diagnostics go to stderr so `--json` keeps stdout machine-parseable.
    eprintln!(
        "starting sentinel topology (master {master_port}, replicas from {replica_base}, sentinels from {sentinel_base})"
    );
    let sentinel = RedisSentinel::builder()
        .master_port(master_port)
        .replica_base_port(replica_base)
        .sentinel_base_port(sentinel_base)
        .replicas(2)
        .sentinels(3)
        .quorum(2)
        .start()
        .await
        .expect("failed to start sentinel topology");
    eprintln!(
        "sentinel topology ready; master at {}",
        sentinel.master_addr()
    );

    let targets = Targets {
        sentinel_addrs: sentinel.sentinel_addrs(),
        master_name: sentinel.master_name().to_string(),
        master_addr: sentinel.master_addr(),
    };

    eprintln!("pre-populating 1024 keys...");
    clients::prepopulate(&targets.master_addr).await;
    eprintln!("pre-populate done");

    let kinds = [
        ClientKind::SentinelClient,
        ClientKind::MultiplexedSentinelClient,
        ClientKind::DirectMux,
    ];

    let workloads = [Workload::Set, Workload::Get];

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
                    let client = match Client::connect(*kind, &targets).await {
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

    drop(sentinel);
    // Exit hard: results are already on stdout, and lingering connection tasks
    // can otherwise keep the runtime alive after the bench completes.
    std::process::exit(0);
}

fn env_parse<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

fn print_table(reports: &[AggregatedReport]) {
    println!(
        "{:<28} {:<10} {:>6} {:>5} {:>12} {:>14} {:>10} {:>10} {:>10} {:>10} {:>10}",
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
    println!("{}", "-".repeat(138));
    for r in reports {
        println!(
            "{:<28} {:<10} {:>6} {:>5} {:>12} {:>14} {:>10} {:>10} {:>10} {:>10} {:>10}",
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
