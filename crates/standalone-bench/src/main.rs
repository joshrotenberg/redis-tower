//! Standalone throughput comparison: redis-tower vs redis-rs.
//!
//! Spins up a Redis server via `redis-server-wrapper`, runs fixed-duration
//! workloads across several concurrency levels, and prints a comparison table.
//!
//! Clients under test:
//! - `RedisTower`    -- redis-tower `RedisClient` (`Arc<Mutex<RedisConnection>>`)
//! - `RedisTowerMux` -- redis-tower `MultiplexedClient` (AutoPipeline)
//! - `RedisRsSync`   -- redis-rs sync client (one conn per thread)
//! - `RedisRsAsync`  -- redis-rs async `MultiplexedConnection`
//!
//! ## Env vars
//!
//! ```text
//! BENCH_SECS=8               measured window per run, in seconds (default: 10)
//! BENCH_WARMUP=2             warmup window discarded per run, in seconds (default: 2)
//! BENCH_RUNS=3               repeats per cell; results report mean +/- stddev (default: 3)
//! BENCH_CONCURRENCY=1,8,...  concurrency levels (default: 1,8,32,128)
//! BENCH_PORT=6480            port for the throwaway server (default: 6480)
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo run -p standalone-bench --release            # human-readable table
//! cargo run -p standalone-bench --release -- --json  # JSON array on stdout
//! ```
//!
//! ## Interpreting results
//!
//! - `ops/s`: higher is better; `p50`/`p90`/`p99`/`p999`: lower latency is better.
//! - `ops/s` is reported as a mean across `BENCH_RUNS` with the standard
//!   deviation; latency percentiles are HDR-histogram values averaged across runs.
//! - `RedisTowerMux` should outperform `RedisTower` at higher concurrency
//!   due to auto-pipelining (concurrent ops batch into one round-trip).
//! - Compare `RedisTowerMux` vs `RedisRsAsync` to see redis-tower's implicit
//!   pipeline efficiency vs redis-rs's multiplexed connection.
//! - The `Pipeline` workload measures explicit batching (100 SETs per call).

mod clients;
mod runner;

use std::time::Duration;

use redis_server_wrapper::RedisServer;

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
    let port: u16 = std::env::var("BENCH_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6480);

    // Diagnostics go to stderr so `--json` keeps stdout machine-parseable.
    eprintln!("starting redis server on port {port}");
    let server = RedisServer::new()
        .port(port)
        .start()
        .await
        .expect("failed to start redis server");
    eprintln!("server ready at {}", server.addr());

    let addr = server.addr();

    eprintln!("pre-populating 1024 keys...");
    clients::prepopulate(&addr).await;
    eprintln!("pre-populate done");

    let kinds = [
        ClientKind::RedisTower,
        ClientKind::RedisTowerMux,
        ClientKind::RedisRsSync,
        ClientKind::RedisRsAsync,
    ];

    let workloads = [Workload::Set, Workload::Get, Workload::Pipeline];

    let mut reports: Vec<AggregatedReport> = Vec::new();

    for wl in workloads {
        // Pipeline workload: only run at concurrency 1 (each worker batches 100 ops).
        let concs: &[usize] = if matches!(wl, Workload::Pipeline) {
            &[1]
        } else {
            &concurrencies
        };
        for &concurrency in concs {
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
                    let client = match Client::connect(*kind, &addr).await {
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

    drop(server);
    // Some sync client resources can keep the tokio runtime alive after the
    // bench completes. Results are in stdout, so exit hard.
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
