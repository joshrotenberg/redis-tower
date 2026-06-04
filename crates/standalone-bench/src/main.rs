//! Standalone throughput comparison: redis-tower vs redis-rs.
//!
//! Spins up a Redis server via `redis-server-wrapper`, runs fixed-duration
//! workloads across several concurrency levels, and prints a comparison table.
//!
//! Clients under test:
//! - `RedisTower`    -- redis-tower `RedisClient` (Arc<Mutex<RedisConnection>>)
//! - `RedisTowerMux` -- redis-tower `MultiplexedClient` (AutoPipeline)
//! - `RedisRsSync`   -- redis-rs sync client (one conn per thread)
//! - `RedisRsAsync`  -- redis-rs async `MultiplexedConnection`
//!
//! ## Env vars
//!
//! ```text
//! BENCH_SECS=8               duration per run (default: 10)
//! BENCH_CONCURRENCY=1,8,...  concurrency levels (default: 1,8,32,128)
//! BENCH_PORT=6480            port for the throwaway server (default: 6480)
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo run -p standalone-bench --release
//! ```
//!
//! ## Interpreting results
//!
//! - `ops/s`: higher is better; `p50`/`p99`: lower latency is better.
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
use crate::runner::{BenchConfig, BenchReport, Workload};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let duration_secs: u64 = std::env::var("BENCH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
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

    println!("starting redis server on port {port}");
    let server = RedisServer::new()
        .port(port)
        .start()
        .await
        .expect("failed to start redis server");
    println!("server ready at {}", server.addr());

    let addr = server.addr();

    println!("pre-populating 1024 keys...");
    clients::prepopulate(&addr).await;
    println!("pre-populate done");

    let kinds = [
        ClientKind::RedisTower,
        ClientKind::RedisTowerMux,
        ClientKind::RedisRsSync,
        ClientKind::RedisRsAsync,
    ];

    let workloads = [Workload::Set, Workload::Get, Workload::Pipeline];

    let mut reports: Vec<BenchReport> = Vec::new();

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
                    concurrency,
                    workload: wl,
                };
                println!("running {kind:?} workload={wl:?} concurrency={concurrency}");
                let client = match Client::connect(*kind, &addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("  connect failed: {e}");
                        continue;
                    }
                };
                let report = runner::run(client, cfg).await;
                reports.push(report);
            }
        }
    }

    println!();
    print_table(&reports);

    drop(server);
    // Some sync client resources can keep the tokio runtime alive after the
    // bench completes. Results are in stdout, so exit hard.
    std::process::exit(0);
}

fn print_table(reports: &[BenchReport]) {
    println!(
        "{:<22} {:<10} {:>8} {:>12} {:>12} {:>12} {:>12}",
        "client", "workload", "conc", "ops", "ops/s", "p50 (us)", "p99 (us)"
    );
    println!("{}", "-".repeat(92));
    for r in reports {
        println!(
            "{:<22} {:<10} {:>8} {:>12} {:>12} {:>12} {:>12}",
            format!("{:?}", r.client),
            format!("{:?}", r.workload),
            r.concurrency,
            r.total_ops,
            format!("{:.0}", r.ops_per_sec),
            format!("{:.0}", r.p50_us),
            format!("{:.0}", r.p99_us),
        );
    }
}
