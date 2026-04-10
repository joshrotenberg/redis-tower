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
//! BENCH_SECS=8               duration per run
//! BENCH_CONCURRENCY=1,8,...  concurrency levels
//! BENCH_BASE_PORT=17000      starting port for the throwaway cluster
//! ```

mod clients;
mod runner;

use std::time::Duration;

use redis_test_harness::cluster::{ClusterConfig, RedisCluster};

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

    let base_port: u16 = std::env::var("BENCH_BASE_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(17000);
    println!(
        "starting 3-master redis cluster on ports {}..{}",
        base_port,
        base_port + 2
    );
    let mut cluster = RedisCluster::new(ClusterConfig {
        masters: 3,
        replicas_per_master: 0,
        base_port,
        work_dir: std::path::PathBuf::from(format!("/tmp/redis-cluster-bench-{base_port}")),
        ..Default::default()
    });
    let _ = cluster.stop();
    std::thread::sleep(Duration::from_millis(500));
    cluster.start().expect("failed to start cluster");
    cluster
        .wait_for_healthy(Duration::from_secs(15))
        .expect("cluster not healthy");
    println!("cluster ready");

    let seed = format!("{}:{}", cluster.config().bind, cluster.config().base_port);
    let seed_urls: Vec<String> = cluster
        .config()
        .ports()
        .take(cluster.config().masters as usize)
        .map(|p| format!("redis://{}:{}/", cluster.config().bind, p))
        .collect();

    let kinds = [
        ClientKind::RedisTower,
        ClientKind::RedisRsSync,
        ClientKind::RedisRsAsync,
        ClientKind::RedisTowerMux,
    ];

    let workloads = [Workload::Set, Workload::Get];

    // Pre-populate keys for GET workload (same keyspace the runner uses).
    println!("pre-populating 1024 keys...");
    clients::prepopulate(&seed, &seed_urls).await;
    println!("pre-populate done");

    let mut reports: Vec<BenchReport> = Vec::new();

    for wl in workloads {
        for &concurrency in &concurrencies {
            for kind in &kinds {
                let cfg = BenchConfig {
                    duration: Duration::from_secs(duration_secs),
                    concurrency,
                    workload: wl,
                };
                println!("running {kind:?} workload={wl:?} concurrency={concurrency}");
                let client = match Client::connect(*kind, &seed, &seed_urls).await {
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

    let _ = cluster.stop();
    // Some sync client resources (blocking thread pool, etc) can keep the
    // tokio runtime alive after the bench completes. The results are in
    // stdout already, so exit hard.
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
