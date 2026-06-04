//! Fixed-duration workload runner with per-op latency histogram.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle as TokioJoinHandle;

use crate::clients::{Client, ClientKind};

#[derive(Clone, Copy, Debug)]
pub enum Workload {
    Set,
    Get,
    /// Explicit 100-command pipeline per iteration.
    Pipeline,
}

#[derive(Clone, Copy, Debug)]
pub struct BenchConfig {
    pub duration: Duration,
    pub concurrency: usize,
    pub workload: Workload,
}

#[derive(Debug)]
pub struct BenchReport {
    pub client: ClientKind,
    pub workload: Workload,
    pub concurrency: usize,
    pub total_ops: u64,
    pub ops_per_sec: f64,
    pub p50_us: f64,
    pub p99_us: f64,
}

pub enum WorkerHandle {
    Async(TokioJoinHandle<Vec<u32>>),
    Thread(JoinHandle<Vec<u32>>),
}

pub async fn run(client: Client, cfg: BenchConfig) -> BenchReport {
    let kind = client.kind();
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let handles = client.spawn_workers(cfg.concurrency, cfg.workload, stop.clone(), ops.clone());

    tokio::time::sleep(cfg.duration).await;
    stop.store(true, Ordering::Relaxed);

    let mut all_latencies: Vec<u32> = Vec::new();
    for h in handles {
        match h {
            WorkerHandle::Async(h) => {
                if let Ok(mut l) = h.await {
                    all_latencies.append(&mut l);
                }
            }
            WorkerHandle::Thread(h) => {
                if let Ok(mut l) =
                    tokio::task::spawn_blocking(move || h.join().unwrap_or_default()).await
                {
                    all_latencies.append(&mut l);
                }
            }
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let total_ops = ops.load(Ordering::Relaxed);
    all_latencies.sort_unstable();
    let p50 = percentile(&all_latencies, 0.50);
    let p99 = percentile(&all_latencies, 0.99);

    BenchReport {
        client: kind,
        workload: cfg.workload,
        concurrency: cfg.concurrency,
        total_ops,
        ops_per_sec: total_ops as f64 / wall.max(f64::MIN_POSITIVE),
        p50_us: p50,
        p99_us: p99,
    }
}

fn percentile(sorted: &[u32], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64) * q).clamp(0.0, (sorted.len() - 1) as f64);
    sorted[idx as usize] as f64
}
