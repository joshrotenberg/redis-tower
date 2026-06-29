//! Fixed-duration workload runner with an HDR latency histogram.
//!
//! Each run discards a warmup window, records every post-warmup op latency into
//! a per-worker [`hdrhistogram::Histogram`] (constant ~few-KB footprint, unlike
//! the old per-op `Vec` storage), and merges the per-worker histograms into one
//! before reporting p50/p90/p99/p999/max. The whole run is repeated
//! `BENCH_RUNS` times and the per-cell results are aggregated into a mean and
//! standard deviation by [`aggregate`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
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
    /// Measured window, after `warmup` has elapsed.
    pub duration: Duration,
    /// Warmup window discarded from the results at the start of every run.
    pub warmup: Duration,
    pub concurrency: usize,
    pub workload: Workload,
}

/// Result of a single measured run.
#[derive(Clone, Copy, Debug)]
pub struct BenchReport {
    pub client: ClientKind,
    pub workload: Workload,
    pub concurrency: usize,
    pub total_ops: u64,
    pub ops_per_sec: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
}

/// Aggregate of `BENCH_RUNS` repeated runs of the same cell. `ops_per_sec`
/// carries a mean and standard deviation; latency percentiles are averaged
/// across runs.
#[derive(Clone, Copy, Debug)]
pub struct AggregatedReport {
    pub client: ClientKind,
    pub workload: Workload,
    pub concurrency: usize,
    pub runs: usize,
    pub total_ops: u64,
    pub ops_per_sec_mean: f64,
    pub ops_per_sec_stddev: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
}

pub enum WorkerHandle {
    Async(TokioJoinHandle<Histogram<u64>>),
    Thread(JoinHandle<Histogram<u64>>),
}

/// A fresh recording histogram. Three significant figures, auto-resizing so a
/// long soak run never overflows its trackable range.
pub fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new(3).expect("valid histogram sigfig")
}

pub async fn run(client: Client, cfg: BenchConfig) -> BenchReport {
    let kind = client.kind();
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let warmup_deadline = Instant::now() + cfg.warmup;
    let handles = client.spawn_workers(
        cfg.concurrency,
        cfg.workload,
        stop.clone(),
        ops.clone(),
        warmup_deadline,
    );

    // Discard the warmup window, then time only the measured window.
    tokio::time::sleep(cfg.warmup).await;
    let measure_start = Instant::now();
    tokio::time::sleep(cfg.duration).await;
    stop.store(true, Ordering::Relaxed);

    let mut merged = new_histogram();
    for h in handles {
        let hist = match h {
            WorkerHandle::Async(h) => h.await.unwrap_or_else(|_| new_histogram()),
            WorkerHandle::Thread(h) => {
                tokio::task::spawn_blocking(move || h.join().unwrap_or_else(|_| new_histogram()))
                    .await
                    .unwrap_or_else(|_| new_histogram())
            }
        };
        let _ = merged.add(&hist);
    }
    let wall = measure_start.elapsed().as_secs_f64();
    let total_ops = ops.load(Ordering::Relaxed);

    BenchReport {
        client: kind,
        workload: cfg.workload,
        concurrency: cfg.concurrency,
        total_ops,
        ops_per_sec: total_ops as f64 / wall.max(f64::MIN_POSITIVE),
        p50_us: merged.value_at_quantile(0.50) as f64,
        p90_us: merged.value_at_quantile(0.90) as f64,
        p99_us: merged.value_at_quantile(0.99) as f64,
        p999_us: merged.value_at_quantile(0.999) as f64,
        max_us: merged.max() as f64,
    }
}

/// Population mean of a sample set. Empty input yields `0.0`.
pub fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// Population standard deviation. Empty or single-element input yields `0.0`.
pub fn std_dev(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let m = mean(xs);
    let var = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / xs.len() as f64;
    var.sqrt()
}

/// Aggregate the repeated runs of a single cell into mean/stddev throughput and
/// averaged latency percentiles. Panics on empty input -- a cell always has at
/// least one run.
pub fn aggregate(reports: &[BenchReport]) -> AggregatedReport {
    let first = reports.first().expect("at least one run per cell");
    let ops: Vec<f64> = reports.iter().map(|r| r.ops_per_sec).collect();
    AggregatedReport {
        client: first.client,
        workload: first.workload,
        concurrency: first.concurrency,
        runs: reports.len(),
        total_ops: reports.iter().map(|r| r.total_ops).sum(),
        ops_per_sec_mean: mean(&ops),
        ops_per_sec_stddev: std_dev(&ops),
        p50_us: mean(&reports.iter().map(|r| r.p50_us).collect::<Vec<_>>()),
        p90_us: mean(&reports.iter().map(|r| r.p90_us).collect::<Vec<_>>()),
        p99_us: mean(&reports.iter().map(|r| r.p99_us).collect::<Vec<_>>()),
        p999_us: mean(&reports.iter().map(|r| r.p999_us).collect::<Vec<_>>()),
        max_us: mean(&reports.iter().map(|r| r.max_us).collect::<Vec<_>>()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(ops_per_sec: f64, p50: f64, total_ops: u64) -> BenchReport {
        BenchReport {
            client: ClientKind::RedisTower,
            workload: Workload::Set,
            concurrency: 8,
            total_ops,
            ops_per_sec,
            p50_us: p50,
            p90_us: p50 * 2.0,
            p99_us: p50 * 3.0,
            p999_us: p50 * 4.0,
            max_us: p50 * 5.0,
        }
    }

    #[test]
    fn mean_and_std_dev_basic() {
        assert_eq!(mean(&[2.0, 4.0, 6.0]), 4.0);
        assert_eq!(std_dev(&[]), 0.0);
        assert_eq!(std_dev(&[5.0]), 0.0);
        // population stddev of {2,4,6} = sqrt(8/3)
        let s = std_dev(&[2.0, 4.0, 6.0]);
        assert!((s - (8.0_f64 / 3.0).sqrt()).abs() < 1e-9);
    }

    #[test]
    fn aggregate_means_across_runs() {
        let runs = [
            report(100.0, 10.0, 1000),
            report(200.0, 20.0, 2000),
            report(300.0, 30.0, 3000),
        ];
        let agg = aggregate(&runs);
        assert_eq!(agg.runs, 3);
        assert_eq!(agg.total_ops, 6000);
        assert_eq!(agg.ops_per_sec_mean, 200.0);
        assert!((agg.ops_per_sec_stddev - (20000.0_f64 / 3.0).sqrt()).abs() < 1e-6);
        assert_eq!(agg.p50_us, 20.0);
        assert_eq!(agg.p90_us, 40.0);
    }

    #[test]
    fn histogram_percentiles_track_recorded_values() {
        let mut h = new_histogram();
        for v in 1..=1000u64 {
            h.saturating_record(v);
        }
        // value_at_quantile is monotonic and within the recorded range.
        assert!(h.value_at_quantile(0.50) <= h.value_at_quantile(0.99));
        assert!(h.value_at_quantile(0.99) <= h.max());
        assert!(h.max() >= 1000 - 1); // 3 sig figs -> tiny rounding
    }

    #[test]
    fn merged_histograms_sum_counts() {
        let mut a = new_histogram();
        let mut b = new_histogram();
        for _ in 0..50 {
            a.saturating_record(100);
        }
        for _ in 0..50 {
            b.saturating_record(200);
        }
        a.add(&b).unwrap();
        assert_eq!(a.len(), 100);
    }
}
