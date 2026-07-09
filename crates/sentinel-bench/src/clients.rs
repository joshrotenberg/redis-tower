//! Unified client adapter for the three sentinel-topology clients under test.
//!
//! Each client provides a `spawn_workers` method that creates N long-lived
//! tokio tasks, each running an op loop until `stop` is set. Workers record
//! post-warmup op latencies into a local HDR histogram and return it when they
//! see `stop`.
//!
//! Clients under test:
//!
//! - `SentinelClient`            -- redis-tower-sentinel `SentinelClient`
//!   (`Arc<Mutex<SentinelConnection>>`, the mutex-based baseline).
//! - `MultiplexedSentinelClient` -- redis-tower-sentinel `MultiplexedSentinelClient`
//!   (factory-reconnect + AutoPipeline, the production high-concurrency path).
//! - `DirectMux`                 -- redis-tower `MultiplexedClient` connected
//!   straight to the discovered master, no sentinel hop. This is the reference
//!   line: `MultiplexedSentinelClient` vs `DirectMux` isolates the steady-state
//!   overhead of routing through the sentinel-discovered connection.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;
use redis_tower::MultiplexedClient;
use redis_tower::commands::{Get as TGet, Set as TSet};
use redis_tower_sentinel::{MultiplexedSentinelClient, SentinelClient};

use crate::runner::{WorkerHandle, Workload, new_histogram};

#[derive(Clone, Copy, Debug)]
pub enum ClientKind {
    SentinelClient,
    MultiplexedSentinelClient,
    DirectMux,
}

/// Connection targets discovered from the running sentinel topology.
#[derive(Clone)]
pub struct Targets {
    pub sentinel_addrs: Vec<String>,
    pub master_name: String,
    pub master_addr: String,
}

/// A client is just a factory that knows how to spin up N workers.
pub enum Client {
    Sentinel(SentinelClient),
    SentinelMux(MultiplexedSentinelClient),
    DirectMux(MultiplexedClient),
}

impl Client {
    pub fn kind(&self) -> ClientKind {
        match self {
            Client::Sentinel(_) => ClientKind::SentinelClient,
            Client::SentinelMux(_) => ClientKind::MultiplexedSentinelClient,
            Client::DirectMux(_) => ClientKind::DirectMux,
        }
    }

    pub async fn connect(kind: ClientKind, targets: &Targets) -> Result<Self, String> {
        match kind {
            ClientKind::SentinelClient => {
                SentinelClient::connect(&targets.sentinel_addrs, &targets.master_name)
                    .await
                    .map(Client::Sentinel)
                    .map_err(|e| e.to_string())
            }
            ClientKind::MultiplexedSentinelClient => {
                MultiplexedSentinelClient::connect_with_reconnect(
                    &targets.sentinel_addrs,
                    &targets.master_name,
                )
                .await
                .map(Client::SentinelMux)
                .map_err(|e| e.to_string())
            }
            ClientKind::DirectMux => MultiplexedClient::connect(&targets.master_addr)
                .await
                .map(Client::DirectMux)
                .map_err(|e| e.to_string()),
        }
    }

    pub fn spawn_workers(
        &self,
        concurrency: usize,
        workload: Workload,
        stop: Arc<AtomicBool>,
        ops: Arc<AtomicU64>,
        warmup_deadline: Instant,
    ) -> Vec<WorkerHandle> {
        let mut handles = Vec::with_capacity(concurrency);
        for worker_id in 0..concurrency {
            let stop = stop.clone();
            let ops = ops.clone();
            let handle = match self {
                Client::Sentinel(c) => {
                    let c = c.clone();
                    tokio::spawn(async move {
                        sentinel_loop(c, worker_id, workload, stop, ops, warmup_deadline).await
                    })
                }
                Client::SentinelMux(c) => {
                    let c = c.clone();
                    tokio::spawn(async move {
                        sentinel_mux_loop(c, worker_id, workload, stop, ops, warmup_deadline).await
                    })
                }
                Client::DirectMux(c) => {
                    let c = c.clone();
                    tokio::spawn(async move {
                        direct_mux_loop(c, worker_id, workload, stop, ops, warmup_deadline).await
                    })
                }
            };
            handles.push(handle);
        }
        handles
    }
}

fn next_key(seq: u64) -> String {
    format!("bench:{}", seq % 1024)
}

async fn sentinel_loop(
    c: SentinelClient,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    warmup_deadline: Instant,
) -> Histogram<u64> {
    let mut hist = new_histogram();
    let mut seq = worker_id as u64;
    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => c.execute(TSet::new(&key, "value")).await.is_ok(),
            Workload::Get => c.execute(TGet::new(&key)).await.is_ok(),
        };
        if ok {
            record(&mut hist, &ops, t0, warmup_deadline);
        }
    }
    hist
}

async fn sentinel_mux_loop(
    c: MultiplexedSentinelClient,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    warmup_deadline: Instant,
) -> Histogram<u64> {
    let mut hist = new_histogram();
    let mut seq = worker_id as u64;
    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => c.execute(TSet::new(&key, "value")).await.is_ok(),
            Workload::Get => c.execute(TGet::new(&key)).await.is_ok(),
        };
        if ok {
            record(&mut hist, &ops, t0, warmup_deadline);
        }
    }
    hist
}

async fn direct_mux_loop(
    c: MultiplexedClient,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    warmup_deadline: Instant,
) -> Histogram<u64> {
    let mut hist = new_histogram();
    let mut seq = worker_id as u64;
    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => c.execute(TSet::new(&key, "value")).await.is_ok(),
            Workload::Get => c.execute(TGet::new(&key)).await.is_ok(),
        };
        if ok {
            record(&mut hist, &ops, t0, warmup_deadline);
        }
    }
    hist
}

/// Record one completed op. Ops that complete before `warmup_deadline` are
/// discarded from both the latency histogram and the throughput counter.
fn record(hist: &mut Histogram<u64>, ops: &AtomicU64, t0: Instant, warmup_deadline: Instant) {
    if Instant::now() < warmup_deadline {
        return;
    }
    let us = t0.elapsed().as_micros() as u64;
    hist.saturating_record(us);
    ops.fetch_add(1, Ordering::Relaxed);
}

/// Pre-populate the keyspace used by the GET workload so every read hits.
/// Keys match the runner pattern `bench:{seq%1024}`. Writes go straight to the
/// master via a direct connection.
pub async fn prepopulate(master_addr: &str) {
    if let Ok(client) = MultiplexedClient::connect(master_addr).await {
        for seq in 0..1024u64 {
            let key = format!("bench:{seq}");
            let _ = client.execute(TSet::new(key, "value")).await;
        }
    }
}
