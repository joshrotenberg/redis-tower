//! Unified client adapter for the four cluster clients under test.
//!
//! Each client provides a `spawn_workers` method that creates N long-lived
//! workers (tokio tasks for async clients, blocking threads for the sync one),
//! each holding its own persistent connection / shared worker. Workers record
//! post-warmup op latencies into a local HDR histogram and return it on `stop`.
//!
//! Clients under test:
//!
//! - `RedisTower` -- redis-tower-cluster ClusterClient (baseline: one
//!   `Arc<Mutex<ClusterConnection>>` serializing all ops).
//! - `RedisTowerMux` -- redis-tower-cluster MultiplexedClusterClient
//!   (factory-backed AutoPipelineService per master).
//! - `RedisRsSync` -- redis 1.2 cluster blocking client, one persistent
//!   connection per worker thread.
//! - `RedisRsAsync` -- redis 1.2 cluster_async ClusterConnection.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;
use redis_tower_cluster::{ClusterClient as TowerClusterClient, MultiplexedClusterClient};
use redis_tower_commands::{Get as TGet, Set as TSet};

use crate::runner::{WorkerHandle, Workload, new_histogram};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug)]
pub enum ClientKind {
    RedisTower,
    RedisTowerMux,
    RedisRsSync,
    RedisRsAsync,
}

/// A client is just a factory that knows how to spin up N workers.
pub enum Client {
    Tower(TowerClusterClient),
    TowerMux(MultiplexedClusterClient),
    RedisRsSync(Arc<redis::cluster::ClusterClient>),
    RedisRsAsync(redis::cluster_async::ClusterConnection),
}

impl Client {
    pub fn kind(&self) -> ClientKind {
        match self {
            Client::Tower(_) => ClientKind::RedisTower,
            Client::TowerMux(_) => ClientKind::RedisTowerMux,
            Client::RedisRsSync(_) => ClientKind::RedisRsSync,
            Client::RedisRsAsync(_) => ClientKind::RedisRsAsync,
        }
    }

    pub async fn connect(
        kind: ClientKind,
        seed: &str,
        seed_urls: &[String],
    ) -> Result<Self, String> {
        match kind {
            ClientKind::RedisTower => TowerClusterClient::connect(seed)
                .await
                .map(Client::Tower)
                .map_err(|e| e.to_string()),
            ClientKind::RedisTowerMux => MultiplexedClusterClient::connect(seed)
                .await
                .map(Client::TowerMux)
                .map_err(|e| e.to_string()),
            ClientKind::RedisRsSync => {
                let c = redis::cluster::ClusterClient::new(seed_urls.to_vec())
                    .map_err(|e| e.to_string())?;
                Ok(Client::RedisRsSync(Arc::new(c)))
            }
            ClientKind::RedisRsAsync => {
                let c = redis::cluster::ClusterClient::new(seed_urls.to_vec())
                    .map_err(|e| e.to_string())?;
                let conn = c.get_async_connection().await.map_err(|e| e.to_string())?;
                Ok(Client::RedisRsAsync(conn))
            }
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
        match self {
            Client::Tower(c) => {
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_loop(c, worker_id, workload, stop, ops, warmup_deadline).await
                    })));
                }
            }
            Client::TowerMux(c) => {
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_mux_loop(c, worker_id, workload, stop, ops, warmup_deadline).await
                    })));
                }
            }
            Client::RedisRsAsync(c) => {
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        redis_rs_async_loop(c, worker_id, workload, stop, ops, warmup_deadline)
                            .await
                    })));
                }
            }
            Client::RedisRsSync(c) => {
                // Each worker gets its own persistent blocking connection,
                // running on a dedicated OS thread.
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    let jh = std::thread::spawn(move || {
                        redis_rs_sync_loop(c, worker_id, workload, stop, ops, warmup_deadline)
                    });
                    handles.push(WorkerHandle::Thread(jh));
                }
            }
        }
        handles
    }
}

fn next_key(seq: u64) -> String {
    format!("bench:{}", seq % 1024)
}

async fn tower_loop(
    c: TowerClusterClient,
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
            Workload::Set => c.execute(TSet::new(key, "value")).await.is_ok(),
            Workload::Get => c.execute(TGet::new(key)).await.is_ok(),
        };
        if ok {
            record(&mut hist, &ops, t0, warmup_deadline);
        }
    }
    hist
}

async fn tower_mux_loop(
    c: MultiplexedClusterClient,
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

async fn redis_rs_async_loop(
    mut c: redis::cluster_async::ClusterConnection,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    warmup_deadline: Instant,
) -> Histogram<u64> {
    use redis::AsyncCommands;
    let mut hist = new_histogram();
    let mut seq = worker_id as u64;
    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => c.set::<_, _, ()>(&key, "value").await.is_ok(),
            Workload::Get => {
                let r: redis::RedisResult<Option<Vec<u8>>> = c.get(&key).await;
                r.is_ok()
            }
        };
        if ok {
            record(&mut hist, &ops, t0, warmup_deadline);
        }
    }
    hist
}

fn redis_rs_sync_loop(
    c: Arc<redis::cluster::ClusterClient>,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    warmup_deadline: Instant,
) -> Histogram<u64> {
    use redis::Commands;
    let mut hist = new_histogram();
    let mut conn = match c.get_connection() {
        Ok(c) => c,
        Err(_) => return hist,
    };
    let mut seq = worker_id as u64;
    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => conn.set::<_, _, ()>(&key, "value").is_ok(),
            Workload::Get => {
                let r: redis::RedisResult<Option<Vec<u8>>> = conn.get(&key);
                r.is_ok()
            }
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
/// Keys match the runner pattern `bench:{seq%1024}`.
pub async fn prepopulate(seed: &str, _seed_urls: &[String]) {
    let client = TowerClusterClient::connect(seed)
        .await
        .expect("prepopulate connect");
    for seq in 0..1024 {
        let key = format!("bench:{}", seq);
        let _ = client.execute(TSet::new(key, "value")).await;
    }
}
