//! Unified client adapter for four standalone clients under test.
//!
//! Each client provides a `spawn_workers` method that creates N long-lived
//! workers (tokio tasks for async clients, blocking threads for the sync one),
//! each running an op loop until `stop` is set. Workers return their latency
//! samples when they see `stop`.
//!
//! Clients under test:
//!
//! - `RedisTower`    -- redis-tower `RedisClient` (Arc<Mutex<RedisConnection>>, baseline)
//! - `RedisTowerMux` -- redis-tower `MultiplexedClient` (AutoPipeline, high-concurrency path)
//! - `RedisRsSync`   -- redis 1.2 sync client, one persistent connection per worker thread
//! - `RedisRsAsync`  -- redis 1.2 async `MultiplexedConnection`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use redis_tower::commands::{Get as TGet, Set as TSet};
use redis_tower::{MultiplexedClient, Pipeline, RedisClient, RedisConnection};

use crate::runner::{WorkerHandle, Workload};

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
    Tower(RedisClient, String),
    TowerMux(MultiplexedClient, String),
    RedisRsSync(redis::Client),
    RedisRsAsync(redis::aio::MultiplexedConnection),
}

impl Client {
    pub fn kind(&self) -> ClientKind {
        match self {
            Client::Tower(..) => ClientKind::RedisTower,
            Client::TowerMux(..) => ClientKind::RedisTowerMux,
            Client::RedisRsSync(_) => ClientKind::RedisRsSync,
            Client::RedisRsAsync(_) => ClientKind::RedisRsAsync,
        }
    }

    pub async fn connect(kind: ClientKind, addr: &str) -> Result<Self, String> {
        match kind {
            ClientKind::RedisTower => RedisClient::connect(addr)
                .await
                .map(|c| Client::Tower(c, addr.to_owned()))
                .map_err(|e| e.to_string()),
            ClientKind::RedisTowerMux => MultiplexedClient::connect(addr)
                .await
                .map(|c| Client::TowerMux(c, addr.to_owned()))
                .map_err(|e| e.to_string()),
            ClientKind::RedisRsSync => {
                let url = format!("redis://{addr}/");
                redis::Client::open(url)
                    .map(Client::RedisRsSync)
                    .map_err(|e| e.to_string())
            }
            ClientKind::RedisRsAsync => {
                let url = format!("redis://{addr}/");
                let c = redis::Client::open(url).map_err(|e| e.to_string())?;
                let conn = c
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| e.to_string())?;
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
    ) -> Vec<WorkerHandle> {
        let mut handles = Vec::with_capacity(concurrency);
        match self {
            Client::Tower(c, addr) => {
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let addr = addr.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_loop(c, addr, worker_id, workload, stop, ops).await
                    })));
                }
            }
            Client::TowerMux(c, addr) => {
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let addr = addr.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_mux_loop(c, addr, worker_id, workload, stop, ops).await
                    })));
                }
            }
            Client::RedisRsAsync(c) => {
                for worker_id in 0..concurrency {
                    let c = c.clone();
                    let stop = stop.clone();
                    let ops = ops.clone();
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        redis_rs_async_loop(c, worker_id, workload, stop, ops).await
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
                        redis_rs_sync_loop(c, worker_id, workload, stop, ops)
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
    c: RedisClient,
    addr: String,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
) -> Vec<u32> {
    let mut latencies = Vec::with_capacity(1 << 16);
    let mut seq = worker_id as u64;

    // For Pipeline workload: each worker opens its own persistent connection
    // so Pipeline::execute has exclusive access to &mut RedisConnection.
    let mut pipe_conn = if matches!(workload, Workload::Pipeline) {
        RedisConnection::connect(&addr).await.ok()
    } else {
        None
    };

    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => c.execute(TSet::new(&key, "value")).await.is_ok(),
            Workload::Get => c.execute(TGet::new(&key)).await.is_ok(),
            Workload::Pipeline => {
                // Explicit 100-command pipeline via a dedicated RedisConnection.
                match pipe_conn.as_mut() {
                    Some(conn) => {
                        let mut p = Pipeline::new();
                        for i in 0..100u64 {
                            p = p.push(TSet::new(format!("bench:pipe:{i}"), "v"));
                        }
                        p.execute(conn).await.is_ok()
                    }
                    None => break,
                }
            }
        };
        if ok {
            record(&mut latencies, &ops, t0);
        }
    }
    latencies
}

async fn tower_mux_loop(
    c: MultiplexedClient,
    _addr: String,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
) -> Vec<u32> {
    let mut latencies = Vec::with_capacity(1 << 16);
    let mut seq = worker_id as u64;
    while !stop.load(Ordering::Relaxed) {
        let key = next_key(seq);
        seq = seq.wrapping_add(1);
        let t0 = Instant::now();
        let ok = match workload {
            Workload::Set => c.execute(TSet::new(&key, "value")).await.is_ok(),
            Workload::Get => c.execute(TGet::new(&key)).await.is_ok(),
            // MultiplexedClient: simulate pipeline by firing 100 concurrent SETs
            // (implicit pipelining via AutoPipelineService batching).
            Workload::Pipeline => {
                let handles: Vec<_> = (0..100u64)
                    .map(|i| {
                        let c = c.clone();
                        tokio::spawn(async move {
                            c.execute(TSet::new(format!("bench:pipe:{i}"), "v"))
                                .await
                                .is_ok()
                        })
                    })
                    .collect();
                let mut all_ok = true;
                for h in handles {
                    if let Ok(ok) = h.await {
                        all_ok = all_ok && ok;
                    }
                }
                all_ok
            }
        };
        if ok {
            record(&mut latencies, &ops, t0);
        }
    }
    latencies
}

async fn redis_rs_async_loop(
    mut c: redis::aio::MultiplexedConnection,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
) -> Vec<u32> {
    use redis::AsyncCommands;
    let mut latencies = Vec::with_capacity(1 << 16);
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
            Workload::Pipeline => {
                let mut p = redis::Pipeline::new();
                for i in 0..100u64 {
                    p.set(format!("bench:pipe:{i}"), "v");
                }
                p.query_async::<()>(&mut c).await.is_ok()
            }
        };
        if ok {
            record(&mut latencies, &ops, t0);
        }
    }
    latencies
}

fn redis_rs_sync_loop(
    c: redis::Client,
    worker_id: usize,
    workload: Workload,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
) -> Vec<u32> {
    use redis::Commands;
    let mut latencies = Vec::with_capacity(1 << 16);
    let mut conn = match c.get_connection() {
        Ok(c) => c,
        Err(_) => return latencies,
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
            Workload::Pipeline => {
                let mut p = redis::Pipeline::new();
                for i in 0..100u64 {
                    p.set(format!("bench:pipe:{i}"), "v");
                }
                p.exec(&mut conn).is_ok()
            }
        };
        if ok {
            record(&mut latencies, &ops, t0);
        }
    }
    latencies
}

fn record(latencies: &mut Vec<u32>, ops: &AtomicU64, t0: Instant) {
    let us = t0.elapsed().as_micros() as u64;
    latencies.push(us.min(u32::MAX as u64) as u32);
    ops.fetch_add(1, Ordering::Relaxed);
}

/// Pre-populate the keyspace used by the GET workload so every read hits.
/// Keys match the runner pattern `bench:{seq%1024}`.
pub async fn prepopulate(addr: &str) {
    if let Ok(client) = RedisClient::connect(addr).await {
        for seq in 0..1024u64 {
            let key = format!("bench:{seq}");
            let _ = client.execute(TSet::new(key, "value")).await;
        }
    }
}
