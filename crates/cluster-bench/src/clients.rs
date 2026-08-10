//! Unified client adapters for the Redis Cluster comparison.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use fred::prelude::{Builder as FredBuilder, ClientLike, Config as FredConfig, KeysInterface};
use hdrhistogram::Histogram;
use redis_tower::{ReadPreference, RedisConnection};
use redis_tower_cluster::{ClusterClient as TowerClusterClient, MultiplexedClusterClient};
use redis_tower_commands::{Get as TGet, Set as TSet, Wait as TWait};
use redis_tower_test::cluster::{ClusterFixture, hash_slot};

use crate::runner::{WorkerHandle, WorkerResult, Workload, new_histogram};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientKind {
    RedisTower,
    RedisTowerMux,
    RedisTowerMuxReplica,
    RedisRsSync,
    RedisRsAsync,
    Fred,
}

impl ClientKind {
    pub const THROUGHPUT_DEFAULTS: [Self; 5] = [
        Self::RedisTower,
        Self::RedisTowerMux,
        Self::RedisRsSync,
        Self::RedisRsAsync,
        Self::Fred,
    ];

    pub const REPLICA_DEFAULTS: [Self; 2] = [Self::RedisTowerMux, Self::RedisTowerMuxReplica];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RedisTower => "redis-tower",
            Self::RedisTowerMux => "redis-tower-mux",
            Self::RedisTowerMuxReplica => "redis-tower-mux-replica",
            Self::RedisRsSync => "redis-rs-sync",
            Self::RedisRsAsync => "redis-rs-async",
            Self::Fred => "fred",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "redis-tower" | "tower" => Some(Self::RedisTower),
            "redis-tower-mux" | "tower-mux" | "mux" => Some(Self::RedisTowerMux),
            "redis-tower-mux-replica" | "tower-replica" | "replica" => {
                Some(Self::RedisTowerMuxReplica)
            }
            "redis-rs-sync" | "redis-sync" => Some(Self::RedisRsSync),
            "redis-rs-async" | "redis-async" => Some(Self::RedisRsAsync),
            "fred" => Some(Self::Fred),
            _ => None,
        }
    }
}

/// A connected client factory that can spin up long-lived workers.
pub enum Client {
    Tower(TowerClusterClient),
    TowerMux(MultiplexedClusterClient),
    TowerMuxReplica(MultiplexedClusterClient),
    RedisRsSync(Arc<redis::cluster::ClusterClient>),
    RedisRsAsync(redis::cluster_async::ClusterConnection),
    Fred(fred::clients::Client),
}

impl Client {
    pub fn kind(&self) -> ClientKind {
        match self {
            Self::Tower(_) => ClientKind::RedisTower,
            Self::TowerMux(_) => ClientKind::RedisTowerMux,
            Self::TowerMuxReplica(_) => ClientKind::RedisTowerMuxReplica,
            Self::RedisRsSync(_) => ClientKind::RedisRsSync,
            Self::RedisRsAsync(_) => ClientKind::RedisRsAsync,
            Self::Fred(_) => ClientKind::Fred,
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
                .map(Self::Tower)
                .map_err(|error| error.to_string()),
            ClientKind::RedisTowerMux => MultiplexedClusterClient::connect(seed)
                .await
                .map(Self::TowerMux)
                .map_err(|error| error.to_string()),
            ClientKind::RedisTowerMuxReplica => MultiplexedClusterClient::builder(seed)
                .read_preference(ReadPreference::Replica)
                .connect()
                .await
                .map(Self::TowerMuxReplica)
                .map_err(|error| error.to_string()),
            ClientKind::RedisRsSync => {
                let client = redis::cluster::ClusterClient::new(seed_urls.to_vec())
                    .map_err(|error| error.to_string())?;
                Ok(Self::RedisRsSync(Arc::new(client)))
            }
            ClientKind::RedisRsAsync => {
                let client = redis::cluster::ClusterClient::new(seed_urls.to_vec())
                    .map_err(|error| error.to_string())?;
                let connection = client
                    .get_async_connection()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Self::RedisRsAsync(connection))
            }
            ClientKind::Fred => {
                let url = format!("redis-cluster://{seed}");
                let config =
                    FredConfig::from_url_clustered(&url).map_err(|error| error.to_string())?;
                let client = FredBuilder::from_config(config)
                    .build()
                    .map_err(|error| error.to_string())?;
                client.init().await.map_err(|error| error.to_string())?;
                Ok(Self::Fred(client))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_workers(
        &self,
        concurrency: usize,
        workload: Workload,
        payload_bytes: usize,
        stop: Arc<AtomicBool>,
        batches: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
        warmup_deadline: Instant,
    ) -> Vec<WorkerHandle> {
        let mut handles = Vec::with_capacity(concurrency);
        let payload: Arc<str> = Arc::from("x".repeat(payload_bytes));
        match self {
            Self::Tower(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_loop(client, context).await
                    })));
                }
            }
            Self::TowerMux(client) | Self::TowerMuxReplica(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_mux_loop(client, context).await
                    })));
                }
            }
            Self::RedisRsAsync(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        redis_rs_async_loop(client, context).await
                    })));
                }
            }
            Self::RedisRsSync(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Thread(std::thread::spawn(move || {
                        redis_rs_sync_loop(client, context)
                    })));
                }
            }
            Self::Fred(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        fred_loop(client, context).await
                    })));
                }
            }
        }
        handles
    }
}

struct WorkerContext {
    worker_id: usize,
    workload: Workload,
    payload: Arc<str>,
    stop: Arc<AtomicBool>,
    batches: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    warmup_deadline: Instant,
}

impl WorkerContext {
    #[allow(clippy::too_many_arguments)]
    fn new(
        worker_id: usize,
        workload: Workload,
        payload: Arc<str>,
        stop: Arc<AtomicBool>,
        batches: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
        warmup_deadline: Instant,
    ) -> Self {
        Self {
            worker_id,
            workload,
            payload,
            stop,
            batches,
            errors,
            warmup_deadline,
        }
    }
}

fn next_key(sequence: u64) -> String {
    format!("bench:{}", sequence % 1024)
}

async fn tower_loop(client: TowerClusterClient, context: WorkerContext) -> WorkerResult {
    let mut histogram = new_histogram();
    let mut sequence = context.worker_id as u64;
    while !context.stop.load(Ordering::Relaxed) {
        let key = next_key(sequence);
        sequence = sequence.wrapping_add(1);
        let started = Instant::now();
        let succeeded = match context.workload {
            Workload::Set => client
                .execute(TSet::new(key, context.payload.as_ref()))
                .await
                .is_ok(),
            Workload::Get => matches!(
                client.execute(TGet::new(key)).await,
                Ok(Some(value)) if value.len() == context.payload.len()
            ),
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn tower_mux_loop(client: MultiplexedClusterClient, context: WorkerContext) -> WorkerResult {
    let mut histogram = new_histogram();
    let mut sequence = context.worker_id as u64;
    while !context.stop.load(Ordering::Relaxed) {
        let key = next_key(sequence);
        sequence = sequence.wrapping_add(1);
        let started = Instant::now();
        let succeeded = match context.workload {
            Workload::Set => client
                .execute(TSet::new(&key, context.payload.as_ref()))
                .await
                .is_ok(),
            Workload::Get => matches!(
                client.execute(TGet::new(&key)).await,
                Ok(Some(value)) if value.len() == context.payload.len()
            ),
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn redis_rs_async_loop(
    mut client: redis::cluster_async::ClusterConnection,
    context: WorkerContext,
) -> WorkerResult {
    use redis::AsyncCommands;

    let mut histogram = new_histogram();
    let mut sequence = context.worker_id as u64;
    while !context.stop.load(Ordering::Relaxed) {
        let key = next_key(sequence);
        sequence = sequence.wrapping_add(1);
        let started = Instant::now();
        let succeeded = match context.workload {
            Workload::Set => client
                .set::<_, _, ()>(&key, context.payload.as_ref())
                .await
                .is_ok(),
            Workload::Get => matches!(
                client.get::<_, Option<Vec<u8>>>(&key).await,
                Ok(Some(value)) if value.len() == context.payload.len()
            ),
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

fn redis_rs_sync_loop(
    client: Arc<redis::cluster::ClusterClient>,
    context: WorkerContext,
) -> WorkerResult {
    use redis::Commands;

    let mut histogram = new_histogram();
    let mut connection = match client.get_connection() {
        Ok(connection) => connection,
        Err(error) => {
            return Err(format!(
                "redis-rs sync worker {} failed to connect: {error}",
                context.worker_id
            ));
        }
    };
    let mut sequence = context.worker_id as u64;
    while !context.stop.load(Ordering::Relaxed) {
        let key = next_key(sequence);
        sequence = sequence.wrapping_add(1);
        let started = Instant::now();
        let succeeded = match context.workload {
            Workload::Set => connection
                .set::<_, _, ()>(&key, context.payload.as_ref())
                .is_ok(),
            Workload::Get => matches!(
                connection.get::<_, Option<Vec<u8>>>(&key),
                Ok(Some(value)) if value.len() == context.payload.len()
            ),
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn fred_loop(client: fred::clients::Client, context: WorkerContext) -> WorkerResult {
    let mut histogram = new_histogram();
    let mut sequence = context.worker_id as u64;
    while !context.stop.load(Ordering::Relaxed) {
        let key = next_key(sequence);
        sequence = sequence.wrapping_add(1);
        let started = Instant::now();
        let succeeded = match context.workload {
            Workload::Set => client
                .set::<(), _, _>(&key, context.payload.as_ref(), None, None, false)
                .await
                .is_ok(),
            Workload::Get => matches!(
                client.get::<Option<String>, _>(&key).await,
                Ok(Some(value)) if value.len() == context.payload.len()
            ),
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

fn record_outcome(
    histogram: &mut Histogram<u64>,
    context: &WorkerContext,
    started: Instant,
    succeeded: bool,
) {
    if Instant::now() < context.warmup_deadline {
        return;
    }
    if succeeded {
        histogram.saturating_record(started.elapsed().as_micros() as u64);
        context.batches.fetch_add(1, Ordering::Relaxed);
    } else {
        context.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Populate every benchmark key and fail on the first rejected write.
pub async fn prepopulate(seed: &str, payload: &str) -> Result<(), String> {
    let client = TowerClusterClient::connect(seed)
        .await
        .map_err(|error| format!("prepopulate connect failed: {error}"))?;
    for sequence in 0..1024u64 {
        let key = next_key(sequence);
        client
            .execute(TSet::new(&key, payload))
            .await
            .map_err(|error| format!("prepopulate SET {key} failed: {error}"))?;
    }
    Ok(())
}

/// Populate a six-node fixture directly through each slot owner, prove every
/// owner's replica acknowledged the writes, then verify all reads through a
/// strict replica-routed redis-tower client.
pub async fn prepopulate_and_verify_replicas(
    fixture: &ClusterFixture,
    payload: &str,
) -> Result<(), String> {
    let topology = fixture
        .topology()
        .await
        .map_err(|error| error.to_string())?;
    let mut seeders = BTreeMap::new();
    for master in topology.masters() {
        let connection = RedisConnection::connect(&master.addr)
            .await
            .map_err(|error| format!("connect to master {} failed: {error}", master.addr))?;
        seeders.insert(master.index, connection);
    }

    for sequence in 0..1024u64 {
        let key = next_key(sequence);
        let slot = hash_slot(key.as_bytes());
        let owner = topology
            .owner_of_slot(slot)
            .ok_or_else(|| format!("slot {slot} has no owner"))?;
        seeders
            .get_mut(&owner.index)
            .ok_or_else(|| format!("slot owner {} has no seeder", owner.addr))?
            .execute(TSet::new(&key, payload))
            .await
            .map_err(|error| format!("prepopulate SET {key} failed: {error}"))?;
    }

    for (index, seeder) in &mut seeders {
        let acknowledgements = seeder
            .execute(TWait::new(1, 5_000))
            .await
            .map_err(|error| format!("WAIT on master {index} failed: {error}"))?;
        if acknowledgements < 1 {
            return Err(format!(
                "master {index} did not receive a replica acknowledgement"
            ));
        }
    }

    let replica_client = MultiplexedClusterClient::builder(fixture.seed_addr())
        .read_preference(ReadPreference::Replica)
        .connect()
        .await
        .map_err(|error| format!("replica verification connect failed: {error}"))?;
    for sequence in 0..1024u64 {
        let key = next_key(sequence);
        match replica_client.execute(TGet::new(&key)).await {
            Ok(Some(value)) if value.len() == payload.len() => {}
            Ok(Some(value)) => {
                return Err(format!(
                    "replica GET {key} returned {} bytes, expected {}",
                    value.len(),
                    payload.len()
                ));
            }
            Ok(None) => return Err(format!("replica GET {key} missed")),
            Err(error) => return Err(format!("replica GET {key} failed: {error}")),
        }
    }
    replica_client.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_aliases_parse() {
        assert_eq!(ClientKind::parse("fred"), Some(ClientKind::Fred));
        assert_eq!(
            ClientKind::parse("tower-replica"),
            Some(ClientKind::RedisTowerMuxReplica)
        );
        assert_eq!(ClientKind::parse("unknown"), None);
    }

    #[test]
    fn stable_client_ids_round_trip() {
        let kinds = [
            ClientKind::RedisTower,
            ClientKind::RedisTowerMux,
            ClientKind::RedisTowerMuxReplica,
            ClientKind::RedisRsSync,
            ClientKind::RedisRsAsync,
            ClientKind::Fred,
        ];
        for kind in kinds {
            assert_eq!(ClientKind::parse(kind.as_str()), Some(kind));
        }
    }
}
