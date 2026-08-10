//! Unified client adapters for the standalone Redis comparison.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use fred::prelude::{Builder as FredBuilder, ClientLike, Config as FredConfig, KeysInterface};
use hdrhistogram::Histogram;
use redis_tower::commands::{Get as TGet, Set as TSet};
use redis_tower::{MultiplexedClient, Pipeline, RedisClient, RedisConnection};

use crate::runner::{WorkerHandle, WorkerResult, Workload, new_histogram};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientKind {
    RedisTower,
    RedisTowerMux,
    RedisRsSync,
    RedisRsAsync,
    RedisRsManager,
    Fred,
}

impl ClientKind {
    pub const DEFAULTS: [Self; 6] = [
        Self::RedisTower,
        Self::RedisTowerMux,
        Self::RedisRsSync,
        Self::RedisRsAsync,
        Self::RedisRsManager,
        Self::Fred,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RedisTower => "redis-tower",
            Self::RedisTowerMux => "redis-tower-mux",
            Self::RedisRsSync => "redis-rs-sync",
            Self::RedisRsAsync => "redis-rs-async",
            Self::RedisRsManager => "redis-rs-manager",
            Self::Fred => "fred",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "redis-tower" | "tower" => Some(Self::RedisTower),
            "redis-tower-mux" | "tower-mux" | "mux" => Some(Self::RedisTowerMux),
            "redis-rs-sync" | "redis-sync" => Some(Self::RedisRsSync),
            "redis-rs-async" | "redis-async" => Some(Self::RedisRsAsync),
            "redis-rs-manager" | "redis-manager" | "manager" => Some(Self::RedisRsManager),
            "fred" => Some(Self::Fred),
            _ => None,
        }
    }
}

pub enum Client {
    Tower(RedisClient, String),
    TowerMux(MultiplexedClient),
    RedisRsSync(redis::Client),
    RedisRsAsync(redis::aio::MultiplexedConnection),
    RedisRsManager(redis::aio::ConnectionManager),
    Fred(fred::clients::Client),
}

impl Client {
    pub fn kind(&self) -> ClientKind {
        match self {
            Self::Tower(..) => ClientKind::RedisTower,
            Self::TowerMux(_) => ClientKind::RedisTowerMux,
            Self::RedisRsSync(_) => ClientKind::RedisRsSync,
            Self::RedisRsAsync(_) => ClientKind::RedisRsAsync,
            Self::RedisRsManager(_) => ClientKind::RedisRsManager,
            Self::Fred(_) => ClientKind::Fred,
        }
    }

    pub async fn connect(kind: ClientKind, addr: &str) -> Result<Self, String> {
        let url = format!("redis://{addr}/");
        match kind {
            ClientKind::RedisTower => RedisClient::connect(addr)
                .await
                .map(|client| Self::Tower(client, addr.to_owned()))
                .map_err(|error| error.to_string()),
            ClientKind::RedisTowerMux => MultiplexedClient::connect(addr)
                .await
                .map(Self::TowerMux)
                .map_err(|error| error.to_string()),
            ClientKind::RedisRsSync => redis::Client::open(url)
                .map(Self::RedisRsSync)
                .map_err(|error| error.to_string()),
            ClientKind::RedisRsAsync => {
                let client = redis::Client::open(url).map_err(|error| error.to_string())?;
                let connection = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Self::RedisRsAsync(connection))
            }
            ClientKind::RedisRsManager => {
                let client = redis::Client::open(url).map_err(|error| error.to_string())?;
                let manager = client
                    .get_connection_manager()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Self::RedisRsManager(manager))
            }
            ClientKind::Fred => {
                let config =
                    FredConfig::from_url_centralized(&url).map_err(|error| error.to_string())?;
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
        pipeline_commands: usize,
        stop: Arc<AtomicBool>,
        batches: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
        warmup_deadline: Instant,
    ) -> Vec<WorkerHandle> {
        let mut handles = Vec::with_capacity(concurrency);
        let payload: Arc<str> = Arc::from("x".repeat(payload_bytes));
        match self {
            Self::Tower(client, addr) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let addr = addr.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        pipeline_commands,
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        tower_loop(client, addr, context).await
                    })));
                }
            }
            Self::TowerMux(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        pipeline_commands,
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
                        pipeline_commands,
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
            Self::RedisRsManager(client) => {
                for worker_id in 0..concurrency {
                    let client = client.clone();
                    let context = WorkerContext::new(
                        worker_id,
                        workload,
                        payload.clone(),
                        pipeline_commands,
                        stop.clone(),
                        batches.clone(),
                        errors.clone(),
                        warmup_deadline,
                    );
                    handles.push(WorkerHandle::Async(tokio::spawn(async move {
                        redis_rs_manager_loop(client, context).await
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
                        pipeline_commands,
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
                        pipeline_commands,
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
    pipeline_commands: usize,
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
        pipeline_commands: usize,
        stop: Arc<AtomicBool>,
        batches: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
        warmup_deadline: Instant,
    ) -> Self {
        Self {
            worker_id,
            workload,
            payload,
            pipeline_commands,
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

fn pipeline_key(worker_id: usize, index: usize) -> String {
    format!("bench:pipe:{worker_id}:{index}")
}

async fn tower_loop(client: RedisClient, addr: String, context: WorkerContext) -> WorkerResult {
    let mut histogram = new_histogram();
    let mut sequence = context.worker_id as u64;
    let mut pipeline_connection = if matches!(context.workload, Workload::Pipeline) {
        Some(RedisConnection::connect(&addr).await.map_err(|error| {
            format!(
                "redis-tower pipeline worker {} failed to connect: {error}",
                context.worker_id
            )
        })?)
    } else {
        None
    };
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
            Workload::Pipeline => match pipeline_connection.as_mut() {
                Some(connection) => {
                    let mut pipeline = Pipeline::new();
                    for index in 0..context.pipeline_commands {
                        pipeline = pipeline.push(TSet::new(
                            pipeline_key(context.worker_id, index),
                            context.payload.as_ref(),
                        ));
                    }
                    pipeline.execute(connection).await.is_ok()
                }
                None => false,
            },
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn tower_mux_loop(client: MultiplexedClient, context: WorkerContext) -> WorkerResult {
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
            Workload::Pipeline => {
                let operations = (0..context.pipeline_commands).map(|index| {
                    let client = client.clone();
                    let payload = context.payload.clone();
                    let key = pipeline_key(context.worker_id, index);
                    async move {
                        client
                            .execute(TSet::new(key, payload.as_ref()))
                            .await
                            .is_ok()
                    }
                });
                futures::future::join_all(operations)
                    .await
                    .into_iter()
                    .all(|succeeded| succeeded)
            }
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn redis_rs_async_loop(
    mut client: redis::aio::MultiplexedConnection,
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
            Workload::Pipeline => {
                let mut pipeline = redis::Pipeline::new();
                for index in 0..context.pipeline_commands {
                    pipeline.set(
                        pipeline_key(context.worker_id, index),
                        context.payload.as_ref(),
                    );
                }
                pipeline.query_async::<()>(&mut client).await.is_ok()
            }
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn redis_rs_manager_loop(
    mut client: redis::aio::ConnectionManager,
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
            Workload::Pipeline => {
                let mut pipeline = redis::Pipeline::new();
                for index in 0..context.pipeline_commands {
                    pipeline.set(
                        pipeline_key(context.worker_id, index),
                        context.payload.as_ref(),
                    );
                }
                pipeline.query_async::<()>(&mut client).await.is_ok()
            }
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

fn redis_rs_sync_loop(client: redis::Client, context: WorkerContext) -> WorkerResult {
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
            Workload::Pipeline => {
                let mut pipeline = redis::Pipeline::new();
                for index in 0..context.pipeline_commands {
                    pipeline.set(
                        pipeline_key(context.worker_id, index),
                        context.payload.as_ref(),
                    );
                }
                pipeline.exec(&mut connection).is_ok()
            }
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
            Workload::Pipeline => fred_pipeline(&client, &context).await,
        };
        record_outcome(&mut histogram, &context, started, succeeded);
    }
    Ok(histogram)
}

async fn fred_pipeline(client: &fred::clients::Client, context: &WorkerContext) -> bool {
    let pipeline = client.pipeline();
    for index in 0..context.pipeline_commands {
        let queued = pipeline
            .set::<fred::types::Value, _, _>(
                pipeline_key(context.worker_id, index),
                context.payload.as_ref(),
                None,
                None,
                false,
            )
            .await;
        if queued.is_err() {
            return false;
        }
    }
    pipeline.all::<Vec<fred::types::Value>>().await.is_ok()
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
pub async fn prepopulate(addr: &str, payload: &str) -> Result<(), String> {
    let client = RedisClient::connect(addr)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_aliases_parse() {
        assert_eq!(ClientKind::parse("fred"), Some(ClientKind::Fred));
        assert_eq!(
            ClientKind::parse("manager"),
            Some(ClientKind::RedisRsManager)
        );
        assert_eq!(ClientKind::parse("unknown"), None);
    }

    #[test]
    fn stable_client_ids_round_trip() {
        for kind in ClientKind::DEFAULTS {
            assert_eq!(ClientKind::parse(kind.as_str()), Some(kind));
        }
    }
}
