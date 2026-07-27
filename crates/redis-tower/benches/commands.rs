use criterion::{Criterion, criterion_group};
use redis_server_wrapper::{RedisServer, RedisServerHandle};
use redis_tower::auto_pipeline::AutoPipelineConfig;
use redis_tower::commands::*;
use redis_tower::pool::{ConnectionPool, PoolConfig};
use redis_tower::{
    MultiplexedClient, Pipeline, RedisClient, RedisConnection, Transaction, TransactionResult,
};
use std::sync::OnceLock;

/// Port the benches start their own server on.
///
/// Deliberately not 6379: a bench run writes and deletes `bench:*` keys, so it
/// must not land on whatever a developer has running there. 6399 (standalone
/// integration tests) and 6480 (standalone-bench) are taken as well, so all
/// three can run side by side.
const BENCH_PORT: u16 = 6482;

static ADDR: OnceLock<String> = OnceLock::new();

/// Address every benchmark connects to. Set by [`start_server`], which `main`
/// calls before any benchmark runs.
fn addr() -> &'static str {
    ADDR.get()
        .expect("benchmark server address not set; start_server must run first")
        .as_str()
}

/// Start the Redis server the benches run against.
///
/// Returns the handle so `main` can keep it alive for the whole run and stop
/// the server on the way out, or `None` when `REDIS_URL` points the benches at
/// a server someone else owns.
///
/// The benches used to connect to a hardcoded `127.0.0.1:6379`, which made an
/// otherwise fine checkout fail: ten benchmark functions panicked outright and
/// four silently returned without measuring anything when nothing was
/// listening there.
fn start_server() -> Option<RedisServerHandle> {
    let (address, handle) = match std::env::var("REDIS_URL") {
        Ok(url) => (
            url.strip_prefix("redis://")
                .unwrap_or(&url)
                .trim_end_matches('/')
                .to_string(),
            None,
        ),
        Err(_) => {
            // A runtime just for startup. Each benchmark builds its own, and
            // `RedisServerHandle`'s shutdown path is synchronous, so nothing
            // depends on this one outliving the call.
            let rt = tokio::runtime::Runtime::new().expect("failed to build a startup runtime");
            let server = rt
                .block_on(RedisServer::new().port(BENCH_PORT).start())
                .expect(
                    "failed to start redis-server for the benches; \
                     install redis-server or set REDIS_URL to an existing instance",
                );
            (server.addr(), Some(server))
        }
    };

    ADDR.set(address).expect("start_server called twice");
    handle
}

fn bench_ping(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    c.bench_function("ping", |b| {
        b.to_async(&rt)
            .iter(|| async { client.execute(Ping::new()).await.unwrap() });
    });
}

fn bench_set(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    c.bench_function("set", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .execute(Set::new("bench:set:key", "value"))
                .await
                .unwrap()
        });
    });

    rt.block_on(async { client.execute(Del::new("bench:set:key")).await.unwrap() });
}

fn bench_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    rt.block_on(async {
        client
            .execute(Set::new("bench:get:key", "value"))
            .await
            .unwrap()
    });

    c.bench_function("get", |b| {
        b.to_async(&rt)
            .iter(|| async { client.execute(Get::new("bench:get:key")).await.unwrap() });
    });

    rt.block_on(async { client.execute(Del::new("bench:get:key")).await.unwrap() });
}

fn bench_incr(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    rt.block_on(async {
        client
            .execute(Set::new("bench:incr:key", "0"))
            .await
            .unwrap()
    });

    c.bench_function("incr", |b| {
        b.to_async(&rt)
            .iter(|| async { client.execute(Incr::new("bench:incr:key")).await.unwrap() });
    });

    rt.block_on(async { client.execute(Del::new("bench:incr:key")).await.unwrap() });
}

fn bench_hset_hget(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    c.bench_function("hset", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .execute(HSet::new("bench:hash", "field", "value"))
                .await
                .unwrap()
        });
    });

    c.bench_function("hget", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .execute(HGet::new("bench:hash", "field"))
                .await
                .unwrap()
        });
    });

    rt.block_on(async { client.execute(Del::new("bench:hash")).await.unwrap() });
}

fn bench_lpush_lpop(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    c.bench_function("lpush+lpop", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .execute(LPush::new("bench:list", "item"))
                .await
                .unwrap();
            client.execute(LPop::new("bench:list")).await.unwrap();
        });
    });

    rt.block_on(async { client.execute(Del::new("bench:list")).await.unwrap() });
}

fn bench_sadd_sismember(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    rt.block_on(async {
        client
            .execute(SAdd::members("bench:set", ["a", "b", "c", "d", "e"]))
            .await
            .unwrap()
    });

    c.bench_function("sismember", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .execute(SIsMember::new("bench:set", "c"))
                .await
                .unwrap()
        });
    });

    rt.block_on(async { client.execute(Del::new("bench:set")).await.unwrap() });
}

fn bench_pipeline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut conn = rt.block_on(async { RedisConnection::connect(addr()).await.unwrap() });

    let mut group = c.benchmark_group("pipeline");

    for size in [10, 50, 100] {
        group.bench_function(format!("{size}_commands"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut pipeline = Pipeline::new();
                    for i in 0..size {
                        pipeline = pipeline.push(Set::new(format!("bench:pipe:{i}"), "v"));
                    }
                    pipeline.execute(&mut conn).await.unwrap();
                });
            });
        });
    }

    group.finish();

    rt.block_on(async {
        for i in 0..100 {
            let _ = conn.execute(Del::new(format!("bench:pipe:{i}"))).await;
        }
    });
}

fn bench_transaction(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut conn = rt.block_on(async { RedisConnection::connect(addr()).await.unwrap() });

    c.bench_function("transaction_3_commands", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = Transaction::new()
                    .push(Set::new("bench:txn:a", "1"))
                    .push(Set::new("bench:txn:b", "2"))
                    .push(Incr::new("bench:txn:a"))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                assert!(matches!(result, TransactionResult::Committed(_)));
            });
        });
    });

    rt.block_on(async {
        conn.execute(Del::keys(["bench:txn:a", "bench:txn:b"]))
            .await
            .unwrap()
    });
}

fn bench_mixed_workload(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect(addr()).await.unwrap() });

    c.bench_function("mixed_10_ops", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .execute(Set::new("bench:mix:k1", "v1"))
                .await
                .unwrap();
            client
                .execute(Set::new("bench:mix:k2", "v2"))
                .await
                .unwrap();
            client.execute(Get::new("bench:mix:k1")).await.unwrap();
            client.execute(Get::new("bench:mix:k2")).await.unwrap();
            client
                .execute(Incr::new("bench:mix:counter"))
                .await
                .unwrap();
            client
                .execute(HSet::new("bench:mix:h", "f", "v"))
                .await
                .unwrap();
            client.execute(HGet::new("bench:mix:h", "f")).await.unwrap();
            client
                .execute(LPush::new("bench:mix:l", "item"))
                .await
                .unwrap();
            client.execute(LPop::new("bench:mix:l")).await.unwrap();
            client.execute(Exists::new("bench:mix:k1")).await.unwrap();
        });
    });

    rt.block_on(async {
        client
            .execute(Del::keys([
                "bench:mix:k1",
                "bench:mix:k2",
                "bench:mix:counter",
                "bench:mix:h",
                "bench:mix:l",
            ]))
            .await
            .unwrap()
    });
}

/// MultiplexedClient throughput under N concurrent tasks.
///
/// Demonstrates auto-pipeline batching benefit: at higher concurrency levels
/// concurrent GET/SET ops are batched into a single pipeline round-trip.
fn bench_multiplexed_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let client = match rt.block_on(async { MultiplexedClient::connect(addr()).await }) {
        Ok(c) => c,
        Err(_) => return, // skip if no server available
    };

    // Pre-populate keys used by the GET benchmarks.
    rt.block_on(async {
        for i in 0..128u32 {
            client
                .execute(Set::new(format!("bench:mux:{i}"), "value"))
                .await
                .ok();
        }
    });

    let mut group = c.benchmark_group("multiplexed_concurrent");
    group.measurement_time(std::time::Duration::from_secs(10));

    for concurrency in [1usize, 8, 32, 128] {
        group.bench_function(format!("get_c{concurrency}"), |b| {
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                async move {
                    let handles: Vec<_> = (0..concurrency)
                        .map(|i| {
                            let c = client.clone();
                            tokio::spawn(async move {
                                c.execute(Get::new(format!("bench:mux:{}", i % 128)))
                                    .await
                                    .ok()
                            })
                        })
                        .collect();
                    for h in handles {
                        h.await.ok();
                    }
                }
            });
        });

        group.bench_function(format!("set_c{concurrency}"), |b| {
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                async move {
                    let handles: Vec<_> = (0..concurrency)
                        .map(|i| {
                            let c = client.clone();
                            tokio::spawn(async move {
                                c.execute(Set::new(format!("bench:mux:{}", i % 128), "value"))
                                    .await
                                    .ok()
                            })
                        })
                        .collect();
                    for h in handles {
                        h.await.ok();
                    }
                }
            });
        });
    }

    group.finish();

    rt.block_on(async {
        for i in 0..128u32 {
            client
                .execute(Del::new(format!("bench:mux:{i}")))
                .await
                .ok();
        }
        client.shutdown().await;
    });
}

/// ConnectionPool throughput at varying pool sizes and fixed concurrency.
///
/// Shows the pool-size sweet spot: too small causes lock contention,
/// too large wastes connections.
fn bench_pool_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Verify server is available before building the group.
    if rt
        .block_on(async { RedisConnection::connect(addr()).await })
        .is_err()
    {
        return;
    }

    let mut group = c.benchmark_group("pool_throughput");
    group.measurement_time(std::time::Duration::from_secs(10));

    for pool_size in [1usize, 4, 8] {
        let pool: ConnectionPool<RedisConnection> = rt.block_on(async {
            ConnectionPool::connect_with_config(PoolConfig::default().size(pool_size), || async {
                RedisConnection::connect(addr()).await
            })
            .await
            .unwrap()
        });

        group.bench_function(format!("get_pool{pool_size}_c32"), |b| {
            b.to_async(&rt).iter(|| {
                let pool = pool.clone();
                async move {
                    let handles: Vec<_> = (0..32)
                        .map(|i| {
                            let p = pool.clone();
                            tokio::spawn(async move {
                                p.execute(Get::new(format!("bench:pool:{}", i % 128)))
                                    .await
                                    .ok()
                            })
                        })
                        .collect();
                    for h in handles {
                        h.await.ok();
                    }
                }
            });
        });
    }

    group.finish();
}

/// GET/SET throughput with large value payloads.
///
/// Tests RESP codec encode/decode allocations at scale. Important for
/// workloads using Redis as a cache for large objects.
fn bench_large_values(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = match rt.block_on(async { RedisClient::connect(addr()).await }) {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut group = c.benchmark_group("large_values");
    group.measurement_time(std::time::Duration::from_secs(10));

    for size in [1_024usize, 10_240, 102_400] {
        let payload = "x".repeat(size);
        let key = format!("bench:large:{size}");

        // Pre-set so GET benchmarks always hit.
        rt.block_on(async {
            client
                .execute(Set::new(key.clone(), payload.clone()))
                .await
                .ok();
        });

        group.bench_function(format!("set_{size}b"), |b| {
            let payload = payload.clone();
            let key = key.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let payload = payload.clone();
                let key = key.clone();
                async move { client.execute(Set::new(key, payload)).await.ok() }
            });
        });

        group.bench_function(format!("get_{size}b"), |b| {
            let key = key.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let key = key.clone();
                async move { client.execute(Get::new(key)).await.ok() }
            });
        });
    }

    group.finish();

    rt.block_on(async {
        for size in [1_024usize, 10_240, 102_400] {
            client
                .execute(Del::new(format!("bench:large:{size}")))
                .await
                .ok();
        }
    });
}

/// AutoPipelineConfig::batch_window tradeoff at high concurrency.
///
/// Quantifies when a non-zero window helps (write-heavy workloads) vs
/// hurts (latency-sensitive workloads). Default is 0ms (flush immediately).
fn bench_batch_window(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    if rt
        .block_on(async { RedisConnection::connect(addr()).await })
        .is_err()
    {
        return;
    }

    let mut group = c.benchmark_group("batch_window");
    group.measurement_time(std::time::Duration::from_secs(10));

    for window_ms in [0u64, 1, 5] {
        let client = rt.block_on(async {
            let conn = RedisConnection::connect(addr()).await.unwrap();
            let config = AutoPipelineConfig {
                batch_window: std::time::Duration::from_millis(window_ms),
                ..Default::default()
            };
            MultiplexedClient::from_connection_with_config(conn, config)
        });

        group.bench_function(format!("c32_window{window_ms}ms"), |b| {
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                async move {
                    let handles: Vec<_> = (0..32usize)
                        .map(|i| {
                            let c = client.clone();
                            tokio::spawn(async move {
                                c.execute(Set::new(format!("bench:window:{}", i % 32), "v"))
                                    .await
                                    .ok()
                            })
                        })
                        .collect();
                    for h in handles {
                        h.await.ok();
                    }
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_ping,
    bench_set,
    bench_get,
    bench_incr,
    bench_hset_hget,
    bench_lpush_lpop,
    bench_sadd_sismember,
    bench_pipeline,
    bench_transaction,
    bench_mixed_workload,
    bench_multiplexed_concurrent,
    bench_pool_throughput,
    bench_large_values,
    bench_batch_window,
);
/// Written out rather than using `criterion_main!` so the server handle lives
/// for the whole run and stops the server on the way out. The rest of the body
/// is what that macro expands to.
fn main() {
    let _server = start_server();
    benches();
    Criterion::default().configure_from_args().final_summary();
}
