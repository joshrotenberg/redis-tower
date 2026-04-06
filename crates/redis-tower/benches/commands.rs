use criterion::{Criterion, criterion_group, criterion_main};
use redis_tower::commands::*;
use redis_tower::{Pipeline, RedisClient, RedisConnection, Transaction, TransactionResult};

fn bench_ping(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

    c.bench_function("ping", |b| {
        b.to_async(&rt)
            .iter(|| async { client.execute(Ping::new()).await.unwrap() });
    });
}

fn bench_set(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
    let mut conn = rt.block_on(async { RedisConnection::connect("127.0.0.1:6379").await.unwrap() });

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
    let mut conn = rt.block_on(async { RedisConnection::connect("127.0.0.1:6379").await.unwrap() });

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
    let client = rt.block_on(async { RedisClient::connect("127.0.0.1:6379").await.unwrap() });

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
);
criterion_main!(benches);
