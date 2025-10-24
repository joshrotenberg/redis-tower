//! Benchmark redis-tower vs fred and redis-rs

use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn benchmark_get(c: &mut Criterion) {
    c.bench_function("redis_tower_get", |b| {
        b.iter(|| {
            // TODO: Implement benchmark
            black_box(());
        });
    });
}

criterion_group!(benches, benchmark_get);
criterion_main!(benches);
