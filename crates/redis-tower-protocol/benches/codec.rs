//! Criterion micro-benchmarks for the RESP3 codec.
//!
//! Establishes a baseline for `RespCodec::encode` and `RespCodec::decode`
//! performance across common frame types. Useful for detecting regressions
//! in the codec allocation path.

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use redis_tower_protocol::{Frame, RespCodec};
use std::hint::black_box;
use tokio_util::codec::{Decoder, Encoder};

fn bulk_wire(size: usize, seed: u8) -> Vec<u8> {
    let mut wire = format!("${size}\r\n").into_bytes();
    wire.extend((0..size).map(|offset| seed.wrapping_add(offset as u8)));
    wire.extend_from_slice(b"\r\n");
    wire
}

fn mixed_pipeline(rounds: usize, include_large_payload: bool) -> Vec<u8> {
    let mut wire = Vec::new();
    for round in 0..rounds {
        wire.extend_from_slice(b"+OK\r\n");
        wire.extend_from_slice(&bulk_wire(0, round as u8));
        wire.extend_from_slice(&bulk_wire(16, round as u8));
        wire.extend_from_slice(&bulk_wire(256, round as u8));
        if include_large_payload {
            wire.extend_from_slice(&bulk_wire(4096, round as u8));
        }
        wire.extend_from_slice(b":42\r\n");
        wire.extend_from_slice(b">2\r\n+invalidate\r\n$3\r\nkey\r\n");
        wire.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }
    wire
}

fn decode_pipeline(bytes: &[u8], retain: bool) -> usize {
    let mut buf = BytesMut::from(bytes);
    let mut codec = RespCodec::new();
    let mut frames = retain.then(Vec::new);
    let mut count = 0;
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        count += 1;
        if let Some(frames) = frames.as_mut() {
            frames.push(frame);
        } else {
            black_box(frame);
        }
    }
    black_box(frames);
    count
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_encode");

    let cases: &[(&str, Frame)] = &[
        (
            "simple_string",
            Frame::SimpleString(Bytes::from_static(b"OK")),
        ),
        (
            "bulk_string_small",
            Frame::BulkString(Some(Bytes::from_static(b"hello"))),
        ),
        (
            "bulk_string_1kb",
            Frame::BulkString(Some(Bytes::from(vec![b'x'; 1024]))),
        ),
        ("integer", Frame::Integer(42)),
        (
            "array_3",
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from_static(b"SET"))),
                Frame::BulkString(Some(Bytes::from_static(b"key"))),
                Frame::BulkString(Some(Bytes::from_static(b"value"))),
            ])),
        ),
    ];

    for (name, frame) in cases {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let mut buf = BytesMut::new();
                let mut codec = RespCodec::new();
                codec.encode(frame.clone(), &mut buf).unwrap();
                buf
            });
        });
    }

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_decode");

    let cases: &[(&str, &[u8])] = &[
        ("simple_string", b"+OK\r\n"),
        ("bulk_string_small", b"$5\r\nhello\r\n"),
        ("integer", b":42\r\n"),
        (
            "array_3",
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        ),
        ("null_bulk_string", b"$-1\r\n"),
    ];

    for (name, bytes) in cases {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let mut buf = BytesMut::from(*bytes);
                let mut codec = RespCodec::new();
                codec.decode(&mut buf).unwrap()
            });
        });
    }

    // Large bulk string decode (1KB payload).
    let large_bytes = {
        let mut v = b"$1024\r\n".to_vec();
        v.extend(vec![b'x'; 1024]);
        v.extend_from_slice(b"\r\n");
        v
    };
    group.bench_function("bulk_string_1kb", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(large_bytes.as_slice());
            let mut codec = RespCodec::new();
            codec.decode(&mut buf).unwrap()
        });
    });

    group.finish();
}

/// Decode N responses from a single BytesMut (pipelined scenario).
///
/// Measures the total overhead of decoding 100 pipelined "+OK\r\n" responses,
/// including buffer advance on each decode call.
fn bench_decode_pipeline(c: &mut Criterion) {
    let single = b"+OK\r\n";
    let n = 100;
    let pipeline_bytes: Vec<u8> = single
        .iter()
        .cycle()
        .take(single.len() * n)
        .cloned()
        .collect();

    c.bench_function("decode_pipeline_100", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(pipeline_bytes.as_slice());
            let mut codec = RespCodec::new();
            let mut count = 0usize;
            while codec.decode(&mut buf).unwrap().is_some() {
                count += 1;
            }
            count
        });
    });

    let mixed = mixed_pipeline(32, true);
    let mut group = c.benchmark_group("codec_decode_pipeline");
    for (name, bytes) in [
        ("simple_100", pipeline_bytes.as_slice()),
        ("mixed_payloads", mixed.as_slice()),
    ] {
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        for retain in [false, true] {
            let lifetime = if retain { "retain" } else { "drop" };
            group.bench_with_input(BenchmarkId::new(lifetime, name), &bytes, |b, bytes| {
                b.iter(|| decode_pipeline(bytes, retain))
            });
        }
    }
    group.finish();
}

/// Decode mixed replies as bytes arrive in deliberately awkward fragments.
///
/// Incomplete attempts must not materialize a frame. This measures scanner
/// work, receive-buffer growth, and the one final first-frame copy together.
fn bench_decode_fragmented(c: &mut Criterion) {
    let wire = mixed_pipeline(16, false);
    let mut group = c.benchmark_group("codec_decode_fragmented");
    group.throughput(Throughput::Bytes(wire.len() as u64));

    for chunk_size in [1usize, 7, 64, 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &chunk_size,
            |b, &chunk_size| {
                b.iter(|| {
                    let mut input = BytesMut::new();
                    let mut codec = RespCodec::new();
                    let mut frames = Vec::new();
                    for chunk in wire.chunks(chunk_size) {
                        input.extend_from_slice(chunk);
                        while let Some(frame) = codec.decode(&mut input).unwrap() {
                            frames.push(frame);
                        }
                    }
                    assert!(input.is_empty());
                    black_box(frames)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_decode_pipeline,
    bench_decode_fragmented
);
criterion_main!(benches);
