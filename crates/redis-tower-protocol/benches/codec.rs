//! Criterion micro-benchmarks for the RESP3 codec.
//!
//! Establishes a baseline for `RespCodec::encode` and `RespCodec::decode`
//! performance across common frame types. Useful for detecting regressions
//! in the codec allocation path.

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use redis_tower_protocol::{Frame, RespCodec};
use tokio_util::codec::{Decoder, Encoder};

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
                let mut codec = RespCodec;
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
                let mut codec = RespCodec;
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
            let mut codec = RespCodec;
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
            let mut codec = RespCodec;
            let mut count = 0usize;
            while codec.decode(&mut buf).unwrap().is_some() {
                count += 1;
            }
            count
        });
    });
}

criterion_group!(benches, bench_encode, bench_decode, bench_decode_pipeline);
criterion_main!(benches);
