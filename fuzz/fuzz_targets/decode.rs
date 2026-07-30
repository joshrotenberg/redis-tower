#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use redis_tower_protocol::{RespCodec, RespLimits};
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let (max_depth, max_frame_size, wire) = match data {
        [depth, size_hi, size_lo, wire @ ..] => (
            usize::from(*depth % 65),
            usize::from(u16::from_be_bytes([*size_hi, *size_lo])),
            wire,
        ),
        _ => (32, 4096, data),
    };

    let mut codec = RespCodec::with_limits(RespLimits {
        max_frame_size,
        max_depth,
    });
    let mut buffer = BytesMut::from(wire);

    loop {
        let before = buffer.len();
        match codec.decode(&mut buffer) {
            Ok(Some(_)) => {
                assert!(
                    buffer.len() < before,
                    "a successfully decoded frame must consume input"
                );
            }
            Ok(None) | Err(_) => break,
        }
    }
});
