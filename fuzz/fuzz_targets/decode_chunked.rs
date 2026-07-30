#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use redis_tower_protocol::{RespCodec, RespLimits};
use tokio_util::codec::Decoder;

fn drain_complete_frames(codec: &mut RespCodec, buffer: &mut BytesMut) {
    loop {
        let before = buffer.len();
        match codec.decode(buffer) {
            Ok(Some(_)) => {
                assert!(
                    buffer.len() < before,
                    "a successfully decoded frame must consume input"
                );
            }
            Ok(None) | Err(_) => break,
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&chunk_seed, wire)) = data.split_first() else {
        return;
    };

    let chunk_size = usize::from(chunk_seed % 32) + 1;
    let mut codec = RespCodec::with_limits(RespLimits {
        max_frame_size: 64 * 1024,
        max_depth: 64,
    });
    let mut buffer = BytesMut::new();

    for chunk in wire.chunks(chunk_size) {
        buffer.extend_from_slice(chunk);
        drain_complete_frames(&mut codec, &mut buffer);
    }

    drain_complete_frames(&mut codec, &mut buffer);
});
