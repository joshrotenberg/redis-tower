#![no_main]

use libfuzzer_sys::fuzz_target;
use redis_tower_fuzz::{decode_whole, split_case};

fuzz_target!(|data: &[u8]| {
    let _ = decode_whole(split_case(data));
});
