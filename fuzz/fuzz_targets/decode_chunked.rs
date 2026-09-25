#![no_main]

use libfuzzer_sys::fuzz_target;
use redis_tower_fuzz::{decode_fragmented, decode_whole, split_case};

fuzz_target!(|data: &[u8]| {
    let case = split_case(data);
    assert_eq!(decode_fragmented(case), decode_whole(case));
});
