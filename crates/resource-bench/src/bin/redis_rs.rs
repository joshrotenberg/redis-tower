use resource_bench::{REDIS_RS_FEATURES, adapters::RedisRsConnection, run_client};

fn main() {
    if let Err(error) = run_client::<RedisRsConnection>("redis-rs", REDIS_RS_FEATURES) {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
