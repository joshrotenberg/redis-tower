use resource_bench::{REDIS_TOWER_FEATURES, adapters::TowerMuxConnection, run_client};

fn main() {
    if let Err(error) = run_client::<TowerMuxConnection>("redis-tower-mux", REDIS_TOWER_FEATURES) {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
