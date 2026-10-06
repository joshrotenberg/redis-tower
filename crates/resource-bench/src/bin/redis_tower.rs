use resource_bench::{REDIS_TOWER_FEATURES, adapters::TowerConnection, run_client};

fn main() {
    if let Err(error) = run_client::<TowerConnection>("redis-tower", REDIS_TOWER_FEATURES) {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
