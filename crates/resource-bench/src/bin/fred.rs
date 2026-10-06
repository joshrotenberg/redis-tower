use resource_bench::{FRED_FEATURES, adapters::FredConnection, run_client};

fn main() {
    if let Err(error) = run_client::<FredConnection>("fred", FRED_FEATURES) {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
