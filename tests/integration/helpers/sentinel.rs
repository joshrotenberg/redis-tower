// This module requires the docker-wrapper crate which is not included by default.
// To run sentinel integration tests:
// 1. Clone docker-wrapper and update the path in Cargo.toml dev-dependencies
// 2. Run: cargo test --test integration_sentinel --features sentinel -- --test-threads=1
//
// Without docker-wrapper, sentinel tests are compile-gated and skipped.

#[allow(dead_code)]
pub async fn setup_sentinel() -> redis_tower::sentinel::SentinelClient {
    panic!(
        "Sentinel tests require the docker-wrapper crate. \
         Uncomment the docker-wrapper dev-dependency in Cargo.toml and point it \
         to your local checkout."
    );
}
