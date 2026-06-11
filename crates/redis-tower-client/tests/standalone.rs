//! Live-server integration coverage for the standalone `UniversalClient` path.
//!
//! Starts a throwaway `redis-server` (or honors `REDIS_URL`) and verifies that
//! `connect_url` selects the standalone variant and that `execute` round-trips
//! a command through the wrapper.

use redis_server_wrapper::RedisServer;
use redis_tower_client::UniversalClient;
use redis_tower_commands::{Get, Set};

#[tokio::test]
async fn standalone_connect_url_executes_commands() {
    // Honor REDIS_URL when set; otherwise start a throwaway server. The handle
    // is held for the whole test so the server stays up.
    let (_handle, url) = match std::env::var("REDIS_URL") {
        Ok(url) => (None, url),
        Err(_) => {
            let handle = RedisServer::new()
                .port(6402)
                .start()
                .await
                .expect("failed to start redis-server");
            let url = format!("redis://{}", handle.addr());
            (Some(handle), url)
        }
    };

    let client = UniversalClient::connect_url(&url)
        .await
        .expect("connect_url should select the standalone variant");
    assert_eq!(client.topology(), "standalone");

    client
        .execute(Set::new("rtc:key", "value"))
        .await
        .expect("SET should succeed");
    let val: Option<bytes::Bytes> = client
        .execute(Get::new("rtc:key"))
        .await
        .expect("GET should succeed");
    assert_eq!(val.as_deref(), Some(&b"value"[..]));
}
