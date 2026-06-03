//! Live-server integration tests for SyncClient.
//!
//! Each test spins up a dedicated Redis instance on a distinct port so the
//! tests can run in parallel without port collisions.  Ports chosen are in
//! the 6390..6393 range (non-default, away from the port 6399 used by the
//! redis-tower integration suite).

use redis_server_wrapper::RedisServer;
use redis_tower_sync::SyncClient;
use redis_tower_sync::commands::{Del, Get, Ping, Set};

const PORT_BASE: u16 = 6390;

// -- sync_connect_and_set_get --

#[test]
fn sync_connect_and_set_get() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt
        .block_on(RedisServer::new().port(PORT_BASE).start())
        .expect("start Redis");
    let addr = server.addr();

    let client = SyncClient::connect(&addr).expect("SyncClient::connect");

    client.execute(Set::new("sync:set_get", "hello")).unwrap();
    let val: Option<bytes::Bytes> = client.execute(Get::new("sync:set_get")).unwrap();
    assert_eq!(val, Some(bytes::Bytes::from("hello")));

    client.execute(Del::new("sync:set_get")).unwrap();

    let pong: String = client.execute(Ping::new()).unwrap();
    assert_eq!(pong, "PONG");
}

// -- sync_connect_url --

#[test]
fn sync_connect_url() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt
        .block_on(RedisServer::new().port(PORT_BASE + 1).start())
        .expect("start Redis");
    let addr = server.addr();
    let url = format!("redis://{addr}");

    let client = SyncClient::connect_url(&url).expect("SyncClient::connect_url");

    client.execute(Set::new("sync:url:key", "value")).unwrap();
    let val: Option<bytes::Bytes> = client.execute(Get::new("sync:url:key")).unwrap();
    assert_eq!(val, Some(bytes::Bytes::from("value")));

    client.execute(Del::new("sync:url:key")).unwrap();
}

// -- sync_concurrent --
//
// Verifies that a cloned SyncClient can be shared across OS threads and that
// the blocking thread-pool serialises commands correctly.
//
// SyncClient is not Clone (the inner runtime is not Clone), so each thread
// constructs its own SyncClient connected to the same server.

#[test]
fn sync_concurrent() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt
        .block_on(RedisServer::new().port(PORT_BASE + 2).start())
        .expect("start Redis");
    let addr = server.addr();

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let addr = addr.clone();
            std::thread::spawn(move || {
                let client = SyncClient::connect(&addr).expect("connect in thread");
                let key = format!("sync:concurrent:{i}");
                client.execute(Set::new(&key, i.to_string())).unwrap();
                let val: Option<bytes::Bytes> = client.execute(Get::new(&key)).unwrap();
                assert_eq!(
                    val,
                    Some(bytes::Bytes::from(i.to_string())),
                    "thread {i} value mismatch"
                );
                client.execute(Del::new(&key)).unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}
