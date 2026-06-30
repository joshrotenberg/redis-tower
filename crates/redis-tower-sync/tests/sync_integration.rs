//! Live-server integration tests for SyncClient.
//!
//! Each test spins up a dedicated Redis instance on a distinct port so the
//! tests can run in parallel without port collisions.  Ports chosen are in
//! the 6390..6396 range (non-default, away from the port 6399 used by the
//! redis-tower integration suite).

use redis_server_wrapper::RedisServer;
use redis_tower_sync::commands::{Del, Get, Incr, Ping, Set};
use redis_tower_sync::{Pipeline, SyncClient, Transaction};

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

// -- sync_health_check --

#[test]
fn sync_health_check() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt
        .block_on(RedisServer::new().port(PORT_BASE + 3).start())
        .expect("start Redis");
    let addr = server.addr();

    let client = SyncClient::connect(&addr).expect("SyncClient::connect");
    client.health_check().expect("health_check should succeed");
}

// -- sync_pipeline --

#[test]
fn sync_pipeline() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt
        .block_on(RedisServer::new().port(PORT_BASE + 4).start())
        .expect("start Redis");
    let addr = server.addr();

    let client = SyncClient::connect(&addr).expect("SyncClient::connect");

    let results = client
        .pipeline(
            Pipeline::new()
                .push(Set::new("sync:pipe:a", "1"))
                .push(Set::new("sync:pipe:b", "2"))
                .push(Get::new("sync:pipe:a")),
        )
        .expect("pipeline should execute");

    let val: &Option<bytes::Bytes> = results.get(2).expect("third result");
    assert_eq!(val, &Some(bytes::Bytes::from("1")));

    client.execute(Del::new("sync:pipe:a")).unwrap();
    client.execute(Del::new("sync:pipe:b")).unwrap();
}

// -- sync_transaction --

#[test]
fn sync_transaction() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt
        .block_on(RedisServer::new().port(PORT_BASE + 5).start())
        .expect("start Redis");
    let addr = server.addr();

    let client = SyncClient::connect(&addr).expect("SyncClient::connect");

    let result = client
        .transaction(
            Transaction::new()
                .push(Set::new("sync:txn:counter", "10"))
                .push(Incr::new("sync:txn:counter")),
        )
        .expect("transaction should execute");

    assert!(result.is_committed());
    let results = result.unwrap();
    let incremented: &i64 = results.get(1).expect("second result");
    assert_eq!(*incremented, 11);

    client.execute(Del::new("sync:txn:counter")).unwrap();
}
