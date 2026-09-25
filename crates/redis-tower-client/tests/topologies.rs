//! Live public-entry-point coverage for Cluster and Sentinel variants.
//!
//! These cases own dedicated process topologies and are ignored by default.
//! The Linux integration gate invokes them explicitly with one test thread.

#![cfg(unix)]

use std::time::{Duration, Instant};

use bytes::Bytes;
use redis_server_wrapper::{RedisCluster, RedisSentinel};
use redis_tower_client::UniversalClient;
use redis_tower_commands::{Get, Set};

#[tokio::test]
#[ignore = "live: starts an authenticated Redis Cluster"]
async fn cluster_url_selects_topology_authenticates_and_executes() {
    let password = "p@ss:w/rd%";
    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17600)
        .password(password)
        .start()
        .await
        .expect("start authenticated cluster for UniversalClient");
    let url = format!("redis+cluster://:p%40ss%3Aw%2Frd%25@{}", cluster.addr());
    let client = UniversalClient::connect_url(&url)
        .await
        .expect("UniversalClient should select and authenticate Cluster");
    assert_eq!(client.topology(), "cluster");

    let key = "universal:{cluster}:roundtrip";
    client.execute(Set::new(key, "cluster")).await.unwrap();
    let value: Option<Bytes> = client.execute(Get::new(key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"cluster")));
}

fn sentinel_master_addr(info: &std::collections::HashMap<String, String>) -> String {
    format!(
        "{}:{}",
        info.get("ip").expect("sentinel response omitted master ip"),
        info.get("port")
            .expect("sentinel response omitted master port")
    )
}

#[tokio::test]
#[ignore = "live: starts and fails over a Redis Sentinel topology"]
async fn sentinel_url_selects_topology_executes_and_recovers_after_failover() {
    let sentinel = RedisSentinel::builder()
        .master_port(6410)
        .replica_base_port(6411)
        .sentinel_base_port(26410)
        .replicas(1)
        .sentinels(3)
        .quorum(2)
        .down_after_ms(500)
        .failover_timeout_ms(10_000)
        .start()
        .await
        .expect("start Sentinel topology for UniversalClient");
    let addrs = sentinel.sentinel_addrs();
    let url = format!("redis+sentinel://{}/mymaster", addrs.join(","));
    let client = UniversalClient::connect_url(&url)
        .await
        .expect("UniversalClient should select Sentinel with reconnect enabled");
    assert_eq!(client.topology(), "sentinel");

    client
        .execute(Set::new("universal:sentinel:before", "before"))
        .await
        .unwrap();
    let value: Option<Bytes> = client
        .execute(Get::new("universal:sentinel:before"))
        .await
        .unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"before")));

    let initial_master = sentinel_master_addr(&sentinel.poke().await.unwrap());
    let master_pid = sentinel.pids()[0];
    let status = std::process::Command::new("kill")
        .args(["-9", &master_pid.to_string()])
        .status()
        .expect("kill original Sentinel master");
    assert!(
        status.success(),
        "kill did not terminate the original master"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(info) = sentinel.poke().await {
            let flags = info.get("flags").map(String::as_str).unwrap_or_default();
            let current_master = sentinel_master_addr(&info);
            if flags == "master" && current_master != initial_master {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "Sentinel did not elect a replacement master within 30 seconds"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let reconnect_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = client
            .execute(Set::new("universal:sentinel:after", "after"))
            .await;
        if result.is_ok() {
            break;
        }
        assert!(
            Instant::now() < reconnect_deadline,
            "UniversalClient did not reconnect through Sentinel: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let value: Option<Bytes> = client
        .execute(Get::new("universal:sentinel:after"))
        .await
        .unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"after")));
}
