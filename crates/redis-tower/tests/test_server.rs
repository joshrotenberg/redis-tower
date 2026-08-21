mod common;

use common::conn;
use redis_server_wrapper::RedisServer;
use redis_tower::commands::*;
use redis_tower::{Frame, RedisConnection};
use std::time::{Duration, Instant};

#[tokio::test]
async fn cover_info() {
    let mut c = conn().await;
    let info = c.execute(Info::new()).await.unwrap();
    assert!(info.contains("redis_version"));
}

#[tokio::test]
async fn cover_info_section() {
    let mut c = conn().await;
    let info = c.execute(Info::new().section("server")).await.unwrap();
    assert!(info.contains("redis_version"));
    // Should not contain memory section when filtering to server only.
    assert!(!info.contains("used_memory:"));
}

#[tokio::test]
async fn cover_time() {
    let mut c = conn().await;
    let (secs, micros) = c.execute(Time::new()).await.unwrap();
    assert!(secs > 0);
    assert!(micros >= 0);
}

#[tokio::test]
async fn cover_command_count() {
    let mut c = conn().await;
    let count = c.execute(CommandCount::new()).await.unwrap();
    assert!(count > 0);
}

#[tokio::test]
async fn cover_command_list() {
    let mut c = conn().await;
    let cmds = c.execute(CommandList::new()).await.unwrap();
    assert!(!cmds.is_empty());
    // GET should be in every Redis server's command list.
    assert!(cmds.iter().any(|c| c.eq_ignore_ascii_case("get")));
}

#[tokio::test]
async fn cover_command_docs() {
    let mut c = conn().await;
    let docs = c.execute(CommandDocs::new().command("get")).await.unwrap();
    assert!(!docs.is_empty());
}

#[tokio::test]
async fn cover_command_overview_and_getkeysandflags() {
    let mut c = conn().await;
    let overview = c.execute(CommandOverview::new()).await.unwrap();
    assert!(matches!(overview, Frame::Array(Some(_)) | Frame::Map(_)));

    let key = b"server:command-flags:\x00";
    let entries = c
        .execute(CommandGetKeysAndFlags::new("SET").arg(key).arg("value"))
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.as_ref(), key);
    assert!(!entries[0].flags.is_empty());
}

#[tokio::test]
async fn cover_bgsave() {
    let mut c = conn().await;
    let resp = c.execute(BgSave::new().schedule()).await.unwrap();
    // Response is "Background saving started" or "Background saving scheduled".
    assert!(resp.contains("Background saving"));
}

#[tokio::test]
async fn cover_lastsave() {
    let mut c = conn().await;
    let ts = c.execute(LastSave::new()).await.unwrap();
    assert!(ts > 0);
}

#[tokio::test]
async fn cover_swapdb() {
    let mut c = conn().await;
    // Swap db 0 and 1, then swap back to restore state.
    c.execute(SwapDb::new(0, 1)).await.unwrap();
    c.execute(SwapDb::new(0, 1)).await.unwrap();
}

// ---------------------------------------------------------------------------
// Diagnostics commands (issue #254)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cover_slowlog_len() {
    let mut c = conn().await;
    let len = c.execute(SlowlogLen::new()).await.unwrap();
    assert!(len >= 0);
}

#[tokio::test]
async fn cover_slowlog_reset() {
    let mut c = conn().await;
    // Reset should succeed.
    c.execute(SlowlogReset::new()).await.unwrap();
    // After reset, log length should be zero.
    let len = c.execute(SlowlogLen::new()).await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn cover_slowlog_get() {
    let mut c = conn().await;
    // May be empty after a reset, but must not error.
    c.execute(SlowlogGet::new()).await.unwrap();
}

#[tokio::test]
async fn cover_memory_usage() {
    let mut c = conn().await;
    // Set a key first so MEMORY USAGE has something to report.
    c.execute(Set::new("test:mem", "hello")).await.unwrap();
    let usage = c.execute(MemoryUsage::new("test:mem")).await.unwrap();
    assert!(usage.is_some());
    assert!(usage.unwrap() > 0);
}

#[tokio::test]
async fn cover_memory_doctor() {
    let mut c = conn().await;
    let report = c.execute(MemoryDoctor::new()).await.unwrap();
    assert!(!report.is_empty());
}

#[tokio::test]
async fn cover_memory_stats() {
    let mut c = conn().await;
    // Returns a complex frame; verify it does not error.
    c.execute(MemoryStats::new()).await.unwrap();
}

#[tokio::test]
async fn cover_memory_purge_and_malloc_stats() {
    let mut c = conn().await;
    c.execute(MemoryPurge::new()).await.unwrap();
    // The report may be empty when Redis is not built with jemalloc, but the
    // command must normalize both RESP2 bulk and RESP3 verbatim replies.
    let _report = c.execute(MemoryMallocStats::new()).await.unwrap();
}

#[tokio::test]
async fn cover_latency_latest() {
    let mut c = conn().await;
    // The list may be empty on a freshly started server; must not error.
    c.execute(LatencyLatest::new()).await.unwrap();
}

#[tokio::test]
async fn cover_latency_reset() {
    let mut c = conn().await;
    // Resets all latency events; returns count of events reset (may be 0).
    let _count = c.execute(LatencyReset::new()).await.unwrap();
}

#[tokio::test]
async fn cover_latency_histogram_and_doctor() {
    let mut c = conn().await;
    let histogram = c
        .execute(LatencyHistogram::new().command("get"))
        .await
        .unwrap();
    assert!(matches!(histogram, Frame::Array(Some(_)) | Frame::Map(_)));

    let report = c.execute(LatencyDoctor::new()).await.unwrap();
    assert!(!report.is_empty());
}

// ---------------------------------------------------------------------------
// Server/admin commands (issue #255)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cover_config_get() {
    let mut c = conn().await;
    let pairs = c.execute(ConfigGet::new("maxmemory")).await.unwrap();
    // CONFIG GET maxmemory always returns exactly one pair.
    assert!(!pairs.is_empty());
}

#[tokio::test]
async fn cover_config_set() {
    let mut c = conn().await;
    // Set hz to 15, then restore to 10.
    c.execute(ConfigSet::new("hz", "15")).await.unwrap();
    c.execute(ConfigSet::new("hz", "10")).await.unwrap();
}

#[tokio::test]
async fn cover_client_list() {
    let mut c = conn().await;
    let list = c.execute(ClientList::new()).await.unwrap();
    let text = String::from_utf8_lossy(&list);
    // Every CLIENT LIST line starts with "id=".
    assert!(text.contains("id="));
}

#[tokio::test]
async fn cover_client_getname() {
    let mut c = conn().await;
    // No name set -- should return None without error.
    let _name = c.execute(ClientGetName::new()).await.unwrap();
}

#[tokio::test]
async fn cover_wait_zero() {
    let mut c = conn().await;
    // WAIT 0 0 returns immediately on a standalone server with 0 replicas.
    let replicas = c.execute(Wait::new(0, 0)).await.unwrap();
    assert_eq!(replicas, 0);
}

// Skipped: CLIENT KILL -- killing the current connection or random connections
// would disrupt other tests running in parallel. REPLICAOF and FAILOVER use the
// isolated two-process topology below instead of the shared standalone server.

/// Reserve two distinct loopback ports and release them for dedicated Redis
/// servers to bind.
///
/// Holding both listeners until both ports have been selected prevents the OS
/// from handing the same ephemeral port back to the second allocation.
fn free_server_ports() -> (u16, u16) {
    let first = std::net::TcpListener::bind("127.0.0.1:0").expect("bind first ephemeral port");
    let second = std::net::TcpListener::bind("127.0.0.1:0").expect("bind second ephemeral port");
    let first_port = first.local_addr().expect("first local_addr").port();
    let second_port = second.local_addr().expect("second local_addr").port();
    assert_ne!(first_port, second_port, "ephemeral ports must be distinct");
    drop((first, second));
    (first_port, second_port)
}

#[tokio::test]
async fn cover_migrate_empty_single_multi_and_nokey() {
    // Compatibility jobs use an externally managed service and cannot start a
    // second destination server. The regular integration job owns both
    // processes and exercises the real cross-server transfer.
    if std::env::var_os("REDIS_EXTERNAL_SERVICE").is_some() {
        return;
    }

    let (source_port, destination_port) = free_server_ports();
    let source_server = RedisServer::new()
        .port(source_port)
        .save(false)
        .start()
        .await
        .expect("failed to start MIGRATE source server");
    let destination_server = RedisServer::new()
        .port(destination_port)
        .save(false)
        .start()
        .await
        .expect("failed to start MIGRATE destination server");

    let source_addr = source_server.addr();
    let destination_addr = destination_server.addr();
    let mut source = RedisConnection::connect(&source_addr)
        .await
        .expect("connect to MIGRATE source");
    let mut destination = RedisConnection::connect(&destination_addr)
        .await
        .expect("connect to MIGRATE destination");
    let prefix = format!("redis-tower:migrate:{}", std::process::id());
    let single = format!("{prefix}:single");
    let first = format!("{prefix}:first");
    let second = format!("{prefix}:second");

    source.execute(Set::new("", "empty-value")).await.unwrap();
    assert_eq!(
        source
            .execute(Migrate::new("127.0.0.1", destination_port, "", 0, 5_000))
            .await
            .unwrap(),
        MigrateResult::Ok
    );
    assert_eq!(source.execute(Get::new("")).await.unwrap(), None);
    assert_eq!(
        destination.execute(Get::new("")).await.unwrap(),
        Some(bytes::Bytes::from_static(b"empty-value"))
    );

    source
        .execute(Set::new(&single, "single-value"))
        .await
        .unwrap();
    assert_eq!(
        source
            .execute(Migrate::new(
                "127.0.0.1",
                destination_port,
                &single,
                0,
                5_000,
            ))
            .await
            .unwrap(),
        MigrateResult::Ok
    );
    assert_eq!(source.execute(Get::new(&single)).await.unwrap(), None);
    assert_eq!(
        destination.execute(Get::new(&single)).await.unwrap(),
        Some(bytes::Bytes::from_static(b"single-value"))
    );

    source.execute(Set::new(&first, "one")).await.unwrap();
    source.execute(Set::new(&second, "two")).await.unwrap();
    assert_eq!(
        source
            .execute(
                Migrate::keys("127.0.0.1", destination_port, &first, 0, 5_000)
                    .key(&second)
                    .copy(),
            )
            .await
            .unwrap(),
        MigrateResult::Ok
    );
    assert_eq!(
        source.execute(Get::new(&first)).await.unwrap().unwrap(),
        bytes::Bytes::from_static(b"one")
    );
    assert_eq!(
        source.execute(Get::new(&second)).await.unwrap().unwrap(),
        bytes::Bytes::from_static(b"two")
    );
    assert_eq!(
        destination
            .execute(Get::new(&first))
            .await
            .unwrap()
            .unwrap(),
        bytes::Bytes::from_static(b"one")
    );
    assert_eq!(
        destination
            .execute(Get::new(&second))
            .await
            .unwrap()
            .unwrap(),
        bytes::Bytes::from_static(b"two")
    );

    assert_eq!(
        source
            .execute(Migrate::new(
                "127.0.0.1",
                destination_port,
                format!("{prefix}:missing"),
                0,
                5_000,
            ))
            .await
            .unwrap(),
        MigrateResult::NoKey
    );
}

fn info_value<'a>(info: &'a str, field: &str) -> Option<&'a str> {
    info.lines().find_map(|line| {
        let (name, value) = line.trim_end_matches('\r').split_once(':')?;
        (name == field).then_some(value)
    })
}

/// Poll INFO replication through fresh connections until a topology state is
/// observable. Replication setup and FAILOVER both complete asynchronously, so
/// asserting immediately after their `OK` replies would be inherently racy.
async fn wait_for_replication_info(
    addr: &str,
    description: &str,
    condition: impl Fn(&str) -> bool,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        let last_observation = match RedisConnection::connect(addr).await {
            Ok(mut connection) => {
                match connection.execute(Info::new().section("replication")).await {
                    Ok(info) if condition(&info) => return info,
                    Ok(info) => info,
                    Err(error) => format!("INFO replication failed: {error}"),
                }
            }
            Err(error) => format!("connection failed: {error}"),
        };

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description} at {addr}; last observation:\n{last_observation}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// REPLICAOF can demote and promote a standalone server, and FAILOVER can then
/// coordinate a role swap with a synchronized replica.
///
/// All destructive topology changes are kept in one ordered test with two
/// dedicated server handles. The handles stop their processes on drop, even if
/// an assertion unwinds the test, so the shared integration server is never
/// affected and the selected ports are released for subsequent tests.
#[tokio::test]
async fn cover_replicaof_and_failover() {
    let (first_port, second_port) = free_server_ports();
    let first_server = RedisServer::new()
        .port(first_port)
        .repl_diskless_sync_delay(0)
        .start()
        .await
        .expect("failed to start first replication server");
    let second_server = RedisServer::new()
        .port(second_port)
        .repl_diskless_sync_delay(0)
        .start()
        .await
        .expect("failed to start second replication server");
    let first_addr = first_server.addr();
    let second_addr = second_server.addr();

    let mut first = RedisConnection::connect(&first_addr)
        .await
        .expect("failed to connect to first replication server");
    let mut second = RedisConnection::connect(&second_addr)
        .await
        .expect("failed to connect to second replication server");

    let initial_key = "test:server:replicaof:initial";
    first
        .execute(Set::new(initial_key, "copied-by-replicaof"))
        .await
        .expect("failed to seed the initial master");

    // Demote the second standalone server and wait for its initial full sync.
    second
        .execute(ReplicaOf::new("127.0.0.1", first_port))
        .await
        .expect("REPLICAOF should accept the first server as master");
    wait_for_replication_info(
        &second_addr,
        "second server to become a synced replica",
        |info| {
            info_value(info, "role") == Some("slave")
                && info_value(info, "master_port") == Some(first_port.to_string().as_str())
                && info_value(info, "master_link_status") == Some("up")
        },
    )
    .await;

    let copied = second
        .execute(Get::new(initial_key))
        .await
        .expect("replica should serve the synchronized key");
    assert_eq!(
        copied,
        Some(bytes::Bytes::from("copied-by-replicaof")),
        "REPLICAOF should synchronize the master's existing data"
    );

    // Promote the replica without discarding its synchronized dataset.
    second
        .execute(ReplicaOf::no_one())
        .await
        .expect("REPLICAOF NO ONE should promote the second server");
    wait_for_replication_info(&second_addr, "second server promotion", |info| {
        info_value(info, "role") == Some("master")
    })
    .await;
    second
        .execute(Set::new("test:server:replicaof:promoted", "writable"))
        .await
        .expect("promoted server should accept writes");

    // Demote it again, wait for a healthy link, and put a marker into the
    // replication stream before asking the original master to fail over.
    second
        .execute(ReplicaOf::new("127.0.0.1", first_port))
        .await
        .expect("second server should reattach as a replica");
    wait_for_replication_info(&second_addr, "second server to reattach", |info| {
        info_value(info, "role") == Some("slave")
            && info_value(info, "master_port") == Some(first_port.to_string().as_str())
            && info_value(info, "master_link_status") == Some("up")
    })
    .await;

    let failover_key = "test:server:failover:before";
    first
        .execute(Set::new(failover_key, "survives-role-swap"))
        .await
        .expect("failed to seed failover marker");
    let acknowledgements = first
        .execute(Wait::new(1, 5_000))
        .await
        .expect("WAIT should observe the attached replica");
    assert_eq!(acknowledgements, 1, "replica should acknowledge the marker");

    // FAILOVER returns when the operation is accepted; the role transition is
    // completed by a background task and is therefore polled below.
    first
        .execute(Failover::new().to("127.0.0.1", second_port).timeout(5_000))
        .await
        .expect("FAILOVER should be accepted by the original master");

    wait_for_replication_info(
        &second_addr,
        "second server to become failover master",
        |info| info_value(info, "role") == Some("master"),
    )
    .await;
    wait_for_replication_info(
        &first_addr,
        "first server to become a synced replica",
        |info| {
            info_value(info, "role") == Some("slave")
                && info_value(info, "master_port") == Some(second_port.to_string().as_str())
                && info_value(info, "master_link_status") == Some("up")
        },
    )
    .await;

    let mut new_master = RedisConnection::connect(&second_addr)
        .await
        .expect("failed to connect to the failover master");
    let preserved = new_master
        .execute(Get::new(failover_key))
        .await
        .expect("new master should serve data written before failover");
    assert_eq!(
        preserved,
        Some(bytes::Bytes::from("survives-role-swap")),
        "coordinated failover should preserve replicated data"
    );

    let after_key = "test:server:failover:after";
    new_master
        .execute(Set::new(after_key, "written-on-new-master"))
        .await
        .expect("new master should accept writes");
    let acknowledgements = new_master
        .execute(Wait::new(1, 5_000))
        .await
        .expect("old master should acknowledge replication after the role swap");
    assert_eq!(
        acknowledgements, 1,
        "demoted old master should replicate from the new master"
    );

    let replicated_back = first
        .execute(Get::new(after_key))
        .await
        .expect("demoted old master should serve replicated data");
    assert_eq!(
        replicated_back,
        Some(bytes::Bytes::from("written-on-new-master")),
        "writes to the new master should replicate back to the old master"
    );
}

// ---------------------------------------------------------------------------
// CLIENT command coverage (issue #353)
// ---------------------------------------------------------------------------

/// CLIENT SETNAME sets the connection name; CLIENT GETNAME retrieves it.
#[tokio::test]
async fn cover_client_setname_getname() {
    use bytes::Bytes;

    let mut c = conn().await;

    c.execute(ClientSetName::new("test-conn-name"))
        .await
        .unwrap();
    let name = c.execute(ClientGetName::new()).await.unwrap();
    assert_eq!(
        name,
        Some(Bytes::from("test-conn-name")),
        "CLIENT GETNAME should return the name set by CLIENT SETNAME"
    );

    // Clear the name so this connection doesn't pollute CLIENT LIST output
    // in other tests.  An empty string resets the name.
    c.execute(ClientSetName::new("")).await.unwrap_or(());
}

/// CLIENT ID returns a positive integer for the current connection.
#[tokio::test]
async fn cover_client_id() {
    let mut c = conn().await;
    let id = c.execute(ClientId::new()).await.unwrap();
    assert!(id > 0, "CLIENT ID should return a positive integer");
}

/// CLIENT INFO returns a text blob describing the current connection.
#[tokio::test]
async fn cover_client_info() {
    let mut c = conn().await;
    let info = c.execute(ClientInfo::new()).await.unwrap();
    let text = String::from_utf8_lossy(&info);
    assert!(
        text.contains("id="),
        "CLIENT INFO should contain 'id=', got: {text}"
    );
}

/// SELECT switches the active database; switching back to 0 must succeed.
#[tokio::test]
async fn cover_select() {
    let mut c = conn().await;
    // Switch to db 1.
    c.execute(Select::new(1)).await.unwrap();
    // Switch back to db 0.
    c.execute(Select::new(0)).await.unwrap();
}

// ---------------------------------------------------------------------------
// HELLO + unverified SERVER/CLIENT commands (issue #390)
// ---------------------------------------------------------------------------

/// Extract the value associated with `key` from a HELLO response frame.
///
/// HELLO replies with a map of server properties: a RESP3 `Map` when the
/// negotiated protocol is 3, or a flat key/value `Array` under RESP2. This
/// walks either shape and returns the matching value frame.
fn hello_field(frame: &Frame, key: &str) -> Option<Frame> {
    match frame {
        Frame::Map(pairs) => pairs
            .iter()
            .find(|(k, _)| k.as_str() == Some(key))
            .map(|(_, v)| v.clone()),
        Frame::Array(Some(items)) => items
            .as_chunks::<2>()
            .0
            .iter()
            .find(|pair| pair[0].as_str() == Some(key))
            .map(|pair| pair[1].clone()),
        _ => None,
    }
}

/// `HELLO` with no arguments returns the current connection's properties.
///
/// The harness uses `RedisConnection::connect`, which now negotiates RESP3 by
/// default (Auto + HELLO 3), so the connection is on RESP3 and a bare HELLO
/// reports `proto` 3 alongside the `server` identity.
#[tokio::test]
async fn cover_hello_default() {
    let mut c = conn().await;
    let reply = c.execute(Hello::new()).await.unwrap();

    let server =
        hello_field(&reply, "server").expect("HELLO reply should contain a 'server' field");
    assert_eq!(
        server.as_str(),
        Some("redis"),
        "HELLO 'server' field should be 'redis'"
    );

    let proto = hello_field(&reply, "proto").expect("HELLO reply should contain a 'proto' field");
    assert_eq!(
        proto.as_integer(),
        Some(3),
        "default RedisConnection::connect now negotiates RESP3, so HELLO reports proto 3"
    );
}

/// `HELLO 3` negotiates RESP3 and replies with a map whose `proto` is 3.
#[tokio::test]
async fn cover_hello_proto3() {
    let mut c = conn().await;
    let reply = c.execute(Hello::new().proto(3)).await.unwrap();

    assert!(
        matches!(reply, Frame::Map(_)),
        "HELLO 3 should reply with a RESP3 map, got: {reply:?}"
    );
    let proto = hello_field(&reply, "proto").expect("HELLO 3 reply should contain 'proto'");
    assert_eq!(proto.as_integer(), Some(3), "HELLO 3 should report proto 3");
}

/// `HELLO 2` negotiates RESP2 and replies with a flat array whose `proto` is 2.
#[tokio::test]
async fn cover_hello_proto2() {
    // Use a dedicated connection: HELLO 2 switches this connection back to
    // RESP2, and we don't want to leak that protocol state to other tests.
    let mut c = conn().await;
    let reply = c.execute(Hello::new().proto(2)).await.unwrap();

    assert!(
        matches!(reply, Frame::Array(Some(_))),
        "HELLO 2 should reply with a flat RESP2 array, got: {reply:?}"
    );
    let proto = hello_field(&reply, "proto").expect("HELLO 2 reply should contain 'proto'");
    assert_eq!(proto.as_integer(), Some(2), "HELLO 2 should report proto 2");
}

/// `HELLO ... SETNAME` sets the connection name as part of negotiation.
#[tokio::test]
async fn cover_hello_setname() {
    let mut c = conn().await;
    c.execute(Hello::new().proto(3).setname("hello-named-conn"))
        .await
        .unwrap();

    // The name set during HELLO should be retrievable via CLIENT GETNAME.
    let name = c.execute(ClientGetName::new()).await.unwrap();
    assert_eq!(
        name,
        Some(bytes::Bytes::from("hello-named-conn")),
        "HELLO SETNAME should set the connection name"
    );
}

/// `FLUSHALL` deletes every key across all databases.
///
/// This test is destructive, so it uses its own dedicated connection, seeds a
/// uniquely-named key, and asserts only on that key after the flush. The CI
/// suite runs single-threaded, so no other test runs concurrently with this
/// one. The standalone harness is shared, so we cannot assume the keyspace was
/// empty before this ran.
#[tokio::test]
async fn cover_flushall() {
    let mut c = conn().await;

    // Seed a key, confirm it exists, then flush everything.
    c.execute(Set::new("test:flushall:marker", "present"))
        .await
        .unwrap();
    let before = c
        .execute(Exists::new("test:flushall:marker"))
        .await
        .unwrap();
    assert_eq!(before, 1, "seeded marker key should exist before FLUSHALL");

    c.execute(FlushAll::new()).await.unwrap();

    let after = c
        .execute(Exists::new("test:flushall:marker"))
        .await
        .unwrap();
    assert_eq!(after, 0, "FLUSHALL should delete all keys");

    // DBSIZE must be zero immediately after a synchronous flush.
    let size = c.execute(DbSize::new()).await.unwrap();
    assert_eq!(size, 0, "DBSIZE should be 0 after FLUSHALL");
}

/// `FLUSHALL SYNC` flushes synchronously and also clears the keyspace.
#[tokio::test]
async fn cover_flushall_sync() {
    let mut c = conn().await;
    c.execute(Set::new("test:flushall:sync", "x"))
        .await
        .unwrap();
    c.execute(FlushAll::new().sync_mode()).await.unwrap();
    let exists = c.execute(Exists::new("test:flushall:sync")).await.unwrap();
    assert_eq!(exists, 0, "FLUSHALL SYNC should delete all keys");
}

/// `BGREWRITEAOF` triggers a background AOF rewrite and returns a status string.
#[tokio::test]
async fn cover_bgrewriteaof() {
    let mut c = conn().await;
    let resp = c.execute(BgRewriteAof::new()).await.unwrap();
    // Redis replies with a "Background append only file rewriting ..." status,
    // or schedules one if a save is already in progress. Either mentions AOF.
    let lower = resp.to_lowercase();
    assert!(
        lower.contains("append only file") || lower.contains("aof"),
        "BGREWRITEAOF status should mention the AOF, got: {resp}"
    );
}

/// `WAITAOF 0 0 0` returns immediately with (local, replicas) acknowledgement
/// counts. With AOF disabled on the harness, the local count is 0.
#[tokio::test]
async fn cover_waitaof() {
    let mut c = conn().await;
    // numlocal=0, numreplicas=0, timeout=0 returns without blocking.
    let (local, replicas) = c.execute(WaitAof::new(0, 0, 0)).await.unwrap();
    assert!(local >= 0, "WAITAOF local count should be non-negative");
    assert_eq!(
        replicas, 0,
        "WAITAOF on a standalone server should report 0 replica acks"
    );
}

/// `CLIENT NO-EVICT ON` and `OFF` both succeed on the current connection.
#[tokio::test]
async fn cover_client_no_evict() {
    let mut c = conn().await;
    c.execute(ClientNoEvict::new(true)).await.unwrap();
    c.execute(ClientNoEvict::new(false)).await.unwrap();
}

/// `CLIENT NO-TOUCH ON` and `OFF` both succeed on the current connection.
#[tokio::test]
async fn cover_client_no_touch() {
    let mut c = conn().await;
    c.execute(ClientNoTouch::new(true)).await.unwrap();
    c.execute(ClientNoTouch::new(false)).await.unwrap();
}

/// `CLIENT PAUSE` then `CLIENT UNPAUSE` round-trip.
///
/// The pause is kept brief (10ms) and immediately lifted with UNPAUSE so the
/// single-threaded suite never stalls. CLIENT PAUSE affects all clients on the
/// server, but the short window plus the explicit unpause keeps the blast
/// radius negligible.
#[tokio::test]
async fn cover_client_pause_unpause() {
    let mut c = conn().await;
    // Pause writes only, for a very short window.
    c.execute(ClientPause::new(10).mode(ClientPauseMode::Write))
        .await
        .unwrap();
    // Immediately resume so nothing else is held up.
    c.execute(ClientUnpause::new()).await.unwrap();
}
