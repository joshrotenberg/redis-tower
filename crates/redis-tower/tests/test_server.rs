mod common;

use common::conn;
use redis_tower::Frame;
use redis_tower::commands::*;

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
// would disrupt other tests running in parallel.
//
// Skipped: REPLICAOF, FAILOVER -- require a primary+replica setup that the
// standalone test harness does not provide.

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
            .chunks_exact(2)
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
