mod common;

use common::conn;
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
// Skipped: CLIENT PAUSE / CLIENT UNPAUSE -- pausing all clients mid-test
// suite would cause other tests to hang unpredictably.
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
