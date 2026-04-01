mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_info() {
    let c = conn().await;
    let info = c.execute(Info::new()).await.unwrap();
    assert!(info.contains("redis_version"));
}

#[tokio::test]
async fn cover_info_section() {
    let c = conn().await;
    let info = c.execute(Info::new().section("server")).await.unwrap();
    assert!(info.contains("redis_version"));
    // Should not contain memory section when filtering to server only.
    assert!(!info.contains("used_memory:"));
}

#[tokio::test]
async fn cover_time() {
    let c = conn().await;
    let (secs, micros) = c.execute(Time::new()).await.unwrap();
    assert!(secs > 0);
    assert!(micros >= 0);
}

#[tokio::test]
async fn cover_command_count() {
    let c = conn().await;
    let count = c.execute(CommandCount::new()).await.unwrap();
    assert!(count > 0);
}

#[tokio::test]
async fn cover_command_list() {
    let c = conn().await;
    let cmds = c.execute(CommandList::new()).await.unwrap();
    assert!(!cmds.is_empty());
    // GET should be in every Redis server's command list.
    assert!(cmds.iter().any(|c| c.eq_ignore_ascii_case("get")));
}

#[tokio::test]
async fn cover_command_docs() {
    let c = conn().await;
    let docs = c.execute(CommandDocs::new().command("get")).await.unwrap();
    assert!(!docs.is_empty());
}

#[tokio::test]
async fn cover_bgsave() {
    let c = conn().await;
    let resp = c.execute(BgSave::new().schedule()).await.unwrap();
    // Response is "Background saving started" or "Background saving scheduled".
    assert!(resp.contains("Background saving"));
}

#[tokio::test]
async fn cover_lastsave() {
    let c = conn().await;
    let ts = c.execute(LastSave::new()).await.unwrap();
    assert!(ts > 0);
}

#[tokio::test]
async fn cover_swapdb() {
    let c = conn().await;
    // Swap db 0 and 1, then swap back to restore state.
    c.execute(SwapDb::new(0, 1)).await.unwrap();
    c.execute(SwapDb::new(0, 1)).await.unwrap();
}
