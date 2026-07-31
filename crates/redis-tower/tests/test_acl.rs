//! Integration tests for ACL commands against a real Redis server.
//!
//! Most tests use the shared unauthenticated server. The `ACL SAVE` / `ACL
//! LOAD` test owns a dedicated process configured with an ACL file so it can
//! verify both runtime reloads and persistence across a server restart.

mod common;

use std::path::{Path, PathBuf};

use common::conn;
use redis_server_wrapper::RedisServer;
use redis_tower::commands::*;
use redis_tower::{Frame, RedisConnection};

/// Reserve and immediately release an ephemeral port for a dedicated Redis
/// process. The wrapper binds it immediately afterward.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("read ephemeral port").port();
    drop(listener);
    port
}

/// Per-test directory removed after both Redis processes have stopped.
struct AclTestDir(PathBuf);

impl AclTestDir {
    fn new(port: u16) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "redis-tower-acl-{}-{port}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create ACL test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for AclTestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn acl_user_exists(frame: &Frame) -> bool {
    !matches!(frame, Frame::Null | Frame::BulkString(None))
}

#[tokio::test]
async fn acl_whoami() {
    let mut c = conn().await;
    let who = c.execute(AclWhoAmI::new()).await.unwrap();
    assert_eq!(who, "default");
}

#[tokio::test]
async fn acl_cat_no_filter() {
    let mut c = conn().await;
    let cats = c.execute(AclCat::new()).await.unwrap();
    assert!(!cats.is_empty());
}

#[tokio::test]
async fn acl_cat_with_category() {
    let mut c = conn().await;
    let cmds = c.execute(AclCat::category("string")).await.unwrap();
    let names: Vec<String> = cmds
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    assert!(names.iter().any(|n| n == "get"));
    assert!(names.iter().any(|n| n == "set"));
}

#[tokio::test]
async fn acl_list() {
    let mut c = conn().await;
    let rules = c.execute(AclList::new()).await.unwrap();
    assert!(!rules.is_empty());
    assert!(
        rules
            .iter()
            .any(|b| String::from_utf8_lossy(b).contains("default"))
    );
}

#[tokio::test]
async fn acl_getuser_default() {
    let mut c = conn().await;
    // GETUSER returns a complex nested frame; just assert it succeeds.
    c.execute(AclGetUser::new("default")).await.unwrap();
}

#[tokio::test]
async fn acl_genpass_default() {
    let mut c = conn().await;
    let pass = c.execute(AclGenPass::new()).await.unwrap();
    assert!(!pass.is_empty());
    assert!(pass.chars().all(|ch| ch.is_ascii_hexdigit()));
}

#[tokio::test]
async fn acl_genpass_bits() {
    let mut c = conn().await;
    let pass = c.execute(AclGenPass::bits(128)).await.unwrap();
    assert!(!pass.is_empty());
    // 128 bits of pseudo-random data == 32 hex characters.
    assert_eq!(pass.len(), 32);
    assert!(pass.chars().all(|ch| ch.is_ascii_hexdigit()));
}

#[tokio::test]
async fn acl_setuser_deluser() {
    let mut c = conn().await;
    let user = "redis_tower_test_user";

    // Clean up any leftover user from a prior interrupted run.
    let _ = c.execute(AclDelUser::new(user)).await;

    // Create the user.
    c.execute(AclSetUser::new(user).rule("on").rule("+@all").rule("~*"))
        .await
        .unwrap();

    // Verify it exists.
    c.execute(AclGetUser::new(user)).await.unwrap();

    // Delete it; exactly one user should be removed.
    let deleted = c.execute(AclDelUser::new(user)).await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn acl_dryrun_allows_and_denies() {
    let mut c = conn().await;
    let user = "redis_tower_dryrun_user";

    // Clean up any leftover user from a prior interrupted run.
    let _ = c.execute(AclDelUser::new(user)).await;

    // Create an enabled user that may only run GET against keys under `dryrun:*`.
    // It explicitly cannot run SET (or anything outside `+get`).
    c.execute(
        AclSetUser::new(user)
            .rule("on")
            .rule(">dryrunpass")
            .rule("~dryrun:*")
            .rule("+get"),
    )
    .await
    .unwrap();

    // GET is permitted: DRYRUN reports success with "OK".
    let allowed = c
        .execute(AclDryRun::new(user, "GET").arg("dryrun:key"))
        .await
        .unwrap();
    assert_eq!(allowed, "OK", "GET should be permitted for {user}");

    // SET is not permitted: DRYRUN returns a non-"OK" message explaining the
    // denial rather than erroring (the command itself succeeds).
    let denied = c
        .execute(AclDryRun::new(user, "SET").arg("dryrun:key").arg("value"))
        .await
        .unwrap();
    assert_ne!(allowed, denied);
    assert!(
        denied.to_lowercase().contains("set"),
        "denial message should mention the SET command, got: {denied}"
    );

    // Clean up.
    let deleted = c.execute(AclDelUser::new(user)).await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn acl_save_load_and_restart_persist_acl_file() {
    // Compatibility jobs point at an externally managed Redis image. This
    // scenario must own and restart its process, so it runs in the normal
    // integration job where redis-server is available on PATH.
    if std::env::var_os("REDIS_EXTERNAL_SERVICE").is_some() {
        return;
    }

    let port = free_port();
    let test_dir = AclTestDir::new(port);
    let acl_file = test_dir.path().join("users.acl");

    // Redis requires the configured ACL file to exist at startup. Seed an
    // unrestricted default user so the test can administer the dedicated
    // server without authentication.
    std::fs::write(&acl_file, "user default on nopass ~* +@all\n").expect("seed ACL file");

    let server = RedisServer::new()
        .port(port)
        .dir(test_dir.path())
        .acl_file(&acl_file)
        .save(false)
        .start()
        .await
        .expect("start Redis with an ACL file");
    let addr = server.addr();
    let mut c = RedisConnection::connect(&addr)
        .await
        .expect("connect to ACL test server");

    let persisted_user = "redis_tower_persisted_user";
    let runtime_only_user = "redis_tower_runtime_only_user";

    c.execute(
        AclSetUser::new(persisted_user)
            .rule("on")
            .rule(">persisted-secret")
            .rule("~persisted:*")
            .rule("+get"),
    )
    .await
    .expect("create user to persist");
    c.execute(AclSave::new()).await.expect("save ACL file");

    let saved = std::fs::read_to_string(&acl_file).expect("read saved ACL file");
    assert!(
        saved.contains(&format!("user {persisted_user} ")),
        "ACL SAVE did not write {persisted_user}: {saved}"
    );

    // Diverge runtime state from disk, then prove ACL LOAD replaces it with
    // the last saved definitions.
    assert_eq!(c.execute(AclDelUser::new(persisted_user)).await.unwrap(), 1);
    c.execute(
        AclSetUser::new(runtime_only_user)
            .rule("on")
            .rule("nopass")
            .rule("~runtime:*")
            .rule("+get"),
    )
    .await
    .expect("create runtime-only user");

    c.execute(AclLoad::new()).await.expect("reload ACL file");

    let persisted = c
        .execute(AclGetUser::new(persisted_user))
        .await
        .expect("query reloaded user");
    assert!(
        acl_user_exists(&persisted),
        "ACL LOAD did not restore {persisted_user}: {persisted:?}"
    );
    let runtime_only = c
        .execute(AclGetUser::new(runtime_only_user))
        .await
        .expect("query runtime-only user after load");
    assert!(
        !acl_user_exists(&runtime_only),
        "ACL LOAD retained unsaved user {runtime_only_user}: {runtime_only:?}"
    );

    // Stop cleanly without saving any database state, then start a fresh
    // process against the same ACL file and verify the saved user survives.
    drop(c);
    drop(server);

    let restarted = RedisServer::new()
        .port(port)
        .dir(test_dir.path())
        .acl_file(&acl_file)
        .save(false)
        .start()
        .await
        .expect("restart Redis with the saved ACL file");
    let mut c = RedisConnection::connect(&restarted.addr())
        .await
        .expect("connect after ACL server restart");

    let persisted = c
        .execute(AclGetUser::new(persisted_user))
        .await
        .expect("query persisted user after restart");
    assert!(
        acl_user_exists(&persisted),
        "saved user did not survive restart: {persisted:?}"
    );
    let runtime_only = c
        .execute(AclGetUser::new(runtime_only_user))
        .await
        .expect("query runtime-only user after restart");
    assert!(
        !acl_user_exists(&runtime_only),
        "unsaved user survived restart: {runtime_only:?}"
    );
}

#[tokio::test]
async fn acl_log() {
    let mut c = conn().await;
    // The log may be empty; we only assert the command succeeds.
    c.execute(AclLog::new()).await.unwrap();
    c.execute(AclLogReset::new()).await.unwrap();
}
