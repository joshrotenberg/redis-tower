//! Integration tests for ACL commands against a real Redis server.
//!
//! The test server is started without auth and without an ACL file on disk,
//! which constrains what can be exercised here:
//!
//! - `AclSave` and `AclLoad` require Redis to be started with an ACL file
//!   (the `aclfile` directive). Without one they return an error, so they are
//!   not tested here.
//! - `AclDryRun` is not exercised directly; the SETUSER/GETUSER/DELUSER cycle
//!   covers user lifecycle behaviour instead.

mod common;
use common::conn;
use redis_tower::commands::*;

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
async fn acl_log() {
    let mut c = conn().await;
    // The log may be empty; we only assert the command succeeds.
    c.execute(AclLog::new()).await.unwrap();
    c.execute(AclLogReset::new()).await.unwrap();
}
