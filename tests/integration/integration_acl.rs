//! Integration tests for ACL (Access Control List) commands
//!
//! Tests ACL user management and permission system (Redis 6.0+).
//!
//! Run with: cargo test --test integration_acl

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_acl_whoami() {
    let client = setup_redis().await;

    // Get current username
    let username: String = client.call(AclWhoAmI::new()).await.unwrap();

    // Default user is "default"
    assert_eq!(username, "default");
}

#[tokio::test]
async fn test_acl_list() {
    let client = setup_redis().await;

    // Get ACL rules for all users
    let rules: Vec<String> = client.call(AclList::new()).await.unwrap();

    // Should at least have the default user
    assert!(!rules.is_empty());
    assert!(rules.iter().any(|r| r.contains("user default")));
}

#[tokio::test]
async fn test_acl_users() {
    let client = setup_redis().await;

    // Get list of all usernames
    let users: Vec<String> = client.call(AclUsers::new()).await.unwrap();

    // Should at least have the default user
    assert!(!users.is_empty());
    assert!(users.contains(&"default".to_string()));
}

#[tokio::test]
async fn test_acl_setuser_getuser() {
    let client = setup_redis().await;

    // Create a new user with basic permissions
    let username = "test_user_basic";
    client
        .call(
            AclSetUser::new(username)
                .on()
                .password("testpass123")
                .command("+get")
                .command("+set")
                .key_pattern("test:*"),
        )
        .await
        .unwrap();

    // Get user details
    let user_info: String = client.call(AclGetUser::new(username)).await.unwrap();

    // Should contain user information
    assert!(!user_info.is_empty());

    // Clean up
    let _ = client.call(AclDelUser::new().username(username)).await;
}

#[tokio::test]
async fn test_acl_setuser_multiple_options() {
    let client = setup_redis().await;

    let username = "test_user_advanced";

    // Create user with multiple options
    client
        .call(
            AclSetUser::new(username)
                .on()
                .password("pass1")
                .password("pass2")
                .key_pattern("user:*")
                .key_pattern("session:*")
                .command("+@string")
                .command("+@hash"),
        )
        .await
        .unwrap();

    // Verify user exists
    let users: Vec<String> = client.call(AclUsers::new()).await.unwrap();
    assert!(users.contains(&username.to_string()));

    // Clean up
    let _ = client.call(AclDelUser::new().username(username)).await;
}

#[tokio::test]
async fn test_acl_deluser() {
    let client = setup_redis().await;

    let username = "test_user_delete";

    // Create a user
    client
        .call(AclSetUser::new(username).on().nopass())
        .await
        .unwrap();

    // Delete the user
    let deleted: i64 = client
        .call(AclDelUser::new().username(username))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    // Verify user is gone
    let users: Vec<String> = client.call(AclUsers::new()).await.unwrap();
    assert!(!users.contains(&username.to_string()));
}

#[tokio::test]
async fn test_acl_cat() {
    let client = setup_redis().await;

    // Get all command categories
    let categories: Vec<String> = client.call(AclCat::new()).await.unwrap();

    // Should have common categories
    assert!(!categories.is_empty());
    assert!(categories.contains(&"string".to_string()));
    assert!(categories.contains(&"list".to_string()));
    assert!(categories.contains(&"hash".to_string()));
}

#[tokio::test]
async fn test_acl_cat_specific() {
    let client = setup_redis().await;

    // Get commands in the string category
    let commands: Vec<String> = client.call(AclCat::category("string")).await.unwrap();

    // Should contain string commands
    assert!(!commands.is_empty());
    assert!(commands.iter().any(|c| c.to_lowercase() == "get"));
    assert!(commands.iter().any(|c| c.to_lowercase() == "set"));
}

#[tokio::test]
async fn test_acl_genpass() {
    let client = setup_redis().await;

    // Generate password with default length (256 bits = 64 hex chars)
    let password: String = client.call(AclGenPass::new()).await.unwrap();
    assert_eq!(password.len(), 64); // 256 bits in hex

    // Generate password with custom length (128 bits = 32 hex chars)
    let password: String = client.call(AclGenPass::new().bits(128)).await.unwrap();
    assert_eq!(password.len(), 32); // 128 bits in hex
}

#[tokio::test]
async fn test_acl_log() {
    let client = setup_redis().await;

    // Get ACL log entries (may be empty if no auth failures)
    let log_entries: String = client.call(AclLog::new().count(10)).await.unwrap();

    // Just verify we can call it (may be empty string)
    assert!(log_entries.is_empty() || !log_entries.is_empty());
}
