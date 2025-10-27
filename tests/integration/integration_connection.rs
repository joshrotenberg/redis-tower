//! Integration tests for connection management commands
//!
//! Tests CLIENT and connection-related commands.
//!
//! Run with: cargo test --test integration_connection

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_client_list() {
    let client = setup_redis().await;

    // Get list of connected clients
    let list: String = client.call(ClientList::new()).await.unwrap();

    // Should contain connection information
    assert!(list.contains("addr="));
    assert!(list.contains("fd="));
}

#[tokio::test]
async fn test_client_id() {
    let client = setup_redis().await;

    // Get our client ID
    let id: i64 = client.call(ClientId).await.unwrap();

    // Should be a positive number
    assert!(id > 0);
}

#[tokio::test]
async fn test_client_setname_getname() {
    let client = setup_redis().await;

    // Set client name
    client
        .call(ClientSetName::new("test-client"))
        .await
        .unwrap();

    // Get client name back
    let name: Option<String> = client.call(ClientGetName).await.unwrap();
    assert_eq!(name, Some("test-client".to_string()));
}

#[tokio::test]
async fn test_client_info() {
    let client = setup_redis().await;

    // Get info about our connection
    let info: String = client.call(ClientInfo).await.unwrap();

    // Should contain connection details
    assert!(info.contains("addr="));
    assert!(info.contains("fd="));
    assert!(info.contains("name="));
}

#[tokio::test]
async fn test_select_database() {
    let client = setup_redis().await;

    // Select database 1
    client.call(Select::new(1)).await.unwrap();

    // Set a key in database 1
    client.call(Set::new("db1_key", "value")).await.unwrap();

    // Switch back to database 0
    client.call(Select::new(0)).await.unwrap();

    // Key should not exist in database 0
    let exists: i64 = client.call(Exists::new("db1_key")).await.unwrap();
    assert_eq!(exists, 0);

    // Switch back to database 1 and clean up
    client.call(Select::new(1)).await.unwrap();
    client
        .call(Del::new(vec!["db1_key".to_string()]))
        .await
        .unwrap();

    // Switch back to database 0 for other tests
    client.call(Select::new(0)).await.unwrap();
}
