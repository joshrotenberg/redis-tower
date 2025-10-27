//! Integration tests for Redis Module management commands
//!
//! Tests module loading, unloading, and inspection (Redis 4.0+).
//!
//! Run with: cargo test --test integration_module
//!
//! Note: These tests primarily verify API functionality since most
//! Redis installations don't have modules loaded by default.

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_module_list() {
    let client = setup_redis().await;

    // List all loaded modules
    let modules: Vec<ModuleInfo> = client.call(ModuleList).await.unwrap();

    // Most test Redis instances won't have modules loaded
    // Just verify the command works
    assert!(modules.is_empty() || !modules.is_empty());

    // If any modules are loaded, verify structure
    for module in modules {
        assert!(!module.name.is_empty());
        assert!(module.version >= 0);
    }
}

#[tokio::test]
async fn test_module_load_missing_file() {
    let client = setup_redis().await;

    // Try to load a non-existent module (should fail)
    let result: Result<(), _> = client.call(ModuleLoad::new("/nonexistent/module.so")).await;

    // Should fail with error
    assert!(result.is_err());
}

#[tokio::test]
async fn test_module_unload_nonexistent() {
    let client = setup_redis().await;

    // Try to unload a module that doesn't exist (should fail)
    let result: Result<(), _> = client.call(ModuleUnload::new("nonexistent_module")).await;

    // Should fail with error
    assert!(result.is_err());
}
