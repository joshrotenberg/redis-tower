//! Serde JSON integration example
//!
//! Demonstrates how to store and retrieve Rust structs in Redis using JSON serialization.
//! This is useful for caching complex data structures without manual serialization.
//!
//! Features demonstrated:
//! - Storing structs with SetJson
//! - Retrieving structs with GetJson
//! - Batch operations with MSetJson
//! - Nested structures and collections
//! - Type safety at compile time
//!
//! Run with:
//! ```bash
//! cargo run --example serde_json --features serde-json
//! ```

use redis_tower::RedisClient;
use redis_tower::commands::{Del, GetJson, MSetJson, SetJson};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    id: u64,
    username: String,
    email: String,
    active: bool,
    tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Product {
    sku: String,
    name: String,
    price: f64,
    in_stock: bool,
    categories: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Address {
    street: String,
    city: String,
    state: String,
    zip: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    order_id: String,
    user_id: u64,
    items: Vec<OrderItem>,
    shipping_address: Address,
    total: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OrderItem {
    product_id: String,
    quantity: u32,
    price: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("redis_tower=info")
        .init();

    println!("=== Redis Serde JSON Integration Example ===\n");

    let client = RedisClient::connect("localhost:6379").await?;

    // Clean up any existing test data
    let _ = client
        .call(Del::new(vec!["user:1", "product:1", "order:1"]))
        .await;

    println!("1. Storing a User struct with SetJson");
    println!("=====================================");

    let user = User {
        id: 1,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
        tags: vec!["premium".to_string(), "beta-tester".to_string()],
    };

    println!("Storing user: {:#?}", user);
    client.call(SetJson::new("user:1", &user)?).await?;
    println!("✓ User stored successfully\n");

    println!("2. Retrieving the User with GetJson");
    println!("====================================");

    let stored_user: Option<User> = client.call(GetJson::new("user:1")).await?;
    println!("Retrieved user: {:#?}", stored_user);
    assert_eq!(Some(user.clone()), stored_user);
    println!("✓ User matches original\n");

    println!("3. Storing a Product with nested data");
    println!("=====================================");

    let product = Product {
        sku: "WIDGET-001".to_string(),
        name: "Super Widget".to_string(),
        price: 29.99,
        in_stock: true,
        categories: vec!["electronics".to_string(), "gadgets".to_string()],
    };

    println!("Storing product: {:#?}", product);
    client.call(SetJson::new("product:1", &product)?).await?;
    println!("✓ Product stored successfully\n");

    println!("4. Batch storing multiple products with MSetJson");
    println!("=================================================");

    let product2 = Product {
        sku: "WIDGET-002".to_string(),
        name: "Mega Widget".to_string(),
        price: 49.99,
        in_stock: true,
        categories: vec!["electronics".to_string()],
    };

    let product3 = Product {
        sku: "WIDGET-003".to_string(),
        name: "Ultra Widget".to_string(),
        price: 99.99,
        in_stock: false,
        categories: vec!["electronics".to_string(), "premium".to_string()],
    };

    let products = vec![
        ("product:2", product2.clone()),
        ("product:3", product3.clone()),
    ];

    println!("Storing {} products in batch...", products.len());
    client.call(MSetJson::new(products)?).await?;
    println!("✓ Batch storage successful\n");

    // Verify batch storage
    let p2: Option<Product> = client.call(GetJson::new("product:2")).await?;
    let p3: Option<Product> = client.call(GetJson::new("product:3")).await?;
    assert_eq!(Some(product2), p2);
    assert_eq!(Some(product3), p3);
    println!("✓ All products retrieved successfully\n");

    println!("5. Storing complex nested structures");
    println!("====================================");

    let order = Order {
        order_id: "ORD-2024-001".to_string(),
        user_id: 1,
        items: vec![
            OrderItem {
                product_id: "WIDGET-001".to_string(),
                quantity: 2,
                price: 29.99,
            },
            OrderItem {
                product_id: "WIDGET-002".to_string(),
                quantity: 1,
                price: 49.99,
            },
        ],
        shipping_address: Address {
            street: "123 Main St".to_string(),
            city: "Springfield".to_string(),
            state: "IL".to_string(),
            zip: "62701".to_string(),
        },
        total: 109.97,
    };

    println!("Storing order with nested items and address: {:#?}", order);
    client.call(SetJson::new("order:1", &order)?).await?;
    println!("✓ Complex order stored successfully\n");

    let stored_order: Option<Order> = client.call(GetJson::new("order:1")).await?;
    println!("Retrieved order: {:#?}", stored_order);
    assert_eq!(Some(order), stored_order);
    println!("✓ Order structure preserved perfectly\n");

    println!("6. Handling non-existent keys");
    println!("=============================");

    let missing: Option<User> = client.call(GetJson::new("user:999")).await?;
    println!(
        "Attempting to get non-existent key 'user:999': {:?}",
        missing
    );
    assert_eq!(None, missing);
    println!("✓ Correctly returns None for missing keys\n");

    println!("7. Type safety demonstration");
    println!("============================");
    println!("The compiler ensures type safety:");
    println!("  - SetJson requires a serializable type");
    println!("  - GetJson returns the exact type you specify");
    println!("  - No runtime type checking needed!");
    println!("  - Compile-time errors if types don't match\n");

    println!("8. Performance characteristics");
    println!("==============================");
    println!("JSON serialization adds overhead but provides:");
    println!("  ✓ Human-readable data in Redis");
    println!("  ✓ Easy debugging with redis-cli");
    println!("  ✓ Cross-language compatibility");
    println!("  ✓ Schema evolution support");
    println!("  ✓ No custom serialization code needed\n");

    println!("9. Use cases");
    println!("============");
    println!("Perfect for:");
    println!("  • Session storage (user sessions, cart data)");
    println!("  • Configuration caching");
    println!("  • API response caching");
    println!("  • Task queue payloads");
    println!("  • User preferences and settings");
    println!("  • Product catalogs");
    println!("  • Any structured data caching\n");

    // Clean up
    client
        .call(Del::new(vec![
            "user:1",
            "product:1",
            "product:2",
            "product:3",
            "order:1",
        ]))
        .await?;

    println!("✓ Example completed successfully!");
    println!("\nTry it with your own structs by:");
    println!("  1. Deriving Serialize and Deserialize");
    println!("  2. Using SetJson::new(key, &your_struct)?");
    println!("  3. Using GetJson::<YourStruct>::new(key)");

    Ok(())
}
