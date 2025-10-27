//! Integration tests for serde JSON support

use redis_tower::RedisClient;
use redis_tower::commands::{GetJson, MSetJson, SetJson};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Config {
    timeout: u64,
    retries: u32,
    endpoints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Address {
    street: String,
    city: String,
    zip: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Person {
    name: String,
    age: u32,
    addresses: Vec<Address>,
}

#[tokio::test]
async fn test_set_and_get_json() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let user = User {
        id: 123,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
    };

    // Store as JSON
    client
        .call(SetJson::new("test:user:123", &user).unwrap())
        .await
        .unwrap();

    // Retrieve and deserialize
    let stored: Option<User> = client.call(GetJson::new("test:user:123")).await.unwrap();

    assert_eq!(Some(user), stored);
}

#[tokio::test]
async fn test_get_json_nonexistent_key() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let result: Option<User> = client
        .call(GetJson::new("test:nonexistent:key"))
        .await
        .unwrap();

    assert_eq!(None, result);
}

#[tokio::test]
async fn test_set_json_overwrite() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let user1 = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
    };

    let user2 = User {
        id: 2,
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
        active: false,
    };

    // Store first user
    client
        .call(SetJson::new("test:user:overwrite", &user1).unwrap())
        .await
        .unwrap();

    // Overwrite with second user
    client
        .call(SetJson::new("test:user:overwrite", &user2).unwrap())
        .await
        .unwrap();

    // Should get second user
    let stored: Option<User> = client
        .call(GetJson::new("test:user:overwrite"))
        .await
        .unwrap();

    assert_eq!(Some(user2), stored);
}

#[tokio::test]
async fn test_json_with_nested_structures() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let person = Person {
        name: "Charlie".to_string(),
        age: 30,
        addresses: vec![
            Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: "12345".to_string(),
            },
            Address {
                street: "456 Oak Ave".to_string(),
                city: "Shelbyville".to_string(),
                zip: "67890".to_string(),
            },
        ],
    };

    client
        .call(SetJson::new("test:person:nested", &person).unwrap())
        .await
        .unwrap();

    let stored: Option<Person> = client
        .call(GetJson::new("test:person:nested"))
        .await
        .unwrap();

    assert_eq!(Some(person), stored);
}

#[tokio::test]
async fn test_json_with_collections() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let config = Config {
        timeout: 5000,
        retries: 3,
        endpoints: vec![
            "http://localhost:8080".to_string(),
            "http://localhost:8081".to_string(),
            "http://localhost:8082".to_string(),
        ],
    };

    client
        .call(SetJson::new("test:config:vec", &config).unwrap())
        .await
        .unwrap();

    let stored: Option<Config> = client.call(GetJson::new("test:config:vec")).await.unwrap();

    assert_eq!(Some(config), stored);
}

#[tokio::test]
async fn test_mset_json() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let user1 = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
    };

    let user2 = User {
        id: 2,
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
        active: true,
    };

    let user3 = User {
        id: 3,
        name: "Charlie".to_string(),
        email: "charlie@example.com".to_string(),
        active: false,
    };

    // Store multiple users with MSET
    let pairs = vec![
        ("test:mset:user:1", user1.clone()),
        ("test:mset:user:2", user2.clone()),
        ("test:mset:user:3", user3.clone()),
    ];

    client.call(MSetJson::new(pairs).unwrap()).await.unwrap();

    // Retrieve each user
    let stored1: Option<User> = client.call(GetJson::new("test:mset:user:1")).await.unwrap();
    let stored2: Option<User> = client.call(GetJson::new("test:mset:user:2")).await.unwrap();
    let stored3: Option<User> = client.call(GetJson::new("test:mset:user:3")).await.unwrap();

    assert_eq!(Some(user1), stored1);
    assert_eq!(Some(user2), stored2);
    assert_eq!(Some(user3), stored3);
}

#[tokio::test]
async fn test_json_with_special_characters() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let user = User {
        id: 999,
        name: "Test \"User\" with 'quotes'".to_string(),
        email: "test@example.com\nwith\nnewlines".to_string(),
        active: true,
    };

    client
        .call(SetJson::new("test:user:special", &user).unwrap())
        .await
        .unwrap();

    let stored: Option<User> = client
        .call(GetJson::new("test:user:special"))
        .await
        .unwrap();

    assert_eq!(Some(user), stored);
}

#[tokio::test]
async fn test_json_empty_collections() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let person = Person {
        name: "No Addresses".to_string(),
        age: 25,
        addresses: vec![],
    };

    client
        .call(SetJson::new("test:person:empty", &person).unwrap())
        .await
        .unwrap();

    let stored: Option<Person> = client
        .call(GetJson::new("test:person:empty"))
        .await
        .unwrap();

    assert_eq!(Some(person), stored);
}

#[tokio::test]
async fn test_json_with_unicode() {
    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let user = User {
        id: 777,
        name: "用户 👋 🎉".to_string(),
        email: "test@例え.jp".to_string(),
        active: true,
    };

    client
        .call(SetJson::new("test:user:unicode", &user).unwrap())
        .await
        .unwrap();

    let stored: Option<User> = client
        .call(GetJson::new("test:user:unicode"))
        .await
        .unwrap();

    assert_eq!(Some(user), stored);
}

#[tokio::test]
async fn test_json_roundtrip_precision() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct NumericData {
        integer: i64,
        unsigned: u64,
        float: f64,
        boolean: bool,
    }

    let client = RedisClient::connect("localhost:6379").await.unwrap();

    let data = NumericData {
        integer: -9223372036854775807,
        unsigned: 18446744073709551615,
        float: 3.141592653589793,
        boolean: true,
    };

    client
        .call(SetJson::new("test:numeric:precision", &data).unwrap())
        .await
        .unwrap();

    let stored: Option<NumericData> = client
        .call(GetJson::new("test:numeric:precision"))
        .await
        .unwrap();

    assert_eq!(Some(data), stored);
}
