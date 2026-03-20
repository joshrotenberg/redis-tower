//! Examples demonstrating Redis Pub/Sub with async streaming.
//!
//! This example shows:
//! - Channel subscriptions (SUBSCRIBE/UNSUBSCRIBE)
//! - Pattern subscriptions (PSUBSCRIBE/PUNSUBSCRIBE)
//! - Publishing messages (PUBLISH)
//! - Async message streaming
//! - Real-world patterns (chat, events, notifications)
//!
//! Run with: cargo run --example pubsub

use redis_tower::client::RedisConnection;
use redis_tower::commands::pubsub::Publish;
use redis_tower::pubsub::{PubSubConnection, PubSubMessage};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Pub/Sub Examples ===\n");

    // Example 1: Basic pub/sub
    example_basic_pubsub().await?;

    // Example 2: Multiple channels
    example_multiple_channels().await?;

    // Example 3: Pattern subscriptions
    example_pattern_subscriptions().await?;

    // Example 4: Chat system
    example_chat_system().await?;

    // Example 5: Event bus
    example_event_bus().await?;

    // Example 6: Notifications
    example_notifications().await?;

    // Example 7: Unsubscribe patterns
    example_unsubscribe().await?;

    // Example 8: Message counting
    example_message_counting().await?;

    Ok(())
}

async fn example_basic_pubsub() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Basic Pub/Sub");

    // Create a subscriber connection
    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to a channel
    subscriber.subscribe(&["news"]).await?;
    println!("   Subscribed to 'news' channel");

    // Create a publisher connection (regular connection can publish)
    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Publish a message in a background task
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = publisher
            .execute(Publish::new("news", "Rust 2.0 announced!"))
            .await;
    });

    // Receive the subscription confirmation and message
    let mut msg_count = 0;
    while let Ok(Some(msg)) = timeout(Duration::from_secs(1), subscriber.next_message()).await {
        match msg? {
            PubSubMessage::Subscribe { channel, count } => {
                println!("   ✓ Subscribed to '{}' (total: {})", channel, count);
            }
            PubSubMessage::Message { channel, payload } => {
                println!(
                    "   📨 Message on '{}': {}",
                    channel,
                    String::from_utf8_lossy(&payload)
                );
                msg_count += 1;
                if msg_count >= 1 {
                    break;
                }
            }
            _ => {}
        }
    }

    println!();
    Ok(())
}

async fn example_multiple_channels() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Multiple Channels");

    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to multiple channels at once
    subscriber.subscribe(&["sports", "weather", "tech"]).await?;
    println!("   Subscribed to 3 channels");

    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Publish to different channels
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = publisher
            .execute(Publish::new("sports", "Team A wins!"))
            .await;
        let _ = publisher
            .execute(Publish::new("weather", "Sunny today"))
            .await;
        let _ = publisher.execute(Publish::new("tech", "New release")).await;
    });

    // Receive messages from all channels
    let mut msg_count = 0;
    while let Ok(Some(msg)) = timeout(Duration::from_secs(1), subscriber.next_message()).await {
        match msg? {
            PubSubMessage::Subscribe { channel, count } => {
                println!("   ✓ Subscribed to '{}' (total: {})", channel, count);
            }
            PubSubMessage::Message { channel, payload } => {
                println!("   📨 [{}]: {}", channel, String::from_utf8_lossy(&payload));
                msg_count += 1;
                if msg_count >= 3 {
                    break;
                }
            }
            _ => {}
        }
    }

    println!();
    Ok(())
}

async fn example_pattern_subscriptions() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Pattern Subscriptions (Wildcards)");

    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to patterns
    subscriber.psubscribe(&["events:*", "logs:error:*"]).await?;
    println!("   Subscribed to patterns: events:*, logs:error:*");

    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Publish to channels matching patterns
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = publisher
            .execute(Publish::new("events:login", "user123"))
            .await;
        let _ = publisher
            .execute(Publish::new("events:logout", "user456"))
            .await;
        let _ = publisher
            .execute(Publish::new("logs:error:db", "Connection failed"))
            .await;
    });

    // Receive pattern-matched messages
    let mut msg_count = 0;
    while let Ok(Some(msg)) = timeout(Duration::from_secs(1), subscriber.next_message()).await {
        match msg? {
            PubSubMessage::PSubscribe { pattern, count } => {
                println!(
                    "   ✓ Subscribed to pattern '{}' (total: {})",
                    pattern, count
                );
            }
            PubSubMessage::PMessage {
                pattern,
                channel,
                payload,
            } => {
                println!(
                    "   📨 Pattern '{}' matched '{}': {}",
                    pattern,
                    channel,
                    String::from_utf8_lossy(&payload)
                );
                msg_count += 1;
                if msg_count >= 3 {
                    break;
                }
            }
            _ => {}
        }
    }

    println!();
    Ok(())
}

async fn example_chat_system() -> Result<(), Box<dyn std::error::Error>> {
    println!("4. Chat System Simulation");

    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to chat rooms
    subscriber
        .subscribe(&["chat:general", "chat:rust", "chat:announcements"])
        .await?;

    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Simulate chat messages
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = publisher
            .execute(Publish::new("chat:general", "alice: Hello everyone!"))
            .await;
        let _ = publisher
            .execute(Publish::new("chat:rust", "bob: Check out this new crate"))
            .await;
        let _ = publisher
            .execute(Publish::new(
                "chat:announcements",
                "Server maintenance in 1 hour",
            ))
            .await;
    });

    // Display chat messages
    let mut msg_count = 0;
    while let Ok(Some(msg)) = timeout(Duration::from_secs(1), subscriber.next_message()).await {
        match msg? {
            PubSubMessage::Subscribe { .. } => {}
            PubSubMessage::Message { channel, payload } => {
                let room = channel.strip_prefix("chat:").unwrap_or(&channel);
                println!("   [{}] {}", room, String::from_utf8_lossy(&payload));
                msg_count += 1;
                if msg_count >= 3 {
                    break;
                }
            }
            _ => {}
        }
    }

    println!();
    Ok(())
}

async fn example_event_bus() -> Result<(), Box<dyn std::error::Error>> {
    println!("5. Event Bus Pattern");

    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to all events with a pattern
    subscriber
        .psubscribe(&["event:user:*", "event:order:*"])
        .await?;

    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Publish different event types
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = publisher
            .execute(Publish::new(
                "event:user:registered",
                r#"{"user_id": 123, "email": "alice@example.com"}"#,
            ))
            .await;
        let _ = publisher
            .execute(Publish::new(
                "event:order:created",
                r#"{"order_id": 456, "total": 99.99}"#,
            ))
            .await;
        let _ = publisher
            .execute(Publish::new(
                "event:user:login",
                r#"{"user_id": 123, "timestamp": 1234567890}"#,
            ))
            .await;
    });

    // Process events
    let mut msg_count = 0;
    while let Ok(Some(msg)) = timeout(Duration::from_secs(1), subscriber.next_message()).await {
        match msg? {
            PubSubMessage::PSubscribe { .. } => {}
            PubSubMessage::PMessage {
                pattern: _,
                channel,
                payload,
            } => {
                let event_type = channel.strip_prefix("event:").unwrap_or(&channel);
                println!(
                    "   🎯 Event: {} - {}",
                    event_type,
                    String::from_utf8_lossy(&payload)
                );
                msg_count += 1;
                if msg_count >= 3 {
                    break;
                }
            }
            _ => {}
        }
    }

    println!();
    Ok(())
}

async fn example_notifications() -> Result<(), Box<dyn std::error::Error>> {
    println!("6. Notification System");

    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to user-specific notifications
    subscriber
        .subscribe(&["notifications:user:123", "notifications:broadcast"])
        .await?;

    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Send notifications
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = publisher
            .execute(Publish::new(
                "notifications:user:123",
                "You have a new message",
            ))
            .await;
        let _ = publisher
            .execute(Publish::new(
                "notifications:broadcast",
                "System update available",
            ))
            .await;
    });

    // Receive notifications
    let mut msg_count = 0;
    while let Ok(Some(msg)) = timeout(Duration::from_secs(1), subscriber.next_message()).await {
        match msg? {
            PubSubMessage::Subscribe { .. } => {}
            PubSubMessage::Message { channel, payload } => {
                let notification_type = if channel.contains("broadcast") {
                    "🌐 Broadcast"
                } else {
                    "👤 Personal"
                };
                println!(
                    "   {} notification: {}",
                    notification_type,
                    String::from_utf8_lossy(&payload)
                );
                msg_count += 1;
                if msg_count >= 2 {
                    break;
                }
            }
            _ => {}
        }
    }

    println!();
    Ok(())
}

async fn example_unsubscribe() -> Result<(), Box<dyn std::error::Error>> {
    println!("7. Dynamic Unsubscribe");

    let mut subscriber = PubSubConnection::connect("127.0.0.1:6379").await?;

    // Subscribe to multiple channels
    subscriber
        .subscribe(&["channel1", "channel2", "channel3"])
        .await?;

    // Wait for subscription confirmations
    for _ in 0..3 {
        if let Ok(Some(msg)) = timeout(Duration::from_millis(100), subscriber.next_message()).await
        {
            if let Ok(PubSubMessage::Subscribe { channel, count }) = msg {
                println!("   ✓ Subscribed to '{}' (total: {})", channel, count);
            }
        }
    }

    // Unsubscribe from one channel
    subscriber.unsubscribe(&["channel2"]).await?;
    println!("   ✗ Unsubscribed from 'channel2'");

    // Wait for unsubscribe confirmation
    if let Ok(Some(msg)) = timeout(Duration::from_millis(100), subscriber.next_message()).await {
        if let Ok(PubSubMessage::Unsubscribe { channel, count }) = msg {
            println!(
                "   ✓ Unsubscribed from '{}' (remaining: {})",
                channel, count
            );
        }
    }

    println!(
        "   Active subscriptions: {} channels, {} patterns",
        subscriber.channel_count(),
        subscriber.pattern_count()
    );

    println!();
    Ok(())
}

async fn example_message_counting() -> Result<(), Box<dyn std::error::Error>> {
    println!("8. Message Counting and Stats");

    let publisher = RedisConnection::connect("127.0.0.1:6379").await?;

    // Create multiple subscribers
    let mut sub1 = PubSubConnection::connect("127.0.0.1:6379").await?;
    let mut sub2 = PubSubConnection::connect("127.0.0.1:6379").await?;

    sub1.subscribe(&["popular"]).await?;
    sub2.subscribe(&["popular"]).await?;

    // Wait for subscriptions
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish and see how many subscribers received it
    let subscriber_count = publisher
        .execute(Publish::new("popular", "Hot news!"))
        .await?;

    println!(
        "   Message delivered to {} active subscribers",
        subscriber_count
    );

    // Clean up - receive the messages
    let _ = timeout(Duration::from_millis(100), sub1.next_message()).await;
    let _ = timeout(Duration::from_millis(100), sub2.next_message()).await;

    println!();
    Ok(())
}
