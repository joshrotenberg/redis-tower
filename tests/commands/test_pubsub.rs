//! Integration tests for Redis Pub/Sub.
//!
//! These tests require a running Redis instance on localhost:6379.
//! Run with: cargo test --test test_pubsub

mod common;

use common::connect;
use redis_tower::commands::Publish;
use redis_tower::pubsub::{PubSubConnection, PubSubMessage};
use std::time::Duration;
use tokio::time::timeout;

const REDIS_ADDR: &str = "127.0.0.1:6379";

#[tokio::test]
async fn test_pubsub_basic_subscribe_publish() {
    // Create subscriber connection
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect subscriber");

    // Subscribe to channel
    subscriber
        .subscribe(&["test:pubsub:basic"])
        .await
        .expect("Failed to subscribe");

    // Wait for subscription confirmation
    match timeout(Duration::from_secs(2), subscriber.next_message()).await {
        Ok(Some(Ok(PubSubMessage::Subscribe { channel, count }))) => {
            assert_eq!(channel, "test:pubsub:basic");
            assert_eq!(count, 1);
            println!("✓ Subscribed to channel: {}", channel);
        }
        _ => panic!("Expected Subscribe message"),
    }

    // Create publisher connection (separate from subscriber)
    let publisher = connect().await.expect("Failed to connect publisher");

    // Publish a message
    let subscribers = publisher
        .execute(Publish::new(
            "test:pubsub:basic",
            b"Hello, Pub/Sub!".to_vec(),
        ))
        .await
        .expect("Failed to publish");

    println!("✓ Published to {} subscribers", subscribers);
    assert_eq!(subscribers, 1);

    // Receive the message
    match timeout(Duration::from_secs(2), subscriber.next_message()).await {
        Ok(Some(Ok(PubSubMessage::Message { channel, payload }))) => {
            assert_eq!(channel, "test:pubsub:basic");
            assert_eq!(payload, bytes::Bytes::from("Hello, Pub/Sub!"));
            println!(
                "✓ Received message: {:?}",
                String::from_utf8_lossy(&payload)
            );
        }
        _ => panic!("Expected Message"),
    }

    // Unsubscribe
    subscriber
        .unsubscribe(&["test:pubsub:basic"])
        .await
        .expect("Failed to unsubscribe");

    match timeout(Duration::from_secs(2), subscriber.next_message()).await {
        Ok(Some(Ok(PubSubMessage::Unsubscribe { channel, count }))) => {
            assert_eq!(channel, "test:pubsub:basic");
            assert_eq!(count, 0);
            println!("✓ Unsubscribed from channel");
        }
        _ => panic!("Expected Unsubscribe message"),
    }
}

#[tokio::test]
async fn test_pubsub_multiple_channels() {
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect");

    // Subscribe to multiple channels at once
    subscriber
        .subscribe(&["test:multi:1", "test:multi:2", "test:multi:3"])
        .await
        .expect("Failed to subscribe");

    // Receive 3 subscription confirmations
    for i in 1..=3 {
        match timeout(Duration::from_secs(2), subscriber.next_message()).await {
            Ok(Some(Ok(PubSubMessage::Subscribe { channel, count }))) => {
                assert!(channel.starts_with("test:multi:"));
                assert_eq!(count, i);
                println!("✓ Subscribed to: {}", channel);
            }
            _ => panic!("Expected Subscribe message {}", i),
        }
    }

    let publisher = connect().await.expect("Failed to connect");

    // Publish to each channel
    for i in 1..=3 {
        let channel = format!("test:multi:{}", i);
        let message = format!("Message {}", i);
        publisher
            .execute(Publish::new(&channel, message.as_bytes().to_vec()))
            .await
            .expect("Failed to publish");
    }

    // Receive messages from all channels
    for _ in 1..=3 {
        match timeout(Duration::from_secs(2), subscriber.next_message()).await {
            Ok(Some(Ok(PubSubMessage::Message { channel, payload }))) => {
                println!(
                    "✓ Received on {}: {:?}",
                    channel,
                    String::from_utf8_lossy(&payload)
                );
            }
            _ => panic!("Expected Message"),
        }
    }

    // Cleanup: unsubscribe from all
    subscriber
        .unsubscribe(&["test:multi:1", "test:multi:2", "test:multi:3"])
        .await
        .ok();
}

#[tokio::test]
async fn test_pubsub_pattern_subscribe() {
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect");

    // Subscribe to pattern
    subscriber
        .psubscribe(&["test:pattern:*"])
        .await
        .expect("Failed to psubscribe");

    // Wait for subscription confirmation
    match timeout(Duration::from_secs(2), subscriber.next_message()).await {
        Ok(Some(Ok(PubSubMessage::PSubscribe { pattern, count }))) => {
            assert_eq!(pattern, "test:pattern:*");
            assert_eq!(count, 1);
            println!("✓ Pattern subscribed: {}", pattern);
        }
        _ => panic!("Expected PSubscribe message"),
    }

    let publisher = connect().await.expect("Failed to connect");

    // Publish to channels matching the pattern
    publisher
        .execute(Publish::new("test:pattern:foo", b"Foo message".to_vec()))
        .await
        .expect("Failed to publish");

    publisher
        .execute(Publish::new("test:pattern:bar", b"Bar message".to_vec()))
        .await
        .expect("Failed to publish");

    // Receive messages via pattern
    for _ in 0..2 {
        match timeout(Duration::from_secs(2), subscriber.next_message()).await {
            Ok(Some(Ok(PubSubMessage::PMessage {
                pattern,
                channel,
                payload,
            }))) => {
                assert_eq!(pattern, "test:pattern:*");
                assert!(channel.starts_with("test:pattern:"));
                println!(
                    "✓ Pattern match: {} on {} = {:?}",
                    pattern,
                    channel,
                    String::from_utf8_lossy(&payload)
                );
            }
            _ => panic!("Expected PMessage"),
        }
    }

    // Unsubscribe from pattern
    subscriber
        .punsubscribe(&["test:pattern:*"])
        .await
        .expect("Failed to punsubscribe");

    match timeout(Duration::from_secs(2), subscriber.next_message()).await {
        Ok(Some(Ok(PubSubMessage::PUnsubscribe { pattern, count }))) => {
            assert_eq!(pattern, "test:pattern:*");
            assert_eq!(count, 0);
            println!("✓ Pattern unsubscribed");
        }
        _ => panic!("Expected PUnsubscribe message"),
    }
}

#[tokio::test]
async fn test_pubsub_no_subscribers() {
    let publisher = connect().await.expect("Failed to connect");

    // Publish to a channel with no subscribers
    let count = publisher
        .execute(Publish::new("test:empty", b"Nobody listening".to_vec()))
        .await
        .expect("Failed to publish");

    assert_eq!(count, 0, "Should return 0 when no subscribers");
    println!("✓ Correctly reported 0 subscribers");
}

#[tokio::test]
async fn test_pubsub_subscribe_then_publish() {
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect");

    subscriber
        .subscribe(&["test:order"])
        .await
        .expect("Failed to subscribe");

    // Clear subscription confirmation
    timeout(Duration::from_secs(2), subscriber.next_message())
        .await
        .ok();

    let publisher = connect().await.expect("Failed to connect");

    // Publish multiple messages in order
    for i in 1..=5 {
        let message = format!("Message {}", i);
        publisher
            .execute(Publish::new("test:order", message.as_bytes().to_vec()))
            .await
            .expect("Failed to publish");
    }

    // Verify messages arrive in order
    for i in 1..=5 {
        match timeout(Duration::from_secs(2), subscriber.next_message()).await {
            Ok(Some(Ok(PubSubMessage::Message { channel, payload }))) => {
                let expected = format!("Message {}", i);
                assert_eq!(channel, "test:order");
                assert_eq!(payload, bytes::Bytes::from(expected.clone()));
                println!("✓ Received in order: {}", expected);
            }
            _ => panic!("Expected Message {}", i),
        }
    }

    subscriber.unsubscribe(&["test:order"]).await.ok();
}

#[tokio::test]
async fn test_pubsub_binary_data() {
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect");

    subscriber
        .subscribe(&["test:binary"])
        .await
        .expect("Failed to subscribe");

    // Clear subscription confirmation
    timeout(Duration::from_secs(2), subscriber.next_message())
        .await
        .ok();

    let publisher = connect().await.expect("Failed to connect");

    // Publish binary data
    let binary_data: Vec<u8> = vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x42];
    publisher
        .execute(Publish::new("test:binary", binary_data.clone()))
        .await
        .expect("Failed to publish");

    // Verify binary data is received correctly
    match timeout(Duration::from_secs(2), subscriber.next_message()).await {
        Ok(Some(Ok(PubSubMessage::Message { channel, payload }))) => {
            assert_eq!(channel, "test:binary");
            assert_eq!(payload.as_ref(), binary_data.as_slice());
            println!("✓ Binary data received correctly: {:02X?}", payload);
        }
        _ => panic!("Expected Message with binary data"),
    }

    subscriber.unsubscribe(&["test:binary"]).await.ok();
}

#[tokio::test]
async fn test_pubsub_mixed_subscribe_psubscribe() {
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect");

    // Subscribe to both exact channel and pattern
    subscriber
        .subscribe(&["test:mixed:exact"])
        .await
        .expect("Failed to subscribe");
    subscriber
        .psubscribe(&["test:mixed:*"])
        .await
        .expect("Failed to psubscribe");

    // Clear subscription confirmations
    timeout(Duration::from_secs(2), subscriber.next_message())
        .await
        .ok();
    timeout(Duration::from_secs(2), subscriber.next_message())
        .await
        .ok();

    let publisher = connect().await.expect("Failed to connect");

    // Publish to the exact channel (will match both subscription and pattern)
    publisher
        .execute(Publish::new("test:mixed:exact", b"Dual match".to_vec()))
        .await
        .expect("Failed to publish");

    // Should receive 2 messages: one from exact subscribe, one from pattern
    let mut received_exact = false;
    let mut received_pattern = false;

    for _ in 0..2 {
        match timeout(Duration::from_secs(2), subscriber.next_message()).await {
            Ok(Some(Ok(PubSubMessage::Message { channel, .. }))) => {
                assert_eq!(channel, "test:mixed:exact");
                received_exact = true;
                println!("✓ Received via exact subscription");
            }
            Ok(Some(Ok(PubSubMessage::PMessage {
                pattern, channel, ..
            }))) => {
                assert_eq!(pattern, "test:mixed:*");
                assert_eq!(channel, "test:mixed:exact");
                received_pattern = true;
                println!("✓ Received via pattern subscription");
            }
            _ => panic!("Unexpected message type"),
        }
    }

    assert!(received_exact, "Should receive via exact subscription");
    assert!(received_pattern, "Should receive via pattern subscription");

    subscriber.unsubscribe(&["test:mixed:exact"]).await.ok();
    subscriber.punsubscribe(&["test:mixed:*"]).await.ok();
}

#[tokio::test]
async fn test_pubsub_timeout_no_message() {
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect");

    subscriber
        .subscribe(&["test:timeout"])
        .await
        .expect("Failed to subscribe");

    // Clear subscription confirmation
    timeout(Duration::from_secs(2), subscriber.next_message())
        .await
        .ok();

    // Try to receive message with timeout (should timeout)
    let result = timeout(Duration::from_millis(500), subscriber.next_message()).await;

    assert!(result.is_err(), "Should timeout when no message published");
    println!("✓ Correctly timed out waiting for message");

    subscriber.unsubscribe(&["test:timeout"]).await.ok();
}

#[tokio::test]
async fn test_pubsub_channels() {
    use redis_tower::commands::pubsub::PubsubChannels;

    let client = connect().await.expect("Failed to connect");

    // Create a subscriber to make a channel active
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect subscriber");

    subscriber
        .subscribe(&["test:channels:1", "test:channels:2", "news:updates"])
        .await
        .expect("Failed to subscribe");

    // Consume subscription confirmations
    for _ in 0..3 {
        timeout(Duration::from_secs(1), subscriber.next_message())
            .await
            .ok();
    }

    // Get all active channels
    let all_channels = client
        .execute(PubsubChannels::all())
        .await
        .expect("Failed to get channels");

    assert!(all_channels.contains(&"test:channels:1".to_string()));
    assert!(all_channels.contains(&"test:channels:2".to_string()));
    println!("✓ Found {} active channels", all_channels.len());

    // Get channels matching pattern
    let test_channels = client
        .execute(PubsubChannels::pattern("test:channels:*"))
        .await
        .expect("Failed to get channels with pattern");

    assert!(test_channels.contains(&"test:channels:1".to_string()));
    assert!(test_channels.contains(&"test:channels:2".to_string()));
    println!("✓ Found {} channels matching pattern", test_channels.len());

    subscriber
        .unsubscribe(&["test:channels:1", "test:channels:2", "news:updates"])
        .await
        .ok();
}

#[tokio::test]
async fn test_pubsub_numsub() {
    use redis_tower::commands::pubsub::PubsubNumsub;

    let client = connect().await.expect("Failed to connect");

    // Create subscribers
    let mut sub1 = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect subscriber 1");
    let mut sub2 = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect subscriber 2");

    // Subscribe to channels
    sub1.subscribe(&["test:numsub:channel1"])
        .await
        .expect("Failed to subscribe");
    sub2.subscribe(&["test:numsub:channel1", "test:numsub:channel2"])
        .await
        .expect("Failed to subscribe");

    // Consume subscription confirmations
    timeout(Duration::from_secs(1), sub1.next_message())
        .await
        .ok();
    timeout(Duration::from_secs(1), sub2.next_message())
        .await
        .ok();
    timeout(Duration::from_secs(1), sub2.next_message())
        .await
        .ok();

    // Get subscriber counts
    let counts = client
        .execute(PubsubNumsub::new(&[
            "test:numsub:channel1",
            "test:numsub:channel2",
        ]))
        .await
        .expect("Failed to get subscriber counts");

    assert_eq!(counts.len(), 2);

    // Channel1 should have 2 subscribers
    let channel1_count = counts
        .iter()
        .find(|(ch, _)| ch == "test:numsub:channel1")
        .map(|(_, count)| *count)
        .unwrap_or(0);
    assert_eq!(channel1_count, 2);

    // Channel2 should have 1 subscriber
    let channel2_count = counts
        .iter()
        .find(|(ch, _)| ch == "test:numsub:channel2")
        .map(|(_, count)| *count)
        .unwrap_or(0);
    assert_eq!(channel2_count, 1);

    println!(
        "✓ Subscriber counts: channel1={}, channel2={}",
        channel1_count, channel2_count
    );

    sub1.unsubscribe(&["test:numsub:channel1"]).await.ok();
    sub2.unsubscribe(&["test:numsub:channel1", "test:numsub:channel2"])
        .await
        .ok();
}

#[tokio::test]
async fn test_pubsub_numpat() {
    use redis_tower::commands::pubsub::PubsubNumpat;

    let client = connect().await.expect("Failed to connect");

    // Create subscriber with pattern subscriptions
    let mut subscriber = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect subscriber");

    subscriber
        .psubscribe(&["test:numpat:*", "news:*"])
        .await
        .expect("Failed to psubscribe");

    // Consume subscription confirmations
    timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .ok();
    timeout(Duration::from_secs(1), subscriber.next_message())
        .await
        .ok();

    // Get pattern subscription count
    let count = client
        .execute(PubsubNumpat::new())
        .await
        .expect("Failed to get numpat");

    assert!(
        count >= 2,
        "Expected at least 2 pattern subscriptions, got {}",
        count
    );
    println!("✓ Active pattern subscriptions: {}", count);

    subscriber
        .punsubscribe(&["test:numpat:*", "news:*"])
        .await
        .ok();
}

#[tokio::test]
async fn test_publish_to_multiple_subscribers() {
    use redis_tower::commands::Publish;

    let client = connect().await.expect("Failed to connect");

    // Create multiple subscribers
    let mut sub1 = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect sub1");
    let mut sub2 = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect sub2");
    let mut sub3 = PubSubConnection::connect(REDIS_ADDR)
        .await
        .expect("Failed to connect sub3");

    let channel = "test:multi:subscribers";

    sub1.subscribe(&[channel])
        .await
        .expect("Failed to subscribe sub1");
    sub2.subscribe(&[channel])
        .await
        .expect("Failed to subscribe sub2");
    sub3.subscribe(&[channel])
        .await
        .expect("Failed to subscribe sub3");

    // Consume subscription confirmations
    for sub in [&mut sub1, &mut sub2, &mut sub3] {
        timeout(Duration::from_secs(1), sub.next_message())
            .await
            .ok();
    }

    // Publish message
    let subscriber_count = client
        .execute(Publish::new(channel, b"broadcast message".to_vec()))
        .await
        .expect("Failed to publish");

    assert_eq!(
        subscriber_count, 3,
        "Expected message delivered to 3 subscribers"
    );
    println!("✓ Message delivered to {} subscribers", subscriber_count);

    // Verify all subscribers received the message
    for (i, sub) in [&mut sub1, &mut sub2, &mut sub3].iter_mut().enumerate() {
        match timeout(Duration::from_secs(1), sub.next_message()).await {
            Ok(Some(Ok(PubSubMessage::Message {
                channel: ch,
                payload,
            }))) => {
                assert_eq!(ch, channel);
                assert_eq!(payload, b"broadcast message");
                println!("✓ Subscriber {} received message", i + 1);
            }
            _ => panic!("Subscriber {} did not receive message", i + 1),
        }
    }

    sub1.unsubscribe(&[channel]).await.ok();
    sub2.unsubscribe(&[channel]).await.ok();
    sub3.unsubscribe(&[channel]).await.ok();
}

#[tokio::test]
async fn test_pubsub_empty_responses() {
    use redis_tower::commands::pubsub::{PubsubChannels, PubsubNumsub};

    let client = connect().await.expect("Failed to connect");

    // Query non-existent channels
    let channels = client
        .execute(PubsubChannels::pattern("nonexistent:*"))
        .await
        .expect("Failed to get channels");

    assert_eq!(channels.len(), 0);
    println!("✓ Empty channel list for non-existent pattern");

    // Query non-existent channel counts
    let counts = client
        .execute(PubsubNumsub::new(&["nonexistent:channel"]))
        .await
        .expect("Failed to get counts");

    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].1, 0);
    println!("✓ Zero subscriber count for non-existent channel");
}
