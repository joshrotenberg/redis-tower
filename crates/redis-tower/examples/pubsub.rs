//! Pub/Sub: subscribe to a channel and publish messages from another connection.

use redis_tower::commands::Publish;
use redis_tower::{PubSubConnection, RedisConnection};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Subscriber connection -- enters pub/sub mode.
    let sub_conn = RedisConnection::connect("127.0.0.1:6379").await?;
    let mut pubsub = PubSubConnection::from_connection(sub_conn)?;
    pubsub.subscribe(&["example:events"]).await?;

    // Publisher connection -- sends messages on the same channel.
    let mut pub_conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // Spawn a task that publishes three messages then stops.
    tokio::spawn(async move {
        for i in 1..=3 {
            let msg = format!("event-{i}");
            pub_conn
                .execute(Publish::new("example:events", &msg))
                .await
                .unwrap();
        }
    });

    // Receive the three messages.
    let mut count = 0;
    while let Some(msg) = pubsub.next().await {
        let msg = msg?;
        let payload = String::from_utf8_lossy(&msg.payload);
        println!("[{}] {payload}", msg.channel);
        count += 1;
        if count >= 3 {
            break;
        }
    }

    Ok(())
}
