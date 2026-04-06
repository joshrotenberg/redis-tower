//! Redis Streams: add entries, read them back, check length.

use redis_tower::RedisConnection;
use redis_tower::commands::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
    let stream_key = "example:stream";

    // Add a few entries to the stream.
    for i in 1..=3 {
        let id = conn
            .execute(
                XAdd::new(stream_key)
                    .field("sensor", format!("s{i}"))
                    .field("temp", format!("{}", 20 + i)),
            )
            .await?;
        println!("Added entry: {id}");
    }

    // Check the stream length.
    let len: i64 = conn.execute(XLen::new(stream_key)).await?;
    println!("Stream length: {len}");

    // Read all entries from the beginning.
    let entries = conn
        .execute(XRead::new(stream_key, "0-0").count(10))
        .await?;

    if let Some(streams) = entries {
        for (name, entries) in &streams {
            for entry in entries {
                println!("[{name}] {}: {:?}", entry.id, entry.fields);
            }
        }
    }

    // Clean up the stream.
    conn.execute(Del::new(stream_key)).await?;

    Ok(())
}
