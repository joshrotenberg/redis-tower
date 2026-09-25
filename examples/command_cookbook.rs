//! Compile harness for every Rust workflow shown in `docs/COMMAND-COOKBOOK.md`.
//!
//! The functions intentionally are not called: each requires an external
//! Redis topology or changes server state. CI checks this example so the
//! mdBook can keep the corresponding snippets readable without pretending a
//! documentation test has a Redis fixture.

#![allow(dead_code)]

use std::error::Error;

use bytes::Bytes;
use redis_tower::commands::{
    Get, HSet, Incr, RawCommand, Scan, Set, SetOutcome, SetPreviousValue, StreamEntry, XAck,
    XGroupCreate, XReadGroup,
};
use redis_tower::{
    BinaryPubSubConnection, MultiplexedClient, Pipeline, RedisConnection, Transaction,
    TransactionResult,
};
use redis_tower_client::UniversalClient;
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_sentinel::MultiplexedSentinelClient;
use tokio_stream::StreamExt;

async fn typed_responses() -> Result<(), Box<dyn Error>> {
    let client = MultiplexedClient::connect("127.0.0.1:6379").await?;

    let missing_or_value: Option<Bytes> = client.execute(Get::new("profile:1")).await?;
    let next: i64 = client.execute(Incr::new("visits")).await?;
    let outcome: SetOutcome = client
        .execute(Set::new("profile:1", "ready").nx().get().with_outcome())
        .await?;
    match &outcome.previous {
        SetPreviousValue::NotRequested => unreachable!("GET was requested"),
        SetPreviousValue::Missing => println!("the key did not exist"),
        SetPreviousValue::Value(value) => println!("previous bytes: {value:?}"),
    }
    let _ = (missing_or_value, next, outcome);
    Ok(())
}

async fn binary_values() -> Result<(), Box<dyn Error>> {
    let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
    let key = b"user:\xff".as_slice();

    client
        .execute(Set::new(key, b"\x00\xfe\xff".as_slice()))
        .await?;
    client
        .execute(HSet::new(key, b"field".as_slice(), b"value\xff".as_slice()))
        .await?;
    let value: Option<Bytes> = client.execute(Get::new(key)).await?;
    let _ = value;
    Ok(())
}

async fn cursor_iteration() -> Result<(), Box<dyn Error>> {
    let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
    let mut cursor = "0".to_string();
    loop {
        let page = client
            .execute(
                Scan::new()
                    .cursor(cursor)
                    .match_pattern("user:*")
                    .count(100),
            )
            .await?;
        let finished = page.is_finished();
        let next_cursor = page.cursor.clone();
        for key in page.results {
            println!("{key:?}");
        }
        if finished {
            break;
        }
        cursor = next_cursor;
    }
    Ok(())
}

async fn pipeline_and_transaction() -> Result<(), Box<dyn Error>> {
    let mut connection = RedisConnection::connect("127.0.0.1:6379").await?;

    let pipeline = Pipeline::new()
        .push(Set::new("{account:1}:name", "Ada"))
        .push(Get::new("{account:1}:name"))
        .execute(&mut connection)
        .await?;
    let name: &Option<Bytes> = pipeline.get(1)?;

    let transaction = Transaction::new()
        .watch(["{account:1}:counter"])
        .push(Incr::new("{account:1}:counter"))
        .execute(&mut connection)
        .await?;
    match transaction {
        TransactionResult::Committed(mut replies) => {
            let counter: i64 = replies.take(0)?;
            println!("{counter}");
        }
        TransactionResult::Aborted => {}
    }
    let _ = name;
    Ok(())
}

async fn process(_entry: &StreamEntry) -> Result<(), Box<dyn Error>> {
    Ok(())
}

async fn streams() -> Result<(), Box<dyn Error>> {
    let mut connection = RedisConnection::connect("127.0.0.1:6379").await?;
    connection
        .execute(XGroupCreate::new("events", "workers", "0").mkstream())
        .await?;

    let streams = connection
        .execute(
            XReadGroup::new("workers", "worker-1", "events")
                .count(10)
                .block(1_000),
        )
        .await?;
    for (_stream, entries) in streams {
        for entry in entries {
            process(&entry).await?;
            connection
                .execute(XAck::new("events", "workers", entry.id))
                .await?;
        }
    }
    Ok(())
}

async fn raw_replies() -> Result<(), Box<dyn Error>> {
    let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
    let count: i64 = client
        .execute(RawCommand::new("SCARD").arg("members").query())
        .await?;
    let members: Vec<Bytes> = client
        .execute(RawCommand::new("SMEMBERS").arg("members").query())
        .await?;
    let _ = (count, members);
    Ok(())
}

async fn binary_pubsub() -> Result<(), Box<dyn Error>> {
    let connection = RedisConnection::connect("127.0.0.1:6379").await?;
    let mut subscriber = BinaryPubSubConnection::from_connection(connection)?;
    subscriber
        .subscribe_bytes(&[b"events\xff".as_slice()])
        .await?;

    if let Some(message) = subscriber.next().await {
        let message = message?;
        println!("{:?}: {:?}", message.channel, message.payload);
    }
    Ok(())
}

async fn topology_entry_points() -> Result<(), Box<dyn Error>> {
    let standalone = MultiplexedClient::connect_url("redis://127.0.0.1:6379").await?;
    let cluster = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
    let sentinel = MultiplexedSentinelClient::connect(&["127.0.0.1:26379"], "mymaster").await?;
    let universal = UniversalClient::connect_url("redis://127.0.0.1:6379").await?;
    let _ = (standalone, cluster, sentinel, universal);
    Ok(())
}

fn main() {
    println!("See docs/COMMAND-COOKBOOK.md; this target compiles its Redis-dependent snippets.");
}
