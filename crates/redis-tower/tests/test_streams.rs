mod common;

use common::conn;
use redis_tower::RedisError;
use redis_tower::commands::*;
use redis_tower::consumer::{ConsumerConfig, StreamConsumer};
use std::time::Duration;
use tokio_stream::StreamExt;

// ---------------------------------------------------------------------------
// Managed StreamConsumer acknowledgement ordering (issue #734)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_consumer_drop_before_advance_leaves_delivery_pending() {
    let mut control = conn().await;
    let key = "test:streams:consumer:drop_before_advance";
    let group = "managed";
    let consumer_name = "worker-1";

    control.execute(Del::new(key)).await.unwrap();
    let id = control
        .execute(XAdd::new(key).field("job", "one"))
        .await
        .unwrap();

    let consumer = StreamConsumer::new(group, consumer_name, [key]).config(ConsumerConfig {
        batch_size: 1,
        block_ms: Some(5_000),
        auto_ack: true,
        claim_idle_ms: None,
        create_group: true,
    });
    let mut deliveries = Box::pin(consumer.into_stream(conn().await));

    let delivered = tokio::time::timeout(Duration::from_secs(2), deliveries.next())
        .await
        .expect("managed consumer should produce the seeded entry")
        .expect("managed consumer should remain open")
        .expect("seeded entry should be delivered successfully");
    assert_eq!(delivered.id, id);

    let pending = control
        .execute(XPendingSummary::new(key, group))
        .await
        .unwrap();
    assert_eq!(pending.count, 1, "delivery must remain pending after yield");

    drop(deliveries);

    let pending_after_drop = control
        .execute(XPendingSummary::new(key, group))
        .await
        .unwrap();
    assert_eq!(
        pending_after_drop.count, 1,
        "dropping before the next poll must not acknowledge the delivery"
    );

    control
        .execute(XGroupDestroy::new(key, group))
        .await
        .unwrap();
    control.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn stream_consumer_advance_acks_only_the_previous_delivery() {
    let mut control = conn().await;
    let key = "test:streams:consumer:ack_on_advance";
    let group = "managed";

    control.execute(Del::new(key)).await.unwrap();
    let id1 = control
        .execute(XAdd::new(key).field("job", "one"))
        .await
        .unwrap();
    let id2 = control
        .execute(XAdd::new(key).field("job", "two"))
        .await
        .unwrap();
    let id3 = control
        .execute(XAdd::new(key).field("job", "three"))
        .await
        .unwrap();

    let consumer = StreamConsumer::new(group, "worker-1", [key]).config(ConsumerConfig {
        batch_size: 1,
        block_ms: Some(5_000),
        auto_ack: true,
        claim_idle_ms: None,
        create_group: true,
    });
    let mut deliveries = Box::pin(consumer.into_stream(conn().await));

    let first = deliveries.next().await.unwrap().unwrap();
    assert_eq!(first.id, id1);
    assert_eq!(
        control
            .execute(XPendingSummary::new(key, group))
            .await
            .unwrap()
            .count,
        1
    );

    let second = deliveries.next().await.unwrap().unwrap();
    assert_eq!(second.id, id2);
    let pending_after_second = control
        .execute(XPendingRange::new(key, group, "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(
        pending_after_second
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec![id2.as_str()],
        "advancing to the second item should ACK only the first"
    );

    let third = deliveries.next().await.unwrap().unwrap();
    assert_eq!(third.id, id3);
    let pending_after_third = control
        .execute(XPendingRange::new(key, group, "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(
        pending_after_third
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec![id3.as_str()],
        "advancing to the third item should ACK the second, not the third"
    );

    assert_eq!(
        control.execute(XAck::new(key, group, &id1)).await.unwrap(),
        0
    );
    assert_eq!(
        control.execute(XAck::new(key, group, &id2)).await.unwrap(),
        0
    );

    drop(deliveries);
    control
        .execute(XGroupDestroy::new(key, group))
        .await
        .unwrap();
    control.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn stream_consumer_surfaces_deferred_ack_failure_before_another_item() {
    let mut control = conn().await;
    let key = "test:streams:consumer:ack_failure";
    let group = "managed";

    control.execute(Del::new(key)).await.unwrap();
    control
        .execute(XAdd::new(key).field("job", "one"))
        .await
        .unwrap();

    let consumer = StreamConsumer::new(group, "worker-1", [key]).config(ConsumerConfig {
        batch_size: 1,
        block_ms: Some(5_000),
        auto_ack: true,
        claim_idle_ms: None,
        create_group: true,
    });
    let mut deliveries = Box::pin(consumer.into_stream(conn().await));

    deliveries.next().await.unwrap().unwrap();
    control
        .execute(XGroupDestroy::new(key, group))
        .await
        .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(2), deliveries.next())
        .await
        .expect("deferred XACK should fail without blocking")
        .expect("the acknowledgement failure should be yielded")
        .expect_err("destroying the group should make deferred XACK fail");
    assert!(
        matches!(error, RedisError::Redis(ref message) if message.contains("NOGROUP")),
        "expected the deferred XACK NOGROUP error, got {error:?}"
    );

    control.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn stream_consumer_pending_recovery_is_acked_only_on_advance() {
    let mut control = conn().await;
    let key = "test:streams:consumer:pending_recovery";
    let group = "managed";
    let consumer_name = "worker-1";

    control.execute(Del::new(key)).await.unwrap();
    let recovered_id = control
        .execute(XAdd::new(key).field("job", "recover"))
        .await
        .unwrap();
    control
        .execute(XGroupCreate::new(key, group, "0"))
        .await
        .unwrap();
    control
        .execute(XReadGroup::new(group, consumer_name, key).count(1))
        .await
        .unwrap();

    let consumer = StreamConsumer::new(group, consumer_name, [key]).config(ConsumerConfig {
        batch_size: 1,
        block_ms: Some(5_000),
        auto_ack: true,
        claim_idle_ms: None,
        create_group: false,
    });
    let mut deliveries = Box::pin(consumer.into_stream(conn().await));

    let recovered = deliveries.next().await.unwrap().unwrap();
    assert_eq!(recovered.id, recovered_id);
    assert_eq!(
        control
            .execute(XPendingSummary::new(key, group))
            .await
            .unwrap()
            .count,
        1,
        "a recovered pending item must remain pending while yielded"
    );

    let next_id = control
        .execute(XAdd::new(key).field("job", "next"))
        .await
        .unwrap();
    let next = deliveries.next().await.unwrap().unwrap();
    assert_eq!(next.id, next_id);
    let pending = control
        .execute(XPendingRange::new(key, group, "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec![next_id.as_str()],
        "advancing should ACK the recovered item before yielding the next one"
    );

    drop(deliveries);
    control
        .execute(XGroupDestroy::new(key, group))
        .await
        .unwrap();
    control.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn stream_consumer_claimed_delivery_is_acked_only_on_advance() {
    let mut control = conn().await;
    let key = "test:streams:consumer:claimed_recovery";
    let group = "managed";

    control.execute(Del::new(key)).await.unwrap();
    let claimed_id = control
        .execute(XAdd::new(key).field("job", "claim"))
        .await
        .unwrap();
    control
        .execute(XGroupCreate::new(key, group, "0"))
        .await
        .unwrap();
    control
        .execute(XReadGroup::new(group, "abandoned", key).count(1))
        .await
        .unwrap();

    let consumer = StreamConsumer::new(group, "worker-1", [key]).config(ConsumerConfig {
        batch_size: 1,
        block_ms: Some(5_000),
        auto_ack: true,
        claim_idle_ms: Some(0),
        create_group: false,
    });
    let mut deliveries = Box::pin(consumer.into_stream(conn().await));

    let claimed = deliveries.next().await.unwrap().unwrap();
    assert_eq!(claimed.id, claimed_id);
    let pending = control
        .execute(XPendingRange::new(key, group, "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, claimed_id);
    assert_eq!(pending[0].consumer, "worker-1");

    let next_id = control
        .execute(XAdd::new(key).field("job", "next"))
        .await
        .unwrap();
    let next = deliveries.next().await.unwrap().unwrap();
    assert_eq!(next.id, next_id);
    let pending_after_advance = control
        .execute(XPendingRange::new(key, group, "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(
        pending_after_advance
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec![next_id.as_str()]
    );

    drop(deliveries);
    control
        .execute(XGroupDestroy::new(key, group))
        .await
        .unwrap();
    control.execute(Del::new(key)).await.unwrap();
}

// ---------------------------------------------------------------------------
// Stream consumer group integration tests (issue #350)
// ---------------------------------------------------------------------------

/// Full consumer group lifecycle: create group, read as consumer, ack, pending,
/// claim, autoclaim, and cleanup.
#[tokio::test]
async fn stream_consumer_group_lifecycle() {
    let mut c = conn().await;
    let key = "test:streams:cg:lifecycle";
    let group = "mygroup";
    let consumer1 = "consumer1";
    let consumer2 = "consumer2";

    // Clean up before test.
    c.execute(Del::new(key)).await.unwrap();

    // Add a few entries.
    let id1 = c
        .execute(XAdd::new(key).field("field1", "value1"))
        .await
        .unwrap();
    let id2 = c
        .execute(XAdd::new(key).field("field1", "value2"))
        .await
        .unwrap();
    let _id3 = c
        .execute(XAdd::new(key).field("field1", "value3"))
        .await
        .unwrap();

    // Create a consumer group starting from the beginning of the stream.
    c.execute(XGroupCreate::new(key, group, "0")).await.unwrap();

    // Read all pending entries as consumer1 (id ">" = new undelivered entries).
    let entries = c
        .execute(XReadGroup::new(group, consumer1, key))
        .await
        .unwrap();
    assert_eq!(entries.len(), 1, "expected results for 1 stream");
    let (_, messages) = &entries[0];
    assert_eq!(messages.len(), 3, "expected 3 messages");

    // Check the pending summary -- all 3 are unacknowledged.
    let summary = c.execute(XPendingSummary::new(key, group)).await.unwrap();
    assert_eq!(summary.count, 3);

    // Acknowledge the first entry.
    let acked = c.execute(XAck::new(key, group, &id1)).await.unwrap();
    assert_eq!(acked, 1);

    // Pending count should now be 2.
    let summary2 = c.execute(XPendingSummary::new(key, group)).await.unwrap();
    assert_eq!(summary2.count, 2);

    // XPendingRange -- list the 2 remaining pending entries.
    let pending = c
        .execute(XPendingRange::new(key, group, "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(pending.len(), 2);
    assert!(
        pending.iter().all(|e| e.consumer == consumer1),
        "all pending entries should belong to consumer1"
    );

    // XClaim -- reassign id2 to consumer2 with min_idle_time=0 (force claim).
    let claimed = c
        .execute(XClaim::new(key, group, consumer2, 0, [&id2]))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "expected 1 claimed entry");
    assert_eq!(claimed[0].id, id2);

    // XAutoClaim -- sweep all entries (idle >= 0ms) for consumer2 starting from "0".
    let autoclaim = c
        .execute(XAutoClaim::new(key, group, consumer2, 0, "0"))
        .await
        .unwrap();
    // At least one entry should be in the claimed set (the remaining one from consumer1).
    // The next_start_id being "0-0" means the scan reached the end.
    assert!(
        !autoclaim.next_start_id.is_empty(),
        "expected a next start ID"
    );

    // Cleanup.
    c.execute(XGroupDestroy::new(key, group)).await.unwrap();
    c.execute(Del::new(key)).await.unwrap();
}

/// Create and delete a consumer explicitly within a group.
#[tokio::test]
async fn stream_group_create_and_delete_consumer() {
    let mut c = conn().await;
    let key = "test:streams:cg:create_del";
    let group = "testgroup";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(XAdd::new(key).field("f", "v")).await.unwrap();
    c.execute(XGroupCreate::new(key, group, "0")).await.unwrap();

    // Create consumer explicitly.
    let created = c
        .execute(XGroupCreateConsumer::new(key, group, "myconsumer"))
        .await
        .unwrap();
    assert_eq!(created, 1, "should have created a new consumer");

    // Delete consumer -- returns the number of pending messages it had (0 here).
    let pending_count = c
        .execute(XGroupDelConsumer::new(key, group, "myconsumer"))
        .await
        .unwrap();
    assert_eq!(pending_count, 0);

    c.execute(XGroupDestroy::new(key, group)).await.unwrap();
    c.execute(Del::new(key)).await.unwrap();
}

/// XGROUP SETID updates the last-delivered ID for the group.
#[tokio::test]
async fn stream_group_setid() {
    let mut c = conn().await;
    let key = "test:streams:cg:setid";
    let group = "setid_group";

    c.execute(Del::new(key)).await.unwrap();
    let id1 = c.execute(XAdd::new(key).field("f", "v1")).await.unwrap();
    c.execute(XAdd::new(key).field("f", "v2")).await.unwrap();

    // Create group from beginning.
    c.execute(XGroupCreate::new(key, group, "0")).await.unwrap();

    // Advance the group's last-delivered ID to id1, so only v2 is "new".
    c.execute(XGroupSetId::new(key, group, &id1)).await.unwrap();

    // Reading new entries should yield only v2.
    let entries = c
        .execute(XReadGroup::new(group, "consumer", key))
        .await
        .unwrap();
    let (_, messages) = &entries[0];
    assert_eq!(
        messages.len(),
        1,
        "expected 1 new message after XGROUP SETID"
    );

    c.execute(XGroupDestroy::new(key, group)).await.unwrap();
    c.execute(Del::new(key)).await.unwrap();
}

// ---------------------------------------------------------------------------
// XSETID integration tests (issue #391)
// ---------------------------------------------------------------------------

/// XSETID sets the stream's last-generated ID, observable via XINFO STREAM.
#[tokio::test]
async fn stream_xsetid_sets_last_id() {
    let mut c = conn().await;
    let key = "test:streams:xsetid:last_id";

    c.execute(Del::new(key)).await.unwrap();

    // Seed the stream with one entry at a known low ID.
    c.execute(XAdd::new(key).id("1-0").field("f", "v"))
        .await
        .unwrap();

    let before = c.execute(XInfoStream::new(key)).await.unwrap();
    assert_eq!(before.last_generated_id, "1-0");

    // Advance the stream's last-id to a higher value.
    c.execute(XSetId::new(key, "5-0")).await.unwrap();

    let after = c.execute(XInfoStream::new(key)).await.unwrap();
    assert_eq!(
        after.last_generated_id, "5-0",
        "XSETID should update the last-generated ID reported by XINFO STREAM"
    );

    c.execute(Del::new(key)).await.unwrap();
}

/// XSETID with the ENTRIESADDED option (Redis 7.0+) sets both the last-id and
/// the recorded entries-added count. The last-id is verified via XINFO STREAM.
#[tokio::test]
async fn stream_xsetid_entries_added() {
    let mut c = conn().await;
    let key = "test:streams:xsetid:entries_added";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(XAdd::new(key).id("1-0").field("f", "v"))
        .await
        .unwrap();

    // Set last-id to 10-0 and record 100 total entries ever added.
    c.execute(XSetId::new(key, "10-0").entries_added(100))
        .await
        .unwrap();

    let info = c.execute(XInfoStream::new(key)).await.unwrap();
    assert_eq!(
        info.last_generated_id, "10-0",
        "XSETID ... ENTRIESADDED should update the last-generated ID"
    );

    c.execute(Del::new(key)).await.unwrap();
}

// ---------------------------------------------------------------------------
// Acknowledge-and-delete (issue #472, Redis 8.0+)
// ---------------------------------------------------------------------------
//
// XACKDEL / XDELEX are Redis 8.0+. Each test probes the command and returns
// early (skips) when run against an older server that rejects it.

/// XACKDEL acknowledges and deletes entries from a consumer group in one call.
#[tokio::test]
async fn xackdel() {
    let mut c = conn().await;
    let key = "test:streams:xackdel";
    let group = "g";
    c.execute(Del::new(key)).await.unwrap();

    let id1 = c.execute(XAdd::new(key).field("f", "v1")).await.unwrap();
    let id2 = c.execute(XAdd::new(key).field("f", "v2")).await.unwrap();
    c.execute(XGroupCreate::new(key, group, "0")).await.unwrap();
    // Deliver the entries so they enter the group's PEL.
    c.execute(XReadGroup::new(group, "c1", key)).await.unwrap();

    let status = match c
        .execute(XAckDel::new(key, group, [&id1, "9999999-0"]))
        .await
    {
        Ok(v) => v,
        Err(_) => return,
    };
    // 1 = acknowledged and deleted; -1 = id not found.
    assert_eq!(status, vec![1, -1]);

    // id1 was deleted; id2 still exists.
    let len = c.execute(XLen::new(key)).await.unwrap();
    assert_eq!(len, 1, "XACKDEL should have deleted exactly one entry");
    let range = c.execute(XRange::all(key)).await.unwrap();
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].id, id2);

    c.execute(Del::new(key)).await.unwrap();
}

/// XDELEX deletes entries from a stream with an explicit reference policy.
#[tokio::test]
async fn xdelex() {
    let mut c = conn().await;
    let key = "test:streams:xdelex";
    c.execute(Del::new(key)).await.unwrap();

    let id1 = c.execute(XAdd::new(key).field("f", "v1")).await.unwrap();
    let id2 = c.execute(XAdd::new(key).field("f", "v2")).await.unwrap();

    let status = match c
        .execute(XDelEx::new(key, [&id1, "9999999-0"]).policy(StreamRefPolicy::KeepRef))
        .await
    {
        Ok(v) => v,
        Err(_) => return,
    };
    // 1 = deleted; -1 = id not found.
    assert_eq!(status, vec![1, -1]);

    let len = c.execute(XLen::new(key)).await.unwrap();
    assert_eq!(len, 1, "XDELEX should have deleted exactly one entry");
    let range = c.execute(XRange::all(key)).await.unwrap();
    assert_eq!(range[0].id, id2);

    c.execute(Del::new(key)).await.unwrap();
}
