mod common;

use common::conn;
use redis_tower::commands::*;

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
