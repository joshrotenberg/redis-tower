mod helpers;

use bytes::Bytes;
use helpers::standalone::setup_redis;
use redis_tower::commands::*;
use std::time::Duration;

#[tokio::test]
async fn test_blpop_immediate() {
    let client = setup_redis().await;
    let key = "blpop_immediate";

    // Push an item first
    client
        .call(LPush::new(key, vec![Bytes::from("value1")]))
        .await
        .unwrap();

    // BLPOP should return immediately
    let result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();

    assert!(result.is_some());
    let (list_key, value) = result.unwrap();
    assert_eq!(list_key.as_ref(), key.as_bytes());
    assert_eq!(value.as_ref(), b"value1");
}

#[tokio::test]
async fn test_blpop_timeout() {
    let client = setup_redis().await;
    let key = "blpop_timeout";

    // BLPOP on empty list with 1 second timeout
    let start = std::time::Instant::now();
    let result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();
    let elapsed = start.elapsed();

    // Should return None after timeout
    assert!(result.is_none());
    // Should have waited approximately 1 second
    assert!(elapsed >= Duration::from_millis(900));
    assert!(elapsed <= Duration::from_millis(1200));
}

#[tokio::test]
async fn test_brpop_immediate() {
    let client = setup_redis().await;
    let key = "brpop_immediate";

    // Push items
    client
        .call(RPush::new(
            key,
            vec![Bytes::from("value1"), Bytes::from("value2")],
        ))
        .await
        .unwrap();

    // BRPOP should pop from right
    let result: Option<(Bytes, Bytes)> =
        client.call(BRPop::new(vec![key.into()], 1)).await.unwrap();

    assert!(result.is_some());
    let (_, value) = result.unwrap();
    assert_eq!(value.as_ref(), b"value2"); // Right-most element
}

#[tokio::test]
async fn test_blpop_multiple_keys() {
    let client = setup_redis().await;
    let key1 = "blpop_multi_1";
    let key2 = "blpop_multi_2";
    let key3 = "blpop_multi_3";

    // Only push to key2
    client
        .call(LPush::new(key2, vec![Bytes::from("from_key2")]))
        .await
        .unwrap();

    // BLPOP checks keys in order, should find key2
    let result: Option<(Bytes, Bytes)> = client
        .call(BLPop::new(vec![key1.into(), key2.into(), key3.into()], 1))
        .await
        .unwrap();

    assert!(result.is_some());
    let (list_key, value) = result.unwrap();
    assert_eq!(list_key.as_ref(), key2.as_bytes());
    assert_eq!(value.as_ref(), b"from_key2");
}

#[tokio::test]
async fn test_blpop_producer_consumer() {
    let client = setup_redis().await;
    let queue = "work_queue";

    // Ensure queue is empty
    let _: i64 = client
        .call(Del::new(vec![queue.to_string()]))
        .await
        .unwrap();

    // Producer adds work first
    client
        .call(RPush::new(queue, vec![Bytes::from("job_data")]))
        .await
        .unwrap();

    // Consumer pops it with BLPOP (will return immediately since data exists)
    let result: Option<(Bytes, Bytes)> = client
        .call(BLPop::new(vec![queue.into()], 1))
        .await
        .unwrap();

    // Consumer should receive the job
    assert!(result.is_some());
    let (_, value) = result.unwrap();
    assert_eq!(value.as_ref(), b"job_data");
}

#[tokio::test]
async fn test_brpop_timeout() {
    let client = setup_redis().await;
    let key = "brpop_timeout";

    let start = std::time::Instant::now();
    let result: Option<(Bytes, Bytes)> =
        client.call(BRPop::new(vec![key.into()], 1)).await.unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_none());
    assert!(elapsed >= Duration::from_millis(900));
}

#[tokio::test]
async fn test_blpop_fifo_order() {
    let client = setup_redis().await;
    let key = "blpop_fifo";

    // Push multiple items
    client
        .call(RPush::new(
            key,
            vec![
                Bytes::from("first"),
                Bytes::from("second"),
                Bytes::from("third"),
            ],
        ))
        .await
        .unwrap();

    // BLPOP should get first item
    let result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();
    assert_eq!(result.unwrap().1.as_ref(), b"first");

    // Next BLPOP gets second
    let result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();
    assert_eq!(result.unwrap().1.as_ref(), b"second");

    // Next gets third
    let result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();
    assert_eq!(result.unwrap().1.as_ref(), b"third");

    // Now empty, should timeout
    let result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_brpop_lifo_order() {
    let client = setup_redis().await;
    let key = "brpop_lifo";

    // Push multiple items
    client
        .call(RPush::new(
            key,
            vec![
                Bytes::from("first"),
                Bytes::from("second"),
                Bytes::from("third"),
            ],
        ))
        .await
        .unwrap();

    // BRPOP should get last item
    let result: Option<(Bytes, Bytes)> =
        client.call(BRPop::new(vec![key.into()], 1)).await.unwrap();
    assert_eq!(result.unwrap().1.as_ref(), b"third");

    // Next gets second from end
    let result: Option<(Bytes, Bytes)> =
        client.call(BRPop::new(vec![key.into()], 1)).await.unwrap();
    assert_eq!(result.unwrap().1.as_ref(), b"second");
}

#[tokio::test]
async fn test_blpop_concurrent_consumers() {
    let client = setup_redis().await;
    let queue = "concurrent_queue";

    // Ensure queue is empty
    let _: i64 = client
        .call(Del::new(vec![queue.to_string()]))
        .await
        .unwrap();

    // Producer adds two items first
    client
        .call(RPush::new(
            queue,
            vec![Bytes::from("job1"), Bytes::from("job2")],
        ))
        .await
        .unwrap();

    // Two BLPOP calls will each get one item
    let result1: Option<(Bytes, Bytes)> = client
        .call(BLPop::new(vec![queue.into()], 1))
        .await
        .unwrap();

    let result2: Option<(Bytes, Bytes)> = client
        .call(BLPop::new(vec![queue.into()], 1))
        .await
        .unwrap();

    assert!(result1.is_some());
    assert!(result2.is_some());

    // They should get different jobs
    let r1 = result1.unwrap();
    let r2 = result2.unwrap();
    let values = [r1.1.as_ref(), r2.1.as_ref()];

    assert!(values.contains(&b"job1".as_ref()));
    assert!(values.contains(&b"job2".as_ref()));
}

#[tokio::test]
async fn test_blpop_zero_timeout() {
    let client = setup_redis().await;
    let queue = "zero_timeout_queue";

    // Ensure queue is empty
    let _: i64 = client
        .call(Del::new(vec![queue.to_string()]))
        .await
        .unwrap();

    // Add item first
    client
        .call(RPush::new(queue, vec![Bytes::from("delayed_job")]))
        .await
        .unwrap();

    // BLPOP with 0 timeout (would wait forever, but returns immediately since data exists)
    let result: Option<(Bytes, Bytes)> = client
        .call(BLPop::new(vec![queue.into()], 0))
        .await
        .unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap().1.as_ref(), b"delayed_job");
}

#[tokio::test]
async fn test_blpop_vs_lpop_behavior() {
    let client = setup_redis().await;
    let key = "blpop_vs_lpop";

    // LPOP on empty list returns None immediately
    let lpop_result: Option<Bytes> = client.call(LPop::new(key)).await.unwrap();
    assert_eq!(lpop_result, None);

    // BLPOP on empty list waits for timeout
    let start = std::time::Instant::now();
    let blpop_result: Option<(Bytes, Bytes)> =
        client.call(BLPop::new(vec![key.into()], 1)).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(blpop_result, None);
    assert!(elapsed >= Duration::from_millis(900)); // Waited for timeout
}

#[tokio::test]
async fn test_brpop_preserves_data() {
    let client = setup_redis().await;
    let key = "brpop_preserves";

    // Push binary data
    let binary_data = vec![0xFF, 0x00, 0xAB, 0xCD];
    client
        .call(RPush::new(key, vec![Bytes::from(binary_data.clone())]))
        .await
        .unwrap();

    // BRPOP should preserve the data exactly
    let result: Option<(Bytes, Bytes)> =
        client.call(BRPop::new(vec![key.into()], 1)).await.unwrap();

    assert!(result.is_some());
    let (_, value) = result.unwrap();
    assert_eq!(value.as_ref(), binary_data.as_slice());
}
