use std::time::Instant;

use redis_tower::commands::{ZAdd, ZCard, ZRemRangeByScore};
use redis_tower::{RedisConnection, Transaction, TransactionResult};

async fn check_rate_limit(
    conn: &mut RedisConnection,
    key: &str,
    max_requests: i64,
    window_secs: u64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as f64;
    let window_start = now - (window_secs as f64 * 1000.0);
    let member = format!("{now}");
    let result = Transaction::new()
        .push(ZRemRangeByScore::new(key, "-inf", window_start.to_string()))
        .push(ZAdd::new(key).member(now, &member))
        .push(ZCard::new(key))
        .execute(conn)
        .await?;

    match result {
        TransactionResult::Committed(results) => {
            let count: &i64 = results.get(2)?;
            Ok(*count <= max_requests)
        }
        TransactionResult::Aborted => Ok(false),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Pre-create one connection per task (transactions need &mut).
    let mut conns = Vec::new();
    for _ in 0..10 {
        conns.push(RedisConnection::connect("127.0.0.1:6379").await?);
    }

    let mut handles = Vec::new();
    let start = Instant::now();
    let allowed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let denied = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    for (task_id, mut conn) in conns.into_iter().enumerate() {
        let allowed = allowed.clone();
        let denied = denied.clone();
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key = format!("ratelimit:tower:user:{}", (task_id * 100 + i) % 50);
                match check_rate_limit(&mut conn, &key, 10, 60).await {
                    Ok(true) => {
                        allowed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok(false) => {
                        denied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("error: {e}"),
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let total = 1000u64;
    let allowed = allowed.load(std::sync::atomic::Ordering::Relaxed);
    let denied = denied.load(std::sync::atomic::Ordering::Relaxed);

    println!("--- redis-tower ---");
    println!("Total checks:  {total}");
    println!("Allowed:       {allowed}");
    println!("Denied:        {denied}");
    println!("Elapsed:       {:.2?}", elapsed);
    println!("Requests/sec:  {:.0}", total as f64 / elapsed.as_secs_f64());

    Ok(())
}
