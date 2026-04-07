use std::time::Instant;

use fred::prelude::*;

async fn check_rate_limit(
    client: &Client,
    key: &str,
    max_requests: i64,
    window_secs: u64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as f64;
    let window_start = now - (window_secs as f64 * 1000.0);
    let member = format!("{now}");

    let txn = client.multi();
    let _: () = txn
        .zremrangebyscore(key, f64::NEG_INFINITY, window_start)
        .await?;
    let _: () = txn.zadd(key, None, None, false, false, (now, member.as_str())).await?;
    let _: () = txn.zcard(key).await?;
    let results: Vec<i64> = txn.exec(true).await?;

    let count = results.get(2).copied().unwrap_or(0);
    Ok(count <= max_requests)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_url("redis://127.0.0.1:6379/")?;
    let client = Client::new(config, None, None, None);
    client.connect();
    client.wait_for_connect().await?;

    let start = Instant::now();
    let allowed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let denied = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::new();

    for task_id in 0..10 {
        let client = client.clone();
        let allowed = allowed.clone();
        let denied = denied.clone();
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key = format!("ratelimit:fred:user:{}", (task_id * 100 + i) % 50);
                match check_rate_limit(&client, &key, 10, 60).await {
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

    println!("--- fred ---");
    println!("Total checks:  {total}");
    println!("Allowed:       {allowed}");
    println!("Denied:        {denied}");
    println!("Elapsed:       {:.2?}", elapsed);
    println!("Requests/sec:  {:.0}", total as f64 / elapsed.as_secs_f64());

    client.quit().await?;
    Ok(())
}
