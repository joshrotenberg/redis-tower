use std::time::Instant;

use rand::Rng;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use redis::AsyncCommands;

const PLAYERS: usize = 1000;
const RANK_SAMPLE: usize = 100;
const UPDATE_COUNT: usize = 500;
const KEY: &str = "leaderboard:redis-rs";

fn player_name(i: usize) -> String {
    format!("player:{i:04}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_multiplexed_async_connection().await?;
    redis::cmd("FLUSHDB").query_async::<()>(&mut con).await?;

    let mut rng = SmallRng::seed_from_u64(42);

    // 1. Add 1000 players with random scores via ZADD
    let t = Instant::now();
    let mut pipe = redis::pipe();
    for i in 0..PLAYERS {
        let score: f64 = rng.random_range(0.0..10000.0);
        pipe.zadd(KEY, player_name(i), score);
    }
    pipe.query_async::<Vec<i64>>(&mut con).await?;
    println!("ZADD  {PLAYERS} players: {:?}", t.elapsed());

    // 2. Pipeline: get top-10 (ZREVRANGE WITHSCORES)
    let t = Instant::now();
    let top10: Vec<(String, f64)> = con.zrevrange_withscores(KEY, 0, 9).await?;
    println!("TOP10 query:          {:?}", t.elapsed());

    // 3. Pipeline: get ranks for 100 random players
    let t = Instant::now();
    let mut pipe = redis::pipe();
    let sampled: Vec<String> = (0..RANK_SAMPLE)
        .map(|_| player_name(rng.random_range(0..PLAYERS)))
        .collect();
    for name in &sampled {
        pipe.zrank(KEY, name);
    }
    let ranks: Vec<Option<i64>> = pipe.query_async(&mut con).await?;
    println!("ZRANK {RANK_SAMPLE} players:    {:?}", t.elapsed());
    println!("  sample: {} -> rank {:?}", sampled[0], ranks[0]);

    // 4. Update 500 scores via ZINCRBY
    let t = Instant::now();
    let mut pipe = redis::pipe();
    for _ in 0..UPDATE_COUNT {
        let idx = rng.random_range(0..PLAYERS);
        let delta: f64 = rng.random_range(-500.0..500.0);
        pipe.zincr(KEY, player_name(idx), delta);
    }
    let new_scores: Vec<f64> = pipe.query_async(&mut con).await?;
    println!("ZINCRBY {UPDATE_COUNT} updates:  {:?}", t.elapsed());
    println!("  last new score: {:.2}", new_scores[UPDATE_COUNT - 1]);

    // 5. Final top-10
    let t = Instant::now();
    let final_top10: Vec<(String, f64)> = con.zrevrange_withscores(KEY, 0, 9).await?;
    println!("FINAL top-10:         {:?}", t.elapsed());

    // 6. Print results
    println!("\n--- Initial Top 10 ---");
    for (i, (name, score)) in top10.iter().enumerate() {
        println!("  {rank}. {name:<14} {score:>10.2}", rank = i + 1);
    }
    println!("\n--- Final Top 10 ---");
    for (i, (name, score)) in final_top10.iter().enumerate() {
        println!("  {rank}. {name:<14} {score:>10.2}", rank = i + 1);
    }

    Ok(())
}
