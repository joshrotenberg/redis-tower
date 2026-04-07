use std::time::Instant;

use fred::prelude::*;
use rand::Rng;
use rand::rngs::SmallRng;
use rand::SeedableRng;

const PLAYERS: usize = 1000;
const RANK_SAMPLE: usize = 100;
const UPDATE_COUNT: usize = 500;
const KEY: &str = "leaderboard:fred";

fn player_name(i: usize) -> String {
    format!("player:{i:04}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Builder::default_centralized().build()?;
    client.init().await?;
    client.flushall::<()>(false).await?;

    let mut rng = SmallRng::seed_from_u64(42);

    // 1. Add 1000 players with random scores via ZADD
    let t = Instant::now();
    let pipeline = client.pipeline();
    for i in 0..PLAYERS {
        let score: f64 = rng.random_range(0.0..10000.0);
        pipeline
            .zadd::<i64, _, _>(KEY, None, None, false, false, (score, player_name(i)))
            .await?;
    }
    pipeline.all::<Vec<i64>>().await?;
    println!("ZADD  {PLAYERS} players: {:?}", t.elapsed());

    // 2. Get top-10 (ZREVRANGEBYSCORE approach: use zrangebylex or raw command)
    // Fred's zrevrange with withscores returns flat vec. Parse manually.
    let t = Instant::now();
    let raw: Vec<String> = client.zrevrange(KEY, 0, 9, true).await?;
    let mut top10: Vec<(String, f64)> = Vec::new();
    for chunk in raw.chunks(2) {
        if chunk.len() == 2 {
            let name = chunk[0].clone();
            let score: f64 = chunk[1].parse().unwrap_or(0.0);
            top10.push((name, score));
        }
    }
    println!("TOP10 query:          {:?}", t.elapsed());

    // 3. Pipeline: get ranks for 100 random players
    let t = Instant::now();
    let pipeline = client.pipeline();
    let sampled: Vec<String> = (0..RANK_SAMPLE)
        .map(|_| player_name(rng.random_range(0..PLAYERS)))
        .collect();
    for name in &sampled {
        pipeline
            .zrank::<Option<i64>, _, _>(KEY, name.as_str(), false)
            .await?;
    }
    let ranks: Vec<Option<i64>> = pipeline.all().await?;
    println!("ZRANK {RANK_SAMPLE} players:    {:?}", t.elapsed());
    println!("  sample: {} -> rank {:?}", sampled[0], ranks[0]);

    // 4. Update 500 scores via ZINCRBY
    let t = Instant::now();
    let pipeline = client.pipeline();
    for _ in 0..UPDATE_COUNT {
        let idx = rng.random_range(0..PLAYERS);
        let delta: f64 = rng.random_range(-500.0..500.0);
        pipeline
            .zincrby::<f64, _, _>(KEY, delta, player_name(idx))
            .await?;
    }
    let new_scores: Vec<f64> = pipeline.all().await?;
    println!("ZINCRBY {UPDATE_COUNT} updates:  {:?}", t.elapsed());
    println!("  last new score: {:.2}", new_scores[UPDATE_COUNT - 1]);

    // 5. Final top-10
    let t = Instant::now();
    let raw: Vec<String> = client.zrevrange(KEY, 0, 9, true).await?;
    let mut final_top10: Vec<(String, f64)> = Vec::new();
    for chunk in raw.chunks(2) {
        if chunk.len() == 2 {
            let name = chunk[0].clone();
            let score: f64 = chunk[1].parse().unwrap_or(0.0);
            final_top10.push((name, score));
        }
    }
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

    client.quit().await?;
    Ok(())
}
