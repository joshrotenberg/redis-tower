use std::time::Instant;

use rand::Rng;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use redis_tower::commands::{FlushDb, RawCommand, ZAdd, ZIncrBy, ZRank};
use redis_tower::{Frame, Pipeline, RedisConnection};

const PLAYERS: usize = 1000;
const RANK_SAMPLE: usize = 100;
const UPDATE_COUNT: usize = 500;
const KEY: &str = "leaderboard:tower";

fn player_name(i: usize) -> String { format!("player:{i:04}") }

fn zrevrange_cmd(key: &str, start: &str, stop: &str) -> RawCommand {
    RawCommand::new("ZREVRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
}

/// Parse a ZREVRANGE ... WITHSCORES Frame into (member, score) pairs.
fn parse_withscores(frame: &Frame) -> Result<Vec<(String, f64)>, Box<dyn std::error::Error>> {
    let Frame::Array(Some(items)) = frame else { return Err("expected array".into()) };
    items
        .chunks(2)
        .map(|pair| {
            let name = match &pair[0] {
                Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                other => return Err(format!("expected bulk string, got {other:?}").into()),
            };
            let score = match &pair[1] {
                Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<f64>()?,
                Frame::Double(d) => *d,
                other => return Err(format!("expected score, got {other:?}").into()),
            };
            Ok((name, score))
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
    conn.execute(FlushDb::new()).await?;
    let mut rng = SmallRng::seed_from_u64(42);

    // 1. Add 1000 players with random scores via ZADD
    let t = Instant::now();
    let mut pipe = Pipeline::new();
    for i in 0..PLAYERS {
        let score: f64 = rng.random_range(0.0..10000.0);
        pipe = pipe.push(ZAdd::new(KEY).member(score, player_name(i)));
    }
    pipe.execute(&mut conn).await?;
    println!("ZADD  {PLAYERS} players: {:?}", t.elapsed());

    // 2. Get top-10 (ZREVRANGE WITHSCORES via RawCommand)
    let t = Instant::now();
    let top10 = parse_withscores(&conn.execute(zrevrange_cmd(KEY, "0", "9")).await?)?;
    println!("TOP10 query:          {:?}", t.elapsed());

    // 3. Pipeline: get ranks for 100 random players (typed as Option<i64>)
    let t = Instant::now();
    let sampled: Vec<String> = (0..RANK_SAMPLE)
        .map(|_| player_name(rng.random_range(0..PLAYERS)))
        .collect();
    let mut pipe = Pipeline::new();
    for name in &sampled {
        pipe = pipe.push(ZRank::new(KEY, name.as_str()));
    }
    let results = pipe.execute(&mut conn).await?;
    let ranks: Vec<Option<i64>> = (0..RANK_SAMPLE)
        .map(|i| results.get::<Option<i64>>(i).cloned())
        .collect::<Result<_, _>>()?;
    println!("ZRANK {RANK_SAMPLE} players:    {:?}", t.elapsed());
    println!("  sample: {} -> rank {:?}", sampled[0], ranks[0]);

    // 4. Update 500 scores via ZINCRBY (response is f64 per command)
    let t = Instant::now();
    let mut pipe = Pipeline::new();
    for _ in 0..UPDATE_COUNT {
        let idx = rng.random_range(0..PLAYERS);
        let delta: f64 = rng.random_range(-500.0..500.0);
        pipe = pipe.push(ZIncrBy::new(KEY, delta, player_name(idx)));
    }
    let results = pipe.execute(&mut conn).await?;
    let last_score: &f64 = results.get(UPDATE_COUNT - 1)?;
    println!("ZINCRBY {UPDATE_COUNT} updates:  {:?}", t.elapsed());
    println!("  last new score: {last_score:.2}");

    // 5. Final top-10
    let t = Instant::now();
    let final_top10 = parse_withscores(&conn.execute(zrevrange_cmd(KEY, "0", "9")).await?)?;
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
