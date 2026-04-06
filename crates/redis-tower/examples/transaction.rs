//! MULTI/EXEC transactions with optional WATCH for optimistic locking.

use redis_tower::commands::*;
use redis_tower::{RedisConnection, Transaction, TransactionResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // Simple transaction: SET then INCR atomically.
    let result = Transaction::new()
        .push(Set::new("tx:counter", "10"))
        .push(Incr::new("tx:counter"))
        .execute(&mut conn)
        .await?;

    match result {
        TransactionResult::Committed(results) => {
            let val: &i64 = results.get(1)?;
            println!("Counter after INCR: {val}");
        }
        TransactionResult::Aborted => {
            println!("Transaction aborted (watched key changed)");
        }
    }

    // Transaction with WATCH -- aborts if "tx:watched" changes
    // between WATCH and EXEC.
    let result = Transaction::new()
        .watch(["tx:watched"])
        .push(Set::new("tx:watched", "safe"))
        .execute(&mut conn)
        .await?;

    match result {
        TransactionResult::Committed(_) => println!("Watch transaction committed"),
        TransactionResult::Aborted => println!("Watch transaction aborted"),
    }

    // Clean up.
    conn.execute(Del::new("tx:counter")).await?;
    conn.execute(Del::new("tx:watched")).await?;

    Ok(())
}
