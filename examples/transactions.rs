//! Example demonstrating Redis transactions (MULTI/EXEC/WATCH)
//!
//! This showcases:
//! - Basic transactions with MULTI/EXEC
//! - DISCARD to cancel transactions
//! - WATCH for optimistic locking
//! - Handling transaction aborts
//! - Atomic operations with type safety

use redis_tower::client::RedisConnection;
use redis_tower::commands::{Del, Get, Incr, Set};
use redis_tower::transaction::{Transaction, Unwatch, Watch};
use redis_tower::types::RedisValue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = RedisConnection::connect("127.0.0.1:6379").await?;
    println!("Connected to Redis");

    // Clean up from previous runs
    let _ = client
        .execute(Del::new(vec![
            "counter".to_string(),
            "balance".to_string(),
            "account:1".to_string(),
            "account:2".to_string(),
            "total".to_string(),
        ]))
        .await;

    println!("\n=== Example 1: Basic Transaction ===");

    // Create a transaction
    let mut tx = Transaction::new(&client);

    // Queue multiple commands
    tx.queue(Set::new("counter", b"0".to_vec())).await?;
    tx.queue(Incr::new("counter")).await?;
    tx.queue(Incr::new("counter")).await?;
    tx.queue(Incr::new("counter")).await?;
    tx.queue(Get::new("counter")).await?;

    // Execute atomically
    let results = tx.exec().await?;

    if let Some(results) = results {
        println!("Transaction executed successfully!");
        println!("Results ({} commands):", results.len());
        for (i, result) in results.iter().enumerate() {
            println!("  Command {}: {:?}", i + 1, result);
        }

        // Last result should be "3"
        if let Some(last) = results.last() {
            if let Ok(Some(bytes)) = last.as_bytes() {
                let value = String::from_utf8_lossy(&bytes);
                println!("Final counter value: {}", value);
                assert_eq!(value, "3");
            }
        }
    } else {
        println!("Transaction was aborted");
    }

    println!("\n=== Example 2: DISCARD Transaction ===");

    let mut tx = Transaction::new(&client);
    tx.queue(Set::new("balance", b"1000".to_vec())).await?;
    tx.queue(Incr::new("balance")).await?;
    println!("Queued 2 commands...");

    // Changed our mind - discard the transaction
    tx.discard().await?;
    println!("Transaction discarded!");

    // Verify nothing was executed
    let balance = client.execute(Get::new("balance")).await?;
    println!("Balance after DISCARD: {:?}", balance);
    assert!(balance.is_none()); // Should be None

    println!("\n=== Example 3: WATCH for Optimistic Locking ===");

    // Set initial balance
    client
        .execute(Set::new("balance", b"1000".to_vec()))
        .await?;
    println!("Initial balance: 1000");

    // Watch the balance key
    let watch_result = client.execute(Watch::new("balance")).await?;
    println!("WATCH balance -> {}", watch_result);

    // Simulate another client modifying the key
    println!("Simulating concurrent modification...");
    client.execute(Set::new("balance", b"999".to_vec())).await?;

    // Try to execute transaction - should abort
    let mut tx = Transaction::new(&client);
    tx.queue(Set::new("balance", b"1100".to_vec())).await?;
    let result = tx.exec().await?;

    if result.is_none() {
        println!("Transaction aborted! Balance was modified by another client.");
    } else {
        println!("ERROR: Transaction should have been aborted!");
    }

    // Verify balance wasn't changed by our transaction
    let balance = client.execute(Get::new("balance")).await?;
    let balance_bytes = balance.unwrap();
    let balance_value = String::from_utf8_lossy(&balance_bytes);
    println!("Balance after aborted transaction: {}", balance_value);
    assert_eq!(balance_value, "999"); // Should still be 999

    println!("\n=== Example 4: WATCH with Successful Transaction ===");

    // Set balance
    client
        .execute(Set::new("balance", b"1000".to_vec()))
        .await?;

    // Watch the key
    client.execute(Watch::new("balance")).await?;
    println!("WATCH balance (no concurrent modification this time)");

    // No modification happens here - transaction should succeed
    let mut tx = Transaction::new(&client);
    tx.queue(Set::new("balance", b"1100".to_vec())).await?;
    let result = tx.exec().await?;

    if result.is_some() {
        println!("Transaction succeeded!");
    } else {
        println!("ERROR: Transaction should have succeeded!");
    }

    let balance = client.execute(Get::new("balance")).await?;
    let balance_bytes = balance.unwrap();
    let balance_value = String::from_utf8_lossy(&balance_bytes);
    println!("Balance after successful transaction: {}", balance_value);
    assert_eq!(balance_value, "1100");

    println!("\n=== Example 5: UNWATCH ===");

    client.execute(Watch::new("balance")).await?;
    println!("WATCH balance");

    // Changed our mind
    client.execute(Unwatch).await?;
    println!("UNWATCH");

    // Modify the key (should not affect transaction now)
    client
        .execute(Set::new("balance", b"2000".to_vec()))
        .await?;

    // Transaction should succeed even though balance was modified
    let mut tx = Transaction::new(&client);
    tx.queue(Set::new("balance", b"2100".to_vec())).await?;
    let result = tx.exec().await?;

    if result.is_some() {
        println!("Transaction succeeded (UNWATCH worked)!");
    }

    println!("\n=== Example 6: Practical Use Case - Bank Transfer ===");

    // Set up two accounts
    client
        .execute(Set::new("account:1", b"1000".to_vec()))
        .await?;
    client
        .execute(Set::new("account:2", b"500".to_vec()))
        .await?;
    println!("Account 1: 1000, Account 2: 500");

    // Watch both accounts
    client
        .execute(Watch::new("account:1").key("account:2"))
        .await?;

    // Simulate checking balances first
    let balance1 = client.execute(Get::new("account:1")).await?;
    let balance1_value: i64 = String::from_utf8_lossy(&balance1.unwrap()).parse().unwrap();
    println!("Current balance in account:1 = {}", balance1_value);

    // Transfer 200 from account 1 to account 2 (atomically)
    let transfer_amount = 200;

    if balance1_value >= transfer_amount {
        let mut tx = Transaction::new(&client);

        // Deduct from account 1
        let new_balance1 = balance1_value - transfer_amount;
        tx.queue(Set::new("account:1", new_balance1.to_string().into_bytes()))
            .await?;

        // Add to account 2
        let balance2 = client.execute(Get::new("account:2")).await?;
        let balance2_value: i64 = String::from_utf8_lossy(&balance2.unwrap()).parse().unwrap();
        let new_balance2 = balance2_value + transfer_amount;
        tx.queue(Set::new("account:2", new_balance2.to_string().into_bytes()))
            .await?;

        // Execute transfer
        let result = tx.exec().await?;

        if result.is_some() {
            println!(
                "Transfer succeeded! Transferred {} from account:1 to account:2",
                transfer_amount
            );

            let final_balance1 = client.execute(Get::new("account:1")).await?;
            let final_balance2 = client.execute(Get::new("account:2")).await?;

            println!(
                "Final balances: account:1 = {}, account:2 = {}",
                String::from_utf8_lossy(&final_balance1.unwrap()),
                String::from_utf8_lossy(&final_balance2.unwrap())
            );
        } else {
            println!("Transfer aborted - accounts were modified during transaction");
        }
    } else {
        println!("Insufficient funds!");
    }

    println!("\n=== Example 7: Complex Transaction with Multiple Types ===");

    let mut tx = Transaction::new(&client);

    // Mix different command types
    tx.queue(Set::new("key1", b"hello".to_vec())).await?;
    tx.queue(Set::new("key2", b"world".to_vec())).await?;
    tx.queue(Incr::new("counter")).await?;
    tx.queue(Get::new("key1")).await?;
    tx.queue(Get::new("key2")).await?;
    tx.queue(Get::new("counter")).await?;

    let results = tx.exec().await?;

    if let Some(results) = results {
        println!("Complex transaction results:");
        for (i, result) in results.iter().enumerate() {
            match result {
                RedisValue::Status(s) => println!("  {}: Status = {}", i + 1, s),
                RedisValue::Integer(n) => println!("  {}: Integer = {}", i + 1, n),
                RedisValue::BulkString(b) => {
                    println!("  {}: String = {}", i + 1, String::from_utf8_lossy(b))
                }
                RedisValue::Nil => println!("  {}: Nil", i + 1),
                other => println!("  {}: {:?}", i + 1, other),
            }
        }
    }

    println!("\n=== Example 8: Retry Pattern with WATCH ===");

    client.execute(Set::new("total", b"0".to_vec())).await?;

    let max_retries = 5;
    let mut retries = 0;

    loop {
        // Watch the key
        client.execute(Watch::new("total")).await?;

        // Read current value
        let current = client.execute(Get::new("total")).await?;
        let current_value: i64 = String::from_utf8_lossy(&current.unwrap()).parse().unwrap();

        // Calculate new value
        let new_value = current_value + 10;

        // Try to update
        let mut tx = Transaction::new(&client);
        tx.queue(Set::new("total", new_value.to_string().into_bytes()))
            .await?;

        let result = tx.exec().await?;

        if result.is_some() {
            println!("Update succeeded on retry {}", retries + 1);
            break;
        } else {
            retries += 1;
            if retries >= max_retries {
                println!("Failed after {} retries", max_retries);
                break;
            }
            println!("Retry {} - transaction aborted, retrying...", retries);
        }
    }

    println!("\n=== All transaction examples completed! ===");
    println!("\nTransaction features demonstrated:");
    println!("  ✅ MULTI/EXEC - Atomic command batching");
    println!("  ✅ DISCARD - Cancel transactions");
    println!("  ✅ WATCH - Optimistic locking");
    println!("  ✅ UNWATCH - Clear watched keys");
    println!("  ✅ Transaction abort detection");
    println!("  ✅ Type-safe result handling with RedisValue");
    println!("  ✅ Practical use case: Bank transfers");
    println!("  ✅ Retry pattern with WATCH");

    Ok(())
}
