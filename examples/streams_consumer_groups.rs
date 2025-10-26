//! Stream Consumer Groups Example
//!
//! Demonstrates Redis Streams with consumer groups for distributed message processing.
//! Shows how multiple consumers can process messages from a stream with load balancing
//! and fault tolerance.

use redis_tower::commands::{XAck, XAdd, XClaim, XGroupCreate, XPending, XReadGroup};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Redis Tower - Stream Consumer Groups Example\n");

    // Note: This example requires a running Redis server
    // Start Redis with: redis-server --port 6379

    let stream = "orders";
    let group = "processors";

    // Simulate creating a stream and consumer group
    println!("1. Setting up stream and consumer group");
    println!("   XGROUP CREATE {} {} 0 MKSTREAM", stream, group);

    let create_group = XGroupCreate::new(stream, group, "0").mkstream();
    println!("   Command: {:?}\n", create_group);

    // Simulate adding messages to the stream
    println!("2. Adding messages to stream");
    for i in 1..=5 {
        let _add_cmd = XAdd::new(stream)
            .field("order_id", format!("ORD-{:03}", i))
            .field("amount", format!("{}.00", i * 10))
            .field("status", "pending");

        println!(
            "   XADD {} * order_id ORD-{:03} amount {}.00 status pending",
            stream,
            i,
            i * 10
        );
    }
    println!();

    // Consumer 1 reads from the group
    println!("3. Consumer 1 reading new messages");
    let read_cmd = XReadGroup::new(group, "consumer1")
        .stream(stream, ">") // ">" means only new messages
        .count(2)
        .block(1000); // Block for 1 second

    println!(
        "   XREADGROUP GROUP {} consumer1 COUNT 2 BLOCK 1000 STREAMS {} >",
        group, stream
    );
    println!("   Command: {:?}", read_cmd);
    println!("   → Would receive: [order_id:ORD-001, order_id:ORD-002]\n");

    // Consumer 2 reads from the group (gets different messages)
    println!("4. Consumer 2 reading new messages (load balancing)");
    let read_cmd2 = XReadGroup::new(group, "consumer2")
        .stream(stream, ">")
        .count(2);

    println!(
        "   XREADGROUP GROUP {} consumer2 COUNT 2 STREAMS {} >",
        group, stream
    );
    println!("   Command: {:?}", read_cmd2);
    println!("   → Would receive: [order_id:ORD-003, order_id:ORD-004]\n");

    // Acknowledge processed messages
    println!("5. Consumer 1 acknowledging processed messages");
    let ack_cmd = XAck::new(stream, group)
        .id("1234567890123-0")
        .id("1234567890124-0");
    println!(
        "   XACK {} {} 1234567890123-0 1234567890124-0",
        stream, group
    );
    println!("   Command: {:?}", ack_cmd);
    println!("   → Returns: 2 (number of messages acknowledged)\n");

    // Check pending messages
    println!("6. Checking pending messages (unacknowledged)");
    let pending_cmd = XPending::new(stream, group);
    println!("   XPENDING {} {}", stream, group);
    println!("   Command: {:?}", pending_cmd);
    println!("   → Would show: messages still being processed\n");

    // Get detailed pending info
    println!("7. Getting detailed pending messages");
    let pending_detail = XPending::new(stream, group)
        .range("-", "+", 10)
        .consumer("consumer2");

    println!("   XPENDING {} {} - + 10 consumer2", stream, group);
    println!("   Command: {:?}", pending_detail);
    println!("   → Would show: message IDs, idle time, delivery count\n");

    // Claim stale messages (fault tolerance)
    println!("8. Consumer 3 claiming stale messages from consumer 2");
    println!("   (useful if consumer 2 crashed)");

    let claim_cmd = XClaim::new(
        stream,
        group,
        "consumer3",
        3600000, // Messages idle for more than 1 hour
    )
    .id("1234567890125-0");

    println!(
        "   XCLAIM {} {} consumer3 3600000 1234567890125-0",
        stream, group
    );
    println!("   Command: {:?}", claim_cmd);
    println!("   → Transfers ownership to consumer3\n");

    // No-acknowledgement mode for high-throughput scenarios
    println!("9. Reading without acknowledgement (NOACK)");
    let noack_read = XReadGroup::new(group, "consumer4")
        .stream(stream, ">")
        .noack(); // Messages won't enter pending list

    println!(
        "   XREADGROUP GROUP {} consumer4 NOACK STREAMS {} >",
        group, stream
    );
    println!("   Command: {:?}", noack_read);
    println!("   → Messages auto-acknowledged (high throughput, lower reliability)\n");

    println!("{}", "=".repeat(60));
    println!("Consumer Group Patterns:\n");

    println!("✓ Load Balancing: Multiple consumers share the workload");
    println!("✓ Fault Tolerance: Pending messages can be claimed by other consumers");
    println!("✓ At-Least-Once: Messages stay pending until acknowledged");
    println!("✓ At-Most-Once: Use NOACK for fire-and-forget semantics");
    println!("✓ Monitoring: XPENDING tracks unprocessed messages");

    println!("\nUse Cases:");
    println!("  • Job queues with multiple workers");
    println!("  • Event processing pipelines");
    println!("  • Distributed task execution");
    println!("  • Message-driven microservices");

    Ok(())
}
