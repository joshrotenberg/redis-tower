//! Redis Transaction Commands
//!
//! Redis transactions allow executing a group of commands atomically using MULTI/EXEC.
//! Commands are queued after MULTI, then executed atomically with EXEC. WATCH provides
//! optimistic locking for check-and-set operations.
//!
//! # Transaction Workflow
//!
//! 1. **MULTI** - Start transaction, queue subsequent commands
//! 2. **Commands** - Each command returns "QUEUED"
//! 3. **EXEC** - Execute all commands atomically, returns array of results
//!    OR **DISCARD** - Abort transaction, discard all queued commands
//!
//! # Optimistic Locking with WATCH
//!
//! Use WATCH before MULTI to implement check-and-set:
//! 1. **WATCH keys** - Monitor keys for changes
//! 2. **Read values** - Check current state
//! 3. **MULTI** - Start transaction if checks pass
//! 4. **Commands** - Queue updates
//! 5. **EXEC** - Execute if watched keys unchanged, or returns None if modified
//!
//! # Complete Transaction Example
//!
//! ```no_run
//! use redis_tower::commands::transactions::{Multi, Exec, Discard};
//! use redis_tower::commands::strings::{Set, Get, Incr};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("127.0.0.1:6379").await?;
//!
//! // Start transaction
//! client.call(Multi).await?;
//!
//! // Queue commands (each returns "QUEUED")
//! client.call(Set::new("key1", "value1")).await?;
//! client.call(Incr::new("counter")).await?;
//! client.call(Set::new("key2", "value2")).await?;
//!
//! // Execute atomically
//! let results = client.call(Exec).await?;
//! if let Some(values) = results {
//!     println!("All {} commands executed atomically", values.len());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Optimistic Locking Example
//!
//! ```no_run
//! use redis_tower::commands::transactions::{Watch, Multi, Exec};
//! use redis_tower::commands::strings::{Get, Set};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = RedisClient::connect("127.0.0.1:6379").await?;
//! // Watch key before reading
//! client.call(Watch::new(vec!["balance"])).await?;
//!
//! // Read current value
//! let balance: Option<String> = client.call(Get::new("balance")).await?
//!     .map(|b| String::from_utf8_lossy(&b).to_string());
//!
//! // Check if we can proceed
//! if let Some(bal) = balance {
//!     let amount: i64 = bal.parse().unwrap_or(0);
//!     if amount >= 100 {
//!         // Start transaction
//!         client.call(Multi).await?;
//!         client.call(Set::new("balance", (amount - 100).to_string())).await?;
//!
//!         // Execute - returns None if balance was modified by another client
//!         let results = client.call(Exec).await?;
//!         if results.is_some() {
//!             println!("Transaction succeeded");
//!         } else {
//!             println!("Transaction aborted - balance was modified");
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::types::value::FromFrame;
use crate::types::{RedisError, RedisValue};
use bytes::Bytes;

use super::Command;

/// MULTI command - Mark the start of a transaction block
///
/// Marks the start of a transaction block. Subsequent commands will be queued (returning
/// "QUEUED") and executed atomically when EXEC is called. This ensures all commands either
/// all succeed or all fail together, with no other client's commands interleaved.
///
/// **Important**: Commands are queued but not executed until EXEC. Syntax errors in queued
/// commands are only detected when EXEC runs.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK to indicate transaction mode started
///
/// # Redis Version
/// Available since Redis 1.2.0
///
/// # Examples
///
/// Basic transaction:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Exec};
/// use redis_tower::commands::strings::Set;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Start transaction
/// client.call(Multi).await?;
///
/// // Queue commands
/// client.call(Set::new("key1", "value1")).await?; // Returns "QUEUED"
/// client.call(Set::new("key2", "value2")).await?; // Returns "QUEUED"
///
/// // Execute atomically
/// let results = client.call(Exec).await?;
/// # Ok(())
/// # }
/// ```
///
/// Transaction with multiple operations:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Exec};
/// use redis_tower::commands::strings::{Set, Incr};
/// use redis_tower::commands::lists::LPush;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Multi).await?;
///
/// // Queue different data type operations
/// client.call(Set::new("user:1000:name", "Alice")).await?;
/// client.call(Incr::new("user:count")).await?;
/// client.call(LPush::new("recent_users").value("Alice")).await?;
///
/// let results = client.call(Exec).await?;
/// if let Some(values) = results {
///     println!("All {} operations succeeded", values.len());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multi;

impl Command for Multi {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("MULTI")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// EXEC command - Execute all commands issued after MULTI
///
/// Executes all previously queued commands in a transaction atomically and restores
/// the connection state to normal. When using WATCH, returns None if any watched key
/// was modified, aborting the transaction.
///
/// **Important**: Commands are executed in the order they were queued. Each element in
/// the result array corresponds to one queued command in the same order.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `Option<Vec<RedisValue>>`:
/// - `Some(results)` - Array of command results, one per queued command in queued order
/// - `None` - Transaction aborted because a WATCH key was modified
///
/// # Redis Version
/// Available since Redis 1.2.0
///
/// # Examples
///
/// Basic transaction execution:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Exec};
/// use redis_tower::commands::strings::{Set, Incr};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Start transaction
/// client.call(Multi).await?;
///
/// // Queue three commands
/// client.call(Set::new("user:1000:name", "Alice")).await?;
/// client.call(Incr::new("user:count")).await?;
/// client.call(Set::new("user:1000:email", "alice@example.com")).await?;
///
/// // Execute - returns array with 3 results
/// let results = client.call(Exec).await?;
/// if let Some(values) = results {
///     println!("Transaction executed: {} commands succeeded", values.len());
///     // values[0] = SET result (OK)
///     // values[1] = INCR result (new count)
///     // values[2] = SET result (OK)
/// }
/// # Ok(())
/// # }
/// ```
///
/// Handling transaction abort with WATCH:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Watch key before reading
/// client.call(Watch::new(vec!["balance"])).await?;
///
/// // Read current balance
/// let balance: Option<bytes::Bytes> = client.call(Get::new("balance")).await?;
///
/// // Start transaction
/// client.call(Multi).await?;
/// client.call(Set::new("balance", "1000")).await?;
///
/// // Execute - returns None if balance was modified by another client
/// let results = client.call(Exec).await?;
/// match results {
///     Some(values) => println!("Transaction succeeded: {:?}", values),
///     None => println!("Transaction aborted - balance was modified by another client"),
/// }
/// # Ok(())
/// # }
/// ```
///
/// Error handling within transactions:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Exec};
/// use redis_tower::commands::strings::Set;
/// use redis_tower::commands::lists::LPush;
/// use redis_tower::RedisClient;
/// use redis_tower::types::RedisValue;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Multi).await?;
///
/// // Queue valid command
/// client.call(Set::new("key1", "value1")).await?;
///
/// // Queue command that might fail (e.g., wrong type operation)
/// client.call(LPush::new("key1").value("item")).await?; // Will fail if key1 is a string
///
/// // Queue another valid command
/// client.call(Set::new("key2", "value2")).await?;
///
/// // Execute - individual command errors are in the result array
/// let results = client.call(Exec).await?;
/// if let Some(values) = results {
///     for (i, value) in values.iter().enumerate() {
///         match value {
///             RedisValue::Error(e) => println!("Command {} failed: {}", i, e),
///             _ => println!("Command {} succeeded", i),
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Multiple data type operations:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Exec};
/// use redis_tower::commands::strings::Set;
/// use redis_tower::commands::hashes::HSet;
/// use redis_tower::commands::lists::LPush;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Multi).await?;
///
/// // Different data types in one transaction
/// client.call(Set::new("session:token", "abc123")).await?;
/// client.call(HSet::new("user:1000").field_value("name", "Alice")).await?;
/// client.call(LPush::new("recent_logins").value("user:1000")).await?;
///
/// let results = client.call(Exec).await?;
/// if let Some(values) = results {
///     println!("All {} operations executed atomically", values.len());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exec;

impl Command for Exec {
    type Response = Option<Vec<RedisValue>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("EXEC")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let values: Result<Vec<_>, _> =
                    items.into_iter().map(RedisValue::from_frame).collect();
                Ok(Some(values?))
            }
            Frame::BulkString(None) => Ok(None), // Transaction aborted (WATCH key modified)
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// DISCARD command - Discard all commands issued after MULTI
///
/// Flushes all previously queued commands in a transaction and restores the connection
/// state to normal. If WATCH was used, all watched keys are unwatched. This is useful
/// when you need to abort a transaction based on application logic or validation failures.
///
/// **Important**: DISCARD only works during a transaction (after MULTI). Calling DISCARD
/// outside a transaction will return an error.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK when called within a transaction
///
/// # Redis Version
/// Available since Redis 2.0.0
///
/// # Examples
///
/// Aborting a transaction based on validation:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Discard};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Start transaction
/// client.call(Multi).await?;
///
/// // Queue some commands
/// client.call(Set::new("user:1000:name", "Alice")).await?;
/// client.call(Set::new("user:1000:email", "alice@example.com")).await?;
///
/// // Check some condition (simplified example)
/// let should_abort = true; // e.g., validation failed
///
/// if should_abort {
///     // Abort the transaction
///     client.call(Discard).await?;
///     println!("Transaction aborted due to validation failure");
/// } else {
///     // Would execute with Exec
/// }
/// # Ok(())
/// # }
/// ```
///
/// DISCARD also unwatches keys:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Discard};
/// use redis_tower::commands::strings::Set;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Watch keys
/// client.call(Watch::new(vec!["balance", "pending"])).await?;
///
/// // Start transaction
/// client.call(Multi).await?;
/// client.call(Set::new("balance", "1000")).await?;
///
/// // Decide to abort
/// client.call(Discard).await?;
///
/// // All watched keys are now unwatched
/// // Connection is back to normal mode
/// # Ok(())
/// # }
/// ```
///
/// Error handling workflow:
/// ```no_run
/// use redis_tower::commands::transactions::{Multi, Discard};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Multi).await?;
///
/// // Queue commands
/// client.call(Set::new("key1", "value1")).await?;
///
/// // Check precondition
/// let balance: Option<bytes::Bytes> = client.call(Get::new("balance")).await?;
/// if let Some(bal) = balance {
///     let amount: i64 = String::from_utf8_lossy(&bal).parse().unwrap_or(0);
///     if amount < 100 {
///         // Insufficient funds - abort transaction
///         client.call(Discard).await?;
///         return Err("Insufficient balance".into());
///     }
/// }
///
/// // Continue with transaction...
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discard;

impl Command for Discard {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("DISCARD")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// WATCH command - Watch keys for conditional transaction execution
///
/// Marks the given keys to be watched for conditional execution of a transaction. If any
/// watched key is modified before EXEC, the entire transaction will be aborted and EXEC
/// returns None. This provides optimistic locking for implementing check-and-set (CAS)
/// operations without using Redis locks.
///
/// **Important**: WATCH must be called BEFORE MULTI. Watching is active from WATCH until
/// EXEC/DISCARD/UNWATCH. If any watched key is modified by another client, EXEC returns None.
///
/// # Request
/// - `keys`: One or more keys to watch for modifications
///
/// # Response
/// Returns `()` - Always returns OK
///
/// # Redis Version
/// Available since Redis 2.2.0
///
/// # Examples
///
/// Optimistic locking for balance deduction:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// loop {
///     // Watch balance before reading
///     client.call(Watch::new(vec!["balance"])).await?;
///
///     // Read current balance
///     let balance: Option<bytes::Bytes> = client.call(Get::new("balance")).await?;
///     let current_balance: i64 = balance
///         .map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0))
///         .unwrap_or(0);
///
///     // Check if deduction is possible
///     if current_balance < 100 {
///         return Err("Insufficient balance".into());
///     }
///
///     // Start transaction
///     client.call(Multi).await?;
///     client.call(Set::new("balance", (current_balance - 100).to_string())).await?;
///
///     // Execute - returns None if balance was modified
///     let results = client.call(Exec).await?;
///     if results.is_some() {
///         println!("Balance deduction succeeded");
///         break; // Success
///     } else {
///         println!("Balance was modified, retrying...");
///         // Retry the operation
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Watching multiple keys for atomic update:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Watch multiple keys
/// client.call(Watch::new(vec!["account:A:balance", "account:B:balance"])).await?;
///
/// // Read both balances
/// let balance_a: Option<bytes::Bytes> = client.call(Get::new("account:A:balance")).await?;
/// let balance_b: Option<bytes::Bytes> = client.call(Get::new("account:B:balance")).await?;
///
/// // Parse balances
/// let a_amount: i64 = balance_a
///     .map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0))
///     .unwrap_or(0);
/// let b_amount: i64 = balance_b
///     .map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0))
///     .unwrap_or(0);
///
/// // Transfer 100 from A to B
/// if a_amount >= 100 {
///     client.call(Multi).await?;
///     client.call(Set::new("account:A:balance", (a_amount - 100).to_string())).await?;
///     client.call(Set::new("account:B:balance", (b_amount + 100).to_string())).await?;
///
///     // Execute - aborts if either balance was modified
///     let results = client.call(Exec).await?;
///     match results {
///         Some(_) => println!("Transfer succeeded"),
///         None => println!("Transfer aborted - account was modified"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Check-and-set pattern with retry logic:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec, Unwatch};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let max_retries = 5;
/// for attempt in 0..max_retries {
///     // Watch key
///     client.call(Watch::new(vec!["counter"])).await?;
///
///     // Read value
///     let current: Option<bytes::Bytes> = client.call(Get::new("counter")).await?;
///     let value: i64 = current
///         .map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0))
///         .unwrap_or(0);
///
///     // Only increment if value is even
///     if value % 2 == 0 {
///         client.call(Multi).await?;
///         client.call(Set::new("counter", (value + 1).to_string())).await?;
///
///         let results = client.call(Exec).await?;
///         if results.is_some() {
///             println!("Incremented counter from {} to {}", value, value + 1);
///             break;
///         }
///         println!("Retry attempt {}/{}", attempt + 1, max_retries);
///     } else {
///         // Condition not met, unwatch and exit
///         client.call(Unwatch).await?;
///         println!("Counter is odd, not incrementing");
///         break;
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
    keys: Vec<Bytes>,
}

impl Watch {
    /// Create a new WATCH command
    ///
    /// # Arguments
    /// * `keys` - Keys to watch
    pub fn new<K: AsRef<[u8]>>(keys: impl IntoIterator<Item = K>) -> Self {
        Self {
            keys: keys
                .into_iter()
                .map(|k| Bytes::copy_from_slice(k.as_ref()))
                .collect(),
        }
    }
}

impl Command for Watch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut parts = Vec::with_capacity(1 + self.keys.len());
        parts.push(Frame::BulkString(Some(Bytes::from("WATCH"))));

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(key.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// UNWATCH command - Forget about all watched keys
///
/// Flushes all the previously watched keys for a transaction. If you call EXEC or DISCARD,
/// there's no need to manually call UNWATCH as they automatically unwatch all keys. This
/// is useful when you decide not to proceed with a transaction after watching keys.
///
/// **Important**: UNWATCH removes watches from ALL keys, not individual keys. EXEC and
/// DISCARD automatically call UNWATCH, so you typically only need this when aborting
/// before starting a transaction with MULTI.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK
///
/// # Redis Version
/// Available since Redis 2.2.0
///
/// # Examples
///
/// Aborting before starting transaction:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Unwatch};
/// use redis_tower::commands::strings::Get;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Watch key before checking condition
/// client.call(Watch::new(vec!["balance"])).await?;
///
/// // Read and check value
/// let balance: Option<bytes::Bytes> = client.call(Get::new("balance")).await?;
/// let amount: i64 = balance
///     .map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0))
///     .unwrap_or(0);
///
/// if amount < 100 {
///     // Condition not met - unwatch without starting transaction
///     client.call(Unwatch).await?;
///     return Err("Insufficient balance".into());
/// }
///
/// // Would proceed with MULTI/EXEC here...
/// # Ok(())
/// # }
/// ```
///
/// Cleanup in error handling:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec, Unwatch};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Watch keys
/// client.call(Watch::new(vec!["key1", "key2"])).await?;
///
/// // Perform validation
/// let value1: Option<bytes::Bytes> = client.call(Get::new("key1")).await?;
/// let value2: Option<bytes::Bytes> = client.call(Get::new("key2")).await?;
///
/// // If validation fails before transaction
/// if value1.is_none() || value2.is_none() {
///     // Clean up watches
///     client.call(Unwatch).await?;
///     return Err("Required keys do not exist".into());
/// }
///
/// // Continue with transaction...
/// # Ok(())
/// # }
/// ```
///
/// Conditional transaction flow:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec, Unwatch};
/// use redis_tower::commands::strings::{Get, Set};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Watch::new(vec!["counter"])).await?;
///
/// let current: Option<bytes::Bytes> = client.call(Get::new("counter")).await?;
/// let value: i64 = current
///     .map(|b| String::from_utf8_lossy(&b).parse().unwrap_or(0))
///     .unwrap_or(0);
///
/// // Only proceed with transaction if value is even
/// if value % 2 == 0 {
///     client.call(Multi).await?;
///     client.call(Set::new("counter", (value + 2).to_string())).await?;
///     client.call(Exec).await?; // EXEC automatically unwatches
/// } else {
///     // Value is odd - explicitly unwatch and skip transaction
///     client.call(Unwatch).await?;
///     println!("Skipped transaction - counter is odd");
/// }
/// # Ok(())
/// # }
/// ```
///
/// Note: EXEC and DISCARD automatically unwatch:
/// ```no_run
/// use redis_tower::commands::transactions::{Watch, Multi, Exec};
/// use redis_tower::commands::strings::Set;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Watch::new(vec!["key1"])).await?;
/// client.call(Multi).await?;
/// client.call(Set::new("key1", "value1")).await?;
/// client.call(Exec).await?;
///
/// // No need to call UNWATCH - EXEC already unwatched all keys
/// // Connection is back to normal mode
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unwatch;

impl Command for Unwatch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("UNWATCH")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_to_frame() {
        let cmd = Multi;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("MULTI")))])
        );
    }

    #[test]
    fn test_multi_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Multi::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_exec_to_frame() {
        let cmd = Exec;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("EXEC")))])
        );
    }

    #[test]
    fn test_exec_parse_array() {
        let frame = Frame::Array(vec![
            Frame::SimpleString(Bytes::from("OK")),
            Frame::Integer(42),
        ]);
        let result = Exec::parse_response(frame).unwrap();
        assert!(result.is_some());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_exec_parse_aborted() {
        let frame = Frame::BulkString(None);
        let result = Exec::parse_response(frame).unwrap();
        assert!(result.is_none()); // Transaction was aborted
    }

    #[test]
    fn test_discard_to_frame() {
        let cmd = Discard;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("DISCARD")))])
        );
    }

    #[test]
    fn test_discard_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Discard::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_watch_single_key() {
        let cmd = Watch::new(vec!["mykey"]);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("WATCH"))),
                Frame::BulkString(Some(Bytes::from("mykey"))),
            ])
        );
    }

    #[test]
    fn test_watch_multiple_keys() {
        let cmd = Watch::new(vec!["key1", "key2", "key3"]);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("WATCH"))),
                Frame::BulkString(Some(Bytes::from("key1"))),
                Frame::BulkString(Some(Bytes::from("key2"))),
                Frame::BulkString(Some(Bytes::from("key3"))),
            ])
        );
    }

    #[test]
    fn test_watch_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Watch::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_unwatch_to_frame() {
        let cmd = Unwatch;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("UNWATCH")))])
        );
    }

    #[test]
    fn test_unwatch_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Unwatch::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_all_commands_parse_error() {
        let error_frame = Frame::Error(Bytes::from("ERR something went wrong"));

        assert!(Multi::parse_response(error_frame.clone()).is_err());
        assert!(Exec::parse_response(error_frame.clone()).is_err());
        assert!(Discard::parse_response(error_frame.clone()).is_err());
        assert!(Watch::parse_response(error_frame.clone()).is_err());
        assert!(Unwatch::parse_response(error_frame).is_err());
    }
}
