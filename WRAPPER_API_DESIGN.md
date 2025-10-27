# Wrapper API Design Analysis

## Question
Should redis-tower add convenience methods like `client.get()`, `client.set()` alongside the existing `client.call(Get::new())` pattern?

## Current API (Command Structs - v0.1.0)

```rust
use redis_tower::RedisClient;
use redis_tower::commands::{Get, Set, Incr};

let client = RedisClient::connect("localhost:6379").await?;

// Explicit command structs
client.call(Set::new("key", "value")).await?;
let value: Option<Bytes> = client.call(Get::new("key")).await?;
let count: i64 = client.call(Incr::new("counter")).await?;

// Builder pattern for complex commands
client.call(
    Set::new("key", "value")
        .ex(3600)
        .nx()
        .get()
).await?;
```

**Strengths:**
- ✅ Type-safe: Each command is a distinct type with compile-time validation
- ✅ Discoverable: IDEs autocomplete command structs in `commands::` module
- ✅ Explicit: Clear what command you're creating
- ✅ Builder pattern: Easy to add optional parameters
- ✅ Testable: Can construct commands without a client
- ✅ Composable: Commands are data, can be stored/passed around
- ✅ Zero ambiguity: `Set::new()` vs `SetEx::new()` are different types

**Weaknesses:**
- ❌ Verbose: `client.call(Get::new("key"))` vs `client.get("key")`
- ❌ Import burden: Need to import command types
- ❌ Not familiar: Most Redis clients use methods

## Competitor APIs

### redis-rs (Methods via Traits)
```rust
use redis::Commands;

let mut con = client.get_connection()?;
con.set("key", "value")?;
let value: String = con.get("key")?;
```

### fred.rs (Methods)
```rust
client.set("foo", "bar", None, None, false).await?;
let value: Option<String> = client.get("foo").await?;
```

## Proposed Solution: Extension Traits (Best of Both Worlds)

Keep the existing command struct API as the foundation, but add **optional** trait-based wrappers for common commands.

### Design: Category-Based Extension Traits

```rust
// src/commands/extensions/strings.rs
pub trait StringCommands {
    /// Shorthand for `self.call(Get::new(key))`
    async fn get<K>(&self, key: K) -> Result<Option<Bytes>, RedisError>
    where
        K: Into<String>;

    /// Shorthand for `self.call(Set::new(key, value))`
    async fn set<K, V>(&self, key: K, value: V) -> Result<(), RedisError>
    where
        K: Into<String>,
        V: Into<Bytes>;

    /// Shorthand for `self.call(Incr::new(key))`
    async fn incr<K>(&self, key: K) -> Result<i64, RedisError>
    where
        K: Into<String>;

    // Note: Only simple variants provided
    // For builder options, use explicit command struct
}

impl StringCommands for RedisClient {
    async fn get<K>(&self, key: K) -> Result<Option<Bytes>, RedisError>
    where
        K: Into<String>,
    {
        self.call(Get::new(key)).await
    }

    async fn set<K, V>(&self, key: K, value: V) -> Result<(), RedisError>
    where
        K: Into<String>,
        V: Into<Bytes>,
    {
        self.call(Set::new(key, value)).await
    }

    async fn incr<K>(&self, key: K) -> Result<i64, RedisError>
    where
        K: Into<String>,
    {
        self.call(Incr::new(key)).await
    }
}

impl StringCommands for ResilientRedisClient { /* same */ }
impl StringCommands for RedisConnection { /* same */ }
```

### Usage Comparison

```rust
// Simple cases: Use convenience methods
use redis_tower::commands::StringCommands;

client.set("key", "value").await?;
let value = client.get("key").await?;
let count = client.incr("counter").await?;

// Complex cases: Use command structs with builder
use redis_tower::commands::Set;

client.call(
    Set::new("key", "value")
        .ex(3600)        // Expire in 1 hour
        .nx()            // Only if not exists
        .get()           // Return old value
).await?;

// Best of both worlds!
```

### Trait Organization

```rust
// src/commands/extensions/mod.rs
pub mod strings;
pub mod hashes;
pub mod lists;
pub mod sets;
pub mod sorted_sets;
pub mod keys;

pub use strings::StringCommands;
pub use hashes::HashCommands;
pub use lists::ListCommands;
pub use sets::SetCommands;
pub use sorted_sets::SortedSetCommands;
pub use keys::KeyCommands;

// Prelude for convenience
pub mod prelude {
    pub use super::{
        StringCommands, HashCommands, ListCommands,
        SetCommands, SortedSetCommands, KeyCommands,
    };
}
```

### Which Commands Get Wrappers?

**Include:**
- ✅ Simple, frequently used commands (GET, SET, DEL, INCR, LPUSH, HGET, etc.)
- ✅ Commands with no optional parameters
- ✅ Commands that are typically used in their basic form

**Exclude:**
- ❌ Commands with many builder options (use command structs)
- ❌ Rarely used commands
- ❌ Module-specific commands (unless commonly used)

**Guideline:** ~50-75 wrapper methods covering top 20% of use cases

## Implementation Strategy

### Phase 1: Proof of Concept
1. Implement `StringCommands` trait with 5-10 methods
2. Add to `RedisClient`, `ResilientRedisClient`, `RedisConnection`
3. Write example showing both styles
4. Get user feedback

### Phase 2: Core Commands
1. Implement traits for:
   - `StringCommands` (GET, SET, INCR, DECR, APPEND, etc.)
   - `HashCommands` (HGET, HSET, HDEL, HINCRBY, etc.)
   - `ListCommands` (LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.)
   - `SetCommands` (SADD, SREM, SMEMBERS, SISMEMBER, etc.)
   - `KeyCommands` (DEL, EXISTS, EXPIRE, TTL, etc.)

### Phase 3: Documentation
1. Update examples to show both patterns
2. Document when to use each approach
3. Add to README

## Pros & Cons Analysis

### Pros of Adding Traits:
1. ✅ **Ergonomics**: Shorter syntax for common cases
2. ✅ **Familiarity**: Matches redis-rs/fred.rs patterns
3. ✅ **Opt-in**: Users choose which traits to import
4. ✅ **No breaking changes**: Command structs still work
5. ✅ **IDE hints**: Methods appear in autocomplete
6. ✅ **Gradual adoption**: Can add traits incrementally

### Cons of Adding Traits:
1. ❌ **Maintenance burden**: 2 APIs to maintain
2. ❌ **Documentation split**: Need to document both approaches
3. ❌ **Import complexity**: `use redis_tower::commands::StringCommands`
4. ❌ **Method explosion**: Could add 50-100 methods per client
5. ❌ **Trait confusion**: Which trait has which method?
6. ❌ **Loss of explicitness**: Less clear what's happening

## Alternative: Macro-Generated Extensions

```rust
// Generate trait methods automatically from command definitions
commands! {
    Get => get(key: String) -> Option<Bytes>,
    Set => set(key: String, value: Bytes) -> (),
    Incr => incr(key: String) -> i64,
}

// Expands to trait definitions + implementations
```

This could reduce maintenance burden significantly.

## Recommendation

### Option A: Add Extension Traits (Recommended)
**Do this if:**
- Users frequently complain about verbosity
- We want to match redis-rs/fred.rs ergonomics
- We're willing to maintain dual APIs

**Implementation:**
- Feature-gate under `convenience-methods` feature flag
- Start with top 50 most-used commands
- Document clearly when to use each approach
- Consider macro generation to reduce boilerplate

### Option B: Keep Command Structs Only (Current)
**Do this if:**
- We value API clarity over brevity
- We want a single obvious way to do things
- We prefer compile-time guarantees
- Verbosity isn't a major complaint

**Marketing:**
- Emphasize type safety as a feature
- Show how builder patterns enable complex commands
- Highlight testability and composability

### Option C: Hybrid - Builder Shortcuts Only
Instead of wrapper methods, provide shortcuts that return command builders:

```rust
impl RedisClient {
    pub fn set(&self, key: impl Into<String>, value: impl Into<Bytes>) -> Set {
        Set::new(key, value)
    }
}

// Usage:
let cmd = client.set("key", "value").ex(60).nx();
client.call(cmd).await?;

// Or shorthand:
client.call(client.set("key", "value")).await?;
```

This gives builder access while keeping things shorter.

## Decision Criteria

Consider:
1. **User feedback**: Are users asking for shorter APIs?
2. **Library maturity**: Is API stable enough to add convenience layer?
3. **Competitive positioning**: Do we need to match redis-rs ergonomics?
4. **Maintenance capacity**: Can we maintain dual APIs?
5. **Documentation**: Can we clearly explain both approaches?

## Conclusion

**My recommendation: Start with Option C (Builder Shortcuts) as a compromise:**

1. Add a feature flag: `convenience-methods`
2. Implement ~20 common command shortcuts that return builders
3. Document the pattern clearly
4. Gather user feedback before expanding

This gives users shorter syntax without duplicating the entire API surface, and maintains our type-safety strengths.

If users love it and want more, we can expand to full trait-based extensions (Option A).
If users don't use it, we remove it before 1.0 and stick with command structs (Option B).
