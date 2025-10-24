//! Redis commands with strong typing

use crate::codec::Frame;
use crate::types::RedisError;

/// Trait for Redis commands
pub trait Command {
    /// Response type for this command
    type Response;

    /// Convert command to RESP frame
    fn to_frame(&self) -> Frame;

    /// Parse response from RESP frame
    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError>;
}

pub mod hashes;
pub mod lists;
pub mod pubsub;
pub mod scan;
pub mod scripting;
pub mod sets;
pub mod sorted_sets;
pub mod streams;
pub mod strings;

pub use hashes::{
    HDel, HExists, HGet, HGetAll, HIncrBy, HIncrByFloat, HKeys, HLen, HMGet, HSet, HStrLen, HVals,
};
pub use lists::{
    BLPop, BRPop, InsertPosition, LIndex, LInsert, LLen, LPop, LPos, LPush, LRange, LRem, LSet,
    LTrim, RPop, RPush,
};
pub use pubsub::{Publish, PubsubNumpat, PubsubNumsub};
pub use scripting::{Eval, EvalSha, ScriptExists, ScriptFlush, ScriptLoad};
pub use sets::{
    SDiffStore, SInterCard, SInterStore, SMIsMember, SMove, SPop, SRandMember, SUnionStore, Sadd,
    Scard, Sdiff, Sinter, Sismember, Smembers, Srem, Sscan, SscanResult, Sunion,
};
pub use sorted_sets::{
    Zadd, Zcard, Zincrby, Zrange, ZrangeResult, Zrank, Zrem, Zrevrange, Zrevrank, Zscan,
    ZscanResult, Zscore,
};
pub use streams::{
    StreamEntry, StreamId, TrimStrategy, XAdd, XDel, XLen, XRange, XRead, XReadResult, XRevRange,
    XTrim,
};
pub use strings::{
    Append, Decr, DecrBy, Del, Echo, Exists, Expire, Get, GetDel, GetEx, GetExExpiration, GetRange,
    Incr, IncrBy, IncrByFloat, MGet, Mset, Ping, Set, SetRange, StrLen, Ttl,
};
