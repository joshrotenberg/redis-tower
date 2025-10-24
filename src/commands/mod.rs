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

pub mod bitmap;
pub mod connection;
pub mod geo;
pub mod hashes;
pub mod hyperloglog;
pub mod keys;
pub mod lists;
pub mod pubsub;
pub mod scan;
pub mod scripting;
pub mod server;
pub mod sets;
pub mod sorted_sets;
pub mod streams;
pub mod strings;

pub use bitmap::{BitCount, BitOp, BitOpCmd, BitPos, GetBit, SetBit};
pub use connection::{Auth, AuthAcl, Quit, ReadOnly, ReadWrite, Select};
pub use geo::{GeoAdd, GeoCoordinate, GeoDist, GeoHash, GeoItem, GeoPos, GeoSearch, GeoUnit};
pub use hashes::{
    HDel, HExists, HGet, HGetAll, HIncrBy, HIncrByFloat, HKeys, HLen, HMGet, HSet, HStrLen, HVals,
};
pub use hyperloglog::{PfAdd, PfCount, PfMerge};
pub use keys::{
    Copy, ExpireAt, ExpireTime, Keys, Move, PExpire, PExpireAt, PExpireTime, PTtl, Persist, Rename,
    RenameNx, Touch, Type, Unlink,
};
pub use lists::{
    BLMPop, BLMove, BLPop, BRPop, InsertPosition, LIndex, LInsert, LLen, LMPop, LMPopResult, LMove,
    LPop, LPos, LPush, LPushX, LRange, LRem, LSet, LTrim, MoveDirection, RPop, RPush, RPushX,
};
pub use pubsub::{Publish, PubsubNumpat, PubsubNumsub};
pub use scripting::{Eval, EvalSha, ScriptExists, ScriptFlush, ScriptLoad};
pub use server::{DbSize, FlushAll, FlushDb, LastSave, RandomKey, Time};
pub use sets::{
    SDiffStore, SInterCard, SInterStore, SMIsMember, SMove, SPop, SRandMember, SUnionStore, Sadd,
    Scard, Sdiff, Sinter, Sismember, Smembers, Srem, Sscan, SscanResult, Sunion,
};
pub use sorted_sets::{
    BZPopMax, BZPopMin, ZCount, ZLexCount, ZMScore, ZPopMax, ZPopMin, ZRangeByLex, ZRangeByScore,
    ZRemRangeByLex, ZRemRangeByRank, ZRemRangeByScore, ZRevRangeByLex, ZRevRangeByScore, Zadd,
    Zcard, Zincrby, Zrange, ZrangeResult, Zrank, Zrem, Zrevrange, Zrevrank, Zscan, ZscanResult,
    Zscore,
};
pub use streams::{
    StreamEntry, StreamId, TrimStrategy, XAdd, XDel, XLen, XRange, XRead, XReadResult, XRevRange,
    XTrim,
};
pub use strings::{
    Append, Decr, DecrBy, Del, Echo, Exists, Expire, Get, GetDel, GetEx, GetExExpiration, GetRange,
    Incr, IncrBy, IncrByFloat, MGet, Mset, Msetnx, Ping, Psetex, Set, SetRange, Setex, Setnx,
    StrLen, Ttl,
};

// ============================================================================
// DEPRECATED COMMANDS (feature-gated with "deprecated")
// ============================================================================

#[cfg(feature = "deprecated")]
pub use lists::{BRPopLPush, RPopLPush};

#[cfg(feature = "deprecated")]
pub use strings::GetSet;
