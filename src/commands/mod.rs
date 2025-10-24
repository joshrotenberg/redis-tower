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
pub mod transactions;

pub use bitmap::{BitCount, BitOp, BitOpCmd, BitPos, GetBit, SetBit};
pub use connection::{
    Auth, AuthAcl, ClientGetName, ClientSetName, Quit, ReadOnly, ReadWrite, Select,
};
pub use geo::{
    GeoAdd, GeoCoordinate, GeoDist, GeoHash, GeoItem, GeoPos, GeoSearch, GeoSearchStore, GeoUnit,
};
pub use hashes::{
    HDel, HExists, HGet, HGetAll, HIncrBy, HIncrByFloat, HKeys, HLen, HMGet, HRandField, HSet,
    HSetNx, HStrLen, HVals,
};
pub use hyperloglog::{PfAdd, PfCount, PfMerge};
pub use keys::{
    Copy, Dump, ExpireAt, ExpireTime, Keys, Move, ObjectEncoding, ObjectFreq, ObjectIdleTime,
    ObjectRefCount, PExpire, PExpireAt, PExpireTime, PTtl, Persist, Rename, RenameNx, Restore,
    Sort, SortOrder, SortResult, Touch, Type, Unlink,
};
pub use lists::{
    BLMPop, BLMove, BLPop, BRPop, InsertPosition, LIndex, LInsert, LLen, LMPop, LMPopResult, LMove,
    LPop, LPos, LPush, LPushX, LRange, LRem, LSet, LTrim, MoveDirection, RPop, RPush, RPushX,
};
pub use pubsub::{Publish, PubsubNumpat, PubsubNumsub};
pub use scripting::{Eval, EvalSha, ScriptExists, ScriptFlush, ScriptLoad};
pub use server::{BgSave, DbSize, FlushAll, FlushDb, Info, LastSave, RandomKey, Save, Time, Wait};
pub use sets::{
    SDiffStore, SInterCard, SInterStore, SMIsMember, SMove, SPop, SRandMember, SUnionStore, Sadd,
    Scard, Sdiff, Sinter, Sismember, Smembers, Srem, Sscan, SscanResult, Sunion,
};
pub use sorted_sets::{
    BZMPop, BZPopMax, BZPopMin, ZCount, ZDiffStore, ZInterStore, ZLexCount, ZMPop, ZMScore,
    ZPopMax, ZPopMin, ZRandMember, ZRangeByLex, ZRangeByScore, ZRemRangeByLex, ZRemRangeByRank,
    ZRemRangeByScore, ZRevRangeByLex, ZRevRangeByScore, ZUnionStore, Zadd, Zcard, Zdiff, Zincrby,
    Zinter, Zrange, ZrangeResult, Zrank, Zrem, Zrevrange, Zrevrank, Zscan, ZscanResult, Zscore,
    Zunion,
};
pub use streams::{
    StreamEntry, StreamId, TrimStrategy, XAck, XAdd, XClaim, XDel, XGroupCreate, XGroupDestroy,
    XLen, XPending, XRange, XRead, XReadGroup, XReadResult, XRevRange, XTrim,
};
pub use strings::{
    Append, Decr, DecrBy, Del, Echo, Exists, Expire, Get, GetDel, GetEx, GetExExpiration, GetRange,
    Incr, IncrBy, IncrByFloat, Lcs, MGet, Mset, Msetnx, Ping, Psetex, Set, SetRange, Setex, Setnx,
    StrLen, Ttl,
};
pub use transactions::{Discard, Exec, Multi, Unwatch, Watch};

// ============================================================================
// DEPRECATED COMMANDS (feature-gated with "deprecated")
// ============================================================================

#[cfg(feature = "deprecated")]
pub use lists::{BRPopLPush, RPopLPush};

#[cfg(feature = "deprecated")]
pub use strings::GetSet;
