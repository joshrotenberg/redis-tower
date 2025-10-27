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

pub mod acl;
pub mod bitmap;
pub mod cluster;
pub mod connection;
pub mod functions;
pub mod geo;
pub mod hashes;
pub mod hyperloglog;
#[cfg(feature = "serde-json")]
pub mod json;
pub mod keys;
pub mod latency;
pub mod lists;
pub mod module;
pub mod pubsub;
pub mod scan;
pub mod scripting;
pub mod server;
pub mod sets;
pub mod sorted_sets;
pub mod streams;
pub mod strings;
pub mod transactions;

pub use acl::{
    AclCat, AclDelUser, AclDryRun, AclGenPass, AclGetUser, AclHelp, AclList, AclLoad, AclLog,
    AclSave, AclSetUser, AclUsers, AclWhoAmI,
};
pub use bitmap::{BitCount, BitOp, BitOpCmd, BitPos, Bitfield, BitfieldRo, GetBit, SetBit};
pub use cluster::{
    ClusterAddSlots, ClusterAddSlotsRange, ClusterBumpEpoch, ClusterCountFailureReports,
    ClusterCountKeysInSlot, ClusterDelSlots, ClusterDelSlotsRange, ClusterFailover,
    ClusterFailoverOption, ClusterFlushSlots, ClusterForget, ClusterGetKeysInSlot, ClusterHelp,
    ClusterInfo, ClusterKeySlot, ClusterLinks, ClusterMeet, ClusterMyId, ClusterMyShardId,
    ClusterNodes, ClusterReplicas, ClusterReplicate, ClusterReset, ClusterSaveConfig,
    ClusterSetConfigEpoch, ClusterSetSlot, ClusterSetSlotState, ClusterShards, ClusterSlaves,
    ClusterSlotStats, ClusterSlots,
};
pub use connection::{
    Asking, Auth, AuthAcl, ClientCaching, ClientGetName, ClientGetRedir, ClientHelp, ClientId,
    ClientInfo, ClientKill, ClientKillFilter, ClientList, ClientNoEvict, ClientNoTouch,
    ClientPause, ClientReply, ClientSetInfo, ClientSetName, ClientTracking, ClientTrackingInfo,
    ClientUnblock, ClientUnpause, Hello, Quit, ReadOnly, ReadWrite, Reset, Select,
};
pub use functions::{
    FCall, FCallReadOnly, FunctionDelete, FunctionDump, FunctionFlush, FunctionHelp, FunctionKill,
    FunctionList, FunctionLoad, FunctionRestore, FunctionStats,
};
pub use geo::{
    GeoAdd, GeoCoordinate, GeoDist, GeoHash, GeoItem, GeoPos, GeoRadius, GeoRadiusByMember,
    GeoRadiusByMemberReadOnly, GeoRadiusReadOnly, GeoSearch, GeoSearchStore, GeoUnit,
};
pub use hashes::{
    HDel, HExists, HExpire, HExpireAt, HExpireTime, HGet, HGetAll, HGetDel, HGetEx, HIncrBy,
    HIncrByFloat, HKeys, HLen, HMGet, HMSet, HPExpire, HPExpireAt, HPExpireTime, HPTtl, HPersist,
    HRandField, HSet, HSetEx, HSetNx, HStrLen, HTtl, HVals,
};
pub use hyperloglog::{PfAdd, PfCount, PfDebug, PfMerge, PfSelfTest};
#[cfg(feature = "serde-json")]
pub use json::{GetJson, MSetJson, SetJson};
pub use keys::{
    Copy, Dump, ExpireAt, ExpireTime, Keys, Migrate, Move, ObjectEncoding, ObjectFreq,
    ObjectIdleTime, ObjectRefCount, PExpire, PExpireAt, PExpireTime, PTtl, Persist, Rename,
    RenameNx, Restore, RestoreAsking, Scan, Sort, SortOrder, SortResult, SortRo, Touch, Type,
    Unlink, WaitAof,
};
pub use latency::{
    LatencyDoctor, LatencyGraph, LatencyHelp, LatencyHistogram, LatencyHistory, LatencyLatest,
    LatencyReset,
};
pub use lists::{
    BLMPop, BLMove, BLPop, BRPop, InsertPosition, LIndex, LInsert, LLen, LMPop, LMPopResult, LMove,
    LPop, LPos, LPush, LPushX, LRange, LRem, LSet, LTrim, MoveDirection, RPop, RPush, RPushX,
};
pub use module::{ModuleInfo, ModuleList, ModuleLoad, ModuleLoadEx, ModuleUnload};
pub use pubsub::{Publish, PubsubHelp, PubsubNumpat, PubsubNumsub};
pub use scripting::{
    Eval, EvalReadOnly, EvalSha, EvalShaReadOnly, ScriptExists, ScriptFlush, ScriptHelp, ScriptLoad,
};
pub use server::{
    BgRewriteAof, BgSave, CommandCmd, CommandCount, CommandDocs, CommandGetKeys,
    CommandGetKeysAndFlags, CommandHelp, CommandInfo, CommandList, CommandListFilter, ConfigGet,
    ConfigHelp, ConfigResetStat, ConfigRewrite, ConfigSet, DbSize, Debug, DebugSubcommand,
    Failover, FlushAll, FlushDb, Info, KeyWithFlags, LastSave, Lolwut, MemoryDoctor, MemoryHelp,
    MemoryMallocStats, MemoryPurge, MemoryStats, MemoryUsage, ModuleHelp, ObjectHelp, PSync,
    RandomKey, ReplConf, ReplicaOf, Role, Save, Shutdown, SlaveOf, SlowlogEntry, SlowlogGet,
    SlowlogHelp, SlowlogLen, SlowlogReset, SwapDb, Sync, Time, Wait,
};
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
    ConsumerInfo, GroupInfo, StreamEntries, StreamEntry, StreamInfo, XAck, XAckDel, XAdd,
    XAutoClaim, XClaim, XDel, XDelEx, XGroupCreate, XGroupCreateConsumer, XGroupDelConsumer,
    XGroupDestroy, XGroupHelp, XGroupSetId, XInfoConsumers, XInfoGroups, XInfoHelp, XInfoStream,
    XLen, XPending, XRange, XRead, XReadGroup, XRevRange, XSetId, XTrim,
};
pub use strings::{
    Append, Decr, DecrBy, Del, Echo, Exists, Expire, Get, GetDel, GetEx, GetExExpiration, GetRange,
    Incr, IncrBy, IncrByFloat, Lcs, MGet, Mset, Msetnx, Ping, Psetex, Set, SetRange, Setex, Setnx,
    StrLen, Substr, Ttl,
};
pub use transactions::{Discard, Exec, Multi, Unwatch, Watch};

// ============================================================================
// DEPRECATED COMMANDS (feature-gated with "deprecated")
// ============================================================================

#[cfg(feature = "deprecated")]
pub use lists::{BRPopLPush, RPopLPush};

#[cfg(feature = "deprecated")]
pub use strings::GetSet;
