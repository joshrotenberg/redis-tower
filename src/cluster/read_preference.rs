//! Read preference for cluster routing
//!
//! Determines whether commands should be routed to master or replica nodes.

/// Read preference for cluster command routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadPreference {
    /// Always route to master nodes (default)
    ///
    /// Ensures strong consistency but puts all load on masters.
    #[default]
    Master,

    /// Route read commands to replica nodes when available
    ///
    /// Reduces load on masters and improves read throughput, but may return
    /// slightly stale data due to replication lag.
    Replica,

    /// Prefer replicas for reads, fall back to master if no replicas available
    ///
    /// Best of both worlds - uses replicas when available but maintains
    /// availability if all replicas are down.
    PreferReplica,
}

/// Trait for commands that can be identified as read-only
///
/// Commands implementing this trait can be routed to replica nodes
/// when ReadPreference is set to Replica or PreferReplica.
///
/// By default, commands are assumed to be write commands (not read-only)
/// for safety. Specific read commands should explicitly implement this trait.
pub trait ReadOnly {
    /// Returns true if this command is read-only
    ///
    /// Read-only commands don't modify data and can be safely routed
    /// to replica nodes. Examples: GET, MGET, HGETALL, LRANGE, etc.
    fn is_read_only(&self) -> bool {
        false
    }
}
