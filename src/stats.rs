//! Observability snapshots.
//!
//! All counters are *approximate observation counters*, sampled with relaxed
//! atomics. They are not synchronized with the producer hot path and may lag a
//! racing push by a few instructions.

/// Aggregate drop statistics across all shards.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SinkStats {
    /// Number of shards.
    pub shards: usize,
    /// Total items dropped because the destination ring was full (overload).
    pub dropped: u64,
}

/// Per-shard drop statistics.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ShardStats {
    /// Shard index.
    pub shard: usize,
    /// Items dropped on this shard because its ring was full (overload).
    pub dropped: u64,
}
