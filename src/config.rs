//! Configuration, validation, and normalization.

use std::thread::available_parallelism;
use std::time::Duration;

/// How a handle-less [`push`](crate::ShardedSink::push) and
/// [`issue`](crate::ShardedSink::issue) family select a shard.
///
/// There is no random shard issuance. Selection is driven by deterministic,
/// thread-local state initialized once per OS thread.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ShardSelection {
    /// Stable home shard per runtime worker thread.
    ///
    /// Best locality when `shards` roughly matches Tokio worker threads and
    /// traffic is naturally spread across workers.
    ThreadLocalHome,

    /// Per-thread deterministic round-robin.
    ///
    /// Better when one runtime worker may produce a lot of traffic or when using
    /// a current-thread runtime. Still no randomness and no shared atomic after
    /// thread-local initialization.
    ThreadLocalRoundRobin,
}

/// Drain-side work-stealing policy.
///
/// Work stealing is strictly drain-side. It never changes the producer hot path.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkStealing {
    /// No work stealing. Each shard is drained only by its home worker.
    Off,

    /// Drain-side-only opportunistic stealing.
    ///
    /// After draining its own shard without filling the batch, an idle worker
    /// checks at most `max_victims_per_idle_tick` other shards and drains at most
    /// `max_items_per_victim` items from each victim before going back to its
    /// home shard.
    Opportunistic {
        /// Maximum number of victim shards inspected per top-up attempt.
        max_victims_per_idle_tick: usize,
        /// Maximum number of items pulled from each victim shard.
        max_items_per_victim: usize,
    },
}

/// Validation errors returned by [`ShardedSink::try_spawn`](crate::ShardedSink::try_spawn).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SinkConfigError {
    /// `name` was empty.
    EmptyName,
    /// `shards` was zero.
    ZeroShards,
    /// `ring_capacity` was zero.
    ZeroRingCapacity,
    /// `drain_batch` was zero.
    ZeroDrainBatch,
    /// `overload_check_interval` was zero.
    ZeroOverloadInterval,
    /// A [`WorkStealing::Opportunistic`] budget was zero.
    InvalidWorkStealing,
    /// `idle_sleep` was zero.
    ZeroIdleSleep,
}

impl std::fmt::Display for SinkConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::EmptyName => "sink name must not be empty",
            Self::ZeroShards => "shards must be greater than zero",
            Self::ZeroRingCapacity => "ring_capacity must be greater than zero",
            Self::ZeroDrainBatch => "drain_batch must be greater than zero",
            Self::ZeroOverloadInterval => "overload_check_interval must be greater than zero",
            Self::InvalidWorkStealing => "work-stealing budgets must be greater than zero",
            Self::ZeroIdleSleep => "DrainMode::Poll idle_sleep must be greater than zero",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for SinkConfigError {}

/// Construction-time configuration for a [`ShardedSink`](crate::ShardedSink).
///
/// Shard count and ring capacity are fixed at construction; there is no dynamic
/// resharding. See [`SinkConfig::default`] for recommended values.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Stable name used in overload notifications and metrics labels.
    pub name: &'static str,
    /// Number of independent shard rings.
    pub shards: usize,
    /// Capacity of each shard ring (items).
    pub ring_capacity: usize,
    /// Maximum items a drain worker batches before calling the sink action.
    pub drain_batch: usize,
    /// How often the overload monitor samples `dropped_full` counters.
    pub overload_check_interval: Duration,
    /// Shard-selection strategy for handle-less and issued pushes.
    pub shard_selection: ShardSelection,
    /// How long a drain worker sleeps after finding its shard empty before
    /// polling again. Drain workers always poll (`try_pop`); they never register
    /// a consumer waker, so producer pushes never pay a consumer-wake cost.
    /// Smaller values lower drain latency at idle; larger values reduce idle
    /// wakeups.
    pub idle_sleep: Duration,
    /// Drain-side work-stealing policy.
    pub work_stealing: WorkStealing,
    /// Maximum time [`shutdown`](crate::ShardedSink::shutdown) waits for workers.
    ///
    /// `None` waits indefinitely.
    pub shutdown_timeout: Option<Duration>,
}

impl Default for SinkConfig {
    fn default() -> Self {
        let shards = available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(1, 8);
        Self {
            name: "sink",
            shards,
            ring_capacity: 8192,
            drain_batch: 256,
            overload_check_interval: Duration::from_secs(5),
            shard_selection: ShardSelection::ThreadLocalRoundRobin,
            idle_sleep: Duration::from_micros(100),
            work_stealing: WorkStealing::Opportunistic {
                max_victims_per_idle_tick: 2,
                max_items_per_victim: 64,
            },
            shutdown_timeout: Some(Duration::from_secs(5)),
        }
    }
}

impl SinkConfig {
    /// Returns a copy of this config with `name` replaced.
    #[must_use]
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Validate and normalize this config.
    pub(crate) fn normalize(&self) -> Result<NormalizedConfig, SinkConfigError> {
        if self.name.is_empty() {
            return Err(SinkConfigError::EmptyName);
        }
        if self.shards == 0 {
            return Err(SinkConfigError::ZeroShards);
        }
        if self.ring_capacity == 0 {
            return Err(SinkConfigError::ZeroRingCapacity);
        }
        if self.drain_batch == 0 {
            return Err(SinkConfigError::ZeroDrainBatch);
        }
        if self.overload_check_interval.is_zero() {
            return Err(SinkConfigError::ZeroOverloadInterval);
        }
        if self.idle_sleep.is_zero() {
            return Err(SinkConfigError::ZeroIdleSleep);
        }

        // `drain_batch` can never exceed a ring's worth of items.
        let drain_batch = self.drain_batch.min(self.ring_capacity);

        let work_stealing = match self.work_stealing {
            WorkStealing::Off => NormalizedWorkStealing::Off,
            WorkStealing::Opportunistic {
                max_victims_per_idle_tick,
                max_items_per_victim,
            } => {
                if max_victims_per_idle_tick == 0 || max_items_per_victim == 0 {
                    return Err(SinkConfigError::InvalidWorkStealing);
                }
                // A single-shard sink has no victims to steal from.
                let max_victims = max_victims_per_idle_tick.min(self.shards.saturating_sub(1));
                if max_victims == 0 {
                    NormalizedWorkStealing::Off
                } else {
                    NormalizedWorkStealing::Opportunistic {
                        max_victims_per_idle_tick: max_victims,
                        max_items_per_victim: max_items_per_victim.min(drain_batch),
                    }
                }
            }
        };

        Ok(NormalizedConfig {
            name: self.name,
            shards: self.shards,
            ring_capacity: self.ring_capacity,
            drain_batch,
            overload_check_interval: self.overload_check_interval,
            shard_selection: self.shard_selection,
            idle_sleep: self.idle_sleep,
            work_stealing,
            shutdown_timeout: self.shutdown_timeout,
        })
    }
}

/// Normalized work-stealing policy with all clamps already applied.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum NormalizedWorkStealing {
    Off,
    Opportunistic {
        max_victims_per_idle_tick: usize,
        max_items_per_victim: usize,
    },
}

/// Validated, normalized configuration stored inside the sink so workers never
/// repeat clamps on the hot drain path.
#[derive(Debug, Clone)]
pub(crate) struct NormalizedConfig {
    pub(crate) name: &'static str,
    pub(crate) shards: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) drain_batch: usize,
    pub(crate) overload_check_interval: Duration,
    pub(crate) shard_selection: ShardSelection,
    pub(crate) idle_sleep: Duration,
    pub(crate) work_stealing: NormalizedWorkStealing,
    pub(crate) shutdown_timeout: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> SinkConfig {
        SinkConfig {
            name: "t",
            shards: 4,
            ring_capacity: 100,
            drain_batch: 32,
            overload_check_interval: Duration::from_secs(1),
            shard_selection: ShardSelection::ThreadLocalRoundRobin,
            idle_sleep: Duration::from_micros(100),
            work_stealing: WorkStealing::Off,
            shutdown_timeout: None,
        }
    }

    #[test]
    fn rejects_zero_idle_sleep() {
        assert_eq!(
            SinkConfig {
                idle_sleep: Duration::ZERO,
                ..base()
            }
            .normalize()
            .expect_err("should be invalid"),
            SinkConfigError::ZeroIdleSleep
        );
    }

    #[test]
    fn rejects_invalid_fields() {
        assert_eq!(
            SinkConfig { name: "", ..base() }
                .normalize()
                .expect_err("should be invalid"),
            SinkConfigError::EmptyName
        );
        assert_eq!(
            SinkConfig {
                shards: 0,
                ..base()
            }
            .normalize()
            .expect_err("should be invalid"),
            SinkConfigError::ZeroShards
        );
        assert_eq!(
            SinkConfig {
                ring_capacity: 0,
                ..base()
            }
            .normalize()
            .expect_err("should be invalid"),
            SinkConfigError::ZeroRingCapacity
        );
        assert_eq!(
            SinkConfig {
                drain_batch: 0,
                ..base()
            }
            .normalize()
            .expect_err("should be invalid"),
            SinkConfigError::ZeroDrainBatch
        );
        assert_eq!(
            SinkConfig {
                overload_check_interval: Duration::ZERO,
                ..base()
            }
            .normalize()
            .expect_err("should be invalid"),
            SinkConfigError::ZeroOverloadInterval
        );
        assert_eq!(
            SinkConfig {
                work_stealing: WorkStealing::Opportunistic {
                    max_victims_per_idle_tick: 0,
                    max_items_per_victim: 4,
                },
                ..base()
            }
            .normalize()
            .expect_err("should be invalid"),
            SinkConfigError::InvalidWorkStealing
        );
    }

    #[test]
    fn clamps_drain_batch_to_ring_capacity() {
        let cfg = SinkConfig {
            ring_capacity: 8,
            drain_batch: 256,
            ..base()
        }
        .normalize()
        .expect("valid");
        assert_eq!(cfg.drain_batch, 8);
    }

    #[test]
    fn clamps_steal_budgets() {
        let cfg = SinkConfig {
            shards: 3,
            ring_capacity: 100,
            drain_batch: 10,
            work_stealing: WorkStealing::Opportunistic {
                max_victims_per_idle_tick: 100,
                max_items_per_victim: 100,
            },
            ..base()
        }
        .normalize()
        .expect("valid");
        // victims clamped to shards - 1 (2); items clamped to drain_batch (10).
        assert_eq!(
            cfg.work_stealing,
            NormalizedWorkStealing::Opportunistic {
                max_victims_per_idle_tick: 2,
                max_items_per_victim: 10,
            }
        );
    }

    #[test]
    fn single_shard_disables_stealing() {
        let cfg = SinkConfig {
            shards: 1,
            work_stealing: WorkStealing::Opportunistic {
                max_victims_per_idle_tick: 2,
                max_items_per_victim: 4,
            },
            ..base()
        }
        .normalize()
        .expect("valid");
        assert_eq!(cfg.work_stealing, NormalizedWorkStealing::Off);
    }
}
