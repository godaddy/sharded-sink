//! The sink itself: [`SinkAction`], [`ShardedSink`], and its internal state.
//!
//! [`ShardedSink`] is a bounded, lossy, sharded fan-in sink built on a set of
//! `crossbeam_queue::ArrayQueue` rings (one per shard). Producers push to a
//! shard via a cheap handle; one poll-drain worker per shard pulls batches off
//! the producer critical path and hands them to a [`SinkAction`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{NormalizedConfig, ShardSelection, SinkConfig, SinkConfigError};
use crate::error::ShutdownError;
use crate::handle::ShardHandle;
use crate::overload::{LogErrorOverload, OverloadAction};
use crate::selection;
use crate::stats::{ShardStats, SinkStats};
use crate::{monitor, worker};

/// What to do with a batch of drained items.
///
/// Runs on drain workers, never on producer tasks. The worker owns and reuses
/// the `Vec<T>` allocation across calls; implementations should consume or
/// inspect the items during the call and must not retain references into the
/// batch after the returned future completes.
///
/// `drain` may perform I/O and await. Slow drain work delays future drains and
/// can cause full rings, but it never blocks producer tasks. A worker yields to
/// the runtime after every batch, so a `drain` that does not itself await will
/// not starve the runtime.
///
/// A panic inside `drain` is caught per batch: the offending batch is dropped
/// (and logged via `tracing`) and the worker keeps draining that shard rather
/// than dying. Implementations should still avoid panicking — a repeatedly
/// panicking `drain` silently loses every batch.
pub trait SinkAction<T>: Send + Sync + 'static {
    /// Process one batch of drained items.
    fn drain(&self, batch: &mut Vec<T>) -> impl Future<Output = ()> + Send;
}

/// Internal shutdown bookkeeping, making `shutdown()` idempotent and
/// concurrent-safe.
struct ShutdownState {
    /// Worker and monitor join handles, taken by the first `shutdown()` call.
    handles: Option<Vec<JoinHandle<()>>>,
    /// Cached result, returned by subsequent `shutdown()` calls.
    result: Option<Result<(), ShutdownError>>,
}

/// Shared sink state behind an `Arc`.
struct Inner<T> {
    cfg: NormalizedConfig,
    queues: Box<[Arc<ArrayQueue<T>>]>,
    dropped: Box<[Arc<CachePadded<AtomicU64>>]>,
    /// Global round-robin cursor for [`ShardedSink::issue`]. This is *not* the
    /// hot path (handles are issued once per request/connection), so a shared
    /// atomic here is fine and spreads handles across shards regardless of which
    /// thread issues them.
    issue_cursor: AtomicUsize,
    cancel: CancellationToken,
    shutdown: tokio::sync::Mutex<ShutdownState>,
}

impl<T> std::fmt::Debug for Inner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedSink")
            .field("name", &self.cfg.name)
            .field("shards", &self.cfg.shards)
            .field("ring_capacity", &self.cfg.ring_capacity)
            .finish_non_exhaustive()
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // Best-effort cleanup if the caller forgot to call `shutdown()`. We
        // cannot await worker completion here, but we can stop the monitor and
        // signal workers so they do not run forever if the runtime keeps going.
        self.cancel.cancel();
    }
}

/// A bounded, lossy, sharded fan-in sink with flat producer latency.
///
/// Backed by one `crossbeam_queue::ArrayQueue` per shard. Clone freely; all
/// clones share one set of shards, workers, and counters. See the
/// [crate docs](crate) for the full behavioral contract.
///
/// Items only need `T: Send + 'static`. There is no per-shard "closed" state:
/// producers and the drain worker share each shard's queue, so a push after
/// shutdown is accepted into the queue (until full) rather than rejected.
pub struct ShardedSink<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for ShardedSink<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> std::fmt::Debug for ShardedSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.inner, f)
    }
}

impl<T> ShardedSink<T>
where
    T: Send + 'static,
{
    /// Validate `cfg`, build the shards, and spawn drain workers plus the
    /// overload monitor on the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`SinkConfigError`] if the configuration is invalid.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context (workers are spawned
    /// with [`tokio::spawn`]). The runtime must also have the **time driver**
    /// enabled (e.g. `enable_all`/`enable_time`): drain workers, the overload
    /// monitor, and `shutdown` all use Tokio timers and will panic on a runtime
    /// built without it.
    pub fn try_spawn<A, O>(
        cfg: SinkConfig,
        action: Arc<A>,
        overload: Arc<O>,
    ) -> Result<Self, SinkConfigError>
    where
        A: SinkAction<T>,
        O: OverloadAction,
    {
        let cfg = cfg.normalize()?;
        let shards = cfg.shards;

        let queues: Box<[Arc<ArrayQueue<T>>]> = (0..shards)
            .map(|_| Arc::new(ArrayQueue::new(cfg.ring_capacity)))
            .collect();
        let dropped: Box<[Arc<CachePadded<AtomicU64>>]> = (0..shards)
            .map(|_| Arc::new(CachePadded::new(AtomicU64::new(0))))
            .collect();

        let cancel = CancellationToken::new();

        // Shared view of every queue, handed to each worker for draining its
        // home shard and stealing from others.
        let all_queues: Arc<[Arc<ArrayQueue<T>>]> = queues.iter().map(Arc::clone).collect();

        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(shards + 1);
        for home in 0..shards {
            let queues = Arc::clone(&all_queues);
            let action = Arc::clone(&action);
            let cancel = cancel.clone();
            let drain_batch = cfg.drain_batch;
            let idle_sleep = cfg.idle_sleep;
            let work_stealing = cfg.work_stealing;
            handles.push(tokio::spawn(async move {
                worker::run_worker::<T, A>(
                    home,
                    queues,
                    action,
                    cancel,
                    drain_batch,
                    idle_sleep,
                    work_stealing,
                )
                .await;
            }));
        }

        let monitor_counters: Vec<Arc<CachePadded<AtomicU64>>> =
            dropped.iter().map(Arc::clone).collect();
        {
            let name = cfg.name;
            let interval = cfg.overload_check_interval;
            let cancel = cancel.clone();
            handles.push(tokio::spawn(async move {
                monitor::run_monitor(name, interval, monitor_counters, overload, cancel).await;
            }));
        }

        Ok(Self {
            inner: Arc::new(Inner {
                cfg,
                queues,
                dropped,
                issue_cursor: AtomicUsize::new(0),
                cancel,
                shutdown: tokio::sync::Mutex::new(ShutdownState {
                    handles: Some(handles),
                    result: None,
                }),
            }),
        })
    }

    /// Like [`try_spawn`](Self::try_spawn), but panics on an invalid config.
    ///
    /// # Panics
    ///
    /// Panics if `cfg` is invalid, or if called outside a Tokio runtime.
    pub fn spawn<A, O>(cfg: SinkConfig, action: Arc<A>, overload: Arc<O>) -> Self
    where
        A: SinkAction<T>,
        O: OverloadAction,
    {
        Self::try_spawn(cfg, action, overload).expect("ShardedSink config should be valid")
    }

    /// Like [`try_spawn`](Self::try_spawn) using the default
    /// [`LogErrorOverload`] action.
    ///
    /// # Errors
    ///
    /// Returns [`SinkConfigError`] if the configuration is invalid.
    pub fn try_spawn_default_overload<A>(
        cfg: SinkConfig,
        action: Arc<A>,
    ) -> Result<Self, SinkConfigError>
    where
        A: SinkAction<T>,
    {
        Self::try_spawn(cfg, action, Arc::new(LogErrorOverload))
    }

    /// Like [`spawn`](Self::spawn) using the default [`LogErrorOverload`] action.
    ///
    /// # Panics
    ///
    /// Panics if `cfg` is invalid, or if called outside a Tokio runtime.
    pub fn spawn_default_overload<A>(cfg: SinkConfig, action: Arc<A>) -> Self
    where
        A: SinkAction<T>,
    {
        Self::spawn(cfg, action, Arc::new(LogErrorOverload))
    }

    /// Build a [`ShardHandle`] bound to the given shard.
    #[inline]
    fn handle_for(&self, shard: usize) -> ShardHandle<T> {
        ShardHandle {
            queue: Arc::clone(&self.inner.queues[shard]),
            dropped: Arc::clone(&self.inner.dropped[shard]),
        }
    }

    /// Issue a reusable producer handle, spread across shards by a global
    /// round-robin cursor.
    ///
    /// This is the preferred API for hot producers: issue one handle per
    /// request, connection, or producer object and reuse it for all events from
    /// that producer. Issuance uses a single shared atomic (not the hot path),
    /// so handles spread evenly across shards no matter which thread issues
    /// them — including a burst of brand-new producer threads.
    #[must_use]
    pub fn issue(&self) -> ShardHandle<T> {
        let shard = self.inner.issue_cursor.fetch_add(1, Ordering::Relaxed) % self.inner.cfg.shards;
        self.handle_for(shard)
    }

    /// Issue a handle bound to the current thread's stable home shard.
    ///
    /// Favors locality over burst smoothing. Use when traffic is already evenly
    /// spread across runtime worker threads.
    #[must_use]
    pub fn issue_thread_local(&self) -> ShardHandle<T> {
        let shard = selection::home_shard(self.inner.cfg.shards);
        self.handle_for(shard)
    }

    /// Handle-less push.
    ///
    /// Selects a shard via [`SinkConfig::shard_selection`] and performs the same
    /// immediate `push` as [`ShardHandle::push`]. Slightly more work than a held
    /// handle (it selects a shard and indexes the shard arrays), but still
    /// non-blocking and allocation-free.
    ///
    /// Returns `true` if the item was accepted into the ring at that instant.
    #[inline]
    #[must_use]
    pub fn push(&self, item: T) -> bool {
        let shards = self.inner.cfg.shards;
        let shard = match self.inner.cfg.shard_selection {
            ShardSelection::ThreadLocalHome => selection::home_shard(shards),
            ShardSelection::ThreadLocalRoundRobin => selection::next_round_robin(shards),
        };
        if self.inner.queues[shard].push(item).is_ok() {
            true
        } else {
            self.inner.dropped[shard].fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Aggregate drop statistics across all shards.
    #[must_use]
    pub fn stats(&self) -> SinkStats {
        SinkStats {
            shards: self.inner.cfg.shards,
            dropped: self.dropped_total(),
        }
    }

    /// Per-shard drop statistics.
    #[must_use]
    pub fn stats_per_shard(&self) -> Vec<ShardStats> {
        (0..self.inner.cfg.shards)
            .map(|shard| ShardStats {
                shard,
                dropped: self.inner.dropped[shard].load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Total items shed (ring full).
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.inner
            .dropped
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }

    /// Number of shards.
    #[must_use]
    pub fn shards(&self) -> usize {
        self.inner.cfg.shards
    }

    /// Stop drain workers and the overload monitor, drain visible buffered
    /// items, and wait for internal tasks to exit.
    ///
    /// Idempotent: repeated calls return the same cached result.
    ///
    /// The caller must quiesce producers first (stop accepting work and drop
    /// producer handles). Pushes racing with shutdown are outside the
    /// graceful-delivery contract; because the queues outlive the workers, a
    /// late push is accepted into a queue but may never be drained.
    ///
    /// If producers are *not* quiesced and keep pushing faster than a shard
    /// drains, that shard's final drain cannot reach empty. With a
    /// `shutdown_timeout` set, this returns [`ShutdownError::TimedOut`]; with
    /// `shutdown_timeout: None` it can block indefinitely. Always quiesce first.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::TimedOut`] if `shutdown_timeout` elapsed before
    /// all tasks finished, or [`ShutdownError::WorkerPanicked`] if a task
    /// panicked.
    pub async fn shutdown(&self) -> Result<(), ShutdownError> {
        let mut guard = self.inner.shutdown.lock().await;
        if let Some(result) = &guard.result {
            return result.clone();
        }

        // Signal workers (final drain) and the monitor (exit) to stop.
        self.inner.cancel.cancel();

        let handles = guard.handles.take().unwrap_or_default();
        let result = join_workers(handles, self.inner.cfg.shutdown_timeout).await;
        guard.result = Some(result.clone());
        result
    }
}

/// Join all internal tasks, honoring an optional timeout, and reporting panics.
async fn join_workers(
    handles: Vec<JoinHandle<()>>,
    timeout: Option<std::time::Duration>,
) -> Result<(), ShutdownError> {
    let join_all = async {
        let mut panicked = false;
        for handle in handles {
            if handle.await.is_err() {
                panicked = true;
            }
        }
        panicked
    };

    let panicked = match timeout {
        Some(dur) => match tokio::time::timeout(dur, join_all).await {
            Ok(panicked) => panicked,
            Err(_elapsed) => return Err(ShutdownError::TimedOut),
        },
        None => join_all.await,
    };

    if panicked {
        Err(ShutdownError::WorkerPanicked)
    } else {
        Ok(())
    }
}
