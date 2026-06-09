//! Reusable producer handle and the producer hot path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;

/// A reusable producer handle bound to a single shard.
///
/// This is the preferred API for hot producers: issue one handle per request,
/// connection, worker-local service, or producer object and reuse it for all
/// events from that producer. A held handle performs no shard selection.
///
/// Cloning a handle is cheap (a queue `Arc` clone plus one counter `Arc` clone)
/// and the clone targets the same shard.
pub struct ShardHandle<T> {
    pub(crate) queue: Arc<ArrayQueue<T>>,
    pub(crate) dropped: Arc<CachePadded<AtomicU64>>,
}

impl<T> Clone for ShardHandle<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            dropped: Arc::clone(&self.dropped),
        }
    }
}

impl<T> std::fmt::Debug for ShardHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardHandle").finish_non_exhaustive()
    }
}

impl<T> ShardHandle<T> {
    /// Push an item into this handle's shard.
    ///
    /// Returns `true` if the item was accepted into the ring at that instant,
    /// `false` if it was shed because the ring was full (which increments this
    /// shard's `dropped` counter).
    ///
    /// `true` means "accepted into a ring," not "durably delivered." This call
    /// never awaits, never blocks, takes no crate-level lock, allocates nothing,
    /// and on success touches no counter — it is a single `ArrayQueue::push`.
    #[inline]
    #[must_use]
    pub fn push(&self, item: T) -> bool {
        if self.queue.push(item).is_ok() {
            true
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}
