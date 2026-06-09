//! Thread-local shard selection.
//!
//! There is no random shard issuance. Each OS thread gets a stable seed and a
//! monotonic cursor, both initialized lazily on first use. Selection is computed
//! as `seed % shards` (home) or `cursor % shards` (round-robin), so the same
//! thread-local state serves any number of sinks with differing shard counts.
//!
//! After thread-local initialization, selection touches no shared atomic: `home`
//! reads a `Cell`, and round-robin reads and bumps a `Cell`.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Process-global counter handing out a stable, unique seed per OS thread.
///
/// Touched exactly once per thread (first time that thread selects a shard).
static GLOBAL_THREAD_SEED: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Stable per-thread seed; `None` until first use. Drives [`ShardSelection::ThreadLocalHome`].
    static THREAD_SEED: Cell<Option<usize>> = const { Cell::new(None) };
    /// Monotonic per-thread cursor. Drives round-robin selection and `issue()`.
    static TLS_CURSOR: Cell<usize> = const { Cell::new(0) };
}

/// Returns this thread's stable seed, initializing it on first use.
#[inline]
fn thread_seed() -> usize {
    THREAD_SEED.with(|cell| match cell.get() {
        Some(seed) => seed,
        None => {
            let seed = GLOBAL_THREAD_SEED.fetch_add(1, Ordering::Relaxed);
            cell.set(Some(seed));
            seed
        }
    })
}

/// Stable home shard for the current thread.
///
/// Deterministic and constant for the life of the thread once initialized.
#[inline]
pub(crate) fn home_shard(shards: usize) -> usize {
    thread_seed() % shards
}

/// Next shard for the current thread under deterministic round-robin.
///
/// Reads the thread-local cursor, advances it, and maps it onto `shards`.
#[inline]
pub(crate) fn next_round_robin(shards: usize) -> usize {
    TLS_CURSOR.with(|cell| {
        let cursor = cell.get();
        cell.set(cursor.wrapping_add(1));
        cursor % shards
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn home_shard_is_stable_on_same_thread() {
        let first = home_shard(8);
        for _ in 0..100 {
            assert_eq!(home_shard(8), first);
        }
        assert!(first < 8);
    }

    #[test]
    fn round_robin_covers_all_shards_in_one_cycle() {
        let shards = 8;
        // From any starting cursor, `shards` consecutive selections cover all
        // shards exactly once.
        let seen: HashSet<usize> = (0..shards).map(|_| next_round_robin(shards)).collect();
        assert_eq!(seen.len(), shards);
    }

    #[test]
    fn round_robin_is_in_bounds() {
        for _ in 0..1000 {
            assert!(next_round_robin(3) < 3);
        }
    }
}
