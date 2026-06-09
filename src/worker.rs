//! Drain worker task.
//!
//! Each shard has one home worker. Workers **poll** their home shard's
//! `ArrayQueue` with `pop` and sleep `idle_sleep` only when it is empty; they
//! never register a waker, so producer pushes never pay a wake cost. After
//! draining its own shard a worker may steal a bounded number of items from
//! other shards to top up its batch, then hands the batch to the [`SinkAction`].
//!
//! `crossbeam_queue::ArrayQueue` is MPMC, so concurrent `pop` from multiple
//! workers is safe — drain-side stealing across shards is sound.

use std::sync::Arc;
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use tokio_util::sync::CancellationToken;

use crate::config::NormalizedWorkStealing;
use crate::sink::SinkAction;

/// Drain `queue` into `buf` until the batch is full or the ring is empty.
#[inline]
fn drain_burst<T>(queue: &ArrayQueue<T>, buf: &mut Vec<T>, drain_batch: usize) {
    while buf.len() < drain_batch {
        match queue.pop() {
            Some(item) => buf.push(item),
            None => break,
        }
    }
}

/// Top up `buf` by stealing from non-home shards, within budget. Returns the
/// advanced victim cursor. Deterministic round-robin victim selection.
fn steal<T>(
    queues: &[Arc<ArrayQueue<T>>],
    home: usize,
    mut victim_cursor: usize,
    buf: &mut Vec<T>,
    drain_batch: usize,
    max_victims: usize,
    max_items_per_victim: usize,
) -> usize {
    let shards = queues.len();
    let mut victims_tried = 0;
    let mut probes = 0;
    // `probes < shards` bounds the loop so skipping the home shard can never spin.
    while victims_tried < max_victims && probes < shards && buf.len() < drain_batch {
        let victim = victim_cursor % shards;
        victim_cursor = victim_cursor.wrapping_add(1);
        probes += 1;
        if victim == home {
            continue;
        }
        victims_tried += 1;

        let mut taken = 0;
        while taken < max_items_per_victim && buf.len() < drain_batch {
            match queues[victim].pop() {
                Some(item) => {
                    buf.push(item);
                    taken += 1;
                }
                None => break,
            }
        }
    }
    victim_cursor
}

/// Top up `buf` from victim shards if it is underfilled and stealing is on.
#[inline]
fn maybe_steal<T>(
    queues: &[Arc<ArrayQueue<T>>],
    home: usize,
    victim_cursor: usize,
    buf: &mut Vec<T>,
    drain_batch: usize,
    work_stealing: NormalizedWorkStealing,
) -> usize {
    if buf.len() < drain_batch
        && let NormalizedWorkStealing::Opportunistic {
            max_victims_per_idle_tick,
            max_items_per_victim,
        } = work_stealing
    {
        return steal(
            queues,
            home,
            victim_cursor,
            buf,
            drain_batch,
            max_victims_per_idle_tick,
            max_items_per_victim,
        );
    }
    victim_cursor
}

/// Run a single shard's drain worker until cancelled and its home shard drained.
pub(crate) async fn run_worker<T, A>(
    home: usize,
    queues: Arc<[Arc<ArrayQueue<T>>]>,
    action: Arc<A>,
    cancel: CancellationToken,
    drain_batch: usize,
    idle_sleep: Duration,
    work_stealing: NormalizedWorkStealing,
) where
    T: Send + 'static,
    A: SinkAction<T>,
{
    let home_q = &queues[home];
    let mut buf: Vec<T> = Vec::with_capacity(drain_batch);
    let mut victim_cursor = home.wrapping_add(1);

    loop {
        if cancel.is_cancelled() {
            break;
        }
        drain_burst(home_q, &mut buf, drain_batch);
        victim_cursor = maybe_steal(
            &queues,
            home,
            victim_cursor,
            &mut buf,
            drain_batch,
            work_stealing,
        );
        if buf.is_empty() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = tokio::time::sleep(idle_sleep) => {}
            }
        } else {
            action.drain(&mut buf).await;
            buf.clear();
        }
    }

    // Final drain: this worker is the sole owner of its home shard now. Drain it
    // to empty with no cross-shard stealing, so each shard has exactly one final
    // owner.
    loop {
        drain_burst(home_q, &mut buf, drain_batch);
        if buf.is_empty() {
            break;
        }
        action.drain(&mut buf).await;
        buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `shards` rings, each pre-filled with `fill` items.
    fn filled(shards: usize, capacity: usize, fill: usize) -> Vec<Arc<ArrayQueue<u64>>> {
        (0..shards)
            .map(|s| {
                let q = Arc::new(ArrayQueue::new(capacity));
                for i in 0..fill {
                    q.push((s * 1000 + i) as u64).expect("capacity");
                }
                q
            })
            .collect()
    }

    #[test]
    fn drain_burst_stops_at_batch() {
        let qs = filled(1, 100, 50);
        let mut buf = Vec::new();
        drain_burst(&qs[0], &mut buf, 10);
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn drain_burst_stops_when_empty() {
        let qs = filled(1, 100, 5);
        let mut buf = Vec::new();
        drain_burst(&qs[0], &mut buf, 100);
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn steal_respects_victim_and_item_budgets() {
        let qs = filled(4, 100, 100);
        let mut buf = Vec::new();
        let _cursor = steal(&qs, 0, 1, &mut buf, 1000, 2, 3);
        assert!(buf.len() <= 2 * 3);
        assert_eq!(buf.len(), 6);
    }

    #[test]
    fn steal_skips_home_shard() {
        let mut qs = filled(1, 100, 100); // shard 0, full
        for _ in 0..3 {
            qs.push(Arc::new(ArrayQueue::new(100)));
        }
        let mut buf = Vec::new();
        steal(&qs, 0, 1, &mut buf, 1000, 3, 10);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn steal_is_bounded_when_batch_nearly_full() {
        let qs = filled(4, 100, 100);
        let mut buf = Vec::new();
        steal(&qs, 0, 1, &mut buf, 2, 4, 100);
        assert_eq!(buf.len(), 2);
    }
}
