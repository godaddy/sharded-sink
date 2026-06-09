//! Overload monitor task.
//!
//! Samples only `dropped_full` counters. `dropped_closed` is lifecycle
//! information and never drives overload notifications.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::overload::{OverloadAction, Overloaded};

/// Run the overload monitor until cancelled.
pub(crate) async fn run_monitor<O>(
    name: &'static str,
    interval: Duration,
    full_counters: Vec<Arc<CachePadded<AtomicU64>>>,
    overload: Arc<O>,
    cancel: CancellationToken,
) where
    O: OverloadAction,
{
    let mut last_full: u64 = 0;
    let mut ticker = tokio::time::interval(interval);
    // The first `tick()` resolves immediately; sampling a zero delta then is
    // harmless. Skip missed ticks so a slow scheduler cannot create a burst of
    // back-to-back samples.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {
                let total_full: u64 = full_counters
                    .iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .sum();
                let delta = total_full.saturating_sub(last_full);
                if delta > 0 {
                    overload.on_overload(Overloaded {
                        sink: name,
                        delta_full: delta,
                        total_full,
                        interval,
                    });

                    #[cfg(feature = "metrics")]
                    {
                        metrics::counter!("sharded_sink.dropped_full", "sink" => name.to_string())
                            .increment(delta);
                    }
                }
                last_full = total_full;
            }
        }
    }
}
