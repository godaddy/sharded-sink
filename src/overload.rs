//! Overload notification: trait, event, and default action.

use std::time::Duration;

/// A sampled overload notification.
///
/// Describes sink health over one monitor interval. It is intentionally not
/// generic over the item type; overload notifications describe sink health, not
/// individual payloads.
#[derive(Debug, Clone, Copy)]
pub struct Overloaded<'sink> {
    /// The sink's configured name.
    pub sink: &'sink str,
    /// Full-drop count observed during the most recent interval (always `> 0`).
    pub delta_full: u64,
    /// Cumulative full-drop count across the sink's lifetime.
    pub total_full: u64,
    /// The monitor sampling interval.
    pub interval: Duration,
}

/// What to do when full-drop counts increase.
///
/// `on_overload` runs on the overload monitor task, never on producer tasks. It
/// is called at most once per monitor interval, and only when the full-drop
/// delta for that interval is positive.
pub trait OverloadAction: Send + Sync + 'static {
    /// Handle a positive-delta overload sample.
    fn on_overload(&self, ev: Overloaded<'_>);
}

/// Default [`OverloadAction`]: log an error, and (with the `metrics` feature)
/// emit an overload counter.
#[derive(Debug, Default, Clone, Copy)]
pub struct LogErrorOverload;

impl OverloadAction for LogErrorOverload {
    fn on_overload(&self, ev: Overloaded<'_>) {
        tracing::error!(
            sink = ev.sink,
            delta_full = ev.delta_full,
            total_full = ev.total_full,
            interval_ms = ev.interval.as_millis() as u64,
            "sharded-sink shed items because shard rings were full",
        );

        #[cfg(feature = "metrics")]
        {
            metrics::counter!("sharded_sink.overload", "sink" => ev.sink.to_string())
                .increment(ev.delta_full);
        }
    }
}
