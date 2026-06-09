//! `sharded-sink` — a small, high-performance **fire-and-forget sink** for
//! fan-in workloads.
//!
//! Many async producers hand off items to a bounded, sharded set of
//! `crossbeam_queue::ArrayQueue` ring buffers (one per shard); one poll-drain
//! worker per shard performs the actual sink action off the producer critical
//! path. See the [README](https://docs.rs/crate/sharded-sink) for the
//! benchmark-driven rationale behind the sharded-crossbeam design.
//!
//! The primary design value is **flat, predictable producer latency under high
//! contention**. A producer never awaits, never intentionally blocks, never
//! performs I/O, and never takes a crate-level lock. When a ring is full, the
//! sink sheds the item and records that fact rather than applying backpressure.
//!
//! This crate is for telemetry, logging, counters, spend-record firehoses, and
//! other loss-tolerant streams. **It is not a durable queue.**
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use sharded_sink::{ShardedSink, SinkAction, SinkConfig};
//!
//! #[derive(Default, Clone, Copy)]
//! struct Event {
//!     status: u16,
//! }
//!
//! struct PrintDrain;
//!
//! impl SinkAction<Event> for PrintDrain {
//!     async fn drain(&self, batch: &mut Vec<Event>) {
//!         // Encode and ship `batch`. Do not retain references past this point.
//!         let _ = batch.len();
//!     }
//! }
//!
//! # async fn run() {
//! let sink = ShardedSink::spawn_default_overload(
//!     SinkConfig::default().with_name("events"),
//!     Arc::new(PrintDrain),
//! );
//!
//! // Hot producers reuse a handle.
//! let handle = sink.issue();
//! let accepted = handle.push(Event { status: 200 });
//! assert!(accepted);
//!
//! // Caller quiesces producers, then drains.
//! sink.shutdown().await.expect("shutdown");
//! # }
//! ```
//!
//! # Producer hot path
//!
//! [`ShardHandle::push`] performs exactly one immediate `ArrayQueue::push`
//! against one shard. On success it touches no counters, locks, timers, or
//! metrics. Only a full-ring rejection increments a single relaxed atomic. See
//! the [behavioral contract](#behavioral-contract) for the guarantees this
//! provides.
//!
//! # Shutdown is producer-quiesced
//!
//! [`ShardedSink::shutdown`] drains items that are buffered *once producers have
//! been quiesced*. The caller is responsible for stopping request acceptance and
//! dropping producer handles before calling [`shutdown`](ShardedSink::shutdown).
//! Pushes racing with shutdown are outside the graceful-delivery contract.
//!
//! # Behavioral contract
//!
//! 1. Producer push never awaits.
//! 2. Successful push performs no crate-level allocation after warmup.
//! 3. Successful push touches no sink stats.
//! 4. Every full-ring rejection increments exactly one shard `dropped` counter.
//! 5. With live producers, non-full rings, and no shutdown, accepted items are
//!    observed by [`SinkAction::drain`] exactly once.
//! 6. Producer-quiesced shutdown drains visible buffered items.
//! 7. Racing shutdown is explicitly lossy (a late push may sit undrained).

mod config;
mod error;
mod handle;
mod monitor;
mod overload;
mod selection;
mod sink;
mod stats;
mod worker;

pub use config::{ShardSelection, SinkConfig, SinkConfigError, WorkStealing};
pub use error::ShutdownError;
pub use handle::ShardHandle;
pub use overload::{LogErrorOverload, OverloadAction, Overloaded};
pub use sink::{ShardedSink, SinkAction};
pub use stats::{ShardStats, SinkStats};
