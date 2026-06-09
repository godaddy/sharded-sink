//! Shared benchmark support: payload type, baseline sinks, and statistics.
//!
//! Pulled into each bench binary with `#[path = "support/mod.rs"] mod support;`.
//!
//! The baseline sinks are deliberately "simpler implementations with less
//! complex data structures": a single shared lock-free queue and a single
//! shared `Mutex<Vec>`. They expose the raw structure plus a stepping drain
//! method so each benchmark can drive draining however it likes (e.g. a tokio
//! task with an artificial delay), keeping the drain mechanism identical across
//! all sinks under test.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;

/// A realistic ~32-byte POD telemetry event, matching the design's example.
#[derive(Debug, Default, Clone, Copy)]
pub struct TelemetryEvent {
    pub request_id: [u8; 16],
    pub timestamp_ns: u64,
    pub route_id: u32,
    pub status: u16,
    pub flags: u16,
}

impl TelemetryEvent {
    #[inline]
    pub fn sample(i: u64) -> Self {
        Self {
            request_id: [0; 16],
            timestamp_ns: i,
            route_id: (i & 0xffff) as u32,
            status: 200,
            flags: 0,
        }
    }
}

/// Compute the value at percentile `p` (0.0..=1.0) of an already-sorted slice.
pub fn percentile(sorted_nanos: &[u64], p: f64) -> u64 {
    if sorted_nanos.is_empty() {
        return 0;
    }
    let max_idx = sorted_nanos.len() - 1;
    let idx = (p * max_idx as f64).round() as usize;
    sorted_nanos[idx.min(max_idx)]
}

/// Single shared `crossbeam_queue::ArrayQueue` (lock-free, but one contention
/// point shared by all producers). The `try_push` is the producer hot path
/// equivalent; `drain_up_to` is the drain-side step.
#[derive(Clone)]
pub struct ArrayQueueSink {
    queue: Arc<crossbeam_queue::ArrayQueue<TelemetryEvent>>,
}

impl ArrayQueueSink {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Arc::new(crossbeam_queue::ArrayQueue::new(capacity)),
        }
    }

    /// Returns `true` if accepted, `false` if shed (queue full).
    #[inline]
    pub fn try_push(&self, ev: TelemetryEvent) -> bool {
        self.queue.push(ev).is_ok()
    }

    /// Pop up to `max` items; returns how many were drained.
    pub fn drain_up_to(&self, max: usize) -> usize {
        let mut n = 0;
        while n < max && self.queue.pop().is_some() {
            n += 1;
        }
        n
    }
}

/// Single shared `Mutex<Vec<T>>` accumulator. Every producer contends on one
/// lock — the classic naive approach.
#[derive(Clone)]
pub struct MutexVecSink {
    buf: Arc<Mutex<Vec<TelemetryEvent>>>,
    capacity: usize,
}

impl MutexVecSink {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
            capacity,
        }
    }

    /// Returns `true` if accepted, `false` if shed (at capacity).
    #[inline]
    pub fn try_push(&self, ev: TelemetryEvent) -> bool {
        let mut guard = self.buf.lock().expect("mutex");
        if guard.len() >= self.capacity {
            return false;
        }
        guard.push(ev);
        true
    }

    /// Remove up to `max` items under the lock; returns how many were drained.
    pub fn drain_up_to(&self, max: usize) -> usize {
        let mut guard = self.buf.lock().expect("mutex");
        let take = guard.len().min(max);
        guard.drain(..take);
        take
    }
}
