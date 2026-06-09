//! Shared test fixtures for integration tests.
//!
//! `mod common;` is compiled independently into each test binary, so not every
//! binary uses every helper; allow dead code here rather than gating each item.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use sharded_sink::{OverloadAction, Overloaded, SinkAction};
use tokio::sync::Notify;

/// A cheap `Copy` event type matching the intended hot-path payload shape.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Ev {
    pub id: u64,
}

impl Ev {
    pub fn new(id: u64) -> Self {
        Self { id }
    }
}

/// A drain action that records every item it observes. Never blocks.
#[derive(Debug, Default)]
pub struct CollectDrain {
    pub items: Arc<Mutex<Vec<Ev>>>,
}

impl CollectDrain {
    pub fn new() -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn observed(&self) -> usize {
        self.items.lock().expect("collect lock").len()
    }

    pub fn snapshot(&self) -> Vec<Ev> {
        self.items.lock().expect("collect lock").clone()
    }
}

impl SinkAction<Ev> for CollectDrain {
    async fn drain(&self, batch: &mut Vec<Ev>) {
        let mut guard = self.items.lock().expect("collect lock");
        guard.extend(batch.iter().copied());
    }
}

/// A drain action that blocks on its first call until released, then becomes a
/// counting no-op. Used to hold items in the rings deterministically.
#[derive(Debug)]
pub struct GateDrain {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    open: Arc<std::sync::atomic::AtomicBool>,
    observed: Arc<AtomicUsize>,
}

impl GateDrain {
    pub fn new() -> (Self, GateControl) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::new(AtomicUsize::new(0));
        let drain = Self {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            open: Arc::clone(&open),
            observed: Arc::clone(&observed),
        };
        let control = GateControl {
            entered,
            release,
            open,
            observed,
        };
        (drain, control)
    }
}

impl SinkAction<Ev> for GateDrain {
    async fn drain(&self, batch: &mut Vec<Ev>) {
        self.observed.fetch_add(batch.len(), Ordering::SeqCst);
        if !self.open.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

/// Control side of a [`GateDrain`].
#[derive(Debug)]
pub struct GateControl {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    open: Arc<std::sync::atomic::AtomicBool>,
    observed: Arc<AtomicUsize>,
}

impl GateControl {
    /// Wait until a worker has entered the gate (and is now blocked).
    pub async fn wait_entered(&self) {
        self.entered.notified().await;
    }

    /// Release the gate permanently; future drains become non-blocking.
    pub fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }

    pub fn observed(&self) -> usize {
        self.observed.load(Ordering::SeqCst)
    }
}

/// An overload action that records every notification it receives.
#[derive(Debug, Default)]
pub struct RecordingOverload {
    pub calls: AtomicU64,
    pub total_delta: AtomicU64,
    pub last_total_full: AtomicU64,
}

impl RecordingOverload {
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn total_delta(&self) -> u64 {
        self.total_delta.load(Ordering::SeqCst)
    }

    pub fn last_total_full(&self) -> u64 {
        self.last_total_full.load(Ordering::SeqCst)
    }
}

impl OverloadAction for RecordingOverload {
    fn on_overload(&self, ev: Overloaded<'_>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.total_delta.fetch_add(ev.delta_full, Ordering::SeqCst);
        self.last_total_full.store(ev.total_full, Ordering::SeqCst);
    }
}
