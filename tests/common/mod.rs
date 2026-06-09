//! Shared test fixtures for integration tests.
//!
//! `mod common;` is compiled independently into each test binary, so not every
//! binary uses every helper; allow dead code here rather than gating each item.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use sharded_sink::{OverloadAction, Overloaded, ShardHandle, SinkAction};
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

/// A drain action that discards items and never awaits — a *non-yielding*
/// drain, used to verify the worker yields cooperatively on the busy path.
#[derive(Debug, Default)]
pub struct BlackholeDrain;

impl SinkAction<Ev> for BlackholeDrain {
    async fn drain(&self, _batch: &mut Vec<Ev>) {}
}

/// A non-yielding drain that re-pushes what it drained (until stopped), keeping
/// its shard deterministically non-empty so the worker stays on the busy path.
/// Used to prove the worker yields cooperatively rather than monopolizing the
/// runtime thread.
#[derive(Debug)]
pub struct FeedingDrain {
    handle: OnceLock<ShardHandle<Ev>>,
    stop: Arc<AtomicBool>,
}

impl FeedingDrain {
    pub fn new(stop: Arc<AtomicBool>) -> Self {
        Self {
            handle: OnceLock::new(),
            stop,
        }
    }

    /// Provide the handle used to re-feed; call once after spawning the sink.
    pub fn set_handle(&self, handle: ShardHandle<Ev>) {
        // Ignore a second set; drop the surplus handle explicitly.
        drop(self.handle.set(handle));
    }
}

impl SinkAction<Ev> for FeedingDrain {
    async fn drain(&self, batch: &mut Vec<Ev>) {
        if !self.stop.load(Ordering::Relaxed)
            && let Some(h) = self.handle.get()
        {
            // Re-feed roughly what we drained to keep the shard non-empty.
            for _ in 0..batch.len() {
                let _ = h.push(Ev::new(0));
            }
        }
    }
}

/// A drain action that panics on its first call, then records items on every
/// subsequent call. Used to verify a panicking drain does not wedge the shard.
#[derive(Debug, Default)]
pub struct PanicOnceDrain {
    panicked: Arc<AtomicBool>,
    observed: Arc<Mutex<Vec<Ev>>>,
}

impl PanicOnceDrain {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the first-call panic has fired.
    pub fn has_panicked(&self) -> bool {
        self.panicked.load(Ordering::SeqCst)
    }

    /// Items observed after the panic.
    pub fn observed(&self) -> usize {
        self.observed.lock().expect("observed lock").len()
    }
}

impl SinkAction<Ev> for PanicOnceDrain {
    // The panic is the whole point of this fixture (testing panic isolation).
    #[allow(clippy::panic)]
    async fn drain(&self, batch: &mut Vec<Ev>) {
        if !self.panicked.swap(true, Ordering::SeqCst) {
            panic!("PanicOnceDrain: intentional panic on first batch");
        }
        self.observed
            .lock()
            .expect("observed lock")
            .extend(batch.iter().copied());
    }
}

/// A drain action that blocks on its first call until released, then becomes a
/// counting no-op. Used to hold items in the rings deterministically.
#[derive(Debug)]
pub struct GateDrain {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    open: Arc<AtomicBool>,
    observed: Arc<AtomicUsize>,
}

impl GateDrain {
    pub fn new() -> (Self, GateControl) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let open = Arc::new(AtomicBool::new(false));
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
    open: Arc<AtomicBool>,
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
