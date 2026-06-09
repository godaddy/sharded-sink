//! Allocation test (design section 16).
//!
//! Asserts that a warmed-up `ShardHandle::push` of a `Copy` event performs no
//! heap allocation. A counting global allocator records every allocation; the
//! measured push loop performs no `.await`, so on a current-thread runtime the
//! drain workers never run and only the producer's push code executes during
//! the measurement window.
//!
//! This file contains the crate's only `unsafe` block: implementing
//! `GlobalAlloc` is inherently unsafe and is the standard way to count
//! allocations. It is scoped to this test and never compiled into the library.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sharded_sink::{ShardSelection, ShardedSink, SinkAction, SinkConfig, WorkStealing};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

// SAFETY: every method forwards directly to the system allocator with the same
// layout it was given, only incrementing an atomic counter alongside. This
// preserves all of `System`'s allocation/deallocation invariants.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `layout` is forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr`/`layout` originate from `System.alloc` above.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `layout` is forwarded unchanged to the system allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `ptr`/`layout` originate from this allocator; `new_size` is
        // forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// A realistic POD-shaped payload; field values are never read back (the drain
// is a no-op), which is fine for an allocation measurement.
#[derive(Default, Clone, Copy)]
#[allow(dead_code)]
struct Ev {
    id: u64,
}

struct NoopDrain;

impl SinkAction<Ev> for NoopDrain {
    async fn drain(&self, _batch: &mut Vec<Ev>) {}
}

#[tokio::test(flavor = "current_thread")]
async fn push_does_not_allocate_after_warmup() {
    let mut cfg = SinkConfig::default();
    cfg.name = "alloc";
    cfg.shards = 4;
    cfg.ring_capacity = 1024;
    cfg.drain_batch = 64;
    cfg.overload_check_interval = Duration::from_secs(3600);
    cfg.shard_selection = ShardSelection::ThreadLocalRoundRobin;
    cfg.idle_sleep = Duration::from_micros(100);
    cfg.work_stealing = WorkStealing::Off;
    cfg.shutdown_timeout = Some(Duration::from_secs(5));
    let sink = ShardedSink::spawn_default_overload(cfg, Arc::new(NoopDrain));
    let handle = sink.issue();

    // Warm up: exercise the exact push path once (and let any one-time
    // initialization settle) before measuring.
    for i in 0..256 {
        let _ = handle.push(Ev { id: i });
    }

    // Measure: a tight push loop with no `.await`, so the drain workers (spawned
    // on this same current-thread runtime) never run and cannot allocate.
    let before = ALLOCS.load(Ordering::Relaxed);
    for i in 0..100_000_u64 {
        let _ = handle.push(Ev { id: i });
    }
    let after = ALLOCS.load(Ordering::Relaxed);

    assert_eq!(
        after - before,
        0,
        "ShardHandle::push allocated {} times over 100k pushes",
        after - before
    );

    // Release the runtime cleanly. (Items left in rings are simply dropped.)
    sink.shutdown().await.expect("shutdown");
}
