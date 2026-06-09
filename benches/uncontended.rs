//! Uncontended, single-producer push microbenchmark (design section 16).
//!
//! Measures the per-push cost of the hot path with no contention, comparing the
//! sharded sink's producer entry points against simpler single-structure sinks.
//! Criterion amortizes the timer over many iterations, the only correct way to
//! resolve a ~10 ns op. Each sink is drained fast in the background so pushes
//! take the accept path.

#[path = "support/mod.rs"]
mod support;

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use sharded_sink::{ShardSelection, ShardedSink, SinkAction, SinkConfig, WorkStealing};
use support::{ArrayQueueSink, MutexVecSink, TelemetryEvent};

struct NoopDrain;

impl SinkAction<TelemetryEvent> for NoopDrain {
    async fn drain(&self, _batch: &mut Vec<TelemetryEvent>) {}
}

fn config() -> SinkConfig {
    SinkConfig {
        name: "bench",
        shards: 4,
        ring_capacity: 16_384,
        drain_batch: 256,
        overload_check_interval: Duration::from_secs(3600),
        shard_selection: ShardSelection::ThreadLocalRoundRobin,
        idle_sleep: Duration::from_micros(100),
        work_stealing: WorkStealing::Off,
        shutdown_timeout: Some(Duration::from_secs(5)),
    }
}

fn bench_uncontended(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = rt.enter();

    let sink: ShardedSink<TelemetryEvent> =
        ShardedSink::spawn_default_overload(config(), Arc::new(NoopDrain));
    let handle = sink.issue();

    // Fast-draining baselines: dedicated OS threads pull continuously so the
    // structures never fill during the microbench.
    let stop = Arc::new(AtomicBool::new(false));
    let array_queue = ArrayQueueSink::with_capacity(16_384);
    let mutex_vec = MutexVecSink::with_capacity(16_384);
    let aq_drainer = {
        let (s, stop) = (array_queue.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if s.drain_up_to(4096) == 0 {
                    std::thread::sleep(Duration::from_micros(20));
                }
            }
        })
    };
    let mv_drainer = {
        let (s, stop) = (mutex_vec.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if s.drain_up_to(4096) == 0 {
                    std::thread::sleep(Duration::from_micros(20));
                }
            }
        })
    };

    // tokio bounded mpsc with a background drain task.
    let (tok_tx, mut tok_rx) = tokio::sync::mpsc::channel::<TelemetryEvent>(16_384);
    rt.spawn(async move { while tok_rx.recv().await.is_some() {} });

    let mut group = c.benchmark_group("uncontended_push");
    let mut i = 0_u64;

    group.bench_function("sharded_sink/held_handle", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(handle.push(black_box(TelemetryEvent::sample(i))))
        });
    });

    group.bench_function("sharded_sink/handleless", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(sink.push(black_box(TelemetryEvent::sample(i))))
        });
    });

    group.bench_function("sharded_sink/issue_then_push", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            let h = sink.issue();
            black_box(h.push(black_box(TelemetryEvent::sample(i))))
        });
    });

    group.bench_function("baseline/single_crossbeam", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(array_queue.try_push(black_box(TelemetryEvent::sample(i))))
        });
    });

    group.bench_function("baseline/single_mutex_vec", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(mutex_vec.try_push(black_box(TelemetryEvent::sample(i))))
        });
    });

    group.bench_function("baseline/tokio_mpsc_try_send", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(
                tok_tx
                    .try_send(black_box(TelemetryEvent::sample(i)))
                    .is_ok(),
            )
        });
    });

    group.finish();

    stop.store(true, Ordering::Relaxed);
    aq_drainer.join().expect("array drainer");
    mv_drainer.join().expect("mutex drainer");
    rt.block_on(async {
        let _done = sink.shutdown().await;
    });
}

criterion_group!(benches, bench_uncontended);
criterion_main!(benches);
