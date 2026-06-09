//! Contended **accept-path** harness — the real sharded framework on both ring
//! backends (apples-to-apples), plus simpler single-structure baselines.
//!
//! This measures the *hot path*: the cost of a **successful** push under
//! many-producer contention. A fresh sink is built for every trial and its rings
//! are sized to hold the whole burst, so every push is accepted (drop% ≈ 0) —
//! we are not measuring the shed path (if you are shedding you are already
//! overloaded; that cost does not matter).
//!
//! `thingbuf_sharded` and `crossbeam_sharded` are the *same* `ShardedSink`
//! framework differing only in the per-shard ring, so any difference is the
//! primitive. This is where thingbuf's per-push consumer notify shows up: with
//! several producers sharing a shard, that single wait-cell bounces between
//! cores, while crossbeam producers only touch the slot they claim.
//!
//! ## Measuring nanoseconds while isolating scheduler noise
//!
//! * **Per-op cost is per-thread CPU time** (`CLOCK_THREAD_CPUTIME_ID`), not
//!   wall time: it counts cycles the thread actually ran (including CAS-retry
//!   and cache-miss stalls — real cost) but excludes OS preemption under
//!   oversubscription (noise). One clock read per thread amortizes the syscall.
//! * **Tail** (p99/p999/max) uses wall-clock per-op timing.
//! * Warmup + median-of-`TRIALS`, fresh sink each trial.
//!
//! Uses one `unsafe` call (`libc::clock_gettime`) and writes to stdout.
#![allow(unsafe_code)]
#![allow(clippy::print_stdout)]

#[path = "support/mod.rs"]
mod support;

use std::hint::black_box;
use std::time::{Duration, Instant};

use sharded_sink::{ShardSelection, ShardedSink, SinkAction, SinkConfig, WorkStealing};
use support::{ArrayQueueSink, MutexVecSink, TelemetryEvent, percentile};

// ---- tunables ----------------------------------------------------------------

const PRODUCER_COUNTS: &[usize] = &[1, 4, 16, 64, 128, 256];
const SHARDS: usize = 4;
const DRAIN_BATCH: usize = 256;
const TRIALS: usize = 3;

fn items_per_producer() -> usize {
    std::env::var("SHARDED_SINK_BENCH_ITEMS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000)
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, owned, properly-aligned `timespec`; only read on success.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return 0;
    }
    (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
}

/// No-op drain: workers poll and pull, but burst-sized rings make draining a
/// bonus, never required to avoid drops.
struct NoopDrain;

impl SinkAction<TelemetryEvent> for NoopDrain {
    async fn drain(&self, _batch: &mut Vec<TelemetryEvent>) {}
}

// ---- measurement -------------------------------------------------------------

struct Stats {
    label: String,
    producers: usize,
    cpu_ns: f64,
    drop_pct: f64,
    throughput_per_sec: f64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

/// CPU-timed burst on a freshly built target `s`.
fn cpu_burst<S, MkPush, Push>(
    s: &S,
    producers: usize,
    per: usize,
    mk: &MkPush,
) -> (f64, u64, u64, Duration)
where
    S: Sync,
    MkPush: Fn(&S) -> Push + Sync,
    Push: FnMut(TelemetryEvent) -> bool,
{
    let wall_start = Instant::now();
    let results: Vec<(f64, u64)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..producers)
            .map(|t| {
                scope.spawn(move || {
                    let mut push = mk(s);
                    let warmup = per / 10;
                    for i in 0..warmup {
                        let _ = black_box(push(black_box(TelemetryEvent::sample(i as u64))));
                    }
                    let mut accepted = 0_u64;
                    let cpu_start = thread_cpu_ns();
                    for i in 0..per {
                        let ev = TelemetryEvent::sample(((t as u64) << 40) | (warmup + i) as u64);
                        if black_box(push(black_box(ev))) {
                            accepted += 1;
                        }
                    }
                    let cpu_ns = thread_cpu_ns().saturating_sub(cpu_start) as f64 / per as f64;
                    (cpu_ns, accepted)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("producer thread"))
            .collect()
    });
    let wall = wall_start.elapsed();
    let cpu_ns = results.iter().map(|(ns, _)| ns).sum::<f64>() / producers as f64;
    let accepted: u64 = results.iter().map(|(_, a)| a).sum();
    (cpu_ns, (producers * per) as u64, accepted, wall)
}

/// Wall-clock per-op burst for the tail, on a freshly built target `s`.
fn tail_burst<S, MkPush, Push>(s: &S, producers: usize, per: usize, mk: &MkPush) -> Vec<u64>
where
    S: Sync,
    MkPush: Fn(&S) -> Push + Sync,
    Push: FnMut(TelemetryEvent) -> bool,
{
    let mut all: Vec<u64> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..producers)
            .map(|t| {
                scope.spawn(move || {
                    let mut push = mk(s);
                    let warmup = per / 10;
                    for i in 0..warmup {
                        let _ = black_box(push(black_box(TelemetryEvent::sample(i as u64))));
                    }
                    let mut lat = Vec::with_capacity(per);
                    for i in 0..per {
                        let ev = TelemetryEvent::sample(((t as u64) << 40) | (warmup + i) as u64);
                        let at = Instant::now();
                        let _ = black_box(push(black_box(ev)));
                        lat.push(at.elapsed().as_nanos() as u64);
                    }
                    lat
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("producer thread"))
            .collect()
    });
    all.sort_unstable();
    all
}

/// Run all trials for one target, building a **fresh** `S` each trial via
/// `mk_sink` and tearing it down with `teardown`.
fn run<S, MkSink, MkPush, Push, Teardown>(
    label: &str,
    producers: usize,
    per: usize,
    mk_sink: MkSink,
    mk_push: MkPush,
    teardown: Teardown,
) -> Stats
where
    S: Sync,
    MkSink: Fn() -> S,
    MkPush: Fn(&S) -> Push + Sync,
    Push: FnMut(TelemetryEvent) -> bool,
    Teardown: Fn(S),
{
    let mut cpu = Vec::with_capacity(TRIALS);
    let mut throughputs = Vec::with_capacity(TRIALS);
    let mut last_accepted = 0_u64;
    let mut attempted = 0_u64;
    for _ in 0..TRIALS {
        let s = mk_sink();
        let (cpu_ns, att, accepted, wall) = cpu_burst(&s, producers, per, &mk_push);
        teardown(s);
        cpu.push(cpu_ns);
        throughputs.push(accepted as f64 / wall.as_secs_f64());
        last_accepted = accepted;
        attempted = att;
    }
    cpu.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    throughputs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let median = |v: &[f64]| v[v.len() / 2];

    let s = mk_sink();
    let tail = tail_burst(&s, producers, per, &mk_push);
    teardown(s);

    let drop_pct = if attempted == 0 {
        0.0
    } else {
        (attempted - last_accepted) as f64 / attempted as f64 * 100.0
    };

    Stats {
        label: label.to_string(),
        producers,
        cpu_ns: median(&cpu),
        drop_pct,
        throughput_per_sec: median(&throughputs),
        p99_ns: percentile(&tail, 0.99),
        p999_ns: percentile(&tail, 0.999),
        max_ns: tail.last().copied().unwrap_or(0),
    }
}

/// Per-shard ring capacity that holds one full burst (incl. warmup) for the
/// busiest shard, with `issue()` spreading producers evenly across shards.
fn shard_capacity(producers: usize, per: usize) -> usize {
    let producers_per_shard = producers.div_ceil(SHARDS);
    producers_per_shard * (per + per / 10) + 1024
}

fn cfg_shards(shards: usize, ring_capacity: usize) -> SinkConfig {
    SinkConfig {
        name: "accept",
        shards,
        ring_capacity,
        drain_batch: DRAIN_BATCH,
        overload_check_interval: Duration::from_secs(3600),
        shard_selection: ShardSelection::ThreadLocalRoundRobin,
        idle_sleep: Duration::from_micros(100),
        work_stealing: WorkStealing::Off,
        shutdown_timeout: Some(Duration::from_secs(10)),
    }
}

fn sharded_config(ring_capacity: usize) -> SinkConfig {
    cfg_shards(SHARDS, ring_capacity)
}

fn print_header(title: &str) {
    println!("\n=== {title} ===");
    println!(
        "{:<28} {:>4}  {:>9}  {:>7}  {:>14}  {:>8} {:>8} {:>10}",
        "impl", "prod", "cpu", "drop%", "throughput", "p99", "p99.9", "max"
    );
    println!(
        "{:<28} {:>4}  {:>9}  {:>7}  {:>14}  {:>8} {:>8} {:>10}",
        "", "", "ns/op", "", "acc/s", "ns", "ns", "ns"
    );
}

fn print_row(s: &Stats) {
    println!(
        "{:<28} {:>4}  {:>9.2}  {:>6.2}%  {:>14.0}  {:>8} {:>8} {:>10}",
        s.label,
        s.producers,
        s.cpu_ns,
        s.drop_pct,
        s.throughput_per_sec,
        s.p99_ns,
        s.p999_ns,
        s.max_ns,
    );
}

fn main() {
    let per = items_per_producer();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(SHARDS.max(4))
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = rt.enter();

    println!(
        "sharded-sink accept-path benchmark (real framework, both backends)\n\
         shards={SHARDS} drain_batch={DRAIN_BATCH} items/producer={per} trials={TRIALS}\n\
         fresh sink per trial, rings sized to hold the burst => drop% ~ 0 (the SUCCESS path).\n\
         cpu ns/op = per-thread CPU time (isolates scheduler preemption); \
         p99+/max are wall-clock per-op (tail)."
    );

    print_header("accept hot path under contention");

    for &producers in PRODUCER_COUNTS {
        let cap = shard_capacity(producers, per);
        let total = producers * (per + per / 10) + 1024;

        // The crate: sharded crossbeam ArrayQueue (the locked-in design).
        let s = run(
            "sharded (4 shards)",
            producers,
            per,
            || {
                ShardedSink::<TelemetryEvent>::spawn_default_overload(
                    sharded_config(cap),
                    std::sync::Arc::new(NoopDrain),
                )
            },
            |sink| {
                let h = sink.issue();
                move |ev| h.push(ev)
            },
            |sink| {
                rt.block_on(async {
                    let _done = sink.shutdown().await;
                });
            },
        );
        print_row(&s);

        // The same crate with a single shard: one queue => global FIFO order,
        // full lifecycle, but all producers contend on one tail (ordered mode).
        let s = run(
            "unsharded (1 shard, ordered)",
            producers,
            per,
            || {
                ShardedSink::<TelemetryEvent>::spawn_default_overload(
                    cfg_shards(1, total),
                    std::sync::Arc::new(NoopDrain),
                )
            },
            |sink| {
                let h = sink.issue();
                move |ev| h.push(ev)
            },
            |sink| {
                rt.block_on(async {
                    let _done = sink.shutdown().await;
                });
            },
        );
        print_row(&s);

        let s = run(
            "single_crossbeam",
            producers,
            per,
            || ArrayQueueSink::with_capacity(total),
            |sink| {
                let sink = sink.clone();
                move |ev| sink.try_push(ev)
            },
            |_sink| {},
        );
        print_row(&s);

        let s = run(
            "single_mutex_vec",
            producers,
            per,
            || MutexVecSink::with_capacity(total),
            |sink| {
                let sink = sink.clone();
                move |ev| sink.try_push(ev)
            },
            |_sink| {},
        );
        print_row(&s);

        println!();
    }
}
