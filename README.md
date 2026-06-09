# sharded-sink

A small, high-performance **fire-and-forget sink** for fan-in workloads.

Many async producers hand off items to a bounded, **sharded** set of
`crossbeam_queue::ArrayQueue` rings (one per shard); one poll-drain worker per
shard performs the actual sink action off the producer critical path. When a
ring is full the sink **sheds** the item and counts it rather than applying
backpressure.

The design value is **flat, predictable producer latency under high
contention**: a successful push is a single lock-free `ArrayQueue::push` to one
shard — no await, no lock, no allocation, and no counter touched on success.

This crate is for telemetry, logging, counters, spend-record firehoses, and
other loss-tolerant streams. **It is not a durable queue** — no at-least-once
delivery, no ordering across shards, no dynamic resharding. Items need only
`T: Send + 'static`.

```toml
[dependencies]
sharded-sink = "0.1"
```

## Quick start

```rust
use std::sync::Arc;
use sharded_sink::{ShardedSink, SinkAction, SinkConfig};

#[derive(Clone, Copy)]
struct Event { status: u16 }

struct PrintDrain;
impl SinkAction<Event> for PrintDrain {
    async fn drain(&self, batch: &mut Vec<Event>) {
        // Encode and ship `batch`; don't retain references past this point.
        let _ = batch.len();
    }
}

# async fn run() {
let sink = ShardedSink::spawn_default_overload(
    SinkConfig::default().with_name("events"),
    Arc::new(PrintDrain),
);

// Hot producers reuse a handle (one per request / connection / producer object).
let handle = sink.issue();
let _accepted = handle.push(Event { status: 200 });

// Caller quiesces producers first, then drains.
sink.shutdown().await.expect("shutdown");
# }
```

## Why sharded crossbeam `ArrayQueue` (the design decision)

This crate originally specified `thingbuf::mpsc` as the per-shard ring. We
implemented the framework generically over the ring primitive and benchmarked
the **real framework** on each — identical handles, workers, and lifecycle,
only the ring swapped — measuring the cost of a **successful** push (the hot
path) under many-producer contention. The numbers picked crossbeam decisively,
so the crate ships only the sharded crossbeam design.

Per-op cost in **per-thread CPU time** (`CLOCK_THREAD_CPUTIME_ID`, which
excludes OS scheduler preemption so it reflects real push cost under
oversubscription). Dev machine, `drop% = 0` (true accept path), median of 3
trials:

| producers | sharded thingbuf | **sharded crossbeam** | unsharded crossbeam (1 shard) | single `ArrayQueue` | single `Mutex<Vec>` |
|---:|---:|---:|---:|---:|---:|
| 1   | 5.6 ns | **4.7 ns** | 4.6 ns | 4.5 ns | 5.8 ns |
| 4   | 5.4 ns | **4.4 ns** | 35 ns | 30 ns | 78 ns |
| 16  | 395 ns | **18.3 ns** | 541 ns | 768 ns | 184 ns |
| 64  | 700 ns | **10.8 ns** | 254 ns | 182 ns | 209 ns |
| 128 | 1077 ns | **13.7 ns** | 220 ns | 208 ns | 187 ns |
| 256 | 508 ns | **15.6 ns** | 202 ns | 190 ns | 188 ns |

What this shows:

- **Sharded crossbeam is the only option that stays flat** — ~4–18 ns from 1 to
  256 producers (300–600 M pushes/sec).
- **Sharded thingbuf collapses under contention** (395–1077 ns): every successful
  `try_send` notifies the shard's single MPSC consumer wait-cell, and with
  several producers per shard that one cache line bounces between cores.
- **A single shared structure is ~20× slower** (`ArrayQueue` 190–768 ns,
  `Mutex<Vec>` ~190 ns) because every successful push hits one shared cursor
  (the queue's tail-CAS, or the lock). Sharding splits that cursor across shards;
  that *is* the win. (The reject/full path of a single queue is cheap — which is
  why this had to be measured on the accept path, not under backpressure.)
- **`Mutex<Vec>` also has a catastrophic tail** under contention — p99.9 reached
  the **milliseconds** (lock-holder preemption / convoy), vs microseconds for the
  lock-free options.

### Ordering trade-off

Sharding sacrifices global ordering: items are FIFO only *within* a shard, with
no order across shards. If you need approximate global insertion order, set
`shards: 1` — that's the "unsharded crossbeam" row above: one queue, global FIFO
(the same ordering a `Mutex<Vec>` gives, but lock-free and without the convoy
tail), at ~200 ns/push under heavy contention. You cannot have both a single
global order and flat-contention push — the global order *is* a single contended
cursor by definition.

## How it works

- **Producer hot path** (`ShardHandle::push`): one immediate `ArrayQueue::push`
  to one shard. On success it touches no counter, lock, timer, or metric. Only a
  full-ring rejection increments one relaxed per-shard atomic.
- **Shard selection** is deterministic, never random:
  - `issue()` — global round-robin cursor; spreads handles across shards
    regardless of issuing thread. Not the hot path.
  - `issue_thread_local()` — stable home shard for the current thread.
  - handle-less `push()` — uses `ShardSelection` (thread-local home or
    round-robin).
- **Drain workers poll** their home shard with `pop` and sleep `idle_sleep` when
  empty; they never register a waker. After draining their own shard they may
  steal a bounded number of items from other shards
  (`WorkStealing::Opportunistic`). `SinkAction::drain` may await and do I/O; a
  slow drain fills rings (and sheds) but never blocks producers.
- **No closed state**: producers and the drain worker share each shard's queue,
  which outlives the worker. A push after shutdown is accepted into the queue
  (until full), never a "closed" drop.
- **Shutdown is producer-quiesced**: stop accepting work and drop producer
  handles first, then call `shutdown()`. It drains buffered items and joins
  workers. Pushes racing with shutdown are explicitly outside the
  graceful-delivery contract (a late push may sit undrained).

## Benchmarks

Run them on your own hardware — absolute numbers are machine-specific; trust the
shape.

```bash
cargo bench --bench uncontended            # Criterion microbench, single producer
cargo bench --bench contended              # accept-path harness, 1..256 producers
SHARDED_SINK_BENCH_ITEMS=50000 cargo bench --bench contended
```

`uncontended` is a Criterion microbench (Criterion amortizes the timer over many
iterations — the only correct way to resolve a ~10 ns op). `contended` is a
custom harness (Criterion can't cleanly do multi-threaded contention at
nanosecond scale): it builds a fresh sink per trial with rings sized to hold the
burst so every push is accepted, and measures per-op cost in **per-thread CPU
time** plus the wall-clock tail (p99/p999/max). It includes the sharded sink,
the unsharded (`shards=1`) mode, and single `ArrayQueue` / `Mutex<Vec>`
baselines.

## Configuration

`SinkConfig::default()` gives sensible values (shards ≈ CPU count clamped to
1–8, 8192-item rings, 256 drain batch, 100 µs `idle_sleep`, opportunistic work
stealing). Validate-and-build with `try_spawn*`, or `spawn*` to panic on invalid
config. Set `shards: 1` for global FIFO ordering. See the API docs for every
field.

## Observability

`stats()`, `stats_per_shard()`, `dropped_total()` — `dropped` counts items shed
because a ring was full. There is intentionally **no** accepted counter; counting
every success would put an atomic on the hot path. Count your own `true` returns
if you need it. The optional `metrics` feature emits drop/overload counters from
the monitor (never from producers).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
