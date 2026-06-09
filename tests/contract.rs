//! Behavioral contract tests (design sections 15 and 16).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{CollectDrain, Ev, GateDrain, RecordingOverload};
use sharded_sink::{
    LogErrorOverload, ShardSelection, ShardedSink, SinkConfig, SinkConfigError, WorkStealing,
};

fn cfg(shards: usize, ring_capacity: usize, drain_batch: usize) -> SinkConfig {
    SinkConfig {
        name: "test",
        shards,
        ring_capacity,
        drain_batch,
        overload_check_interval: Duration::from_millis(50),
        shard_selection: ShardSelection::ThreadLocalRoundRobin,
        idle_sleep: Duration::from_micros(50),
        work_stealing: WorkStealing::Off,
        shutdown_timeout: Some(Duration::from_secs(5)),
    }
}

// ---- push acceptance ---------------------------------------------------------

#[tokio::test]
async fn push_returns_true_when_accepted() {
    let sink = ShardedSink::spawn_default_overload(cfg(2, 64, 16), Arc::new(CollectDrain::new()));
    let handle = sink.issue();
    assert!(handle.push(Ev::new(1)));
    sink.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn push_returns_false_and_counts_drop_when_ring_full() {
    // Single shard, capacity 4, batch 1. Block the worker on its first drain so
    // the ring can fill deterministically.
    let (gate, control) = GateDrain::new();
    let sink = ShardedSink::spawn_default_overload(cfg(1, 4, 1), Arc::new(gate));
    let handle = sink.issue();

    // Prime the worker: it pops one item and blocks inside `drain`.
    assert!(handle.push(Ev::new(0)));
    control.wait_entered().await;

    // Ring is now empty and the worker is parked. Fill it, then overflow.
    for _ in 0..4 {
        assert!(handle.push(Ev::new(1)));
    }
    let mut rejected = 0;
    for _ in 0..10 {
        if !handle.push(Ev::new(2)) {
            rejected += 1;
        }
    }
    assert_eq!(rejected, 10);
    assert_eq!(sink.dropped_total(), 10);

    control.open();
    sink.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_after_shutdown_is_accepted_no_closed_state() {
    // A shared crossbeam queue has no closed state: producers and the worker
    // share each shard's queue, which outlives the worker. So a push after
    // shutdown is accepted into the queue (until full), never a "closed" drop.
    let sink = ShardedSink::spawn_default_overload(cfg(1, 64, 16), Arc::new(CollectDrain::new()));
    let handle = sink.issue();
    assert!(handle.push(Ev::new(1)));
    sink.shutdown().await.expect("shutdown");

    assert!(handle.push(Ev::new(2)));
    assert!(sink.push(Ev::new(3)));
    assert_eq!(sink.dropped_total(), 0);
}

// ---- stats -------------------------------------------------------------------

#[tokio::test]
async fn stats_total_equals_sum_of_shards() {
    let (gate, control) = GateDrain::new();
    let sink = ShardedSink::spawn_default_overload(cfg(1, 2, 1), Arc::new(gate));
    let handle = sink.issue();
    assert!(handle.push(Ev::new(0)));
    control.wait_entered().await;
    for _ in 0..2 {
        assert!(handle.push(Ev::new(1)));
    }
    for _ in 0..5 {
        let _ = handle.push(Ev::new(2));
    }

    let stats = sink.stats();
    let per_shard: u64 = sink.stats_per_shard().iter().map(|s| s.dropped).sum();
    assert_eq!(stats.dropped, per_shard);
    assert_eq!(stats.dropped, sink.dropped_total());

    control.open();
    sink.shutdown().await.expect("shutdown");
}

// ---- shard selection ---------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn handleless_round_robin_uses_all_shards() {
    // Capacity 1 per shard, no `.await` between pushes so workers never run.
    // Round-robin over `shards` consecutive pushes must hit each shard once.
    let shards = 4;
    let sink =
        ShardedSink::spawn_default_overload(cfg(shards, 1, 1), Arc::new(CollectDrain::new()));
    for _ in 0..shards {
        assert!(sink.push(Ev::new(0)));
    }
    assert_eq!(sink.dropped_total(), 0);
    sink.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "current_thread")]
async fn issue_round_robin_uses_all_shards() {
    let shards = 4;
    let sink =
        ShardedSink::spawn_default_overload(cfg(shards, 1, 1), Arc::new(CollectDrain::new()));
    for _ in 0..shards {
        let h = sink.issue();
        assert!(h.push(Ev::new(0)));
    }
    assert_eq!(sink.dropped_total(), 0);
    sink.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "current_thread")]
async fn thread_local_home_is_stable_on_same_thread() {
    // Two handles from `issue_thread_local` on the same thread target the same
    // (home) shard. With capacity 1 and no draining, the second push overflows.
    let mut cfg = cfg(4, 1, 1);
    cfg.shard_selection = ShardSelection::ThreadLocalHome;
    let sink = ShardedSink::spawn_default_overload(cfg, Arc::new(CollectDrain::new()));
    let h1 = sink.issue_thread_local();
    let h2 = sink.issue_thread_local();
    assert!(h1.push(Ev::new(0)));
    assert!(!h2.push(Ev::new(1)));
    assert_eq!(sink.dropped_total(), 1);
    sink.shutdown().await.expect("shutdown");
}

// ---- no loss / shutdown drain ------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_loss_under_non_overload_conditions() {
    let collect = Arc::new(CollectDrain::new());
    let sink = ShardedSink::spawn_default_overload(cfg(4, 4096, 128), Arc::clone(&collect));

    let n = 5_000_u64;
    let mut accepted = 0_u64;
    for i in 0..n {
        if sink.push(Ev::new(i)) {
            accepted += 1;
        }
        if i % 256 == 0 {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(accepted, n);
    assert_eq!(sink.dropped_total(), 0);

    sink.shutdown().await.expect("shutdown");
    assert_eq!(collect.observed() as u64, accepted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_quiesced_shutdown_drains_all_buffered_items() {
    let collect = Arc::new(CollectDrain::new());
    let sink = ShardedSink::spawn_default_overload(cfg(4, 8192, 64), Arc::clone(&collect));

    let n = 2_000_u64;
    let mut accepted = 0_u64;
    for i in 0..n {
        if sink.push(Ev::new(i)) {
            accepted += 1;
        }
    }
    sink.shutdown().await.expect("shutdown");

    assert_eq!(collect.observed() as u64, accepted);
    let mut ids: Vec<u64> = collect.snapshot().iter().map(|e| e.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len() as u64, accepted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_is_idempotent() {
    let sink = ShardedSink::spawn_default_overload(cfg(2, 64, 16), Arc::new(CollectDrain::new()));
    let first = sink.shutdown().await;
    let second = sink.shutdown().await;
    assert!(first.is_ok());
    assert_eq!(first, second);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_with_live_producers_does_not_hang_or_panic() {
    let collect = Arc::new(CollectDrain::new());
    let sink = ShardedSink::spawn_default_overload(cfg(4, 1024, 64), Arc::clone(&collect));

    let producer_sink = sink.clone();
    let producer = tokio::spawn(async move {
        for i in 0..1_000_u64 {
            let _ = producer_sink.push(Ev::new(i));
            if i % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    let result = sink.shutdown().await;
    producer.await.expect("producer task");
    assert!(result.is_ok());
}

// ---- overload monitor --------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn drops_trigger_overload_once_per_positive_delta_tick() {
    let overload = Arc::new(RecordingOverload::default());
    let (gate, control) = GateDrain::new();
    let mut config = cfg(1, 4, 1);
    config.overload_check_interval = Duration::from_millis(100);
    let sink = ShardedSink::try_spawn(config, Arc::new(gate), Arc::clone(&overload))
        .expect("valid config");

    let handle = sink.issue();
    assert!(handle.push(Ev::new(0)));
    control.wait_entered().await;

    for _ in 0..4 {
        assert!(handle.push(Ev::new(1)));
    }
    for _ in 0..6 {
        let _ = handle.push(Ev::new(2));
    }
    assert_eq!(sink.dropped_total(), 6);

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(overload.calls() >= 1);
    let calls_after_first = overload.calls();
    assert_eq!(overload.total_delta(), 6);
    assert_eq!(overload.last_total_full(), 6);

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(overload.calls(), calls_after_first);

    control.open();
    sink.shutdown().await.expect("shutdown");
}

// ---- config ------------------------------------------------------------------

#[tokio::test]
async fn try_spawn_rejects_invalid_config() {
    let bad = SinkConfig {
        shards: 0,
        ..cfg(1, 16, 4)
    };
    let result = ShardedSink::<Ev>::try_spawn(
        bad,
        Arc::new(CollectDrain::new()),
        Arc::new(LogErrorOverload),
    );
    assert_eq!(result.err(), Some(SinkConfigError::ZeroShards));
}

// ---- work stealing -----------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_stealing_drains_skewed_load_without_loss() {
    let collect = Arc::new(CollectDrain::new());
    let mut config = cfg(4, 4096, 128);
    config.work_stealing = WorkStealing::Opportunistic {
        max_victims_per_idle_tick: 3,
        max_items_per_victim: 32,
    };
    config.shard_selection = ShardSelection::ThreadLocalHome;
    let sink = ShardedSink::spawn_default_overload(config, Arc::clone(&collect));

    let handle = sink.issue_thread_local();
    let n = 10_000_u64;
    let mut accepted = 0_u64;
    for i in 0..n {
        if handle.push(Ev::new(i)) {
            accepted += 1;
        }
        if i % 128 == 0 {
            tokio::task::yield_now().await;
        }
    }

    sink.shutdown().await.expect("shutdown");
    assert_eq!(collect.observed() as u64, accepted);
    let mut ids: Vec<u64> = collect.snapshot().iter().map(|e| e.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len() as u64, accepted);
}
