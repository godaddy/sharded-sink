//! Property-style invariant tests (design section 16).
//!
//! Deterministic enumerations over shard counts and item counts rather than
//! randomized property tests, which keeps them dependency-free and stable.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{CollectDrain, Ev};
use sharded_sink::{ShardSelection, ShardedSink, SinkConfig, WorkStealing};

fn cfg(shards: usize, ring_capacity: usize, drain_batch: usize) -> SinkConfig {
    let mut c = SinkConfig::default();
    c.name = "prop";
    c.shards = shards;
    c.ring_capacity = ring_capacity;
    c.drain_batch = drain_batch;
    c.overload_check_interval = Duration::from_secs(3600);
    c.shard_selection = ShardSelection::ThreadLocalRoundRobin;
    c.idle_sleep = Duration::from_micros(100);
    c.work_stealing = WorkStealing::Off;
    c.shutdown_timeout = Some(Duration::from_secs(5));
    c
}

/// `attempted == accepted + rejected` and `rejected == dropped`, with no
/// shutdown racing the pushes.
///
/// Run on a current-thread runtime with no `.await` in the push loop, so the
/// drain workers never run: rings fill and surplus pushes are shed.
#[tokio::test(flavor = "current_thread")]
async fn accounting_identity_holds_across_shapes() {
    for &shards in &[1_usize, 2, 4, 8] {
        for &capacity in &[1_usize, 16, 100] {
            let sink = ShardedSink::spawn_default_overload(
                cfg(shards, capacity, 1),
                Arc::new(CollectDrain::new()),
            );

            let attempted = (shards * capacity * 4 + 7) as u64;
            let mut accepted = 0_u64;
            let mut rejected = 0_u64;
            for i in 0..attempted {
                if sink.push(Ev::new(i)) {
                    accepted += 1;
                } else {
                    rejected += 1;
                }
            }

            assert_eq!(
                accepted + rejected,
                attempted,
                "shards={shards} cap={capacity}"
            );
            assert_eq!(
                sink.dropped_total(),
                rejected,
                "every rejection is a drop (shards={shards} cap={capacity})"
            );

            sink.shutdown().await.expect("shutdown");
        }
    }
}

/// Exactly-once: every item the sink *accepts* is observed by the sink action
/// exactly once (no loss, no duplication), across shard counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_items_are_observed_exactly_once() {
    for &shards in &[1_usize, 2, 4, 8] {
        let collect = Arc::new(CollectDrain::new());
        let sink =
            ShardedSink::spawn_default_overload(cfg(shards, 8192, 128), Arc::clone(&collect));

        let n = 20_000_u64;
        let mut accepted = 0_u64;
        for i in 0..n {
            if sink.push(Ev::new(i)) {
                accepted += 1;
            }
            if i % 200 == 0 {
                tokio::task::yield_now().await;
            }
        }

        sink.shutdown().await.expect("shutdown");

        assert_eq!(
            collect.observed() as u64,
            accepted,
            "every accepted item observed once (shards={shards})"
        );
        let mut ids: Vec<u64> = collect.snapshot().iter().map(|e| e.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids.len() as u64,
            accepted,
            "no duplicates (shards={shards})"
        );
    }
}
