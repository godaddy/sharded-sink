# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

Initial release.

- Bounded, lossy, **sharded fan-in sink** built on one
  `crossbeam_queue::ArrayQueue` per shard, with one poll-drain worker per shard
  running a caller-supplied `SinkAction` off the producer critical path.
- Producer hot path is a single lock-free `ArrayQueue::push`; no await, lock,
  allocation, or counter touched on success. Items need only `T: Send`.
- Deterministic shard selection (`issue`, `issue_thread_local`, handle-less
  `push`), bounded drain-side work stealing, overload monitoring, and
  producer-quiesced shutdown.
- Drain workers yield cooperatively after each batch, and a panic in
  `SinkAction::drain` is caught so it cannot wedge a shard.
- Set `shards: 1` for approximate global FIFO ordering.
- Optional `metrics` feature for drop/overload counters from the monitor.

[Unreleased]: https://github.com/godaddy/sharded-sink/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/godaddy/sharded-sink/releases/tag/v0.1.0
