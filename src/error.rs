//! Error types for the sink lifecycle.

use std::fmt;

/// Error returned by [`ShardedSink::shutdown`](crate::ShardedSink::shutdown).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ShutdownError {
    /// The configured `shutdown_timeout` elapsed before all internal tasks
    /// (drain workers and the overload monitor) finished.
    ///
    /// This usually means a [`SinkAction::drain`](crate::SinkAction::drain) call
    /// is still running and exceeded the budget. Buffered items may remain
    /// undrained.
    TimedOut,

    /// One or more internal tasks panicked.
    ///
    /// The sink still cancels and joins the remaining tasks, but the panic is
    /// surfaced so it is not silently swallowed.
    WorkerPanicked,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut => f.write_str("sink shutdown timed out before all workers finished"),
            Self::WorkerPanicked => f.write_str("a sink worker panicked during shutdown"),
        }
    }
}

impl std::error::Error for ShutdownError {}
