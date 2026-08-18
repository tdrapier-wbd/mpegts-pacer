//! Error and result types for the pacer.

use std::time::Duration;

/// Errors surfaced by [`crate`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
	/// A byte buffer was not a well-formed 188-byte MPEG-TS packet.
	#[error("invalid MPEG-TS packet: {0}")]
	InvalidPacket(&'static str),

	/// An output sink (or packet source) failed with an I/O error.
	#[error(transparent)]
	Io(#[from] std::io::Error),

	/// The pacer's background emitter task has stopped, so no more packets can
	/// be pushed. Inspect the task's return value (via [`crate::TsPacer::close`])
	/// for the underlying cause.
	#[error("pacer output task has stopped")]
	Closed,

	/// A [`Config`](crate::Config) asks for something the mode cannot deliver —
	/// checked by [`Config::validate`](crate::Config::validate) before a run
	/// starts, rather than discovered as output that does not merge.
	#[error("invalid configuration: {0}")]
	Config(&'static str),

	/// The source delivered no content for longer than
	/// [`Config::stall`](crate::Config::stall), under
	/// [`StallPolicy::Fail`](crate::StallPolicy::Fail). A stalled source is not an
	/// I/O error: the transport is usually still open and simply silent, which is
	/// why it needs its own variant.
	#[error("source stalled: no content for {silent_for:?}")]
	SourceStalled {
		/// How long the input had been silent when the pacer gave up.
		silent_for: Duration,
	},
}

/// Convenience alias for a [`Result`](std::result::Result) with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
