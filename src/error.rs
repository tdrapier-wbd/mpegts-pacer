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

	/// The source's PCR byte positions do not advance with its PCR values, by
	/// more than the buffer can absorb, under
	/// [`PcrPositionPolicy::Fail`](crate::PcrPositionPolicy::Fail).
	///
	/// Not a malformed stream: the PCR values can be an exact grid and every
	/// check on them pass. It is that the packets carrying them are clustered, so
	/// spreading each run across the span its own values imply displaces the grid
	/// further on every cycle instead of recovering. Past `max_latency` the leg's
	/// live edge outruns its own output clock and programme is discarded by slot,
	/// which is why this is an error rather than a statistic: the output would
	/// still carry a byte-locked PCR and still pass a downstream conformance
	/// check, over a stream that had quietly lost content.
	#[error(
		"source PCR positions do not track its values: placement ran {displacement_packets} \
		 packets past the source grid ({overruns} intervals over their own span)"
	)]
	SourcePcrPosition {
		/// Furthest placement ran past the slot the source's PCR values imply, in
		/// 188-byte packets.
		displacement_packets: u64,
		/// PCR intervals that carried more packets than their own span had slots for.
		overruns: u64,
	},
}

/// Convenience alias for a [`Result`](std::result::Result) with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
