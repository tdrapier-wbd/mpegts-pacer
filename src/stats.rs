//! Pacing statistics.

/// A running tally of what the pacer has emitted and dropped.
///
/// All counters are cumulative for the life of the pacer. `null_packets /
/// output_packets` is the stuffing ratio; a climbing `dropped_packets` means the
/// input sustained a rate above the configured bitrate (or burst past
/// `max_latency`), and a climbing `underruns` means the input starved the buffer
/// so null packets were sent to hold the rate.
///
/// `stalls` and `muted_packets` are the content-liveness counters: they say the
/// source stopped delivering altogether, which no other counter here can express
/// (an output holding its rate on pure stuffing looks identical to a healthy one).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
	/// Total 188-byte packets written to the sink (content + null).
	pub output_packets: u64,
	/// Content (non-null) packets written to the sink.
	pub content_packets: u64,
	/// Null (stuffing) packets inserted to hold the constant bitrate.
	pub null_packets: u64,
	/// Input packets dropped because the jitter buffer was at `max_latency`.
	pub dropped_packets: u64,
	/// Input null/stuffing packets stripped at ingest (their padding is replaced
	/// by the pacer's own rate stuffing).
	pub input_nulls_stripped: u64,
	/// Output slots that emitted a null because the buffer was empty (starved)
	/// while the input was still live. Slots skipped under a stall are counted by
	/// `muted_packets` instead: a starved buffer and an absent source are
	/// different faults and conflating them hides both.
	pub underruns: u64,
	/// PCR anchor re-bases from a genuine source discontinuity (>5 s PCR jump).
	pub pcr_rebases: u64,
	/// Byte-locked PCR-only packets inserted to hold the repetition limit.
	pub pcr_inserted: u64,
	/// Times the input went silent past
	/// [`Config::stall`](crate::Config::stall). A non-zero value
	/// means the source died (or paused) at least once, whatever the carrier
	/// looked like.
	pub stalls: u64,
	/// Output slots not emitted because the input was stalled (under
	/// [`StallPolicy::Mute`](crate::StallPolicy::Mute)). The output byte clock
	/// still advanced across them, so this is the length of the carrier gap in
	/// 188-byte packets.
	pub muted_packets: u64,
	/// Longest interval, in milliseconds, that the input carried no content.
	pub content_gap_max_ms: u64,
	/// Packets discarded under [`Clocking::Stream`](crate::Clocking) because
	/// their output slot had already been transmitted.
	///
	/// The alternative — emitting them in the next free slot — would make their
	/// position depend on how late they were, which is precisely the per-process
	/// variation stream clocking exists to remove. A climbing count means the
	/// release latency is too short for the path's jitter, not that the pair has
	/// diverged: both legs drop by slot, so a leg that receives the packet in time
	/// simply covers for one that did not.
	pub late_drops: u64,
	/// Times the output clock was moved to the live edge under
	/// [`Clocking::Stream`](crate::Clocking) because the leg was holding more
	/// stream than `max_latency`.
	///
	/// One at start-up is a leg joining a broadcast already in progress and
	/// taking delivery of the relay's backlog. A count that keeps climbing means
	/// the path is delivering faster than the mux rate for long enough to build
	/// a buffer, i.e. the configured rate is below the content rate.
	pub resyncs: u64,
	/// Stream in hand when the output clock started, in 188-byte packets at the
	/// mux rate — the depth of the backlog a leg was handed as it joined.
	///
	/// Zero for a leg that started with the broadcast. A large value on a leg
	/// that joined one already running says the relay served it from well behind
	/// the live edge, which is where its phase relative to a partner comes from.
	pub start_backlog: u64,

	/// Largest burst the input delivered, in 188-byte packets, grouping arrivals
	/// separated by more than [`BURST_SEPARATION`](crate::BURST_SEPARATION).
	///
	/// This is the pacer measuring the burstiness of its own input, at the same
	/// grouping threshold an external cadence instrument would use, so the two are
	/// comparable. It is the figure that differs by two orders of magnitude
	/// between data planes: tens of kilobytes on an object transport, megabytes on
	/// a segment-fetching one.
	pub burst_max_packets: u64,

	/// Bursts the input delivered, at the same grouping threshold.
	///
	/// With `burst_max_packets` and `content_gap_max_ms` this is the arrival
	/// pattern in three numbers: how many deliveries, how big the largest, and how
	/// long the longest silence between them.
	pub bursts: u64,

	/// Largest lead the input built, in milliseconds — the media it handed over
	/// ahead of real time, and so the buffer occupancy its arrival pattern forces.
	///
	/// Near zero on a feed delivered at the media rate, whatever its burst size;
	/// about one segment duration on a feed fetched a segment at a time. This is
	/// what an adaptive cushion is sized from.
	pub arrival_lead_ms: u64,

	/// De-jitter cushion currently in force, in milliseconds.
	///
	/// Fixed for the run under [`Latency::Fixed`](crate::Latency); under
	/// [`Latency::Adaptive`](crate::Latency) it is what the pacer concluded it
	/// needed from `arrival_lead_ms`, and worth logging next to it.
	pub latency_target_ms: u64,

	/// Deepest the de-jitter buffer ever got, in 188-byte packets.
	///
	/// Against the configured or derived bound, this says how much of the buffer
	/// the input actually used. Sitting at the bound with `dropped_packets`
	/// climbing means the bound is too low for the arrival pattern.
	pub buffer_high_water: u64,
}

impl Stats {
	/// Fraction of output packets that were null stuffing, in `0.0..=1.0`.
	pub fn null_ratio(&self) -> f64 {
		if self.output_packets == 0 {
			0.0
		} else {
			self.null_packets as f64 / self.output_packets as f64
		}
	}
}
