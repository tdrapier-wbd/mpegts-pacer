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
#[derive(Clone, Copy, Debug, Default, PartialEq)]
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

	/// Content rate the pacer has recovered from the source PCR, in bits per
	/// second, and so the rate at which it is releasing media.
	///
	/// Against the rate the input is actually delivering, this is the one figure
	/// that separates a groomer shedding because the path handed it more than the
	/// mux can carry from a groomer shedding because it mis-read the stream and is
	/// releasing too slowly. The two look identical in `dropped_packets`.
	pub media_rate_bps: u64,

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

	/// De-jitter buffer occupancy at the moment of the snapshot, in 188-byte
	/// packets — the standing depth rather than the peak.
	///
	/// The release loop settles where this equals the cushion, so sampled over a
	/// long run it is the loop's error signal and the first place a slow drift
	/// shows. `buffer_high_water` cannot serve: it only ever rises, so it reports
	/// a single transient hours after the fact and says nothing about whether the
	/// stage is still holding its set point.
	pub buffer_packets: u64,

	/// Numerator of the recovered media rate: decayed packet count over the
	/// estimator's window.
	///
	/// `media_rate_bps` is a *ratio*, and a ratio that has gone wrong says
	/// nothing about which half did. The two accumulators reported separately
	/// distinguish a numerator that grows without bound from a denominator that
	/// vanishes, which are different defects with different fixes. In a healthy
	/// steady state this sits near `rate_pps * RATE_WINDOW`.
	pub rate_decayed_packets: f64,

	/// Denominator of the recovered media rate: decayed media seconds over the
	/// estimator's window. See `rate_decayed_packets`.
	///
	/// This one has a known correct value to check against rather than merely a
	/// plausible range: the window is an exponential decay with a fixed time
	/// constant, so whatever the input does, a converged denominator sits close
	/// to that constant. A denominator far below it means intervals are being
	/// admitted whose durations do not sum to real elapsed media time.
	pub rate_decayed_secs: f64,

	/// Source PCR intervals admitted to the rate estimate.
	///
	/// With the two accumulators this gives the mean admitted interval, and
	/// against the count of PCRs *seen* it says how many were rejected as
	/// zero-delta or discontinuous. An estimator fed one interval in a thousand
	/// is not smoothing, it is sampling.
	pub rate_intervals: u64,

	/// Source PCR-bearing packets seen by the rate estimator, admitted or not.
	pub rate_pcrs_seen: u64,

	/// Largest packet count attributed to any single admitted PCR interval.
	///
	/// The estimator sums packets and media seconds separately and divides
	/// once, which is unbiased *provided each interval's packet count belongs
	/// to that interval's media span*. Where it does not — a long run of
	/// packets carrying no PCR, closed by one PCR a short media step later —
	/// the pair lands in the sum as a large numerator against a small
	/// denominator. The mean interval stays normal, so only this maximum shows
	/// it.
	pub rate_max_packets_in_interval: u64,

	/// Admitted PCR intervals too short to serve as a rate sample on their own.
	pub rate_sub_ms_intervals: u64,

	/// The source's clock has stopped advancing usefully: programme keeps
	/// arriving while its PCR does not move.
	///
	/// This is a *source* fault the groomer can see and the wire cannot, which
	/// is the whole reason it is a counter. The groomer restamps PCR at the mux
	/// rate, so its output stays conformant — correct continuity, correct
	/// repetition, exact CBR — while its input has no usable timebase at all.
	/// Measured on the media-aware lane: the exporter's PCR degenerated into a
	/// one-tick-per-packet counter and every downstream check stayed green.
	pub rate_clock_stalled: bool,

	/// Packets held back from the rate estimate waiting for a source interval
	/// long enough to serve as a sample. Climbing means `rate_clock_stalled`.
	pub rate_pending_packets: u64,

	/// Source PCR intervals, under [`Clocking::Stream`](crate::Clocking), that
	/// delivered more packets than the interval's own PCR values leave room for.
	///
	/// Stream clocking places a run of packets across the slot span its two
	/// bounding PCR *values* imply, so it assumes the source's PCR *byte
	/// positions* advance with its PCR values — that comparable spans of media
	/// time carry comparable numbers of bytes. A source that emits exact PCR
	/// values at clustered byte positions satisfies every check on the values and
	/// still breaks that assumption. This counts how often.
	///
	/// Non-zero is normal: video is not flat, and a peak legitimately spills into
	/// the slots after it (see
	/// [`Stats::pcr_position_displacement`](Stats::pcr_position_displacement) for
	/// the figure that tells a peak from a pathology).
	pub pcr_position_overruns: u64,

	/// Furthest placement ever ran past the slot the source's own PCR values
	/// imply, in 188-byte packets — the high-water mark of
	/// `next_free - next_pcr_slot`.
	///
	/// **This is the counter that distinguishes a rate peak from a positionally
	/// clustered source, and neither `dropped_packets` nor `resyncs` can.** A peak
	/// displaces the grid and then recovers, because the runs after it under-fill
	/// their span and the displacement decays; so this settles at a bound. A
	/// source whose PCR byte cadence does not track its PCR value cadence
	/// displaces the grid a little further on every cycle and never recovers, so
	/// this climbs without limit.
	///
	/// It matters because the symptom is otherwise misattributed. Displacement
	/// past `max_latency` makes the leg's live edge outrun its own output clock,
	/// which trips [`Stats::resyncs`](Stats::resyncs) and discards programme by
	/// slot — and `resyncs` reads as "the configured rate is below the content
	/// rate". For a clustered source that diagnosis is wrong: the average rate can
	/// be exactly what the grid was provisioned for, and raising the rate does not
	/// help, because the excess is positional rather than volumetric.
	pub pcr_position_displacement: u64,

	/// [`Stats::pcr_position_displacement`] as a duration at the mux rate — the
	/// buffer depth a leg needs in order to absorb it.
	///
	/// The figure in packets cannot be read without the rate, and this is the one
	/// an operator sets `max_latency` against.
	pub pcr_position_displacement_ms: u64,
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
