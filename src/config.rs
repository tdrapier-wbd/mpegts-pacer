//! Pacer configuration.

use std::time::Duration;

use crate::error::Error;

/// How the pacer treats the PCR (Program Clock Reference) already present in the
/// input.
///
/// The choice depends on the downstream receiver, not on the source transport:
///
/// - [`PcrMode::Preserve`] leaves every PCR value byte-for-byte untouched and
///   only paces transmission. A soft IRD / player that recovers the clock from
///   PCR *values* and re-buffers plays this back cleanly. Note that once null
///   packets are stuffed to hit the target rate, a preserved PCR no longer sits
///   at the byte position a constant-rate demuxer would expect, so a hardware
///   IRD that checks PCR-vs-byte accuracy (`tsp -P pcrverify`) will flag it.
/// - [`PcrMode::Regenerate`] rewrites each PCR-bearing packet's PCR to the value
///   implied by its byte offset at the target rate, so `PCR == byte_offset * 8 *
///   27_000_000 / bitrate` by construction. This is what a CBR/ASI hardware IRD's
///   PCR-accuracy and repetition checks require. Only the six PCR octets (and the
///   discontinuity indicator across a genuine source discontinuity) are touched;
///   PID structure, continuity counters, and PSI/PES payloads are preserved.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PcrMode {
	/// Keep source PCR values verbatim; pace transmission only.
	Preserve,
	/// Byte-lock PCR to the output rate for hardware-IRD PCR accuracy.
	#[default]
	Regenerate,
}

/// What the pacer does once its input has been silent for longer than
/// [`Config::stall`] allows.
///
/// A pacer holds the wire at a constant rate by stuffing null packets, which is
/// exactly right for absorbing transport jitter and exactly wrong for absorbing
/// the *absence* of a source: left alone, it emits a byte-perfect carrier — valid
/// transport, correct rate, PCR present — carrying no programme at all, for as
/// long as it is left running. Downstream monitoring and 1+1 receivers key on
/// packet arrival, loss and continuity, all of which read healthy, so a dead feed
/// looks like a live one. This enum is where that is decided explicitly.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StallPolicy {
	/// Stop emitting while the input is stalled, then resume when content
	/// returns. The task stays alive and the output byte clock keeps running
	/// through the gap, so the regenerated PCR is still wall-clock-aligned on
	/// resume. Downstream sees the carrier stop, which is what an IRD input
	/// failover or an ST 2022-7 receiver can actually detect.
	#[default]
	Mute,

	/// Keep emitting the constant-bitrate carrier through the stall. The output
	/// stays at rate with no programme in it, so nothing downstream that keys on
	/// packet arrival can tell the source has gone; use it only where something
	/// else supervises content liveness and the carrier must not drop. Even here
	/// the pacer stops inserting its own PCR while stalled, so the stream does not
	/// claim a clock it no longer has.
	Continue,

	/// Stop and return [`Error::SourceStalled`](crate::Error::SourceStalled), so a
	/// supervisor (a process manager, or the gateway that spawned the pacer) makes
	/// the decision.
	Fail,
}

/// What decides *which* packet occupies each output slot.
///
/// Both modes hold the wire at the configured constant rate, and in both the wall
/// clock decides *when* a slot is transmitted. They differ in whether the wall
/// clock also decides *what* goes in it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Clocking {
	/// Release content at the media rate recovered from the source PCR, measured
	/// against the pacer's own emit clock, and stuff the remaining slots.
	///
	/// Correct for a single output, and the mode every published measurement in
	/// this crate was made in. Its content/stuffing interleave depends on the
	/// instant each datagram is emitted, so two pacers fed the same input over
	/// independent paths produce different — though individually valid — output.
	#[default]
	Arrival,

	/// Place every packet on the absolute slot its source PCR implies at the
	/// locked mux rate, independent of arrival time, start time and emit jitter.
	///
	/// Two pacers in this mode fed the same objects emit the same bytes in the
	/// same slots, which is what an ST 2022-7 receiver needs from a redundant
	/// pair built out of two independent chains. It also makes a leg's numbering
	/// a property of the stream rather than of the leg, so one that starts late,
	/// or stops and returns, lands on the grid its partner is already using.
	///
	/// Requires [`Bitrate::Constant`] — an auto rate measured from an arrival
	/// window is exactly the kind of per-process quantity this mode exists to
	/// remove — and [`PcrMode::Regenerate`], since the emitted PCR *is* the slot
	/// position. The source must carry PCR; without it there is no grid.
	Stream,
}

/// How deep the de-jitter buffer is: explicit depths, or depths derived from the
/// arrival pattern the input turns out to have.
///
/// Burst granularity differs by two orders of magnitude between data planes. A
/// MoQ egress arrives in ~12 kB bursts whose worst silence is around 150 ms, so
/// milliseconds of buffer are enough. A segmented-HTTP egress carrying the same
/// programme arrives in megabyte bursts with silences of several seconds, so it
/// needs seconds. A groomer that only tolerates one of those patterns is a
/// groomer for one data plane, and the depth is the whole difference.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Latency {
	/// Exactly these depths, whatever the input does.
	///
	/// `target` is the priming cushion and the steady-state depth the scheduler
	/// aims to hold; `max` is the hard cap past which input is dropped oldest-
	/// first. Output starts one `target` after the first packet arrives.
	///
	/// Use this when the arrival pattern is known — a segment duration fixed by
	/// the packager, say — or when the depth must not be a property of the run,
	/// which is why [`Clocking::Stream`] requires it.
	Fixed {
		/// Priming cushion and steady-state target depth.
		target: Duration,
		/// Hard cap on buffered media.
		max: Duration,
	},

	/// Sized from how far ahead of real time the input delivers.
	///
	/// The pacer measures the *lead* its input builds — the media it hands over
	/// faster than real time, which is precisely the occupancy the arrival
	/// pattern forces — and holds `lead * factor`, bounded by `floor` and
	/// `ceiling`. A continuous feed delivered at the media rate never gets ahead,
	/// so it settles on `floor` and behaves as it always has; a feed that arrives
	/// one segment at a time settles on a multiple of the segment duration.
	///
	/// `factor` covers the difference between a normal gap and the worst one. A
	/// segment-fetching client that misses a publish cycle waits two segment
	/// periods rather than one, so the default leaves room for roughly double.
	///
	/// The output start is gated on content rather than on a timer here: a burst
	/// whose size is not yet known cannot size a cushion, so the pacer waits for
	/// the delivery to finish before committing to a depth, up to `ceiling`.
	Adaptive {
		/// Depth never goes below this, so a continuous feed is unaffected.
		floor: Duration,
		/// Depth never goes above this, which also bounds the start deadline.
		ceiling: Duration,
		/// Multiple of the observed lead to hold.
		factor: f64,
	},
}

/// When silence stops being jitter and starts being a dead source.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stall {
	/// Never. Silence is absorbed for as long as it lasts, which is what every
	/// pacer did before this was configurable.
	Off,

	/// After exactly this long without content.
	After(Duration),

	/// Once the cushion is spent and `grace` has passed on top of it.
	///
	/// The cushion is what the pacer is *prepared* to ride out, so the timeout
	/// belongs on the far side of it: a feed holding four seconds of programme
	/// has not lost its source at one second of silence, and a feed holding two
	/// hundred milliseconds has. Deriving it removes the failure mode where
	/// raising the buffer for a segmented input leaves the stall timeout an order
	/// of magnitude too tight, and mutes the carrier through every ordinary
	/// inter-segment gap.
	Adaptive {
		/// Grace on top of the cushion before the source counts as gone.
		grace: Duration,
	},
}

/// The output bitrate target: an explicit constant rate, or one derived
/// automatically from the source.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Bitrate {
	/// Exact constant output rate, in bits per second, measured over the full
	/// 188-byte packets (content plus stuffing).
	Constant(u64),

	/// Derive the output rate from the source's measured content bitrate, times
	/// `1 + headroom`. The rate is measured over a short warm-up window (a few
	/// PCR samples) and then locked for the rest of the run, so the output is
	/// still true CBR.
	///
	/// This recovers the source's true *content* rate, not a padded original mux
	/// rate: MoQ (and most IP transports) carry only media and strip the
	/// source's null stuffing, so the original mux rate no longer exists by the
	/// time packets reach the pacer. `headroom` (e.g. `0.15` for +15%) leaves
	/// slack above the measured average for VBR peaks and the pacer's own
	/// stuffing. Too little risks buffer overflow on peaks; too much wastes
	/// bandwidth on nulls. When the source has no usable PCR to measure, the
	/// rate falls back to [`DEFAULT_AUTO_FALLBACK`].
	Auto {
		/// Fractional headroom above the measured content rate (`0.15` = +15%).
		headroom: f64,
	},
}

/// Configuration for a [`crate::TsPacer`] / [`crate::pace`] run.
///
/// Construct with [`Config::new`] (an explicit target bitrate) or
/// [`Config::auto`] (derive the rate from the source), then refine with the
/// `with_*` setters. The struct is `#[non_exhaustive]` so future options can be
/// added without a breaking change; build it through the constructors rather than
/// a struct literal.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Config {
	/// Output bitrate target: a fixed [`Bitrate::Constant`] rate or
	/// [`Bitrate::Auto`] measured from the source. A constant rate must be at
	/// least the peak instantaneous input rate or the jitter buffer will
	/// overflow.
	pub bitrate: Bitrate,

	/// How deep the de-jitter buffer is: explicit depths, or depths derived from
	/// the arrival pattern. See [`Latency`].
	pub latency: Latency,

	/// How the PCR is handled. See [`PcrMode`].
	pub pcr: PcrMode,

	/// Number of 188-byte packets coalesced into each output datagram. The
	/// broadcast default is 7 (7 * 188 = 1316 bytes, the standard MPEG-TS-over-
	/// UDP/RTP payload that fits a 1500-byte MTU).
	pub packets_per_datagram: usize,

	/// Maximum PCR repetition interval to hold on the output (TR 101 290 P1 caps
	/// this at 40 ms). Under [`PcrMode::Regenerate`], when the source's PCR is
	/// sparser than this, the pacer inserts extra byte-locked PCR-only packets on
	/// the PCR PID (using otherwise-null rate-stuffing slots) so a hardware IRD's
	/// PLL never starves. Ignored under [`PcrMode::Preserve`], which inherits the
	/// source's PCR cadence untouched.
	pub pcr_max_interval: Duration,

	/// When the input's silence stops being jitter and starts being a dead source,
	/// at which point [`Config::stall_policy`] applies. See [`Stall`].
	///
	/// However it is set, it must comfortably exceed the worst recovery the
	/// transport does invisibly — a relay reselect, a reconnect, the wait for the
	/// next segment to be published — or the pacer will tear down the carrier for
	/// the very events it exists to absorb.
	pub stall: Stall,

	/// What to do once the input has been silent past [`Config::stall`].
	/// See [`StallPolicy`].
	pub stall_policy: StallPolicy,

	/// What decides which packet occupies each output slot. See [`Clocking`].
	pub clocking: Clocking,

	/// Added to the slot-derived RTP sequence number in [`Clocking::Stream`], so
	/// a pair can be offset as a whole without either leg's numbering ceasing to
	/// be a function of stream position. Both legs of a pair must use the same
	/// seed. Ignored in [`Clocking::Arrival`].
	pub sequence_seed: u16,
}

/// Default packets per output datagram: 7 * 188 = 1316 bytes.
pub const DEFAULT_PACKETS_PER_DATAGRAM: usize = 7;
/// Default de-jitter cushion floor, and the depth an explicit
/// [`Latency::Fixed`] gets when only its cap is set.
///
/// A continuous feed's lead never approaches this, so it is the depth every
/// rate-matched input ends up with — which is what the pacer has always used.
pub const DEFAULT_LATENCY: Duration = Duration::from_millis(200);
/// Default hard latency bound, and the floor under an adaptive cap.
pub const DEFAULT_MAX_LATENCY: Duration = Duration::from_millis(2_000);
/// Default ceiling on an adaptively sized cushion, and so on the wait before
/// output starts.
///
/// Eight seconds covers a two-second segment fetched over a path that
/// occasionally misses a publish cycle and collects two segments at once, which
/// is the worst arrival pattern measured on a segmented-HTTP egress. Raise it for
/// longer segments; at ten megabits it costs about ten megabytes resident.
pub const DEFAULT_LATENCY_CEILING: Duration = Duration::from_secs(8);
/// Default multiple of the observed lead to hold as the cushion.
///
/// A segment-fetching client that misses a publish cycle waits two segment
/// periods instead of one, so the cushion has to be about twice the lead a normal
/// cycle builds. The extra half is margin.
pub const DEFAULT_LATENCY_FACTOR: f64 = 2.5;
/// Default maximum PCR repetition interval (TR 101 290 P1 limit).
pub const DEFAULT_PCR_MAX_INTERVAL: Duration = Duration::from_millis(40);
/// Default grace on top of the cushion before the input counts as stalled.
///
/// One second is past the sub-second recovery an IP transport performs on its own
/// (a relay reselect, a reconnect), so it does not fire on events the pacer is
/// meant to absorb, while still declaring a dead source inside the window an IRD
/// input-failover would notice.
pub const DEFAULT_STALL_GRACE: Duration = Duration::from_secs(1);
/// Default headroom for [`Bitrate::Auto`]: 15% above the measured content rate.
pub const DEFAULT_AUTO_HEADROOM: f64 = 0.15;
/// Fallback output rate for [`Bitrate::Auto`] when the source carries no usable
/// PCR to measure (bits per second).
pub const DEFAULT_AUTO_FALLBACK: u64 = 4_000_000;

impl Default for Latency {
	fn default() -> Self {
		Self::Adaptive {
			floor: DEFAULT_LATENCY,
			ceiling: DEFAULT_LATENCY_CEILING,
			factor: DEFAULT_LATENCY_FACTOR,
		}
	}
}

impl Latency {
	/// The cushion to hold, given the lead the input has been observed to build.
	///
	/// `lead` is ignored under [`Latency::Fixed`], which is the point of it.
	pub fn target(&self, lead: Duration) -> Duration {
		match *self {
			Latency::Fixed { target, .. } => target,
			Latency::Adaptive { floor, ceiling, factor } => lead.mul_f64(factor.max(0.0)).clamp(floor, ceiling),
		}
	}

	/// The hard cap on buffered media, given the cushion in force.
	///
	/// Under an adaptive cushion the cap is twice the target: the pacer overshoots
	/// its target by at most one delivery while waiting to start, and a path that
	/// occasionally hands over two segments at once needs room for the second.
	/// [`DEFAULT_MAX_LATENCY`] is the floor, so a continuous feed keeps the cap it
	/// has always had rather than having it tightened around its small lead.
	pub fn cap(&self, target: Duration) -> Duration {
		match *self {
			Latency::Fixed { max, .. } => max,
			Latency::Adaptive { .. } => (target * 2).max(DEFAULT_MAX_LATENCY),
		}
	}

	/// The longest the pacer will hold output back waiting to size its cushion.
	/// `None` under [`Latency::Fixed`], which starts on a timer instead.
	pub fn start_deadline(&self) -> Option<Duration> {
		match *self {
			Latency::Fixed { .. } => None,
			Latency::Adaptive { ceiling, .. } => Some(ceiling),
		}
	}
}

impl Default for Stall {
	fn default() -> Self {
		Self::Adaptive {
			grace: DEFAULT_STALL_GRACE,
		}
	}
}

impl Stall {
	/// The silence after which the source counts as gone, given the cushion in
	/// force. `None` disables stall detection.
	pub fn timeout(&self, target: Duration) -> Option<Duration> {
		match *self {
			Stall::Off => None,
			Stall::After(timeout) => Some(timeout),
			Stall::Adaptive { grace } => Some(target.saturating_add(grace)),
		}
	}
}

impl Config {
	/// Create a config for the given target `bitrate` (bits per second) with
	/// default latency, PCR regeneration, and 7-packet datagrams.
	pub fn new(bitrate: u64) -> Self {
		Self::with(Bitrate::Constant(bitrate))
	}

	/// Create a config that derives the output rate from the source's measured
	/// content bitrate (see [`Bitrate::Auto`]), with [`DEFAULT_AUTO_HEADROOM`].
	///
	/// Auto-rate adds a short measurement window before the first packet is
	/// emitted, so the effective startup latency is higher than an explicit
	/// [`Config::new`]. Prefer an explicit rate when you know it and want minimal
	/// latency.
	pub fn auto() -> Self {
		Self::with(Bitrate::Auto {
			headroom: DEFAULT_AUTO_HEADROOM,
		})
	}

	fn with(bitrate: Bitrate) -> Self {
		Self {
			bitrate,
			latency: Latency::default(),
			pcr: PcrMode::default(),
			packets_per_datagram: DEFAULT_PACKETS_PER_DATAGRAM,
			pcr_max_interval: DEFAULT_PCR_MAX_INTERVAL,
			stall: Stall::default(),
			stall_policy: StallPolicy::default(),
			clocking: Clocking::default(),
			sequence_seed: 0,
		}
	}

	/// Set the output bitrate target.
	pub fn with_bitrate(mut self, bitrate: Bitrate) -> Self {
		self.bitrate = bitrate;
		self
	}

	/// The resolved constant output rate, or `None` for [`Bitrate::Auto`] (which
	/// is resolved from the source at run time by [`crate::pace`] /
	/// [`crate::TsPacer`]).
	pub fn resolved_bitrate(&self) -> Option<u64> {
		match self.bitrate {
			Bitrate::Constant(bitrate) => Some(bitrate),
			Bitrate::Auto { .. } => None,
		}
	}

	/// Pin the de-jitter cushion to `latency`, leaving the hard cap alone.
	///
	/// Setting a depth explicitly turns adaptive sizing *off*: an operator who has
	/// said how deep the buffer should be has said it, and a pacer that then went
	/// and chose its own would be the harder thing to reason about. Use
	/// [`Config::with_adaptive_latency`] to go back.
	pub fn with_latency(mut self, latency: Duration) -> Self {
		self.latency = Latency::Fixed {
			target: latency,
			max: self.latency.cap(latency),
		};
		self
	}

	/// Pin the hard upper bound on buffered media, leaving the cushion alone.
	pub fn with_max_latency(mut self, max_latency: Duration) -> Self {
		self.latency = Latency::Fixed {
			target: self.latency.target(Duration::ZERO),
			max: max_latency,
		};
		self
	}

	/// Size the de-jitter buffer from the input's observed arrival pattern,
	/// between `floor` and `ceiling`. See [`Latency::Adaptive`].
	pub fn with_adaptive_latency(mut self, floor: Duration, ceiling: Duration, factor: f64) -> Self {
		self.latency = Latency::Adaptive { floor, ceiling, factor };
		self
	}

	/// Set the de-jitter and stall behaviour from a known segment duration.
	///
	/// The escape hatch for a segmented input whose packager's segment duration is
	/// known: it pins the same depths adaptive sizing would converge on, without
	/// waiting to observe them, so output starts one cushion after the first
	/// packet rather than after two deliveries.
	pub fn with_segment_duration(self, segment: Duration) -> Self {
		let target = segment.mul_f64(DEFAULT_LATENCY_FACTOR);
		self.with_latency(target).with_max_latency(target * 2)
	}

	/// Set the PCR handling mode.
	pub fn with_pcr_mode(mut self, pcr: PcrMode) -> Self {
		self.pcr = pcr;
		self
	}

	/// Set the number of packets coalesced into each output datagram.
	pub fn with_packets_per_datagram(mut self, packets: usize) -> Self {
		self.packets_per_datagram = packets.max(1);
		self
	}

	/// Set the maximum PCR repetition interval (PCR re-insertion threshold).
	pub fn with_pcr_max_interval(mut self, interval: Duration) -> Self {
		self.pcr_max_interval = interval;
		self
	}

	/// Pin the input-silence timeout, or `None` to disable stall detection.
	///
	/// As with [`Config::with_latency`], an explicit value turns the derived one
	/// off. Note that a timeout shorter than the cushion can never fire, because
	/// buffered media is still programme going to air.
	pub fn with_stall_timeout(mut self, timeout: Option<Duration>) -> Self {
		self.stall = match timeout {
			Some(timeout) => Stall::After(timeout),
			None => Stall::Off,
		};
		self
	}

	/// Derive the input-silence timeout from the cushion in force, plus `grace`.
	/// See [`Stall::Adaptive`].
	pub fn with_stall_grace(mut self, grace: Duration) -> Self {
		self.stall = Stall::Adaptive { grace };
		self
	}

	/// Set what happens once the input is stalled. See [`StallPolicy`].
	pub fn with_stall_policy(mut self, policy: StallPolicy) -> Self {
		self.stall_policy = policy;
		self
	}

	/// Set what decides which packet occupies each output slot. See [`Clocking`].
	pub fn with_clocking(mut self, clocking: Clocking) -> Self {
		self.clocking = clocking;
		self
	}

	/// Set the RTP sequence seed used in [`Clocking::Stream`].
	pub fn with_sequence_seed(mut self, seed: u16) -> Self {
		self.sequence_seed = seed;
		self
	}

	/// Reject combinations that cannot deliver what the mode promises.
	///
	/// [`Clocking::Stream`] exists so that two independent pacers agree; a rate
	/// measured per process, or a PCR left at its source value, would make the
	/// output depend on the process again. Failing here is better than emitting
	/// something that looks right and does not merge.
	pub fn validate(&self) -> Result<(), Error> {
		if let Latency::Fixed { target, max } = self.latency
			&& target > max
		{
			return Err(Error::Config(
				"latency exceeds max_latency: the priming cushion is deeper than the buffer is \
				 allowed to be, so the input would be dropped to make room for itself",
			));
		}
		if self.clocking != Clocking::Stream {
			return Ok(());
		}
		if self.resolved_bitrate().is_none() {
			return Err(Error::Config(
				"stream clocking needs an explicit constant bitrate: an auto rate is measured \
				 from one process's arrival window, so two legs would lock different grids",
			));
		}
		if self.pcr != PcrMode::Regenerate {
			return Err(Error::Config(
				"stream clocking needs PcrMode::Regenerate: the emitted PCR is the slot position",
			));
		}
		if matches!(self.latency, Latency::Adaptive { .. }) {
			return Err(Error::Config(
				"stream clocking needs an explicit latency: an adaptive cushion is measured from \
				 one leg's arrival window, so two legs would start on different slots and hold \
				 different depths, spending skew budget a receiver needs",
			));
		}
		Ok(())
	}
}
