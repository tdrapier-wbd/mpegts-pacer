//! The deterministic constant-bitrate scheduler: the heart of the pacer.
//!
//! This is a pure, synchronous state machine with no I/O and no timers, so it is
//! exhaustively unit-testable by feeding it packets and synthetic [`Instant`]s.
//! The async driver in [`crate::pacer`] is a thin loop around it.
//!
//! It runs two clocks:
//!
//! - a **transport byte clock** at the configured mux rate. Output packet `n` is
//!   due at `anchor + n * 188 * 8 / bitrate`, so the wire rate is exactly
//!   constant. This clock also byte-locks the regenerated PCR.
//! - a **media clock** recovered from the source PCR. Content packets are
//!   released at the source's own media rate (estimated from PCR deltas), delayed
//!   by the configured latency, so the output preserves the source duration
//!   instead of draining a burst faster than real time. The rate is a feed-forward
//!   term only: release is closed-loop on buffer occupancy, because a feed that
//!   runs permanently cannot afford to integrate the error in an estimate. See
//!   [`MediaClock::observe`] and [`MediaClock::due`].
//!
//! Every output slot emits a content packet when the media clock says one is due
//! and the buffer has it, otherwise a null packet — except that a slot falling on
//! a PCR deadline is taken for the PCR whatever else wanted it, the content it
//! displaced waiting one slot. So a burst is absorbed by the [`JitterBuffer`] and
//! released at media rate, the wire stays CBR, and the repetition limit is held
//! independently of both. That last independence is the point: on a media-aware
//! source there is no spare slot inside a burst, and a PCR that waits for one
//! waits for the frame. See [`Scheduler::reinsert_pcr`].
//!
//! How much burst it can absorb is the one thing that differs by two orders of
//! magnitude between data planes, so under [`Latency::Adaptive`] the depths are
//! not configured but measured: an [`ArrivalProfile`] reports how far ahead of
//! real time the input delivers, and the cushion, the hard cap and the stall
//! timeout all follow from it. That also moves the decision to start emitting off
//! a timer and onto content, because a cushion cannot be sized against a burst
//! whose size is not yet known — and starting mid-burst on a segmented input is
//! what makes an ordinary inter-segment gap look like an underrun.
//!
//! It also tracks *content liveness* ([`SourceState`]), because stuffing to hold
//! a rate is the right answer to a late source and the wrong answer to an absent
//! one. Past the stall timeout the scheduler stops claiming a clock it no longer
//! has — no PCR is inserted into a stream with no content in it — and the driver
//! applies [`StallPolicy`](crate::StallPolicy).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::arrival::ArrivalProfile;
use crate::config::{Clocking, Config, DEFAULT_AUTO_FALLBACK, Latency, PcrMode, Stall};
use crate::jitter_buffer::JitterBuffer;
use crate::null_insertion::{NULL_PID, null_packet, pcr_only_packet};
use crate::observe::SourceState;
use crate::output::Framing;
use crate::packet::{Packet, TS_PACKET_SIZE};
use crate::pcr::{self, PACKET_BITS, PcrRegen};
use crate::slot::SlotMap;
use crate::stats::Stats;

/// Media-time constant of the content-rate estimator: how far back a PCR
/// interval still carries weight.
///
/// Long enough to span a group of pictures at broadcast frame rates, because the
/// estimator has to average over the *content* structure to see the content rate.
const RATE_WINDOW: f64 = 2.0;

/// How hard the release rate is trimmed to hold the buffer at the cushion: the
/// fractional rate change applied when occupancy is a full cushion away from it.
///
/// Small, because this is a media clock and the trim is a deliberate slew of it.
/// It only has to out-run the drift of a rate estimate, not chase a burst — the
/// buffer is what absorbs bursts.
const RATE_SERVO_GAIN: f64 = 0.05;

/// Recovers the source media rate from PCR deltas and paces content release.
#[derive(Debug)]
struct MediaClock {
	/// Wall time content packet 0 becomes eligible (first enqueue + latency).
	anchor: Option<Instant>,
	/// Wall time the release credit was last advanced.
	ticked: Option<Instant>,
	/// Content packets the release clock has authorised, fractional part kept so
	/// the rate is not quantised by the slot interval.
	credit: f64,
	/// Content packets released so far.
	released: u64,
	/// Estimated content rate in packets per second.
	rate_pps: f64,
	/// Whether `rate_pps` is a real PCR-derived estimate yet (vs the fallback).
	estimated: bool,
	/// Last source PCR observed, for delta-based rate estimation.
	last_pcr: Option<u64>,
	/// Packets enqueued since the last PCR (the numerator of a rate sample).
	packets_since_pcr: u64,
	/// Decayed packet count and decayed media seconds over [`RATE_WINDOW`]. The
	/// rate is their *ratio*; see [`MediaClock::observe`].
	decayed_packets: f64,
	decayed_secs: f64,
}

impl MediaClock {
	fn new(fallback_pps: f64) -> Self {
		Self {
			anchor: None,
			ticked: None,
			credit: 0.0,
			released: 0,
			rate_pps: fallback_pps,
			estimated: false,
			last_pcr: None,
			packets_since_pcr: 0,
			decayed_packets: 0.0,
			decayed_secs: 0.0,
		}
	}

	/// Observe an enqueued packet, refining the media-rate estimate on each PCR.
	///
	/// **The estimate is a ratio of sums, not a mean of ratios**, and on a
	/// media-aware source the two are not close. Averaging the per-interval rate
	/// `count / seconds` assumes each interval carries a comparable number of
	/// packets; a demuxed export does not, because a PCR interval holds either a
	/// coded frame or nothing much. Measured on a 20 s export: intervals on an
	/// exact 25 ms grid, but 1 to 4,631 packets each with a median of 8. The
	/// per-interval rates then have a median of 320 pps against a true rate of
	/// 6,191, so a smoothed average of them sits far below the truth for most of
	/// its life — 4,744 pps at the end of that clip, a 23% under-read. Release is
	/// `rate * elapsed`, so the pacer holds media back, the buffer fills to its
	/// bound, and it sheds the oldest programme while the wire runs a third
	/// stuffing. Summing the packets and the media seconds separately and dividing
	/// once is unbiased however the packets are distributed between the intervals.
	fn observe(&mut self, packet: &Packet) {
		self.packets_since_pcr = self.packets_since_pcr.saturating_add(1);
		let Some(pcr) = packet.pcr() else {
			return;
		};
		if let Some(last) = self.last_pcr {
			let delta = pcr::forward_delta(last, pcr);
			let secs = pcr::ticks_to_duration(delta).as_secs_f64();
			// Ignore zero deltas and discontinuities (loop wrap / splice); those
			// don't reflect a real elapsed interval.
			if delta > 0 && secs > 0.0 && pcr::ticks_to_duration(delta) <= pcr::PCR_DISCONTINUITY_GAP {
				let decay = (-secs / RATE_WINDOW).exp();
				self.decayed_packets = self.decayed_packets * decay + self.packets_since_pcr as f64;
				self.decayed_secs = self.decayed_secs * decay + secs;
				if self.decayed_secs > 0.0 {
					self.rate_pps = self.decayed_packets / self.decayed_secs;
					self.estimated = true;
				}
			}
		}
		self.last_pcr = Some(pcr);
		self.packets_since_pcr = 0;
	}

	/// The content rate recovered from the source PCR, or `None` while the
	/// estimate is still the fallback.
	///
	/// Only a real estimate is any use for sizing a buffer against arrival: the
	/// fallback is derived from the configured mux rate, so comparing arrival
	/// against it would measure the configuration rather than the stream.
	fn estimated_rate_pps(&self) -> Option<f64> {
		self.estimated.then_some(self.rate_pps)
	}

	/// Number of content packets that should have been released by `now`, holding
	/// the buffer at `target` packets deep.
	///
	/// **Release is a closed loop on buffer occupancy, not an open loop on the
	/// rate estimate**, and for a feed that runs permanently the difference is the
	/// whole thing. Releasing at `rate * elapsed` integrates every error in `rate`:
	/// a 2.5% under-read costs 2.5% of *uptime* in buffer depth, so occupancy and
	/// latency climb without limit and the leg eventually sheds the oldest
	/// programme to stay under its bound — measured on the media-aware lane as
	/// +1.8 s of delivery latency across a 90 s window and 10,279 packets shed at
	/// a 2.5 s bound. No rate estimator is exact enough to fix that, because
	/// nothing bounds the integral. Correcting the release rate by the occupancy
	/// error does bound it: the loop settles where occupancy equals the cushion,
	/// which is by definition where the leg is releasing at the rate it is being
	/// delivered. The estimate then only has to be close enough to keep the
	/// correction inside its clamp.
	fn due(&mut self, now: Instant, buffered: usize, target: f64) -> u64 {
		let Some(anchor) = self.anchor else {
			return 0;
		};
		if now < anchor {
			return 0;
		}
		let last = self.ticked.unwrap_or(anchor);
		if now > last {
			let dt = now.duration_since(last).as_secs_f64();
			self.ticked = Some(now);
			self.credit += dt * self.release_rate(buffered, target);
		}
		self.credit as u64 + 1
	}

	/// The rate to release at now: the estimated content rate, trimmed towards
	/// whatever would return the buffer to `target`. See [`MediaClock::due`].
	fn release_rate(&self, buffered: usize, target: f64) -> f64 {
		if target <= 0.0 {
			return self.rate_pps;
		}
		let error = ((buffered as f64 - target) / target).clamp(-1.0, 1.0);
		self.rate_pps * (1.0 + error * RATE_SERVO_GAIN)
	}
}

/// Places packets on the absolute output grid from the stream alone.
///
/// Packets are accumulated into *runs* — a PCR-bearing packet and everything up
/// to the next one — because a run's length is only known once the closing PCR
/// arrives. The run is then spread evenly between the two slots its PCRs imply,
/// or from the first free slot if the previous run overran, so where a packet
/// lands is a function of the delivered bytes and nothing else. The cost is one
/// PCR interval of look-ahead (~20 ms on a typical mux) inside the release
/// latency the pacer already holds.
///
/// Content that turns up after its slot has been transmitted is dropped rather
/// than carried forward, so a leg returning from an outage with a backlog behind
/// it rejoins at the current position instead of replaying what its partner has
/// already delivered.
#[derive(Debug)]
struct StreamGrid {
	map: SlotMap,
	/// The run being accumulated: its opening PCR packet, then the rest.
	run: Vec<Packet>,
	/// `(source PCR, absolute slot)` of the packet that opened the run.
	open: Option<(u64, u64)>,
	/// When the open run's PCR arrived. Compared against the next PCR's advance
	/// to tell a source discontinuity from a spell of silence; see
	/// [`StreamGrid::slot_for`].
	open_at: Option<Instant>,
	/// Placed packets in slot order, awaiting their slot.
	placed: VecDeque<(u64, Packet)>,
	/// Added to every computed slot to keep the grid monotonic across a 33-bit
	/// PCR wrap or a source discontinuity. Signed, because a splice can move the
	/// source clock either way.
	epoch: i128,
	/// Slots spanned by the previous run, used to place a trailing run at the
	/// same spacing when the source ends mid-run.
	last_span: Option<(u64, usize)>,
	/// The lowest slot still free. A PCR interval carrying more packets than the
	/// mux rate has room for spills into the slots after it rather than losing
	/// the excess; see [`StreamGrid::close_run`].
	next_free: u64,
	/// Runs that carried more packets than their own PCR span had slots for.
	overruns: u64,
	/// How far `next_free` ran past the slot the next source PCR asks for, at its
	/// worst. See [`Stats::pcr_position_displacement`].
	displacement_high_water: u64,
	capacity: usize,
}

impl StreamGrid {
	fn new(mux_rate_bps: u64, capacity: usize) -> Self {
		Self {
			map: SlotMap::new(mux_rate_bps),
			run: Vec::new(),
			open: None,
			open_at: None,
			placed: VecDeque::new(),
			epoch: 0,
			last_span: None,
			next_free: 0,
			overruns: 0,
			displacement_high_water: 0,
			capacity: capacity.max(1),
		}
	}

	/// Raise the placement bound to `capacity`, never lowering it. See
	/// [`JitterBuffer::grow_to`].
	fn grow_to(&mut self, capacity: usize) {
		self.capacity = self.capacity.max(capacity.max(1));
	}

	/// Absolute slot for a source PCR seen after `since` of wall time, keeping the
	/// grid monotonic.
	///
	/// A large forward PCR jump is ambiguous on its own: the source clock may have
	/// moved (a splice or a loop wrap), or it may have carried on while this leg
	/// missed the middle of the stream. Elapsed time tells the two apart. A jump
	/// that matches the silence is a *gap* — the source is still on the clock the
	/// grid was built from, so the returning content belongs in the slots its PCR
	/// says and needs no adjustment, which is how a leg that was cut off for
	/// twenty seconds comes back into its partner's numbering.
	///
	/// Returns the slot and whether it sits beyond the open run because the
	/// source went quiet, which the caller must not spread that run across.
	fn slot_for(&mut self, pcr: u64, since: Duration) -> (u64, bool) {
		let raw = self.map.slot_of_pcr(pcr);
		let mut after_silence = false;
		if let Some((open_pcr, open_slot)) = self.open {
			let delta = pcr::forward_delta(open_pcr, pcr);
			let advanced = pcr::ticks_to_duration(delta);
			let jumped = delta == 0 || advanced > pcr::PCR_DISCONTINUITY_GAP;
			// Content cannot arrive from the future: if we have been silent for
			// roughly as long as the stream advanced, we simply missed the middle.
			// The factor is loose because delivery is bursty and the returning
			// packet may sit in a relay buffer before it reaches us.
			let explained_by_silence = delta > 0 && advanced <= since.saturating_mul(2);
			if jumped && !explained_by_silence {
				// The source clock moved. Continue the grid from where the open run
				// ends: both legs of a pair compute the same continuation, so an
				// aligned pair stays aligned — but a leg that joins *after* a splice
				// cannot know it happened, which is a documented limit of stream
				// clocking.
				let resume_at = open_slot.saturating_add(self.run.len().max(1) as u64);
				self.epoch = i128::from(resume_at) - i128::from(raw);
			} else if i128::from(raw) + self.epoch < i128::from(open_slot) {
				// The 33-bit PCR value wrapped; the stream did not.
				self.epoch += i128::from(self.map.slots_per_wrap());
			} else {
				after_silence = jumped;
			}
		}
		((i128::from(raw) + self.epoch).max(0) as u64, after_silence)
	}

	/// Accept one input packet. Returns the number of packets dropped for want of
	/// room, so the caller can count them.
	fn push(&mut self, packet: Packet, now: Instant) -> u64 {
		let Some(pcr) = packet.pcr() else {
			// Before the first PCR there is no grid to place against. Buffering
			// these would only let them land in slots chosen by arrival order.
			if self.open.is_none() {
				return 1;
			}
			self.run.push(packet);
			return 0;
		};

		let since = self
			.open_at
			.map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
		let (slot, after_silence) = self.slot_for(pcr, since);
		// Content from before the silence belongs where it was going, not smeared
		// across the gap: spreading it would drop fresh packets into slots the
		// partner leg is filling with the programme that ran while we were away.
		let dropped = if after_silence {
			self.flush()
		} else {
			self.close_run(slot)
		};
		self.open = Some((pcr, slot));
		self.open_at = Some(now);
		self.run.clear();
		self.run.push(packet);
		dropped
	}

	/// Spread the open run between its own slot and `next_slot`.
	///
	/// **The assumption this makes, stated because it is not the same assumption
	/// as "the source's PCR values are correct".** A run is spread across the slot
	/// span its two bounding PCR *values* imply, so the placement is only faithful
	/// while the source's PCR *byte positions* advance with those values — while
	/// comparable spans of media time carry comparable numbers of bytes. Value
	/// cadence and positional cadence are separate properties of a source and are
	/// not interchangeable: a source can hold an exact 25 ms PCR value grid while
	/// emitting the packets that carry it back-to-back, with the media bytes they
	/// label heaped between the clusters. Every check on the values passes, and
	/// this function is then handed runs of one packet across a full span,
	/// alternating with runs of thousands across the same span.
	///
	/// A PCR interval can legitimately carry more packets than the mux rate has
	/// slots for — video is not flat, and a groomer is normally provisioned against
	/// the average rate rather than the peak. The excess spills into the slots
	/// after the run instead of being discarded, and later runs start from the
	/// first free slot, so a peak is absorbed by the stuffing that follows it and
	/// the stream catches back up. Placement stays a function of the delivered
	/// packets alone, which is what keeps two legs identical.
	///
	/// That recovery is what separates the two cases, so it is measured rather
	/// than assumed. A peak displaces the grid once and the displacement decays; a
	/// source whose positional cadence does not track its value cadence displaces
	/// it further every cycle. `displacement_high_water` is that difference, and
	/// [`Stats::pcr_position_displacement`] is where it surfaces — because the
	/// symptom otherwise arrives as a climbing `resyncs`, which reads as a rate
	/// that is too low and is not.
	fn close_run(&mut self, next_slot: u64) -> u64 {
		let Some((_, open_slot)) = self.open else {
			return 0;
		};
		let count = self.run.len();
		if count == 0 {
			return 0;
		}
		let span = next_slot.saturating_sub(open_slot);
		self.last_span = Some((span, count));
		if count as u64 > span {
			self.overruns = self.overruns.saturating_add(1);
		}
		let mut dropped = 0;
		for (index, packet) in self.run.drain(..).enumerate() {
			let ideal = open_slot + (index as u64).saturating_mul(span) / count as u64;
			let slot = ideal.max(self.next_free);
			self.next_free = slot + 1;
			self.placed.push_back((slot, packet));
			if self.placed.len() > self.capacity {
				self.placed.pop_front();
				dropped += 1;
			}
		}
		// Measured against where the *next* PCR asks the grid to be, not against
		// this run's own span, so a run that spills and is then absorbed reads as
		// zero on the following interval instead of accumulating.
		let displacement = self.next_free.saturating_sub(next_slot);
		self.displacement_high_water = self.displacement_high_water.max(displacement);
		dropped
	}

	/// How far placement has run past the source's own PCR grid, at its worst.
	fn displacement_high_water(&self) -> u64 {
		self.displacement_high_water
	}

	/// Runs that carried more packets than their own PCR span had slots for.
	fn overruns(&self) -> u64 {
		self.overruns
	}

	/// Place a run left open by the end of the source, at the previous run's
	/// spacing. Without this the last PCR interval of every stream is discarded.
	fn flush(&mut self) -> u64 {
		let Some((_, open_slot)) = self.open else {
			return 0;
		};
		let (span, count) = self.last_span.unwrap_or((self.run.len() as u64, self.run.len().max(1)));
		let scaled = span.saturating_mul(self.run.len() as u64) / count.max(1) as u64;
		let end = open_slot.saturating_add(scaled.max(self.run.len() as u64));
		self.close_run(end)
	}

	/// The packet due at `slot`, discarding any whose slot has already passed.
	/// A late packet is never re-placed: moving it would make its position a
	/// function of the delay rather than of the stream.
	fn take(&mut self, slot: u64) -> (Option<Packet>, u64) {
		let mut late = 0;
		while let Some((placed_slot, _)) = self.placed.front() {
			if *placed_slot < slot {
				self.placed.pop_front();
				late += 1;
			} else if *placed_slot == slot {
				let (_, packet) = self.placed.pop_front().expect("front checked");
				return (Some(packet), late);
			} else {
				break;
			}
		}
		(None, late)
	}

	/// The slot of the earliest packet awaiting emission.
	fn first_slot(&self) -> Option<u64> {
		self.placed.front().map(|(slot, _)| *slot)
	}

	/// The newest placed slot: the live edge of what this leg is holding.
	fn last_slot(&self) -> Option<u64> {
		self.placed.back().map(|(slot, _)| *slot)
	}

	/// Whether any content is placed and still to be transmitted.
	///
	/// The run still accumulating does not count. It cannot be placed until its
	/// closing PCR arrives, and if the source has died that PCR is never coming —
	/// counting it would leave the pacer believing it had programme in hand for
	/// as long as it was left running, which is the exact failure stall detection
	/// exists to prevent.
	fn has_content(&self) -> bool {
		!self.placed.is_empty()
	}
}

/// The CBR scheduler. See the module docs.
#[derive(Debug)]
pub struct Scheduler {
	mux_rate_bps: u64,
	/// How the depths are decided: pinned, or measured from arrival.
	latency: Latency,
	/// The cushion currently in force, from [`Latency::target`].
	target: Duration,
	/// What the input's arrival pattern has been observed to be, and so what the
	/// depths are sized against under [`Latency::Adaptive`].
	profile: ArrivalProfile,
	packets_per_datagram: usize,
	buffer: JitterBuffer,
	media: MediaClock,
	pcr_regen: Option<PcrRegen>,
	null: [u8; TS_PACKET_SIZE],
	/// PCR PID, learned from the first PCR-bearing packet.
	pcr_pid: Option<u16>,
	/// Continuity counter last seen on the PCR PID, for re-inserted packets.
	pcr_pid_cc: u8,
	/// Output index of the most recent PCR (content or re-inserted).
	last_pcr_index: Option<u64>,
	/// PCR re-insertion threshold, in output packets at the mux rate.
	pcr_max_packets: u64,
	/// Content admitted to the output but not yet transmitted, because a slot it
	/// would have used was taken by a re-inserted PCR. See [`Scheduler::admit`].
	deferred: VecDeque<Packet>,
	/// Wall time the output slot at `anchor_slot` is due (first content + latency).
	anchor: Option<Instant>,
	/// The output slot the wall-clock anchor refers to. Zero under
	/// [`Clocking::Arrival`]; the first placed slot under [`Clocking::Stream`],
	/// where the grid is absolute and a leg does not start at zero.
	anchor_slot: u64,
	/// Position of the next output packet: a running count under
	/// [`Clocking::Arrival`], an absolute grid index under [`Clocking::Stream`].
	slot: u64,
	/// Slot placement, present under [`Clocking::Stream`] only — its presence is
	/// what selects the mode everywhere below.
	grid: Option<StreamGrid>,
	/// Release latency and its hard upper bound, in output packets: the depth the
	/// leg aims to hold, and the depth past which it is behind the stream rather
	/// than buffering it.
	latency_packets: u64,
	max_latency_packets: u64,
	/// Added to the slot-derived RTP sequence number.
	sequence_seed: u16,
	/// When input silence stops being jitter and becomes a dead source.
	stall: Stall,
	/// Wall time content first arrived, which is where the start deadline runs
	/// from under [`Latency::Adaptive`].
	first_content_at: Option<Instant>,
	/// Wall time content last arrived (after stripping input stuffing).
	last_content_at: Option<Instant>,
	/// Whether the input was stalled as of the last output slot.
	stalled: bool,
	scratch: Vec<u8>,
	stats: Stats,
}

impl Scheduler {
	/// Build a scheduler from a [`Config`].
	///
	/// The scheduler runs at a fixed rate, so a [`Bitrate::Auto`](crate::Bitrate)
	/// config must be resolved to a concrete rate before it reaches here;
	/// [`crate::pace`] and [`crate::TsPacer`] do that from the source. Passed an
	/// unresolved auto config directly, it falls back to
	/// [`DEFAULT_AUTO_FALLBACK`].
	pub fn new(config: &Config) -> Self {
		let bitrate = config.resolved_bitrate().unwrap_or(DEFAULT_AUTO_FALLBACK).max(1);
		let fallback_pps = bitrate as f64 / PACKET_BITS as f64;
		// Nothing has arrived, so an adaptive cushion starts at its floor and
		// grows from there; a pinned one is already what it will be.
		let target = config.latency.target(Duration::ZERO);
		let capacity = latency_to_packets(config.latency.cap(target), bitrate).max(1);
		let pcr_regen = match config.pcr {
			PcrMode::Regenerate => Some(PcrRegen::new(bitrate)),
			PcrMode::Preserve => None,
		};
		Self {
			mux_rate_bps: bitrate,
			latency: config.latency,
			target,
			profile: ArrivalProfile::new(),
			packets_per_datagram: config.packets_per_datagram.max(1),
			buffer: JitterBuffer::new(capacity),
			media: MediaClock::new(fallback_pps),
			pcr_regen,
			null: null_packet(),
			pcr_pid: None,
			pcr_pid_cc: 0,
			last_pcr_index: None,
			// Inject with margin below the hard limit so datagram granularity and
			// waiting for a stuffing slot can't push an interval past it.
			pcr_max_packets: latency_to_packets(config.pcr_max_interval.mul_f64(0.75), bitrate).max(1) as u64,
			deferred: VecDeque::new(),
			anchor: None,
			anchor_slot: 0,
			slot: 0,
			grid: (config.clocking == Clocking::Stream).then(|| StreamGrid::new(bitrate, capacity)),
			latency_packets: latency_to_packets(target, bitrate) as u64,
			max_latency_packets: capacity as u64,
			sequence_seed: config.sequence_seed,
			stall: config.stall,
			first_content_at: None,
			last_content_at: None,
			stalled: false,
			scratch: Vec::with_capacity(config.packets_per_datagram.max(1) * TS_PACKET_SIZE),
			stats: Stats::default(),
		}
	}

	/// Enqueue an input packet observed at `now`. The first packet arms both
	/// clocks (output starts `latency` later, priming the buffer).
	///
	/// Incoming null/stuffing packets (PID `0x1FFF`) are stripped here: they are
	/// pure padding, carry no PID/PSI/PES structure, and the pacer inserts its
	/// own stuffing to hit the target rate. Re-pacing an already-stuffed source
	/// would otherwise double-count the source's padding as media.
	pub fn enqueue(&mut self, packet: Packet, now: Instant) {
		if packet.pid() == NULL_PID {
			self.stats.input_nulls_stripped = self.stats.input_nulls_stripped.saturating_add(1);
			return;
		}
		if self.stalled {
			self.resume(now);
		}
		self.note_content_gap(now);
		self.first_content_at.get_or_insert(now);
		self.last_content_at = Some(now);
		if self.pcr_pid.is_none() && packet.has_pcr() {
			self.pcr_pid = Some(packet.pid());
		}
		// Measured against the stream's own media rate, so a burst cannot inflate
		// the rate it is being compared with. Before the second PCR there is no
		// rate yet and the profile has nothing to say, which is why an adaptive
		// start also has a deadline.
		self.profile
			.observe(now, self.media.estimated_rate_pps().unwrap_or(0.0));
		self.resize();

		if let Some(grid) = self.grid.as_mut() {
			let dropped = grid.push(packet, now);
			self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(dropped);
			self.stats.pcr_position_overruns = grid.overruns();
			self.stats.pcr_position_displacement = grid.displacement_high_water();
			// The clock can only start once something has been placed: where the
			// grid starts is what the wall clock is anchored to.
			//
			// A leg joining a running broadcast is handed whatever the relay has
			// buffered, oldest first. Starting on that first packet would fix its
			// output a backlog's worth behind its partner for the rest of the
			// run — it emits at the mux rate, so it can never catch up. It starts
			// at the live edge of what it holds instead, one release latency back,
			// and lets the rest age out: content whose slot has passed is content
			// the partner has already delivered. The choice is revisited until the
			// first datagram goes out, because the burst is still arriving.
			//
			// Datagram boundaries have to come off the grid as well. A leg whose
			// first content lands mid-datagram would otherwise split every
			// datagram thereafter at an offset from its partner's — the same
			// bytes, packed differently, which merges no better than different
			// bytes do. Rounding down to a boundary costs a few slots of leading
			// stuffing and makes the packing a property of the stream.
			if self.stats.output_packets == 0
				&& let (Some(first), Some(edge)) = (grid.first_slot(), grid.last_slot())
			{
				let per_datagram = self.packets_per_datagram as u64;
				let start = edge.saturating_sub(self.latency_packets).max(first);
				self.anchor = Some(self.anchor.unwrap_or(now + self.target));
				self.anchor_slot = start - (start % per_datagram);
				self.slot = self.anchor_slot;
				self.stats.start_backlog = edge - first;
			}
			self.note_depth();
			return;
		}

		self.media.observe(&packet);
		if self.buffer.push(packet) {
			self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
		}
		self.note_depth();
		self.arm(now);
	}

	/// Record how deep the buffer got, so the bound can be judged against use.
	fn note_depth(&mut self) {
		let depth = self.buffered_packets() as u64;
		self.stats.buffer_high_water = self.stats.buffer_high_water.max(depth);
	}

	/// Re-derive the depths from the arrival pattern observed so far.
	///
	/// A no-op under [`Latency::Fixed`]. The cushion may fall as well as rise —
	/// a burst ages out of the measurement window — but the buffer bound only
	/// rises, because shrinking it underneath media already accepted would drop
	/// programme to satisfy a revised estimate.
	fn resize(&mut self) {
		if !matches!(self.latency, Latency::Adaptive { .. }) {
			return;
		}
		self.target = self.latency.target(self.profile.lead());
		let cap = self.latency.cap(self.target);
		self.latency_packets = latency_to_packets(self.target, self.mux_rate_bps) as u64;
		let capacity = latency_to_packets(cap, self.mux_rate_bps).max(1);
		self.max_latency_packets = self.max_latency_packets.max(capacity as u64);
		self.buffer.grow_to(capacity);
		if let Some(grid) = self.grid.as_mut() {
			grid.grow_to(capacity);
		}
	}

	/// Start the output clock if it is time to, under [`Latency::Adaptive`].
	///
	/// Two conditions, and the second is the one a timer cannot express. The
	/// cushion has to be full, or the pacer starts on less programme than it has
	/// decided it needs. And the input has to be between deliveries, because the
	/// size of a burst is not known until it ends: a segmented feed reaches a
	/// 200 ms cushion 25 ms into a 2 s segment fetch, and a pacer that started
	/// there would drain what it held and then read the ordinary gap before the
	/// next segment as an underrun. Waiting for the delivery to finish is what
	/// lets the cushion be sized against the whole burst.
	///
	/// A continuous feed is never mid-delivery in that sense — its lead stays
	/// below [`crate::DELIVERY_GAP`] — so it starts as soon as it is primed,
	/// exactly as it did before any of this existed. The deadline covers the
	/// input that fits neither description.
	fn arm(&mut self, now: Instant) {
		if self.anchor.is_some() {
			return;
		}
		let anchor = match self.latency.start_deadline() {
			// Pinned depths start on a timer: the cushion is already known, so
			// there is nothing to learn by waiting for content to prove it.
			None => now + self.target,
			Some(deadline) => {
				let primed = self.buffered_packets() as u64 >= self.latency_packets;
				let settled = !self.profile.bursty() || self.profile.between_deliveries(now);
				let expired = self
					.first_content_at
					.is_some_and(|first| now.saturating_duration_since(first) >= deadline);
				if !((primed && settled) || expired) {
					return;
				}
				now
			}
		};
		self.anchor = Some(anchor);
		self.media.anchor = Some(anchor);
	}

	/// Give an unstarted output clock the chance to start without a packet having
	/// arrived, so a cushion that filled during a delivery starts emitting in the
	/// silence that follows rather than waiting for the next delivery to prove it.
	///
	/// The driver calls this while [`Scheduler::next_due`] is `None`. A no-op once
	/// the clock is running, and under [`Latency::Fixed`], which starts on the
	/// first packet.
	pub fn poll_start(&mut self, now: Instant) {
		if self.first_content_at.is_some() {
			self.arm(now);
		}
	}

	/// Start the output clock now, whatever the cushion holds.
	///
	/// For an input that ended before it delivered a cushion's worth: nothing more
	/// is coming, so continuing to wait for it would discard the programme already
	/// in hand rather than pace it out.
	pub fn start_now(&mut self, now: Instant) {
		if self.anchor.is_none() {
			self.anchor = Some(now);
			self.media.anchor = Some(now);
		}
	}

	/// Place any packets held back waiting for a closing PCR, because the source
	/// has ended and none is coming. A no-op outside [`Clocking::Stream`].
	pub fn flush(&mut self) {
		if let Some(grid) = self.grid.as_mut() {
			let dropped = grid.flush();
			self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(dropped);
		}
	}

	/// Restart the media clock after a stall, leaving the output byte clock alone.
	///
	/// The media clock kept accruing due packets against a dead source, so without
	/// this the first datagrams after a stall would find every slot overdue and
	/// dump the refilled buffer at the mux rate instead of at media rate. The byte
	/// clock deliberately ran through the gap, so the regenerated PCR stays
	/// wall-clock-aligned; only the media timeline has a hole in it, which is what
	/// the discontinuity indicator is for.
	fn resume(&mut self, now: Instant) {
		self.stalled = false;
		if let Some(regen) = self.pcr_regen.as_mut() {
			regen.flag_discontinuity();
		}
		// Under stream clocking there is no arrival-derived state to restore: the
		// returning content carries the slots it belongs in, which is the whole
		// point — a leg that has been silent for ten seconds resumes on the grid
		// its partner has been using throughout.
		if self.grid.is_some() {
			return;
		}
		let anchor = now + self.target;
		self.media.anchor = Some(anchor);
		self.media.ticked = None;
		self.media.credit = 0.0;
		self.media.released = 0;
		// A rate sample taken across the gap would read as the outage length, not
		// as a media interval; drop the pending one and re-seed on the next PCR.
		self.media.last_pcr = None;
		self.media.packets_since_pcr = 0;
		self.media.decayed_packets = 0.0;
		self.media.decayed_secs = 0.0;
	}

	/// What the input is currently doing. See [`SourceState`].
	pub fn state(&self, now: Instant) -> SourceState {
		let Some(last) = self.last_content_at else {
			return SourceState::Priming;
		};
		// An adaptive start holds the clock unarmed while it fills the cushion it
		// has sized, which is priming by any reading of the word.
		if self.anchor.is_none_or(|anchor| now < anchor) {
			return SourceState::Priming;
		}
		// Buffered media is still programme going to air. The input may already be
		// dead, but the output has not run out of content, so the clock the source
		// gave us is still worth holding — the stall only begins when the cushion
		// is spent. It is also what keeps an offline run, where packets are handed
		// over on a synthetic clock and the file simply ends, from reading as a
		// stall while it flushes its tail.
		if self.has_pending() {
			return SourceState::Live;
		}
		let silent = now.saturating_duration_since(last);
		if self.stall_timeout().is_some_and(|timeout| silent >= timeout) {
			SourceState::Stalled
		} else if silent >= self.target {
			SourceState::Starved
		} else {
			SourceState::Live
		}
	}

	/// The silence after which the input counts as gone rather than late, given
	/// the cushion currently in force. `None` disables stall detection.
	///
	/// Under [`Stall::Adaptive`] this rises and falls with the cushion, which is
	/// the point: a leg holding four seconds of segmented programme has not lost
	/// its source one second into an ordinary inter-segment gap.
	pub fn stall_timeout(&self) -> Option<Duration> {
		self.stall.timeout(self.target)
	}

	/// How long the input has carried no content, zero before the first packet.
	pub fn silent_for(&self, now: Instant) -> Duration {
		match self.last_content_at {
			Some(last) => now.saturating_duration_since(last),
			None => Duration::ZERO,
		}
	}

	/// Skip this output slot's worth of packets without emitting them, advancing
	/// the byte clock across the gap.
	///
	/// This is the [`StallPolicy::Mute`](crate::StallPolicy::Mute) path: the wire
	/// goes quiet so downstream can see the source is gone, while the output index
	/// keeps counting so a resumed leg's PCR — and any sink numbering derived from
	/// the output position — picks up where real time says it should.
	pub fn advance_muted(&mut self, now: Instant) {
		self.observe_state(now);
		let packets = self.packets_per_datagram as u64;
		self.slot = self.slot.saturating_add(packets);
		self.stats.muted_packets = self.stats.muted_packets.saturating_add(packets);
	}

	/// Fold the current state into the stall bookkeeping, returning it.
	fn observe_state(&mut self, now: Instant) -> SourceState {
		let state = self.state(now);
		let stalled = state.is_stalled();
		if stalled {
			if !self.stalled {
				self.stats.stalls = self.stats.stalls.saturating_add(1);
			}
			// Track the gap as it grows, so a stall still in progress when the run
			// ends is reported at its true length rather than not at all.
			self.note_content_gap(now);
		}
		self.stalled = stalled;
		state
	}

	/// Record the silence up to `now` as a content gap, keeping the longest.
	fn note_content_gap(&mut self, now: Instant) {
		if self.last_content_at.is_none() {
			return;
		}
		let gap = self.silent_for(now).as_millis().min(u128::from(u64::MAX)) as u64;
		self.stats.content_gap_max_ms = self.stats.content_gap_max_ms.max(gap);
	}

	/// Wall-clock instant the next output datagram is due, or `None` until the
	/// first packet has armed the clock.
	pub fn next_due(&self) -> Option<Instant> {
		self.anchor
			.map(|a| a + self.packets_to_duration(self.slot.saturating_sub(self.anchor_slot)))
	}

	/// Where the next datagram sits in the stream, for a sink that numbers its
	/// output — `None` under [`Clocking::Arrival`], where the slot is only a send
	/// count and a sequence derived from it says no more than the sink's own.
	///
	/// Under [`Clocking::Stream`] the numbering is a function of stream position,
	/// so a leg that starts late, or mutes and returns, carries the numbers its
	/// partner is already using rather than its own count of what it has sent.
	pub fn framing(&self) -> Option<Framing> {
		let grid = self.grid.as_ref()?;
		let datagram_index = self.slot / self.packets_per_datagram as u64;
		Some(Framing {
			slot: self.slot,
			sequence: (datagram_index as u16).wrapping_add(self.sequence_seed),
			// The 90 kHz RTP timestamp is the 27 MHz slot PCR divided down, so the
			// framing and the payload are locked to the same grid.
			timestamp_90khz: (grid.map.pcr_of_slot(self.slot) / 300) as u32,
		})
	}

	/// Whether any content remains to be emitted.
	pub fn has_pending(&self) -> bool {
		if !self.deferred.is_empty() {
			return true;
		}
		match self.grid.as_ref() {
			Some(grid) => grid.has_content(),
			None => !self.buffer.is_empty(),
		}
	}

	/// Whether the source's PCR positions have diverged from its PCR values by
	/// more than the buffer can absorb.
	///
	/// The bound is `max_latency` because that is the point at which the existing
	/// behaviour stops being a deferral and starts being a loss: below it the
	/// displacement is spilled packets waiting in a buffer that is allowed to be
	/// that deep, and above it the live edge outruns the output clock, so the
	/// catch-up path moves the clock and the skipped slots are dropped.
	/// Always `false` under [`Clocking::Arrival`], which has no grid to displace.
	pub fn pcr_position_diverged(&self) -> bool {
		self.grid
			.as_ref()
			.is_some_and(|grid| grid.displacement_high_water() > self.max_latency_packets)
	}

	/// Current jitter-buffer occupancy in packets (the de-jitter cushion depth).
	pub fn buffered_packets(&self) -> usize {
		let held = match self.grid.as_ref() {
			Some(grid) => grid.placed.len() + grid.run.len(),
			None => self.buffer.len(),
		};
		held + self.deferred.len()
	}

	/// A snapshot of the pacing statistics.
	///
	/// The arrival and depth figures are read from live state rather than kept as
	/// counters, since they describe what the input is doing now rather than
	/// tallying what it has done.
	pub fn stats(&self) -> Stats {
		Stats {
			burst_max_packets: self.profile.burst_max_packets(),
			bursts: self.profile.bursts(),
			arrival_lead_ms: self.profile.lead().as_millis().min(u128::from(u64::MAX)) as u64,
			latency_target_ms: self.target.as_millis().min(u128::from(u64::MAX)) as u64,
			media_rate_bps: (self.media.rate_pps * PACKET_BITS as f64) as u64,
			buffer_packets: self.buffered_packets() as u64,
			pcr_position_displacement_ms: self
				.packets_to_duration(self.stats.pcr_position_displacement)
				.as_millis()
				.min(u128::from(u64::MAX)) as u64,
			..self.stats
		}
	}

	/// Emit one output datagram (`packets_per_datagram` transport packets) at
	/// `now`, advancing the byte clock. Returns a borrow of the internal scratch
	/// buffer, valid until the next call. Never allocates after construction.
	pub fn emit_datagram(&mut self, now: Instant) -> &[u8] {
		self.catch_up(now);
		self.scratch.clear();
		// The cushion in *content* packets, not carrier packets: the buffer holds
		// media, and at 11 Mb/s of carrier over 9.4 Mb/s of content the two differ
		// by the stuffing ratio, which would leave the loop holding the buffer a
		// sixth deeper than it was asked to.
		let target = self.media.rate_pps * self.target.as_secs_f64();
		let due = self.media.due(now, self.buffer.len(), target);
		// With no content arriving there is no clock to hold: inserting PCR into a
		// programme-free carrier is what makes a dead feed look conformant to
		// everything downstream, so re-insertion stops with the content.
		let stalled = self.observe_state(now).is_stalled();
		for _ in 0..self.packets_per_datagram {
			let index = self.slot;
			let want_content = self.media.released < due;
			self.admit(index, want_content);
			if let Some(pcr) = self.reinsert_pcr(index, stalled) {
				// The PCR takes the slot whether or not content wanted it. Content
				// it displaces waits one slot in `deferred`; see `reinsert_pcr`.
				let packet = pcr_only_packet(self.pcr_pid.expect("pid set"), self.pcr_pid_cc, pcr);
				self.scratch.extend_from_slice(&packet);
				self.last_pcr_index = Some(index);
				self.stats.pcr_inserted = self.stats.pcr_inserted.saturating_add(1);
			} else if let Some(packet) = self.deferred.pop_front() {
				let is_pcr = packet.has_pcr();
				if self.pcr_pid == Some(packet.pid()) {
					self.pcr_pid_cc = packet.as_bytes()[3] & 0x0f;
				}
				let start = self.scratch.len();
				self.scratch.extend_from_slice(packet.as_bytes());
				if let Some(regen) = self.pcr_regen.as_mut() {
					let slice = &mut self.scratch[start..start + TS_PACKET_SIZE];
					let rebased = match self.grid.as_ref() {
						// The slot *is* the clock, so there is no anchor to re-base
						// on and no per-process history in the emitted value.
						Some(grid) => {
							regen.rewrite_absolute(slice, grid.map.pcr_of_slot(index));
							false
						}
						None => regen.rewrite(slice, index),
					};
					if rebased {
						self.stats.pcr_rebases = self.stats.pcr_rebases.saturating_add(1);
					}
				}
				if is_pcr {
					self.last_pcr_index = Some(index);
				}
				self.stats.content_packets = self.stats.content_packets.saturating_add(1);
			} else {
				self.scratch.extend_from_slice(&self.null);
				self.stats.null_packets = self.stats.null_packets.saturating_add(1);
				// A null while content was due but unavailable is a genuine
				// underrun (input starved); a null while holding the cushion is
				// ordinary rate stuffing. A stalled source is neither: it is
				// counted as a stall, so it can't hide inside the underrun figure.
				if want_content && !stalled {
					self.stats.underruns = self.stats.underruns.saturating_add(1);
				}
			}
			self.slot = self.slot.saturating_add(1);
			self.stats.output_packets = self.stats.output_packets.saturating_add(1);
		}
		&self.scratch
	}

	/// Move at most one packet from the input side into the emission queue.
	///
	/// Release is still governed by the clock the mode selects — the media clock
	/// under arrival clocking, the slot map under stream clocking — so admitting
	/// one packet per slot changes nothing about *when* content becomes eligible.
	/// It only separates becoming eligible from being transmitted, which is what
	/// lets a PCR take a slot without the content that wanted it being lost.
	///
	/// Under stream clocking the grid must be asked at every slot even while
	/// `deferred` is occupied, because a packet whose slot passes unasked is
	/// discarded by [`StreamGrid::take`] as late.
	fn admit(&mut self, index: u64, want_content: bool) {
		if let Some(packet) = self.next_content(index, want_content) {
			self.media.released = self.media.released.saturating_add(1);
			self.deferred.push_back(packet);
		}
	}

	/// Move the output clock to the live edge when the leg is holding more stream
	/// than its buffer is allowed to be deep.
	///
	/// A leg that subscribes to a running broadcast is served from where the
	/// relay's buffer starts, not from the live edge, and takes delivery of the
	/// backlog at whatever rate the path gives it. Emitting at the mux rate it
	/// cannot catch up, so without this it runs a buffer's depth behind its
	/// partner indefinitely — the depth being an operator's tuning choice, which
	/// is no basis for the phase of a redundant pair. The alternative already in
	/// the code is worse: dropping the oldest packets to stay under the bound
	/// keeps the leg late *and* deletes programme to do it.
	///
	/// Backlog beyond the release latency is programme the partner has already
	/// delivered, so it is discarded by [`StreamGrid::take`] as the clock passes
	/// it. Under arrival clocking there is no grid and nothing to skip to.
	fn catch_up(&mut self, now: Instant) {
		let Some(edge) = self.grid.as_ref().and_then(StreamGrid::last_slot) else {
			return;
		};
		if edge.saturating_sub(self.slot) <= self.max_latency_packets {
			return;
		}
		let per_datagram = self.packets_per_datagram as u64;
		let target = edge.saturating_sub(self.latency_packets);
		let target = target - (target % per_datagram);
		if target <= self.slot {
			return;
		}
		self.anchor = Some(now);
		self.anchor_slot = target;
		self.slot = target;
		// Content admitted for a slot the clock has just jumped over is backlog by
		// the same definition as the placed packets `take` will now discard.
		self.stats.late_drops = self.stats.late_drops.saturating_add(self.deferred.len() as u64);
		self.deferred.clear();
		self.stats.resyncs = self.stats.resyncs.saturating_add(1);
	}

	/// The content packet for output slot `index`, if one belongs there.
	///
	/// The two clocking modes differ here and nowhere else that matters: arrival
	/// clocking asks whether the media clock says a packet is *due now*, stream
	/// clocking asks whether a packet *belongs in this slot*. The first depends on
	/// when this process got round to emitting; the second does not.
	fn next_content(&mut self, index: u64, want_content: bool) -> Option<Packet> {
		let Some(grid) = self.grid.as_mut() else {
			return want_content.then(|| self.buffer.pop()).flatten();
		};
		let (packet, late) = grid.take(index);
		if late > 0 {
			// Arriving too late for its slot is a drop, not a re-placement: moving
			// the packet would make its position depend on the delay, and the pair
			// would diverge from exactly the jitter this design removes.
			self.stats.late_drops = self.stats.late_drops.saturating_add(late);
		}
		packet
	}

	/// The byte-locked PCR to re-insert at output `index`, or `None` when
	/// re-insertion doesn't apply: a stalled source, preserve mode, no PCR PID
	/// learned yet, no real PCR seen yet, the repetition limit not yet reached, or
	/// the packet already queued for this slot carrying a PCR of its own.
	///
	/// **Re-insertion pre-empts content; it does not wait for a spare slot.** The
	/// obvious cheaper rule — insert only where the scheduler was going to stuff
	/// anyway — holds the repetition limit on any stream that has stuffing
	/// *distributed*, and fails on precisely the stream this pacer exists for. A
	/// media-aware source delivers a coded frame as one burst, so its output has
	/// ample stuffing overall and none at all inside the burst: the deadline falls
	/// where every slot is spoken for, no spare slot appears until the burst has
	/// drained, and the interval runs to the length of the frame. Taking the slot
	/// costs one packet of carrier per PCR — 0.34% at a 40 ms limit and 11 Mb/s —
	/// against an interval bounded by the frame size, which no cushion shortens.
	fn reinsert_pcr(&self, index: u64, stalled: bool) -> Option<u64> {
		if stalled {
			return None;
		}
		let regen = self.pcr_regen.as_ref()?;
		self.pcr_pid?;
		let last = self.last_pcr_index?;
		if index.saturating_sub(last) < self.pcr_max_packets {
			return None;
		}
		if self.deferred.front().is_some_and(Packet::has_pcr) {
			return None;
		}
		match self.grid.as_ref() {
			Some(grid) => Some(grid.map.pcr_of_slot(index)),
			None => regen.locked_for_index(index),
		}
	}

	/// Wall-clock duration to transmit `packets` at the mux rate.
	fn packets_to_duration(&self, packets: u64) -> Duration {
		let nanos = u128::from(packets) * PACKET_BITS * 1_000_000_000_u128 / u128::from(self.mux_rate_bps);
		Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
	}
}

/// How many 188-byte packets a `latency` window holds at `bitrate`.
fn latency_to_packets(latency: Duration, bitrate: u64) -> usize {
	let bits = latency.as_secs_f64() * bitrate as f64;
	(bits / PACKET_BITS as f64).ceil() as usize
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::{Config, DEFAULT_LATENCY};
	use crate::error::Error;
	use crate::pcr::PCR_CLOCK_HZ;

	const MUX_RATE: u64 = 12_000_000;
	// 188 * 8 * 27_000_000 / 12_000_000 = 3384 ticks per packet.
	const TICKS_PER_PACKET: u64 = 3_384;

	fn config() -> Config {
		Config::new(MUX_RATE)
			.with_latency(Duration::ZERO)
			.with_packets_per_datagram(1)
	}

	fn content_packet(pid: u16, pcr_ticks: Option<u64>) -> Packet {
		let mut p = [0x00_u8; TS_PACKET_SIZE];
		p[0] = 0x47;
		p[1] = (pid >> 8) as u8 & 0x1f;
		p[2] = pid as u8;
		if let Some(ticks) = pcr_ticks {
			p[3] = 0x30;
			p[4] = 7;
			p[5] = 0x10;
			pcr::write_pcr(&mut p[6..12], ticks);
		} else {
			p[3] = 0x10;
		}
		Packet::from_slice(&p).unwrap()
	}

	fn pid_of(packet: &[u8]) -> u16 {
		(u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2])
	}

	fn is_null(packet: &[u8]) -> bool {
		pid_of(packet) == crate::null_insertion::NULL_PID
	}

	#[test]
	fn next_due_advances_at_the_mux_rate() {
		let mut sched = Scheduler::new(&config().with_packets_per_datagram(1));
		let t0 = Instant::now();
		assert!(sched.next_due().is_none(), "no clock before the first packet");
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		let first = sched.next_due().unwrap();
		assert_eq!(first, t0, "zero latency: output packet 0 is due immediately");
		sched.emit_datagram(first);
		let second = sched.next_due().unwrap();
		// One 188-byte packet at 12 Mb/s = 188 * 8 / 12e6 s.
		let expected = Duration::from_nanos(188 * 8 * 1_000_000_000 / 12_000_000);
		assert_eq!(second.duration_since(t0), expected);
	}

	#[test]
	fn stuffs_null_when_input_is_below_rate() {
		// Media rate ~1000 pps; mux rate ~7978 pps. Most slots should be null.
		let mut sched = Scheduler::new(&config().with_packets_per_datagram(1));
		let t0 = Instant::now();
		// Two PCRs 1 ms apart carrying 1 content packet each => ~1000 pps.
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		sched.enqueue(content_packet(0x100, Some(27_000)), t0);
		// Emit a datagram every packet-interval for 5 ms of output.
		let mut nulls = 0;
		let mut content = 0;
		for i in 0..100 {
			let due = sched.next_due().unwrap();
			let _ = i;
			let dg = sched.emit_datagram(due).to_vec();
			if is_null(&dg) {
				nulls += 1;
			} else {
				content += 1;
			}
		}
		assert!(
			nulls > content,
			"below-rate input should be mostly null ({nulls} null, {content} content)"
		);
		assert!(sched.stats().null_packets >= 1);
	}

	#[test]
	fn preserve_mode_leaves_pcr_untouched() {
		let cfg = config().with_pcr_mode(PcrMode::Preserve).with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(123_456)), t0);
		let dg = sched.emit_datagram(sched.next_due().unwrap()).to_vec();
		assert_eq!(pcr::read_pcr(&dg), Some(123_456), "preserve keeps the source PCR value");
	}

	#[test]
	fn regenerate_mode_byte_locks_pcr() {
		let cfg = config().with_pcr_mode(PcrMode::Regenerate).with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		// Enqueue enough content so the media clock releases several packets.
		for i in 0..8 {
			sched.enqueue(content_packet(0x100, Some(i * 90_000)), t0);
		}
		// Drain: pull datagrams well past the media schedule so all release.
		let mut pcrs = Vec::new();
		let mut now = t0;
		for _ in 0..64 {
			let dg = sched.emit_datagram(now).to_vec();
			if let Some(p) = pcr::read_pcr(&dg) {
				pcrs.push((sched.stats().output_packets - 1, p));
			}
			now += Duration::from_millis(1);
		}
		assert!(pcrs.len() >= 2, "expected multiple PCRs, got {}", pcrs.len());
		// Consecutive regenerated PCRs differ by exactly the byte distance.
		let (i0, p0) = pcrs[0];
		let (i1, p1) = pcrs[1];
		assert_eq!(
			p1 - p0,
			(i1 - i0) * TICKS_PER_PACKET,
			"PCR is byte-locked to the mux rate"
		);
	}

	#[test]
	fn reinserts_pcr_to_hold_the_repetition_limit() {
		// A source PCR far sparser than the limit forces re-insertion. With a
		// tiny limit, once the first real PCR anchors the clock every subsequent
		// stuffing slot on the PCR PID carries a byte-locked PCR.
		let cfg = config()
			.with_pcr_mode(PcrMode::Regenerate)
			.with_pcr_max_interval(Duration::from_micros(100))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		// One PCR packet then plain content (no further PCRs).
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		for _ in 0..3 {
			sched.enqueue(content_packet(0x100, None), t0);
		}

		let mut now = t0;
		let mut pcr_packets = 0;
		let mut injected_on_pcr_pid = 0;
		for _ in 0..40 {
			let dg = sched.emit_datagram(now).to_vec();
			if pcr::read_pcr(&dg).is_some() {
				pcr_packets += 1;
				// An adaptation-only (afc=10) PCR packet on 0x100 is a re-insertion.
				if pid_of(&dg) == 0x100 && (dg[3] >> 4) & 0x03 == 0b10 {
					injected_on_pcr_pid += 1;
				}
			}
			now += Duration::from_millis(1);
		}

		assert!(sched.stats().pcr_inserted > 0, "expected PCR re-insertion");
		assert!(injected_on_pcr_pid > 0, "injected PCR must sit on the PCR PID");
		assert!(pcr_packets > 1, "output carries more PCRs than the single source PCR");
	}

	/// The output slots of an emitted run, split into PCR positions and PIDs.
	fn drain_saturated(sched: &mut Scheduler, from: Instant, slots: usize) -> (Vec<u64>, Vec<u16>) {
		let mut pcr_at = Vec::new();
		let mut pids = Vec::new();
		// Emitting far ahead of the media clock keeps content due at every slot,
		// which is the condition under which stuffing is unavailable.
		let mut now = from;
		for i in 0..slots {
			let dg = sched.emit_datagram(now).to_vec();
			if pcr::read_pcr(&dg).is_some() {
				pcr_at.push(i as u64);
			}
			pids.push(pid_of(&dg));
			now += Duration::from_millis(1);
		}
		(pcr_at, pids)
	}

	#[test]
	fn media_rate_is_unbiased_by_clustered_pcr_intervals() {
		// The media-aware arrival pattern: an exact PCR grid whose intervals carry
		// wildly unequal numbers of packets. Every 25th interval is a coded frame,
		// the rest are nearly empty; the true rate is the total over the total.
		let mut sched = Scheduler::new(&config().with_latency(Duration::from_millis(500)));
		let t0 = Instant::now();
		let mut ticks = 0_u64;
		let mut packets = 0_u64;
		let mut seen = Vec::new();
		let intervals = 400;
		for i in 0..intervals {
			let burst = if i % 25 == 0 { 500 } else { 4 };
			sched.enqueue(content_packet(0x100, Some(ticks)), t0);
			for _ in 1..burst {
				sched.enqueue(content_packet(0x100, None), t0);
			}
			packets += burst;
			ticks += PCR_CLOCK_HZ / 40; // a 25 ms grid
			// Sampled only once the window has filled: the first intervals are
			// dominated by whichever kind of interval arrived first.
			if i as f64 * 0.025 > RATE_WINDOW {
				seen.push(sched.stats().media_rate_bps as f64 / PACKET_BITS as f64);
			}
		}
		// (500 + 24*4) / (25 * 0.025) = 954.5 packets per second.
		let truth = packets as f64 / (intervals as f64 * 0.025);
		// A finite window on a bursty source oscillates; what matters is that it
		// oscillates *around* the rate rather than sitting under it, because
		// release is `rate * elapsed` and a persistent under-read fills the buffer.
		// The mean-of-ratios estimator this replaced returned a median near a third
		// of the truth on the same input.
		seen.sort_by(f64::total_cmp);
		let median = seen[seen.len() / 2];
		assert!(
			median > truth * 0.85,
			"median estimate {median:.0} pps sits below the true {truth:.0} pps"
		);
		assert!(
			seen[seen.len() - 1] < truth * 1.5,
			"peak estimate {:.0} pps overshoots the true {truth:.0} pps",
			seen[seen.len() - 1]
		);
	}

	#[test]
	fn media_rate_survives_pcr_intervals_that_carry_almost_no_media_time() {
		// The test above puts every PCR on an exact 25 ms grid, which is the one
		// thing a media-aware exporter does not do. T19 measured its PCR packets
		// arriving byte-adjacent -- pairs a tick or two apart -- among intervals of
		// the ordinary length.
		//
		// A near-zero interval is degenerate for a rate estimate: it contributes
		// packets to the numerator and almost nothing to the denominator, and
		// because the window decays on *media* time it also decays almost nothing
		// out. Enough of them and the sums ratchet up without bound, so the
		// estimate ramps linearly for as long as the feed runs. Nothing in a short
		// window shows it, and the wire stays perfectly conformant while it
		// happens: the release loop simply empties the buffer and stuffs the
		// difference.
		let mut sched = Scheduler::new(&config().with_latency(Duration::from_millis(500)));
		let t0 = Instant::now();
		let mut ticks = 0_u64;
		let mut packets = 0_u64;
		let intervals = 4_000;
		for i in 0..intervals {
			let burst = if i % 25 == 0 { 500 } else { 4 };
			sched.enqueue(content_packet(0x100, Some(ticks)), t0);
			for _ in 1..burst {
				sched.enqueue(content_packet(0x100, None), t0);
			}
			packets += burst;
			// Every other PCR is byte-adjacent to the one before it: one tick of
			// media time, not 25 ms of it.
			ticks += if i % 2 == 0 { 1 } else { PCR_CLOCK_HZ / 40 };
		}
		let elapsed = ticks as f64 / PCR_CLOCK_HZ as f64;
		let truth = packets as f64 / elapsed;
		let estimate = sched.stats().media_rate_bps as f64 / PACKET_BITS as f64;
		assert!(
			estimate < truth * 2.0,
			"estimate {estimate:.0} pps ran away from the true {truth:.0} pps \
			 -- degenerate PCR intervals are ratcheting the window"
		);
		assert!(
			estimate > truth * 0.5,
			"estimate {estimate:.0} pps collapsed below the true {truth:.0} pps"
		);
	}

	#[test]
	fn pcr_reinsertion_pre_empts_a_saturated_run() {
		// The media-aware failure mode in miniature: one PCR, then a coded frame's
		// worth of content with no PCR in it and no slot the scheduler declines.
		// Opportunistic insertion has nowhere to put a PCR and the interval runs to
		// the length of the frame.
		let cfg = config()
			.with_latency(Duration::from_millis(500))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		for _ in 0..2_000 {
			sched.enqueue(content_packet(0x100, None), t0);
		}
		let (pcr_at, pids) = drain_saturated(&mut sched, t0 + Duration::from_millis(500), 2_100);

		// 0.75 * 40 ms at 12 Mb/s.
		let limit = latency_to_packets(Duration::from_millis(30), MUX_RATE) as u64;
		let worst = pcr_at.windows(2).map(|w| w[1] - w[0]).max().expect("several PCRs");
		assert!(
			worst <= limit + 1,
			"worst PCR interval {worst} packets exceeds the {limit}-packet deadline"
		);
		assert!(sched.stats().pcr_inserted >= 6, "expected repeated pre-emption");
		// Pre-emption defers content, it does not discard it.
		assert_eq!(sched.stats().content_packets, 2_001, "every content packet was emitted");
		assert_eq!(sched.stats().dropped_packets, 0);
		assert_eq!(sched.stats().late_drops, 0);
		assert!(
			pids.iter()
				.all(|&pid| pid == 0x100 || pid == crate::null_insertion::NULL_PID),
			"output carries only the programme PID and stuffing"
		);
	}

	#[test]
	fn pre_emption_yields_to_a_content_pcr_in_the_same_slot() {
		// Content that carries its own PCR satisfies the deadline, so taking the
		// slot for a synthetic one would emit two PCRs back to back for nothing.
		let cfg = config()
			.with_latency(Duration::from_millis(500))
			.with_pcr_max_interval(Duration::from_micros(200))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		// Every packet carries a PCR, so the deadline is met without help.
		for i in 0..200 {
			sched.enqueue(content_packet(0x100, Some(i * TICKS_PER_PACKET)), t0);
		}
		let (_, _) = drain_saturated(&mut sched, t0 + Duration::from_millis(500), 200);
		assert_eq!(
			sched.stats().pcr_inserted,
			0,
			"no synthetic PCR belongs next to a content PCR"
		);
		assert_eq!(sched.stats().content_packets, 200);
	}

	#[test]
	fn deferred_content_drains_into_stuffing() {
		// The carrier cost of pre-emption is one packet per PCR, repaid at the next
		// slot the content clock does not want: the queue must not accumulate.
		let cfg = config()
			.with_latency(Duration::from_millis(500))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		for _ in 0..1_000 {
			sched.enqueue(content_packet(0x100, None), t0);
		}
		drain_saturated(&mut sched, t0 + Duration::from_millis(500), 1_200);
		assert!(
			sched.deferred.is_empty(),
			"the deferral queue drained once content ran out"
		);
		assert_eq!(sched.stats().content_packets, 1_001);
	}

	#[test]
	fn a_snapshot_reports_the_standing_buffer_depth_not_its_peak() {
		// The soak instrument. A feed that runs for months is not graded by the
		// report it prints when it stops, and `buffer_high_water` cannot answer
		// "is the release loop still holding its set point" because it only ever
		// rises: an hour after one transient it still reports the transient. So
		// the snapshot has to carry the live occupancy alongside the peak.
		let cfg = config()
			.with_latency(Duration::from_millis(500))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		for _ in 0..1_000 {
			sched.enqueue(content_packet(0x100, None), t0);
		}

		let filled = sched.stats();
		assert_eq!(
			filled.buffer_packets,
			sched.buffered_packets() as u64,
			"the snapshot disagrees with the scheduler about what it is holding"
		);
		assert_eq!(
			filled.buffer_packets, filled.buffer_high_water,
			"nothing has drained yet"
		);

		// Drain it. The peak is a bound that must not move; the standing depth is
		// the reading that must fall — a field wired to the high-water mark by
		// mistake passes every assertion above and fails this one.
		drain_saturated(&mut sched, t0 + Duration::from_millis(500), 1_200);
		let drained = sched.stats();
		assert_eq!(
			drained.buffer_high_water, filled.buffer_high_water,
			"the peak moved while the buffer was emptying"
		);
		assert!(
			drained.buffer_packets < filled.buffer_packets,
			"standing depth {} did not fall below the {} it started at",
			drained.buffer_packets,
			filled.buffer_packets
		);
		assert_eq!(
			drained.buffer_packets,
			sched.buffered_packets() as u64,
			"the snapshot and the scheduler disagree after the drain"
		);
	}

	#[test]
	fn strips_input_null_packets() {
		let mut sched = Scheduler::new(&config().with_packets_per_datagram(1));
		let t0 = Instant::now();
		// A null packet (PID 0x1FFF) is stuffing: it must not be buffered as media.
		let mut null = [0x00_u8; TS_PACKET_SIZE];
		null[0] = 0x47;
		null[1] = 0x1f;
		null[2] = 0xff;
		null[3] = 0x10;
		sched.enqueue(Packet::from_slice(&null).unwrap(), t0);
		assert_eq!(sched.stats().input_nulls_stripped, 1);
		assert_eq!(sched.buffered_packets(), 0, "input nulls are not buffered");
	}

	#[test]
	fn state_tracks_content_arrival_not_output() {
		let cfg = config()
			.with_latency(Duration::from_millis(10))
			.with_stall_timeout(Some(Duration::from_millis(200)))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();

		assert_eq!(sched.state(t0), SourceState::Priming, "no input yet");
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		assert_eq!(sched.state(t0), SourceState::Priming, "still filling the cushion");
		assert_eq!(sched.state(t0 + Duration::from_millis(11)), SourceState::Live);

		// Drain the one packet, then the buffer is dry with no new input.
		sched.emit_datagram(t0 + Duration::from_millis(11));
		assert_eq!(sched.state(t0 + Duration::from_millis(30)), SourceState::Starved);
		assert_eq!(sched.state(t0 + Duration::from_millis(300)), SourceState::Stalled);

		// Content returning clears the stall outright.
		sched.emit_datagram(t0 + Duration::from_millis(300));
		let resumed = t0 + Duration::from_millis(400);
		sched.enqueue(content_packet(0x100, None), resumed);
		assert_eq!(sched.state(resumed), SourceState::Live);
		assert_eq!(sched.stats().stalls, 1, "one stall, not one per output slot");
		assert!(sched.stats().content_gap_max_ms >= 300);
	}

	#[test]
	fn stops_inserting_pcr_when_the_source_stalls() {
		// The pacer's own PCR insertion is what makes a dead feed look conformant
		// downstream, so it must stop with the content.
		let cfg = config()
			.with_pcr_mode(PcrMode::Regenerate)
			.with_pcr_max_interval(Duration::from_micros(100))
			.with_stall_timeout(Some(Duration::from_millis(200)))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);

		let mut now = t0;
		for _ in 0..5 {
			sched.emit_datagram(now);
			now += Duration::from_millis(1);
		}
		let live_insertions = sched.stats().pcr_inserted;
		assert!(live_insertions > 0, "a merely late source still gets its clock held");

		// Past the stall timeout the stream has no clock to hold.
		now += Duration::from_millis(300);
		for _ in 0..40 {
			let datagram = sched.emit_datagram(now).to_vec();
			assert_eq!(pcr::read_pcr(&datagram), None, "no PCR in a programme-free carrier");
			now += Duration::from_millis(1);
		}
		assert_eq!(sched.stats().pcr_inserted, live_insertions);
		assert_eq!(sched.stats().stalls, 1);
		assert_eq!(sched.stats().underruns, 0, "a stall is not counted as an underrun");
	}

	#[test]
	fn muted_slots_hold_the_byte_clock_without_emitting() {
		let cfg = config()
			.with_stall_timeout(Some(Duration::from_millis(100)))
			.with_packets_per_datagram(7);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		// Drain the cushion first: buffered media is still content going to air.
		sched.emit_datagram(t0);
		let due_before = sched.next_due().unwrap();
		let emitted_before = sched.stats().output_packets;

		sched.advance_muted(t0 + Duration::from_millis(200));

		let stats = sched.stats();
		assert_eq!(stats.output_packets, emitted_before, "nothing reached the sink");
		assert_eq!(stats.muted_packets, 7);
		assert_eq!(stats.stalls, 1);
		// The byte clock still moved one datagram, so the output position (and the
		// PCR locked to it) stays aligned with real time across the gap.
		let one_datagram = Duration::from_nanos(7 * 188 * 8 * 1_000_000_000 / MUX_RATE);
		assert_eq!(sched.next_due().unwrap() - due_before, one_datagram);
	}

	#[test]
	fn resume_re_anchors_the_media_clock() {
		// Two PCRs 1 ms apart carrying one packet each => ~1000 pps media rate.
		let cfg = config()
			.with_latency(Duration::from_millis(20))
			.with_stall_timeout(Some(Duration::from_millis(200)))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		sched.enqueue(content_packet(0x100, Some(27_000)), t0);

		// Release the cushion, then run past the stall timeout with no new input.
		let mut now = t0 + Duration::from_millis(20);
		for _ in 0..4 {
			sched.emit_datagram(now);
			now += Duration::from_millis(1);
		}
		assert_eq!(sched.buffered_packets(), 0, "the cushion is spent");
		now = t0 + Duration::from_secs(2);
		sched.advance_muted(now);
		assert_eq!(sched.stats().stalls, 1);

		// Content returns as a burst, as a reconnecting transport delivers it.
		for _ in 0..50 {
			sched.enqueue(content_packet(0x100, None), now);
		}
		let released_before = sched.stats().content_packets;
		for _ in 0..10 {
			sched.emit_datagram(now);
			now += Duration::from_micros(150);
		}
		assert_eq!(
			sched.stats().content_packets,
			released_before,
			"resume re-primes the cushion instead of dumping the backlog at line rate"
		);

		// Past the priming window content flows again, still at media rate: 15 ms
		// of output at ~1000 pps is nowhere near the 50 packets buffered.
		now += Duration::from_millis(20);
		for _ in 0..100 {
			sched.emit_datagram(now);
			now += Duration::from_micros(150);
		}
		let released = sched.stats().content_packets - released_before;
		assert!(
			(1..30).contains(&released),
			"expected a media-rate trickle after resume, got {released} packets"
		);
	}

	#[test]
	fn flags_discontinuity_on_the_first_pcr_after_a_resume() {
		// The output byte clock ran through the stall, so the emitted PCR is still
		// monotonic; it is the media timeline that has a hole, and that is what the
		// discontinuity indicator is for.
		let cfg = config()
			.with_pcr_mode(PcrMode::Regenerate)
			.with_stall_timeout(Some(Duration::from_millis(200)))
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		let first = sched.emit_datagram(t0).to_vec();
		assert_eq!(first[5] & 0x80, 0, "stream start is not a discontinuity");

		let mut now = t0 + Duration::from_secs(2);
		sched.advance_muted(now);
		sched.enqueue(content_packet(0x100, Some(54_000_000)), now);

		let mut flagged = false;
		for _ in 0..40 {
			let datagram = sched.emit_datagram(now).to_vec();
			if pid_of(&datagram) == 0x100 && pcr::read_pcr(&datagram).is_some() && datagram[5] & 0x80 != 0 {
				flagged = true;
			}
			now += Duration::from_millis(1);
		}
		assert!(flagged, "resumed content carries the discontinuity indicator");
	}

	// --- stream clocking -------------------------------------------------------
	//
	// The property under test throughout is the one arrival clocking cannot have:
	// what a leg emits, and where, is a function of the stream it was given and of
	// nothing about the leg. So these tests compare two schedulers rather than
	// checking one against an expected transcript.

	/// Packets per PCR interval in the synthetic stream below, and the output
	/// slots that interval spans — so the stream runs at a fifth of the mux rate
	/// and every run is spread across stuffing.
	const RUN_LEN: usize = 8;
	const RUN_SLOTS: u64 = 40;

	fn stream_config() -> Config {
		Config::new(MUX_RATE)
			.with_latency(Duration::from_millis(50))
			.with_packets_per_datagram(1)
			.with_clocking(Clocking::Stream)
	}

	/// A synthetic source: `runs` PCR intervals of [`RUN_LEN`] packets, each
	/// packet tagged with the media time at which a well-behaved path delivers it.
	fn stream_packets(runs: usize) -> Vec<(Packet, Duration)> {
		stream_packets_with_peak(runs, usize::MAX, 0)
	}

	/// The same, but every `every` runs carries `peak` packets instead of
	/// [`RUN_LEN`] — a stream whose instantaneous rate exceeds the mux rate for
	/// one PCR interval, as an I-frame does.
	fn stream_packets_with_peak(runs: usize, every: usize, peak: usize) -> Vec<(Packet, Duration)> {
		let mut out = Vec::new();
		for run in 0..runs {
			let pcr = run as u64 * RUN_SLOTS * TICKS_PER_PACKET;
			let at = pcr::ticks_to_duration(pcr);
			let len = if run > 0 && run % every == 0 { peak } else { RUN_LEN };
			for index in 0..len {
				let packet = content_packet(0x100, (index == 0).then_some(pcr));
				out.push((packet, at + Duration::from_micros(index as u64 * 10)));
			}
		}
		out
	}

	/// Content packets in the output, by continuity counter, so a test can say
	/// what reached the wire rather than only where it sat.
	fn content_count(leg: &[(Framing, Vec<u8>)]) -> usize {
		leg.iter()
			.flat_map(|(_, dg)| dg.chunks_exact(TS_PACKET_SIZE))
			.filter(|p| pid_of(p) == 0x100)
			.count()
	}

	/// Everything about a leg that is a property of the leg rather than of the
	/// stream: when packets turn up, and how late the OS runs the emit timer.
	///
	/// Two legs of a pair differ in exactly these two ways and in nothing else.
	struct Path {
		arrival: fn(usize) -> Duration,
		wake: fn(u64) -> Duration,
	}

	/// A path with no delay of either kind, as a reference to vary from.
	const PUNCTUAL: Path = Path {
		arrival: |_| Duration::ZERO,
		wake: |_| Duration::ZERO,
	};

	/// A plausible one: delivery spread inside the release latency, and a timer
	/// that fires up to a packet time late. Neither pattern has any relation to
	/// the media structure, which is the point.
	const JITTERY: Path = Path {
		arrival: |i| Duration::from_micros((i as u64 * 7_919) % 20_000),
		wake: |slot| Duration::from_micros((slot * 4_099) % 120),
	};

	/// Drive a scheduler over `arrivals` down `path`, and return what came out
	/// with the framing it carried.
	fn run_leg(
		config: &Config,
		arrivals: &[(Packet, Duration)],
		t0: Instant,
		path: &Path,
		until_slot: u64,
	) -> Vec<(Framing, Vec<u8>)> {
		let mut sched = Scheduler::new(config);
		let mut out = Vec::new();
		let mut next = 0;
		loop {
			while next < arrivals.len() {
				let (packet, at) = &arrivals[next];
				let at = t0 + *at + (path.arrival)(next);
				if sched.next_due().is_some_and(|due| due < at) {
					break;
				}
				sched.enqueue(packet.clone(), at);
				next += 1;
			}
			let Some(due) = sched.next_due() else {
				break;
			};
			// Arrival clocking offers no framing, so the comparison falls back to
			// the position on the wire — which is exactly the point at issue: the
			// nth datagram of one leg is not the nth of the other.
			let framing = sched.framing().unwrap_or(Framing {
				slot: sched.stats().output_packets,
				sequence: sched.stats().output_packets as u16,
				timestamp_90khz: 0,
			});
			if framing.slot >= until_slot {
				break;
			}
			// The timer fires when the OS gets round to it, not when the byte clock
			// says it should. Under arrival clocking that lateness decides how much
			// content is released into the slot; under stream clocking it decides
			// nothing.
			let woke = due + (path.wake)(framing.slot);
			// A stalled source mutes rather than emitting, exactly as the driver
			// does under StallPolicy::Mute — and the slot still advances.
			if sched.state(woke).is_stalled() {
				sched.advance_muted(woke);
				continue;
			}
			out.push((framing, sched.emit_datagram(woke).to_vec()));
		}
		out
	}

	/// The output as an ST 2022-7 receiver sees it: what byte went in which slot.
	fn by_slot(leg: &[(Framing, Vec<u8>)]) -> Vec<(u64, Vec<u8>)> {
		leg.iter().map(|(f, dg)| (f.slot, dg.clone())).collect()
	}

	/// A source at the mux rate whose PCR *values* are the same exact grid as
	/// [`stream_packets`]'s, but whose PCR *positions* are clustered: `cluster`
	/// PCR-bearing packets back-to-back, then the media bytes belonging to all the
	/// slots that cluster just claimed, then the next cluster.
	///
	/// This is the measured shape of a `moq export ts` capture. In
	/// `~/t19-pcrfix/exp-new/export.ts` — 393,311 packets, 2,473 PCRs, an exact
	/// 25 ms value grid with 0 intervals over the 40 ms repetition limit — 87.2 %
	/// of the PCR packets sit one packet from the previous one, and the media bytes
	/// they label are heaped between the clusters at gaps up to 2,730 packets,
	/// against the 165 an even grid at that rate implies. Every check on the values
	/// passes; the positional cadence is degenerate.
	///
	/// At the mux rate, so `RUN_SLOTS` slots carry `RUN_SLOTS` packets and the
	/// average rate is exactly what the grid is provisioned for. That is the point:
	/// the defect is positional, not volumetric, and a rate check cannot see it.
	fn stream_packets_clustered(runs: usize, cluster: usize) -> Vec<(Packet, Duration)> {
		let mut out = Vec::new();
		let mut run = 0;
		while run < runs {
			let burst = cluster.min(runs - run);
			let at = pcr::ticks_to_duration(run as u64 * RUN_SLOTS * TICKS_PER_PACKET);
			let mut offset = 0;
			// The cluster: consecutive packets, one PCR each, values a full run
			// apart. Each claims a whole interval's span and carries one packet.
			for step in 0..burst {
				let pcr = (run + step) as u64 * RUN_SLOTS * TICKS_PER_PACKET;
				out.push((
					content_packet(0x100, Some(pcr)),
					at + Duration::from_micros(offset * 10),
				));
				offset += 1;
			}
			// The media those intervals were labelling, with no PCR among it, so it
			// all falls in the single run the last PCR of the cluster left open.
			for _ in 0..(burst as u64 * RUN_SLOTS) {
				out.push((content_packet(0x100, None), at + Duration::from_micros(offset * 10)));
				offset += 1;
			}
			run += burst;
		}
		out
	}

	/// The same volume and the same PCR values, spread evenly — the control.
	fn stream_packets_at_rate(runs: usize) -> Vec<(Packet, Duration)> {
		let mut out = Vec::new();
		for run in 0..runs {
			let pcr = run as u64 * RUN_SLOTS * TICKS_PER_PACKET;
			let at = pcr::ticks_to_duration(pcr);
			for index in 0..RUN_SLOTS {
				let packet = content_packet(0x100, (index == 0).then_some(pcr));
				out.push((packet, at + Duration::from_micros(index * 10)));
			}
		}
		out
	}

	#[test]
	fn a_clustered_pcr_position_displaces_the_grid_and_is_counted() {
		// The assumption `close_run` makes is that packets between two PCRs are
		// proportional to the media time between them. This source breaks it while
		// keeping the values exact, so nothing that grades the values can object.
		//
		// What it must not do is pass silently. The displacement is the figure that
		// says by how much, and it is what tells this apart from a rate peak.
		let cfg = stream_config();
		let clustered = stream_packets_clustered(64, 8);
		let even = stream_packets_at_rate(64);

		let t0 = Instant::now();
		let mut bad = Scheduler::new(&cfg);
		for (packet, at) in &clustered {
			bad.enqueue(packet.clone(), t0 + *at);
		}
		let mut good = Scheduler::new(&cfg);
		for (packet, at) in &even {
			good.enqueue(packet.clone(), t0 + *at);
		}

		let bad = bad.stats();
		let good = good.stats();

		// The control carries the same bytes and the same PCR values at the same
		// average rate, so anything it also reports is not the positional defect.
		assert_eq!(
			good.pcr_position_overruns, 0,
			"an evenly spread source overran its own span"
		);
		assert_eq!(
			good.pcr_position_displacement, 0,
			"an evenly spread source displaced the grid by {}",
			good.pcr_position_displacement
		);

		assert!(
			bad.pcr_position_overruns > 0,
			"the clustered source did not register an overrun"
		);
		// A cluster of 8 leaves 8 intervals' worth of media in one interval's span,
		// so placement runs about 8 * RUN_SLOTS slots past where the source's own
		// PCR asks the grid to be.
		let expected = (8 * RUN_SLOTS).saturating_sub(RUN_SLOTS);
		assert!(
			bad.pcr_position_displacement >= expected,
			"displacement {} did not reach the {} slots the clustering implies",
			bad.pcr_position_displacement,
			expected
		);
	}

	#[test]
	fn a_rate_peak_is_not_read_as_a_positional_defect() {
		// The discriminator, stated as a test: an I-frame overruns its interval's
		// span too, and that is legitimate — the excess spills into the stuffing
		// after it and the grid recovers. So `overruns` alone cannot be the guard,
		// and the displacement a peak reaches must stay far below what clustering
		// reaches on the same stream.
		let cfg = stream_config();
		let t0 = Instant::now();

		let mut peaky = Scheduler::new(&cfg);
		for (packet, at) in &stream_packets_with_peak(64, 8, RUN_SLOTS as usize * 2) {
			peaky.enqueue(packet.clone(), t0 + *at);
		}
		let mut clustered = Scheduler::new(&cfg);
		for (packet, at) in &stream_packets_clustered(64, 8) {
			clustered.enqueue(packet.clone(), t0 + *at);
		}

		let peaky = peaky.stats().pcr_position_displacement;
		let clustered = clustered.stats().pcr_position_displacement;
		assert!(
			clustered > peaky * 2,
			"a peak displaced {peaky} slots and clustering {clustered}: the two are not distinguishable"
		);
	}

	#[test]
	fn a_clustered_source_does_not_break_byte_identity() {
		// The guard must not buy its diagnosis with the property the mode exists
		// for. Two legs given the clustered source over independent paths still
		// have to agree, because placement is still a function of the delivered
		// packets alone — a displaced grid is displaced identically on both legs.
		let cfg = stream_config();
		let arrivals = stream_packets_clustered(64, 8);
		let t0 = Instant::now();
		let smooth = run_leg(&cfg, &arrivals, t0, &PUNCTUAL, 2_000);
		let rough = run_leg(&cfg, &arrivals, t0, &JITTERY, 2_000);
		assert!(!smooth.is_empty(), "the leg emitted nothing");
		assert_eq!(
			by_slot(&smooth),
			by_slot(&rough),
			"the path changed the output on a clustered source"
		);
	}

	#[test]
	fn stream_clocking_ignores_the_path() {
		// Two legs of a pair see the same objects over independent paths, so they
		// see them at different times and emit them on independently scheduled
		// timers. That is the whole difference between the legs, and it must make
		// no difference to the bytes.
		let cfg = stream_config();
		let arrivals = stream_packets(60);
		let t0 = Instant::now();
		let smooth = run_leg(&cfg, &arrivals, t0, &PUNCTUAL, 2_000);
		let rough = run_leg(&cfg, &arrivals, t0, &JITTERY, 2_000);
		assert!(!smooth.is_empty(), "the leg emitted nothing");
		assert_eq!(by_slot(&smooth), by_slot(&rough), "the path changed the output");
	}

	#[test]
	fn arrival_clocking_does_not() {
		// The negative control, and the reason Arm D exists: the same stream down
		// two paths through the existing mode is two different transports, which is
		// what T12 measured at the receiver.
		let cfg = stream_config().with_clocking(Clocking::Arrival);
		let arrivals = stream_packets(60);
		let t0 = Instant::now();
		let smooth = run_leg(&cfg, &arrivals, t0, &PUNCTUAL, 2_000);
		let rough = run_leg(&cfg, &arrivals, t0, &JITTERY, 2_000);
		assert_ne!(by_slot(&smooth), by_slot(&rough));
	}

	#[test]
	fn a_stream_clocked_leg_still_notices_its_source_die() {
		// A run is held until its closing PCR arrives. If the source dies mid-run
		// that PCR never comes, and treating the held packets as programme in hand
		// would keep the leg reporting Live over a dead feed for as long as it ran
		// — the failure stall detection exists to prevent, reintroduced by the
		// mode that is supposed to make a leg's death visible to its partner.
		let cfg = stream_config().with_stall_timeout(Some(Duration::from_millis(200)));
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		// Two full runs, then the first half of a third: content with no closing PCR.
		for (packet, at) in stream_packets(3).into_iter().take(2 * RUN_LEN + 4) {
			sched.enqueue(packet, t0 + at);
		}
		// Long past the stall timeout, but with placed content still to transmit:
		// buffered media is programme going to air, however old the silence.
		let now = t0 + Duration::from_secs(1);
		assert_eq!(sched.state(now), SourceState::Live);

		// Once it has all gone out, what is left is a run whose closing PCR will
		// never arrive. That is not programme in hand.
		for _ in 0..4 * RUN_SLOTS {
			sched.emit_datagram(now);
		}
		assert_eq!(sched.state(now), SourceState::Stalled);
		assert_eq!(sched.stats().stalls, 1);
	}

	#[test]
	fn a_rate_peak_spills_forward_instead_of_being_dropped() {
		// A PCR interval carrying more packets than the mux rate has room for is
		// ordinary VBR video against a groomer provisioned on the average rate.
		// The excess has to go in the slots after the peak: dropping it would be a
		// groomer quietly deleting programme, and dropping it identically on both
		// legs would make a redundant pair agree on damage.
		let cfg = stream_config();
		// 90 packets where the grid has 40 slots: two and a bit intervals' worth.
		let arrivals = stream_packets_with_peak(60, 10, 90);
		let t0 = Instant::now();
		let smooth = run_leg(&cfg, &arrivals, t0, &PUNCTUAL, 3_000);
		let rough = run_leg(&cfg, &arrivals, t0, &JITTERY, 3_000);

		// Everything the source produced, less the packets before the first PCR
		// (there is no grid to place those on) and the run left open at the end.
		let peaks = arrivals.len() - RUN_LEN;
		assert!(
			content_count(&smooth) >= peaks - RUN_LEN,
			"the peak cost content: {} of {peaks} packets reached the wire",
			content_count(&smooth)
		);
		assert_eq!(by_slot(&smooth), by_slot(&rough), "the path changed the output");
	}

	#[test]
	fn a_leg_that_joins_late_lands_on_the_running_grid() {
		// Restart-into-alignment: the operational property that a leg brought back
		// after maintenance protects its partner without co-starting the pair.
		let cfg = stream_config();
		let arrivals = stream_packets(60);
		let t0 = Instant::now();
		let running = run_leg(&cfg, &arrivals, t0, &PUNCTUAL, 2_000);
		let joined = run_leg(&cfg, &arrivals[20 * RUN_LEN..], t0, &JITTERY, 2_000);

		let running: std::collections::HashMap<_, _> = by_slot(&running).into_iter().collect();
		let overlap = by_slot(&joined);
		assert!(overlap.len() > 500, "too little overlap to conclude anything");
		for (slot, datagram) in overlap {
			match running.get(&slot) {
				Some(theirs) => assert_eq!(&datagram, theirs, "slot {slot} differs"),
				None => panic!("the late leg emitted slot {slot}, which the running leg never used"),
			}
		}
	}

	#[test]
	fn a_muted_leg_returns_on_its_partner_s_numbering() {
		// The finding from T12's recovery cell: under arrival clocking a leg that
		// stops and returns comes back short by however long it was away, because
		// its numbering counted its own sends. Here it counts stream position, so
		// the outage costs it content and nothing else.
		let cfg = stream_config().with_stall_timeout(Some(Duration::from_millis(200)));
		let whole = stream_packets(120);
		let outage = 40 * RUN_LEN..80 * RUN_LEN;
		let gapped: Vec<_> = whole
			.iter()
			.enumerate()
			.filter(|(i, _)| !outage.contains(i))
			.map(|(_, p)| p.clone())
			.collect();

		let t0 = Instant::now();
		let partner = run_leg(&cfg, &whole, t0, &PUNCTUAL, 4_000);
		let interrupted = run_leg(&cfg, &gapped, t0, &JITTERY, 4_000);

		let partner: std::collections::HashMap<_, _> = partner
			.iter()
			.map(|(f, dg)| (f.slot, (f.sequence, f.timestamp_90khz, dg.clone())))
			.collect();
		let after_outage: Vec<_> = interrupted.iter().filter(|(f, _)| f.slot > (80 * RUN_SLOTS)).collect();
		assert!(after_outage.len() > 200, "the leg did not come back");
		for (framing, datagram) in after_outage {
			let (sequence, timestamp, theirs) = partner
				.get(&framing.slot)
				.unwrap_or_else(|| panic!("slot {} is not on the partner's grid", framing.slot));
			assert_eq!(framing.sequence, *sequence, "RTP sequence diverged after the outage");
			assert_eq!(framing.timestamp_90khz, *timestamp, "RTP timestamp diverged");
			assert_eq!(datagram, theirs, "slot {} differs after the outage", framing.slot);
		}
	}

	#[test]
	fn a_leg_joining_a_running_broadcast_starts_at_the_live_edge() {
		// A relay hands a new subscriber what it has buffered, oldest first. A leg
		// that started on that first packet would sit a backlog behind its partner
		// for the rest of the run: it emits at the mux rate, so it has no way to
		// close the gap. What the pair needs is the phase a leg reaches when it
		// starts from empty on a live feed, whatever the relay had in hand.
		let cfg = stream_config();
		let whole = stream_packets(1_000);
		// Three seconds of programme delivered in one go, then delivery at rate.
		let join_at = pcr::ticks_to_duration(600 * RUN_SLOTS * TICKS_PER_PACKET);
		let backlogged: Vec<_> = whole
			.iter()
			.map(|(packet, at)| (packet.clone(), (*at).max(join_at)))
			.collect();

		let t0 = Instant::now();
		let partner = run_leg(&cfg, &whole, t0, &PUNCTUAL, 40_000);
		let joiner = run_leg(&cfg, &backlogged, t0, &JITTERY, 40_000);

		let first = joiner.first().expect("the joining leg emitted nothing").0.slot;
		let edge = 600 * RUN_SLOTS;
		assert!(
			first > edge - 1_000,
			"the leg started {} slots behind the edge: it took the backlog as its phase",
			edge.saturating_sub(first)
		);

		// Having skipped the backlog, it must still be on the same grid.
		let partner: std::collections::HashMap<_, _> = by_slot(&partner).into_iter().collect();
		let overlap = by_slot(&joiner[1..]);
		assert!(overlap.len() > 1_000, "too little overlap to conclude anything");
		for (slot, datagram) in overlap {
			let theirs = partner
				.get(&slot)
				.unwrap_or_else(|| panic!("slot {slot} is not on the partner's grid"));
			assert_eq!(&datagram, theirs, "slot {slot} differs");
		}
	}

	#[test]
	fn a_backlog_delivered_after_the_clock_starts_moves_it_to_the_live_edge() {
		// The shape a relay actually serves a new subscriber: a little content,
		// then the rest of its buffer once the subscription is going. Choosing the
		// phase at the first packet is not enough — by the time the backlog lands
		// the leg is already running, and it would stay a buffer's depth behind
		// its partner for the rest of the run.
		let cfg = stream_config()
			.with_latency(Duration::from_millis(50))
			.with_max_latency(Duration::from_millis(500));
		let mut sched = Scheduler::new(&cfg);
		let stream = stream_packets(1_000);
		let t0 = Instant::now();
		for (packet, at) in &stream[..10 * RUN_LEN] {
			sched.enqueue(packet.clone(), t0 + *at);
		}
		let now = t0 + Duration::from_millis(50);
		sched.emit_datagram(now);
		let before = sched.framing().expect("stream clocking").slot;

		for (packet, _) in &stream[10 * RUN_LEN..] {
			sched.enqueue(packet.clone(), now);
		}
		sched.emit_datagram(now);
		let after = sched.framing().expect("stream clocking").slot;

		assert_eq!(sched.stats().resyncs, 1, "the leg did not move to the edge");
		let edge = 999 * RUN_SLOTS;
		assert!(
			after > edge - 1_000 && after > before + 30_000,
			"slot went {before} -> {after}, with the stream's edge at {edge}"
		);
		// And it is at the edge to carry it, not to sit in front of it.
		assert!(sched.has_pending(), "nothing left to send at the edge");
	}

	#[test]
	fn an_outage_longer_than_a_splice_is_still_an_outage() {
		// A leg off the air for longer than a source discontinuity is allowed to
		// look like has to come back onto the grid, not re-anchor itself as though
		// the programme had been spliced. Getting this wrong is silent: the leg
		// resumes, its numbering is right, and every packet it carries is placed in
		// the past and dropped, so it protects nothing.
		let cfg = stream_config().with_stall_timeout(Some(Duration::from_millis(200)));
		// One run is ~5 ms, so the outage here is ~6 s: comfortably past the point
		// at which a forward PCR jump would otherwise be read as a splice.
		let whole = stream_packets(1_600);
		let outage = 300 * RUN_LEN..1_500 * RUN_LEN;
		let gapped: Vec<_> = whole
			.iter()
			.enumerate()
			.filter(|(i, _)| !outage.contains(i))
			.map(|(_, p)| p.clone())
			.collect();

		let t0 = Instant::now();
		let partner = run_leg(&cfg, &whole, t0, &PUNCTUAL, 64_000);
		let interrupted = run_leg(&cfg, &gapped, t0, &JITTERY, 64_000);

		let partner: std::collections::HashMap<_, _> = partner
			.iter()
			.map(|(f, dg)| (f.slot, (f.sequence, dg.clone())))
			.collect();
		let after: Vec<_> = interrupted.iter().filter(|(f, _)| f.slot > 1_500 * RUN_SLOTS).collect();
		assert!(after.len() > 1_000, "the leg did not come back");
		for (framing, datagram) in &after {
			let (sequence, theirs) = partner
				.get(&framing.slot)
				.unwrap_or_else(|| panic!("slot {} is not on the partner's grid", framing.slot));
			assert_eq!(framing.sequence, *sequence, "RTP sequence diverged across the outage");
			assert_eq!(datagram, theirs, "slot {} differs after the outage", framing.slot);
		}

		// Numbering alone is not a return to service. The leg has to be carrying
		// the programme again, which is what the splice reading cost it.
		let carried = after
			.iter()
			.flat_map(|(_, dg)| dg.chunks_exact(TS_PACKET_SIZE))
			.filter(|p| pid_of(p) == 0x100)
			.count();
		assert!(
			carried > 500,
			"the leg resumed carrying no programme: {carried} packets"
		);
	}

	#[test]
	fn datagram_boundaries_come_off_the_grid_too() {
		// Same bytes packed into differently-cut datagrams merge no better than
		// different bytes, so where a leg's first content lands must not decide
		// where its datagrams are split.
		let cfg = stream_config().with_packets_per_datagram(7);
		let arrivals = stream_packets(60);
		let t0 = Instant::now();
		let running = run_leg(&cfg, &arrivals, t0, &PUNCTUAL, 2_000);
		// Cut mid-datagram: run 21 opens at slot 840, which is 7-aligned, so drop
		// three more packets to make sure the leg's first content is not.
		let joined = run_leg(&cfg, &arrivals[21 * RUN_LEN + 3..], t0, &JITTERY, 2_000);

		let running: std::collections::HashMap<_, _> = by_slot(&running).into_iter().collect();
		// The first datagram is partial by construction: the leg joined inside it,
		// so it is short the content its partner already had. Alignment is a claim
		// about every datagram after the one the leg arrived in.
		let overlap = by_slot(&joined[1..]);
		assert!(overlap.len() > 100, "too little overlap to conclude anything");
		for (slot, datagram) in overlap {
			assert_eq!(slot % 7, 0, "datagram starts off the boundary at slot {slot}");
			let theirs = running
				.get(&slot)
				.unwrap_or_else(|| panic!("slot {slot} is not on the partner's grid"));
			assert_eq!(&datagram, theirs, "slot {slot} differs");
		}
	}

	#[test]
	fn emitted_pcr_is_the_slot_it_sits_in() {
		// Under arrival clocking the emitted PCR is byte-locked to an anchor this
		// process chose. Under stream clocking there is no anchor: the value is the
		// slot, so two legs agree on it without having agreed on anything.
		let cfg = stream_config();
		let map = SlotMap::new(MUX_RATE);
		let leg = run_leg(&cfg, &stream_packets(40), Instant::now(), &PUNCTUAL, 1_200);
		let mut seen = 0;
		for (framing, datagram) in &leg {
			let Some(value) = pcr::read_pcr(datagram) else {
				continue;
			};
			assert_eq!(value, map.pcr_of_slot(framing.slot), "slot {}", framing.slot);
			seen += 1;
		}
		assert!(seen > 10, "expected PCRs in the output, saw {seen}");
	}

	#[test]
	fn a_sequence_seed_offsets_the_pair_without_unlocking_it() {
		let base = stream_config();
		let seeded = base.with_sequence_seed(9_000);
		let arrivals = stream_packets(20);
		let t0 = Instant::now();
		let plain = run_leg(&base, &arrivals, t0, &PUNCTUAL, 600);
		let offset = run_leg(&seeded, &arrivals, t0, &JITTERY, 600);
		assert_eq!(by_slot(&plain), by_slot(&offset), "the seed must not move any bytes");
		for ((a, _), (b, _)) in plain.iter().zip(offset.iter()) {
			assert_eq!(b.sequence, a.sequence.wrapping_add(9_000));
		}
	}

	#[test]
	fn stream_clocking_rejects_a_config_it_cannot_honour() {
		// Both of these would silently produce a leg whose grid is its own: an
		// auto rate is measured from one process's arrival window, and a preserved
		// PCR is not the slot. Better a refused config than output that does not
		// merge.
		let auto = Config::auto().with_clocking(Clocking::Stream);
		assert!(matches!(auto.validate(), Err(Error::Config(_))));
		let preserved = Config::new(MUX_RATE)
			.with_clocking(Clocking::Stream)
			.with_pcr_mode(PcrMode::Preserve);
		assert!(matches!(preserved.validate(), Err(Error::Config(_))));
		assert!(stream_config().validate().is_ok());
		assert!(config().validate().is_ok(), "arrival clocking constrains nothing");
	}

	// --- adaptive sizing ---------------------------------------------------------

	/// The mux rate in packets per second, and the media rate of the synthetic
	/// segmented stream below (a fifth under the mux rate, as a groomed feed is).
	const MUX_PPS: f64 = MUX_RATE as f64 / PACKET_BITS as f64;
	const MEDIA_PPS: f64 = MUX_PPS * 0.8;

	fn adaptive_config() -> Config {
		Config::new(MUX_RATE).with_packets_per_datagram(1)
	}

	/// A contiguous stream, one PCR every `PCR_EVERY` packets, handed to `sched` a
	/// segment at a time: `segments` deliveries of `segment` of media each, every
	/// `segment` of wall time, each fetched in `fetch`. The PCR timeline is
	/// continuous across the deliveries, as a segmenter's output is.
	const PCR_EVERY: u64 = 20;

	fn deliver_segments(
		sched: &mut Scheduler,
		t0: Instant,
		segment: Duration,
		fetch: Duration,
		segments: u32,
	) -> Instant {
		let per_segment = (segment.as_secs_f64() * MEDIA_PPS) as u64;
		let mut index = 0_u64;
		for n in 0..segments {
			let at = t0 + segment.mul_f64(f64::from(n));
			for i in 0..per_segment {
				let pcr = index * (PCR_CLOCK_HZ / MEDIA_PPS as u64);
				let packet = content_packet(0x100, (index % PCR_EVERY == 0).then_some(pcr));
				sched.enqueue(packet, at + fetch.mul_f64(i as f64 / per_segment as f64));
				index += 1;
			}
		}
		t0 + segment.mul_f64(f64::from(segments))
	}

	#[test]
	fn a_segmented_input_sizes_its_own_cushion() {
		// The whole point of §9.1: fed a feed that arrives a segment at a time, the
		// pacer works out that it needs seconds of buffer rather than the
		// milliseconds a continuous feed needs, without being told.
		let mut sched = Scheduler::new(&adaptive_config());
		let t0 = Instant::now();
		deliver_segments(&mut sched, t0, Duration::from_secs(2), Duration::from_millis(300), 3);

		let stats = sched.stats();
		assert!(
			(1_500..2_100).contains(&stats.arrival_lead_ms),
			"expected a lead near the segment duration, got {} ms",
			stats.arrival_lead_ms
		);
		assert!(
			(4_000..8_001).contains(&stats.latency_target_ms),
			"expected a cushion of several seconds, got {} ms",
			stats.latency_target_ms
		);
		// And the derived stall timeout is past the doubled gap a segment fetcher
		// produces when it misses a publish cycle.
		let timeout = sched.stall_timeout().expect("a derived timeout");
		assert!(
			timeout >= Duration::from_secs(4),
			"a 4 s inter-segment gap would still mute at {timeout:?}"
		);
	}

	#[test]
	fn a_continuous_input_keeps_the_millisecond_cushion() {
		// The regression guard: serving a segmented plane must cost the object
		// plane nothing. A feed delivered at the media rate never gets ahead, so
		// adaptive sizing leaves it on the floor and the run is indistinguishable
		// from one on a pinned 200 ms cushion.
		let mut sched = Scheduler::new(&adaptive_config());
		let t0 = Instant::now();
		let slot = Duration::from_secs_f64(1.0 / MUX_PPS);
		let per_packet = Duration::from_secs_f64(1.0 / MEDIA_PPS);
		let mut delivered = 0_u64;

		// Interleaved, as a live run is: input trickles in at the media rate while
		// the output byte clock runs at the mux rate.
		for output in 0..(20.0 * MUX_PPS) as u64 {
			let now = t0 + slot.mul_f64(output as f64);
			while per_packet.mul_f64(delivered as f64) <= now.saturating_duration_since(t0) {
				let pcr = delivered * (PCR_CLOCK_HZ / MEDIA_PPS as u64);
				let packet = content_packet(0x100, (delivered % PCR_EVERY == 0).then_some(pcr));
				sched.enqueue(packet, now);
				delivered += 1;
			}
			if sched.next_due().is_some() {
				sched.emit_datagram(now);
			}
		}

		let stats = sched.stats();
		assert_eq!(
			stats.latency_target_ms,
			DEFAULT_LATENCY.as_millis() as u64,
			"a continuous feed must stay on the configured floor (lead {} ms)",
			stats.arrival_lead_ms
		);
		assert_eq!(stats.dropped_packets, 0, "and must not have its buffer tightened");
		assert_eq!(stats.stalls, 0, "nor be read as a dead source");
		assert!(
			stats.buffer_high_water < (DEFAULT_LATENCY.as_secs_f64() * MEDIA_PPS * 2.0) as u64,
			"the cushion grew to {} packets on a feed that never got ahead",
			stats.buffer_high_water
		);
	}

	#[test]
	fn the_start_gate_waits_out_a_delivery_before_committing() {
		// A segmented feed reaches a 200 ms cushion a few milliseconds into a 2 s
		// segment fetch. Starting there is what makes the ordinary gap before the
		// next segment read as an underrun, so the gate has to wait for the
		// delivery to end — at which point the burst has sized the cushion, and the
		// cushion is deeper than what is in hand.
		let mut sched = Scheduler::new(&adaptive_config());
		let t0 = Instant::now();
		deliver_segments(&mut sched, t0, Duration::from_secs(2), Duration::from_millis(300), 1);
		assert!(
			sched.next_due().is_none(),
			"the clock started on one segment, holding {} ms against a {} ms cushion",
			(sched.buffered_packets() as f64 / MEDIA_PPS * 1000.0) as u64,
			sched.stats().latency_target_ms
		);
		assert_eq!(sched.state(t0 + Duration::from_secs(1)), SourceState::Priming);

		// Enough deliveries to fill the cushion it decided on, and it starts.
		deliver_segments(&mut sched, t0, Duration::from_secs(2), Duration::from_millis(300), 5);
		sched.poll_start(t0 + Duration::from_secs(10));
		assert!(sched.next_due().is_some(), "the clock never started");
	}

	#[test]
	fn a_pinned_cushion_still_starts_on_a_timer() {
		// Setting a depth explicitly turns the content gate off: the operator has
		// said how deep the buffer is, so there is nothing to learn by waiting.
		let cfg = adaptive_config().with_latency(Duration::from_millis(50));
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		sched.enqueue(content_packet(0x100, Some(0)), t0);
		assert_eq!(
			sched.next_due(),
			Some(t0 + Duration::from_millis(50)),
			"a pinned cushion arms on the first packet"
		);
	}

	#[test]
	fn a_segment_larger_than_the_buffer_is_not_dropped() {
		// The failure that made a groomer configured for an object transport
		// unusable on a segmented one: a 2 s segment against a 2 s buffer bound
		// overflows on arrival, so the groomer deletes programme from a healthy
		// feed, once per segment, silently.
		let mut sched = Scheduler::new(&adaptive_config());
		let t0 = Instant::now();
		deliver_segments(&mut sched, t0, Duration::from_secs(2), Duration::from_millis(300), 4);
		assert_eq!(
			sched.stats().dropped_packets,
			0,
			"dropped programme from a healthy segmented feed"
		);
		assert!(
			sched.stats().buffer_high_water > (2.0 * MEDIA_PPS) as u64,
			"the buffer never held a whole segment: high water {}",
			sched.stats().buffer_high_water
		);
	}

	#[test]
	fn a_long_gap_on_a_contiguous_stream_is_not_a_splice() {
		// A segment fetcher that misses a publish cycle waits two periods and then
		// collects both segments, so the stream it hands over is contiguous however
		// long the silence was. Nothing may read that silence as a source
		// discontinuity: a spurious re-base steps the emitted PCR off the byte
		// clock, which is the one thing a groomer exists to guarantee.
		let cfg = adaptive_config().with_pcr_mode(PcrMode::Regenerate);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		let segment = Duration::from_secs(6);
		// Two deliveries twelve seconds apart: one missed publish cycle at the
		// longest segment duration the lab measured.
		deliver_segments(&mut sched, t0, segment * 2, Duration::from_millis(900), 2);

		let mut now = t0;
		let mut flagged = 0;
		for _ in 0..(30.0 * MUX_PPS) as u64 {
			let datagram = sched.emit_datagram(now).to_vec();
			if pcr::read_pcr(&datagram).is_some() && datagram[5] & 0x80 != 0 {
				flagged += 1;
			}
			now += Duration::from_secs_f64(1.0 / MUX_PPS);
		}
		assert_eq!(sched.stats().pcr_rebases, 0, "the silence was read as a splice");
		assert_eq!(flagged, 0, "{flagged} PCRs falsely flagged discontinuous");
	}

	#[test]
	fn drops_oldest_past_max_latency() {
		// Tiny max_latency => small buffer; a burst overflows and drops.
		let cfg = Config::new(MUX_RATE)
			.with_latency(Duration::ZERO)
			.with_max_latency(Duration::from_micros(200)) // ~2 packets at 12 Mb/s
			.with_packets_per_datagram(1);
		let mut sched = Scheduler::new(&cfg);
		let t0 = Instant::now();
		for _ in 0..50 {
			sched.enqueue(content_packet(0x100, None), t0);
		}
		assert!(sched.stats().dropped_packets > 0, "a burst past max_latency must drop");
	}
}
