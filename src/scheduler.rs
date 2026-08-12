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
//!   instead of draining a burst faster than real time.
//!
//! Every output slot emits a content packet when the media clock says one is due
//! and the buffer has it, otherwise a null packet. So a burst is absorbed by the
//! [`JitterBuffer`] and released at media rate, while the wire stays CBR.
//!
//! It also tracks *content liveness* ([`SourceState`]), because stuffing to hold
//! a rate is the right answer to a late source and the wrong answer to an absent
//! one. Past the stall timeout the scheduler stops claiming a clock it no longer
//! has — no PCR is inserted into a stream with no content in it — and the driver
//! applies [`StallPolicy`](crate::StallPolicy).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::config::{Clocking, Config, DEFAULT_AUTO_FALLBACK, PcrMode};
use crate::jitter_buffer::JitterBuffer;
use crate::null_insertion::{NULL_PID, null_packet, pcr_only_packet};
use crate::observe::SourceState;
use crate::output::Framing;
use crate::packet::{Packet, TS_PACKET_SIZE};
use crate::pcr::{self, PACKET_BITS, PcrRegen};
use crate::slot::SlotMap;
use crate::stats::Stats;

/// Smoothing factor for the media-rate EMA (weight of each new sample).
const RATE_EMA_ALPHA: f64 = 0.1;

/// Recovers the source media rate from PCR deltas and paces content release.
#[derive(Debug)]
struct MediaClock {
	/// Wall time content packet 0 becomes eligible (first enqueue + latency).
	anchor: Option<Instant>,
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
}

impl MediaClock {
	fn new(fallback_pps: f64) -> Self {
		Self {
			anchor: None,
			released: 0,
			rate_pps: fallback_pps,
			estimated: false,
			last_pcr: None,
			packets_since_pcr: 0,
		}
	}

	/// Observe an enqueued packet, refining the media-rate estimate on each PCR.
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
				let sample = self.packets_since_pcr as f64 / secs;
				self.rate_pps = if self.estimated {
					self.rate_pps * (1.0 - RATE_EMA_ALPHA) + sample * RATE_EMA_ALPHA
				} else {
					sample
				};
				self.estimated = true;
			}
		}
		self.last_pcr = Some(pcr);
		self.packets_since_pcr = 0;
	}

	/// Number of content packets that should have been released by `now`.
	fn due(&self, now: Instant) -> u64 {
		let Some(anchor) = self.anchor else {
			return 0;
		};
		if now < anchor {
			return 0;
		}
		let elapsed = now.duration_since(anchor).as_secs_f64();
		(elapsed * self.rate_pps) as u64 + 1
	}
}

/// Places packets on the absolute output grid from their source PCR alone.
///
/// Packets are accumulated into *runs* — a PCR-bearing packet and everything up
/// to the next one — because a run's length is only known once the closing PCR
/// arrives. The run is then spread evenly between the two slots its PCRs imply,
/// so where a packet lands is a function of the delivered bytes and nothing else.
/// The cost is one PCR interval of look-ahead (~20 ms on a typical mux) inside
/// the release latency the pacer already holds.
#[derive(Debug)]
struct StreamGrid {
	map: SlotMap,
	/// The run being accumulated: its opening PCR packet, then the rest.
	run: Vec<Packet>,
	/// `(source PCR, absolute slot)` of the packet that opened the run.
	open: Option<(u64, u64)>,
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
	capacity: usize,
}

impl StreamGrid {
	fn new(mux_rate_bps: u64, capacity: usize) -> Self {
		Self {
			map: SlotMap::new(mux_rate_bps),
			run: Vec::new(),
			open: None,
			placed: VecDeque::new(),
			epoch: 0,
			last_span: None,
			next_free: 0,
			capacity: capacity.max(1),
		}
	}

	/// Absolute slot for a source PCR, keeping the grid monotonic.
	fn slot_for(&mut self, pcr: u64) -> u64 {
		let raw = self.map.slot_of_pcr(pcr);
		if let Some((open_pcr, open_slot)) = self.open {
			let delta = pcr::forward_delta(open_pcr, pcr);
			let spliced = delta == 0 || pcr::ticks_to_duration(delta) > pcr::PCR_DISCONTINUITY_GAP;
			if spliced {
				// A loop wrap or splice moves the source clock arbitrarily. Continue
				// the grid from where the open run ends: both legs of a pair compute
				// the same continuation, so an aligned pair stays aligned — but a leg
				// that joins *after* a splice cannot know it happened, which is the
				// documented limit of stream clocking.
				let resume_at = open_slot.saturating_add(self.run.len().max(1) as u64);
				self.epoch = i128::from(resume_at) - i128::from(raw);
			} else if i128::from(raw) + self.epoch < i128::from(open_slot) {
				// The 33-bit PCR value wrapped; the stream did not.
				self.epoch += i128::from(self.map.slots_per_wrap());
			}
		}
		(i128::from(raw) + self.epoch).max(0) as u64
	}

	/// Accept one input packet. Returns the number of packets dropped for want of
	/// room, so the caller can count them.
	fn push(&mut self, packet: Packet) -> u64 {
		let Some(pcr) = packet.pcr() else {
			// Before the first PCR there is no grid to place against. Buffering
			// these would only let them land in slots chosen by arrival order.
			if self.open.is_none() {
				return 1;
			}
			self.run.push(packet);
			return 0;
		};

		let slot = self.slot_for(pcr);
		let dropped = self.close_run(slot);
		self.open = Some((pcr, slot));
		self.run.clear();
		self.run.push(packet);
		dropped
	}

	/// Spread the open run between its own slot and `next_slot`.
	///
	/// A PCR interval can carry more packets than the mux rate has slots for —
	/// video is not flat, and a groomer is normally provisioned against the
	/// average rate rather than the peak. The excess spills into the slots after
	/// the run instead of being discarded, and later runs start from the first
	/// free slot, so a peak is absorbed by the stuffing that follows it and the
	/// stream catches back up. Placement stays a function of the delivered
	/// packets alone, which is what keeps two legs identical.
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
		dropped
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

	/// Whether any content is in hand, placed or still accumulating.
	fn has_content(&self) -> bool {
		!self.placed.is_empty() || !self.run.is_empty()
	}
}

/// The CBR scheduler. See the module docs.
#[derive(Debug)]
pub struct Scheduler {
	mux_rate_bps: u64,
	latency: Duration,
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
	/// Added to the slot-derived RTP sequence number.
	sequence_seed: u16,
	/// Input-silence grace period before the source counts as stalled.
	stall_timeout: Option<Duration>,
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
		let capacity = latency_to_packets(config.max_latency, bitrate).max(1);
		let pcr_regen = match config.pcr {
			PcrMode::Regenerate => Some(PcrRegen::new(bitrate)),
			PcrMode::Preserve => None,
		};
		Self {
			mux_rate_bps: bitrate,
			latency: config.latency,
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
			anchor: None,
			anchor_slot: 0,
			slot: 0,
			grid: (config.clocking == Clocking::Stream).then(|| StreamGrid::new(bitrate, capacity)),
			sequence_seed: config.sequence_seed,
			stall_timeout: config.stall_timeout,
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
		self.last_content_at = Some(now);
		if self.pcr_pid.is_none() && packet.has_pcr() {
			self.pcr_pid = Some(packet.pid());
		}

		if let Some(grid) = self.grid.as_mut() {
			let dropped = grid.push(packet);
			self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(dropped);
			// The clock can only start once something has been placed: the grid
			// position of the first packet is what the wall clock is anchored to.
			//
			// Datagram boundaries have to come off the grid as well. A leg whose
			// first content lands mid-datagram would otherwise split every
			// datagram thereafter at an offset from its partner's — the same
			// bytes, packed differently, which merges no better than different
			// bytes do. Rounding down to a boundary costs a few slots of leading
			// stuffing and makes the packing a property of the stream.
			if self.anchor.is_none()
				&& let Some(first) = grid.first_slot()
			{
				let per_datagram = self.packets_per_datagram as u64;
				self.anchor = Some(now + self.latency);
				self.anchor_slot = first - (first % per_datagram);
				self.slot = self.anchor_slot;
			}
			return;
		}

		if self.anchor.is_none() {
			let anchor = now + self.latency;
			self.anchor = Some(anchor);
			self.media.anchor = Some(anchor);
		}
		self.media.observe(&packet);
		if self.buffer.push(packet) {
			self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
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
		let anchor = now + self.latency;
		self.media.anchor = Some(anchor);
		self.media.released = 0;
		// A rate sample taken across the gap would read as the outage length, not
		// as a media interval; drop the pending one and re-seed on the next PCR.
		self.media.last_pcr = None;
		self.media.packets_since_pcr = 0;
	}

	/// What the input is currently doing. See [`SourceState`].
	pub fn state(&self, now: Instant) -> SourceState {
		let Some(last) = self.last_content_at else {
			return SourceState::Priming;
		};
		if self.anchor.is_some_and(|anchor| now < anchor) {
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
		if self.stall_timeout.is_some_and(|timeout| silent >= timeout) {
			SourceState::Stalled
		} else if silent >= self.latency {
			SourceState::Starved
		} else {
			SourceState::Live
		}
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
		match self.grid.as_ref() {
			Some(grid) => grid.has_content(),
			None => !self.buffer.is_empty(),
		}
	}

	/// Current jitter-buffer occupancy in packets (the de-jitter cushion depth).
	pub fn buffered_packets(&self) -> usize {
		match self.grid.as_ref() {
			Some(grid) => grid.placed.len() + grid.run.len(),
			None => self.buffer.len(),
		}
	}

	/// A snapshot of the pacing statistics.
	pub fn stats(&self) -> Stats {
		self.stats
	}

	/// Emit one output datagram (`packets_per_datagram` transport packets) at
	/// `now`, advancing the byte clock. Returns a borrow of the internal scratch
	/// buffer, valid until the next call. Never allocates after construction.
	pub fn emit_datagram(&mut self, now: Instant) -> &[u8] {
		self.scratch.clear();
		let due = self.media.due(now);
		// With no content arriving there is no clock to hold: inserting PCR into a
		// programme-free carrier is what makes a dead feed look conformant to
		// everything downstream, so re-insertion stops with the content.
		let stalled = self.observe_state(now).is_stalled();
		for _ in 0..self.packets_per_datagram {
			let index = self.slot;
			let want_content = self.media.released < due;
			if let Some(packet) = self.next_content(index, want_content) {
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
				self.media.released = self.media.released.saturating_add(1);
				self.stats.content_packets = self.stats.content_packets.saturating_add(1);
			} else if let Some(pcr) = self.reinsert_pcr(index, stalled) {
				// A stuffing slot doubles as a re-inserted PCR when the source's
				// own PCR would otherwise fall past the repetition limit.
				let packet = pcr_only_packet(self.pcr_pid.expect("pid set"), self.pcr_pid_cc, pcr);
				self.scratch.extend_from_slice(&packet);
				self.last_pcr_index = Some(index);
				self.stats.pcr_inserted = self.stats.pcr_inserted.saturating_add(1);
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
	/// learned yet, no real PCR seen yet, or the repetition limit not yet reached.
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
	use crate::config::Config;
	use crate::error::Error;

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
