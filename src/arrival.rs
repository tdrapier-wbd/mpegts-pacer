//! Measuring the shape of the input's arrival pattern, so the buffer can be
//! sized from what the transport actually does rather than from an assumption.
//!
//! Every Internet-native transport delivers media in bursts, but the burst
//! granularity differs by two orders of magnitude between data planes: a MoQ
//! egress arrives in ~12 kB bursts whose worst silence is around 150 ms, while a
//! segmented-HTTP egress carrying the same programme arrives in megabyte bursts
//! with silences of several seconds. A groomer configured for the first and fed
//! the second drops programme on every segment and mutes the carrier through
//! every inter-segment gap; one configured for the second holds seconds of
//! needless latency on a feed that never needed it.
//!
//! # What is actually measured
//!
//! The useful quantity is not the burst size but the **lead**: how much media
//! the input has delivered ahead of real time. That is precisely the buffer
//! occupancy the arrival pattern forces, so the cushion has to cover it.
//!
//! It is measured as a token bucket rather than by cutting the stream into
//! bursts, because burst segmentation needs a silence threshold and any such
//! threshold is a guess about the transport. The lead is not:
//!
//! - a continuous feed delivered at the media rate never gets ahead, so its lead
//!   sits near zero however long it runs and whatever hiccups it suffers;
//! - a segment fetched at line rate gets a whole segment ahead, and its lead is
//!   one segment's worth of media, which is the answer we want.
//!
//! The distinction matters more than it sounds. Cutting the stream at a silence
//! threshold means one 300 ms hiccup on an otherwise continuous feed closes a
//! "burst" containing every packet since the run began, inferring a cadence of
//! minutes. The lead cannot make that mistake.
//!
//! Whenever delivery falls level with real time the lead is spent, which ends a
//! *cycle*; the largest lead of each of the last [`WINDOW`] cycles is kept, so a
//! single outage ages out instead of inflating the buffer for the rest of the
//! run.
//!
//! # Burst statistics
//!
//! Separately, and for reporting only, arrivals are grouped into bursts at a
//! [`BURST_SEPARATION`] threshold. This is deliberately not used for sizing: it
//! exists so the pacer reports the same headline figure the lab's external
//! cadence instrument measures, at the same 1 ms grouping, without needing the
//! instrument.

use std::time::{Duration, Instant};

/// Cycles whose peak lead is kept. A one-off outage delivers one abnormal cycle,
/// so a short window lets it age out while still remembering a cadence that
/// genuinely varies from segment to segment.
pub const WINDOW: usize = 8;

/// Silence separating one reported burst from the next.
///
/// Used for the burst statistics only, and set to match the 1 ms grouping the
/// lab's cadence instrument uses so the two are directly comparable.
pub const BURST_SEPARATION: Duration = Duration::from_millis(1);

/// Silence after which the input counts as between deliveries rather than mid-
/// delivery, for the start gate.
///
/// A MoQ egress's worst measured silence is around 150 ms and the shortest
/// inter-segment gap a segmented-HTTP feed produces is most of a second, so the
/// threshold sits between the two with margin at both ends. It decides only
/// *when* the start gate is allowed to open, never how deep the buffer is, so
/// misjudging it costs startup latency rather than correctness.
pub const DELIVERY_GAP: Duration = Duration::from_millis(250);

/// The lead above which the input is treated as bursty rather than continuous.
///
/// Below this the pacer starts as soon as it holds its target cushion, which is
/// what a continuous feed needs and what every release before adaptive sizing
/// did. Above it the pacer waits for a delivery to finish before starting, since
/// the size of a burst is not known until it ends.
pub const BURSTY_LEAD: Duration = Duration::from_millis(250);

/// The arrival pattern of the input, as observed. See the module docs.
#[derive(Clone, Debug)]
pub struct ArrivalProfile {
	/// Wall time delivery was last level with real time: the start of the cycle
	/// whose lead is currently being measured.
	anchor: Option<Instant>,
	/// Content packets delivered since `anchor`.
	packets: u64,
	/// Largest lead reached in the current cycle.
	peak: Duration,
	/// Largest lead of each of the last [`WINDOW`] completed cycles, oldest
	/// overwritten first.
	recent: [Duration; WINDOW],
	next: usize,
	/// Completed cycles, i.e. times delivery came back level with real time.
	cycles: u64,

	/// Wall time of the most recent arrival, for both the silence tests and the
	/// burst grouping.
	last_at: Option<Instant>,
	/// Packets in the burst currently being grouped.
	burst: u64,
	/// Largest completed burst, in packets.
	burst_max: u64,
	/// Completed bursts.
	bursts: u64,
}

impl Default for ArrivalProfile {
	fn default() -> Self {
		Self::new()
	}
}

impl ArrivalProfile {
	/// A profile that has observed nothing.
	pub fn new() -> Self {
		Self {
			anchor: None,
			packets: 0,
			peak: Duration::ZERO,
			recent: [Duration::ZERO; WINDOW],
			next: 0,
			cycles: 0,
			last_at: None,
			burst: 0,
			burst_max: 0,
			bursts: 0,
		}
	}

	/// Note one content packet arriving at `now`, against the media rate the
	/// stream's own PCR implies.
	///
	/// The rate is passed in rather than held because it is recovered from PCR
	/// deltas and refined as the run goes on, and because taking it from the
	/// stream rather than from arrival is what keeps this measurement honest: a
	/// burst cannot inflate the rate it is being compared against.
	pub fn observe(&mut self, now: Instant, media_rate_pps: f64) {
		self.group_burst(now);
		self.last_at = Some(now);

		if media_rate_pps <= 0.0 {
			return;
		}
		let anchor = *self.anchor.get_or_insert(now);
		self.packets = self.packets.saturating_add(1);
		let media = media_duration(self.packets, media_rate_pps);
		let elapsed = now.saturating_duration_since(anchor);
		if media <= elapsed {
			// Delivery is level with real time, so whatever lead this cycle built
			// is spent. Bank it and measure the next cycle from here.
			self.close_cycle();
			self.anchor = Some(now);
			self.packets = 1;
			return;
		}
		self.peak = self.peak.max(media - elapsed);
	}

	/// Bank the current cycle's peak lead and start a fresh one.
	fn close_cycle(&mut self) {
		self.recent[self.next] = self.peak;
		self.next = (self.next + 1) % WINDOW;
		self.cycles = self.cycles.saturating_add(1);
		self.peak = Duration::ZERO;
	}

	/// Fold `now` into the burst statistics, closing the open burst if the
	/// silence before it separates one delivery from the next.
	fn group_burst(&mut self, now: Instant) {
		if let Some(last) = self.last_at
			&& now.saturating_duration_since(last) > BURST_SEPARATION
		{
			self.burst_max = self.burst_max.max(self.burst);
			self.bursts = self.bursts.saturating_add(1);
			self.burst = 0;
		}
		self.burst = self.burst.saturating_add(1);
	}

	/// How much media the input has delivered ahead of real time, at its worst
	/// over the window.
	///
	/// This is the depth the de-jitter cushion has to cover: a feed whose lead
	/// peaks at two seconds has, twice a second apart, handed over two seconds of
	/// programme that the output can only emit at the mux rate.
	pub fn lead(&self) -> Duration {
		self.recent.iter().copied().fold(self.peak, Duration::max)
	}

	/// Whether the input delivers in bursts rather than continuously.
	pub fn bursty(&self) -> bool {
		self.lead() > BURSTY_LEAD
	}

	/// Whether the input has been silent long enough to count as between
	/// deliveries, so a burst it was in the middle of has finished arriving.
	pub fn between_deliveries(&self, now: Instant) -> bool {
		match self.last_at {
			Some(last) => now.saturating_duration_since(last) >= DELIVERY_GAP,
			// Nothing has arrived, so nothing is arriving.
			None => true,
		}
	}

	/// Largest completed burst, in packets, grouped at [`BURST_SEPARATION`].
	pub fn burst_max_packets(&self) -> u64 {
		self.burst_max.max(self.burst)
	}

	/// Bursts completed so far.
	pub fn bursts(&self) -> u64 {
		self.bursts
	}
}

/// How long `packets` of content lasts at `media_rate_pps`.
fn media_duration(packets: u64, media_rate_pps: f64) -> Duration {
	if media_rate_pps <= 0.0 {
		return Duration::ZERO;
	}
	Duration::from_secs_f64(packets as f64 / media_rate_pps)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A 10 Mb/s stream: 10_000_000 / (188 * 8) = 6648.9 packets per second.
	const RATE_PPS: f64 = 10_000_000.0 / (188.0 * 8.0);

	/// Deliver `packets` as fast as the loop can, i.e. at line rate.
	fn burst(profile: &mut ArrivalProfile, at: Instant, packets: u64, over: Duration) {
		for i in 0..packets {
			let offset = over.mul_f64(i as f64 / packets as f64);
			profile.observe(at + offset, RATE_PPS);
		}
	}

	/// Deliver `packets` spread evenly at exactly the media rate.
	fn continuous(profile: &mut ArrivalProfile, at: Instant, packets: u64) -> Instant {
		for i in 0..packets {
			profile.observe(at + media_duration(i, RATE_PPS), RATE_PPS);
		}
		at + media_duration(packets, RATE_PPS)
	}

	#[test]
	fn a_continuous_feed_never_gets_ahead() {
		// The MoQ case, and the property that makes the lead safe to size from:
		// a rate-matched feed's lead stays near zero however long it runs, so
		// adaptive sizing leaves it on the configured floor.
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		continuous(&mut profile, t0, 60 * RATE_PPS as u64);
		assert!(
			profile.lead() < Duration::from_millis(50),
			"a minute of rate-matched delivery led by {:?}",
			profile.lead()
		);
		assert!(!profile.bursty(), "continuous delivery is not bursty");
	}

	#[test]
	fn a_segment_fetched_at_line_rate_leads_by_its_own_duration() {
		// The segmented-HTTP case: a 2 s segment arrives in 300 ms, so the input
		// is ~2 s of programme ahead and the cushion has to cover it.
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		let segment = (2.0 * RATE_PPS) as u64;
		burst(&mut profile, t0, segment, Duration::from_millis(300));

		let lead = profile.lead();
		assert!(
			(Duration::from_millis(1_600)..Duration::from_millis(2_000)).contains(&lead),
			"expected a lead near the segment duration less the fetch time, got {lead:?}"
		);
		assert!(profile.bursty(), "a line-rate segment fetch is bursty");
	}

	#[test]
	fn a_hiccup_on_a_continuous_feed_is_not_a_burst() {
		// The failure mode that rules out cutting the stream at a silence
		// threshold: one 300 ms stall would close a "burst" holding every packet
		// since the run began, and infer a cadence of minutes from it.
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		let resume = continuous(&mut profile, t0, 30 * RATE_PPS as u64) + Duration::from_millis(300);
		continuous(&mut profile, resume, RATE_PPS as u64);
		assert!(
			profile.lead() < Duration::from_millis(50),
			"a hiccup inflated the lead to {:?}",
			profile.lead()
		);
	}

	#[test]
	fn a_one_off_outage_ages_out_of_the_window() {
		// A recovered outage delivers a backlog in one go, which is a real lead
		// and should be remembered — but not for the rest of the run, or one
		// hiccup would hold seconds of latency for ever.
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		let backlog = (4.0 * RATE_PPS) as u64;
		burst(&mut profile, t0, backlog, Duration::from_millis(200));
		let after = t0 + Duration::from_millis(200);
		assert!(profile.lead() > Duration::from_secs(3), "the backlog is a real lead");

		// Enough steady delivery to turn the window over completely.
		let mut at = after + Duration::from_secs(4);
		for _ in 0..=WINDOW {
			at = continuous(&mut profile, at, RATE_PPS as u64) + Duration::from_millis(1);
		}
		assert!(
			profile.lead() < Duration::from_millis(50),
			"the outage is still sizing the buffer at {:?}",
			profile.lead()
		);
	}

	#[test]
	fn burst_statistics_group_at_the_instruments_threshold() {
		// Reported separately from the sizing measurement, and comparable with the
		// lab's external cadence instrument, which groups at 1 ms.
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		for segment in 0..3 {
			let at = t0 + Duration::from_secs(2 * segment);
			burst(&mut profile, at, 12_900, Duration::from_micros(500));
		}
		assert_eq!(profile.bursts(), 2, "three deliveries, two of them closed");
		assert_eq!(profile.burst_max_packets(), 12_900);
	}

	#[test]
	fn silence_marks_the_input_as_between_deliveries() {
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		assert!(
			profile.between_deliveries(t0),
			"nothing is arriving before the first packet"
		);
		profile.observe(t0, RATE_PPS);
		assert!(!profile.between_deliveries(t0 + Duration::from_millis(10)));
		assert!(profile.between_deliveries(t0 + DELIVERY_GAP));
	}

	#[test]
	fn a_zero_media_rate_is_survivable() {
		// Before the first two PCRs there is no rate to compare arrival against.
		// The burst statistics still work; the lead simply has nothing to say.
		let mut profile = ArrivalProfile::new();
		let t0 = Instant::now();
		for i in 0..10 {
			profile.observe(t0 + Duration::from_micros(i * 10), 0.0);
		}
		assert_eq!(profile.lead(), Duration::ZERO);
		assert_eq!(profile.burst_max_packets(), 10);
	}
}
