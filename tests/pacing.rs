//! End-to-end pacing tests over the async engine.
//!
//! These drive the real [`mpegts_pacer::pace`] loop under `tokio`'s paused clock, so
//! they are deterministic (virtual time auto-advances to each scheduled emit)
//! yet exercise the full source -> scheduler -> sink path.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mpegts_pacer::{
	CallbackObserver, Clocking, Config, Error, Framing, Health, IterSource, Packet, PcrMode, Result, Sink, SourceState,
	StallPolicy, TS_PACKET_SIZE, pace, pace_with,
};

const MUX_RATE: u64 = 10_000_000;
const MEDIA_PID: u16 = 0x100;
// 188 * 8 * 27_000_000 / 10_000_000 = 4060.8; not integer, so PCR steps are
// checked as byte-proportional rather than a fixed constant.

/// A `Sink` that appends every datagram to a shared buffer.
struct CollectSink(Arc<Mutex<Vec<u8>>>);

impl Sink for CollectSink {
	async fn send(&mut self, datagram: &[u8]) -> mpegts_pacer::Result<()> {
		self.0.lock().unwrap().extend_from_slice(datagram);
		Ok(())
	}
}

/// Datagrams recorded with the (virtual) instant each was sent, so a test can ask
/// *when* the carrier stopped rather than only what it carried.
type Timeline = Arc<Mutex<Vec<(Duration, Vec<u8>)>>>;

/// A `Sink` that timestamps every datagram against the test's start instant.
struct StampedSink {
	start: tokio::time::Instant,
	sent: Timeline,
}

impl StampedSink {
	fn new(sent: Timeline) -> Self {
		Self {
			start: tokio::time::Instant::now(),
			sent,
		}
	}
}

impl Sink for StampedSink {
	async fn send(&mut self, datagram: &[u8]) -> Result<()> {
		let at = self.start.elapsed();
		self.sent.lock().unwrap().push((at, datagram.to_vec()));
		Ok(())
	}
}

/// Datagrams recorded with the RTP sequence number each went out under, which is
/// what a receiver merges a redundant pair on.
type Numbered = Arc<Mutex<Vec<(u16, Vec<u8>)>>>;

/// A `Sink` that records what it sent and under what number.
///
/// Where the pacer offers framing, that is the number; otherwise the sink counts
/// its own sends, as `RtpSink` does.
struct FramedSink {
	sent: Numbered,
	framing: Option<Framing>,
	count: u16,
}

impl FramedSink {
	fn new(sent: Numbered) -> Self {
		Self {
			sent,
			framing: None,
			count: 0,
		}
	}
}

impl Sink for FramedSink {
	fn set_framing(&mut self, framing: Framing) {
		self.framing = Some(framing);
	}

	async fn send(&mut self, datagram: &[u8]) -> Result<()> {
		let sequence = match self.framing.take() {
			Some(framing) => framing.sequence,
			None => self.count,
		};
		self.count = self.count.wrapping_add(1);
		self.sent.lock().unwrap().push((sequence, datagram.to_vec()));
		Ok(())
	}
}

/// A source that hands over its packets in chunks, pausing between them: a path
/// that delivers the same objects as its partner, but not at the same times.
///
/// The chunk is delivered without awaiting, so on a paused clock it lands at one
/// instant — a fetch at line rate. Two shapes matter, and they differ by two
/// orders of magnitude in both dimensions: kilobyte chunks milliseconds apart from
/// an object transport, and megabyte chunks seconds apart from a segment-fetching
/// one, whose gap occasionally doubles when the client misses a publish cycle.
struct BurstySource {
	packets: std::vec::IntoIter<Packet>,
	chunk: usize,
	gap: Duration,
	/// `(every, times)`: every `every`th cycle waits `times` as long and then
	/// delivers `times` as much, which is what a client that missed a publish
	/// cycle does — it collects the segments it is owed. The two halves are the
	/// same event, and modelling only the long gap misses that the burst which
	/// ends it is the one that overflows the buffer.
	stretch: Option<(usize, u32)>,
	bursts: usize,
	/// Packets the burst being waited for will carry.
	pending: usize,
	remaining: usize,
	next_burst: Option<tokio::time::Instant>,
}

impl BurstySource {
	fn new(packets: Vec<Packet>, chunk: usize, gap: Duration) -> Self {
		Self {
			packets: packets.into_iter(),
			chunk,
			gap,
			stretch: None,
			bursts: 0,
			pending: chunk,
			remaining: chunk,
			next_burst: None,
		}
	}

	/// The same, with every `every`th cycle `times` as long and as large.
	fn uneven(packets: Vec<Packet>, chunk: usize, gap: Duration, every: usize, times: u32) -> Self {
		Self {
			stretch: Some((every, times)),
			..Self::new(packets, chunk, gap)
		}
	}
}

impl mpegts_pacer::Source for BurstySource {
	async fn recv(&mut self) -> Result<Option<Packet>> {
		if self.remaining == 0 {
			// The pacer drops and remakes this future on every output slot, so the
			// cycle is decided once and memoised rather than re-rolled per poll.
			let deadline = match self.next_burst {
				Some(deadline) => deadline,
				None => {
					self.bursts += 1;
					let times = match self.stretch {
						Some((every, times)) if self.bursts % every == 0 => times,
						_ => 1,
					};
					self.pending = self.chunk * times as usize;
					let deadline = tokio::time::Instant::now() + self.gap * times;
					self.next_burst = Some(deadline);
					deadline
				}
			};
			tokio::time::sleep_until(deadline).await;
			self.next_burst = None;
			self.remaining = self.pending;
		}
		self.remaining -= 1;
		Ok(self.packets.next())
	}
}

/// A source that delivers a run of packets and then stops delivering *without*
/// ending: a publisher killed behind a pipe that stays open, which is the case a
/// pacer otherwise stuffs straight through. Optionally it comes back.
struct StallingSource {
	packets: std::vec::IntoIter<Packet>,
	/// `None` means silent for good.
	silence: Option<Duration>,
	resume_at: Option<tokio::time::Instant>,
	resumed: std::vec::IntoIter<Packet>,
}

impl StallingSource {
	/// Deliver `packets`, then go quiet forever.
	fn silent_after(packets: Vec<Packet>) -> Self {
		Self {
			packets: packets.into_iter(),
			silence: None,
			resume_at: None,
			resumed: Vec::new().into_iter(),
		}
	}

	/// Deliver `packets`, go quiet for `silence`, then deliver `resumed`.
	fn resuming(packets: Vec<Packet>, silence: Duration, resumed: Vec<Packet>) -> Self {
		Self {
			packets: packets.into_iter(),
			silence: Some(silence),
			resume_at: None,
			resumed: resumed.into_iter(),
		}
	}
}

impl mpegts_pacer::Source for StallingSource {
	async fn recv(&mut self) -> Result<Option<Packet>> {
		if let Some(packet) = self.packets.next() {
			return Ok(Some(packet));
		}
		let Some(silence) = self.silence else {
			return std::future::pending().await;
		};
		// An absolute deadline: the pacer drops and remakes this future on every
		// output slot, so a fresh relative sleep would never elapse.
		let deadline = *self
			.resume_at
			.get_or_insert_with(|| tokio::time::Instant::now() + silence);
		tokio::time::sleep_until(deadline).await;
		match self.resumed.next() {
			Some(packet) => Ok(Some(packet)),
			None => std::future::pending().await,
		}
	}
}

/// Whether a packet carries a PCR, counting the adaptation-only packets the pacer
/// inserts itself (`read_pcr` above only reads content packets).
fn carries_pcr(packet: &[u8]) -> bool {
	matches!(packet[3] >> 4 & 0x03, 0b10 | 0b11) && packet[4] >= 7 && packet[5] & 0x10 != 0
}

/// An observer that records every health snapshot it is given.
fn recording_observer(log: Arc<Mutex<Vec<Health>>>) -> CallbackObserver<impl FnMut(Health) + Send> {
	CallbackObserver::new(move |health: Health| log.lock().unwrap().push(health))
}

/// A content packet on `MEDIA_PID`, optionally carrying a PCR.
fn packet(pcr_ticks: Option<u64>, cc: u8) -> Packet {
	let mut p = [0x00_u8; TS_PACKET_SIZE];
	p[0] = 0x47;
	p[1] = (MEDIA_PID >> 8) as u8 & 0x1f;
	p[2] = MEDIA_PID as u8;
	match pcr_ticks {
		Some(ticks) => {
			p[3] = 0x30 | (cc & 0x0f); // adaptation + payload
			p[4] = 7;
			p[5] = 0x10;
			write_pcr(&mut p[6..12], ticks);
		}
		None => p[3] = 0x10 | (cc & 0x0f),
	}
	Packet::from_slice(&p).unwrap()
}

fn write_pcr(target: &mut [u8], pcr_ticks: u64) {
	let base = (pcr_ticks / 300) & ((1_u64 << 33) - 1);
	let ext = pcr_ticks % 300;
	target[0] = (base >> 25) as u8;
	target[1] = (base >> 17) as u8;
	target[2] = (base >> 9) as u8;
	target[3] = (base >> 1) as u8;
	target[4] = ((base & 0x01) as u8) << 7 | 0x7e | ((ext >> 8) as u8 & 0x01);
	target[5] = ext as u8;
}

fn read_pcr(packet: &[u8]) -> Option<u64> {
	if packet[3] >> 4 & 0x03 != 0b11 || packet[4] < 7 || packet[5] & 0x10 == 0 {
		return None;
	}
	let pcr = &packet[6..12];
	let base = (u64::from(pcr[0]) << 25)
		| (u64::from(pcr[1]) << 17)
		| (u64::from(pcr[2]) << 9)
		| (u64::from(pcr[3]) << 1)
		| (u64::from(pcr[4]) >> 7);
	let ext = (u64::from(pcr[4] & 0x01) << 8) | u64::from(pcr[5]);
	Some(base * 300 + ext)
}

fn pid(packet: &[u8]) -> u16 {
	(u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2])
}

/// Build a ~5 Mb/s media stream of `count` packets, a PCR every `pcr_every`.
fn media_stream(count: usize, media_rate_bps: u64, pcr_every: usize) -> Vec<Packet> {
	let ticks_per_packet = (TS_PACKET_SIZE as u64 * 8 * 27_000_000) / media_rate_bps;
	(0..count)
		.map(|i| {
			let cc = (i % 16) as u8;
			if i % pcr_every == 0 {
				packet(Some(i as u64 * ticks_per_packet), cc)
			} else {
				packet(None, cc)
			}
		})
		.collect()
}

#[tokio::test(start_paused = true)]
async fn produces_constant_bitrate_output_with_null_stuffing() {
	let input = media_stream(700, 5_000_000, 7);
	let content_in = input.len() as u64;
	let out = Arc::new(Mutex::new(Vec::new()));

	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_pcr_mode(PcrMode::Regenerate);
	let stats = pace(config, IterSource::new(input), CollectSink(out.clone()))
		.await
		.unwrap();

	// Every content packet is released (none dropped): duration is preserved.
	assert_eq!(stats.content_packets, content_in, "all content forwarded");
	assert_eq!(stats.dropped_packets, 0, "no drops below max_latency");
	// Mux rate (10 Mb/s) is double the media rate (5 Mb/s), so ~half is null.
	assert!(stats.null_packets > 0, "below-rate input must be null-stuffed");
	assert!(
		stats.output_packets >= content_in,
		"output ({}) carries content plus stuffing",
		stats.output_packets
	);

	let bytes = out.lock().unwrap().clone();
	assert_eq!(bytes.len() % TS_PACKET_SIZE, 0, "whole packets only");

	// Structural: every emitted packet is a valid 188-byte packet.
	let mut content = 0u64;
	let mut nulls = 0u64;
	let mut pcrs: Vec<(u64, u64)> = Vec::new();
	for (index, chunk) in bytes.chunks_exact(TS_PACKET_SIZE).enumerate() {
		assert_eq!(chunk[0], 0x47, "sync byte at packet {index}");
		if pid(chunk) == mpegts_pacer::NULL_PID {
			nulls += 1;
		} else {
			content += 1;
			if let Some(p) = read_pcr(chunk) {
				pcrs.push((index as u64, p));
			}
		}
	}
	assert_eq!(content, content_in, "content count matches input");
	assert_eq!(nulls, stats.null_packets);

	// Regenerated PCR is byte-locked: the PCR delta equals the output byte
	// distance clocked at the mux rate (within one tick of integer rounding).
	assert!(pcrs.len() >= 3, "expected several PCRs, got {}", pcrs.len());
	for pair in pcrs.windows(2) {
		let (i0, p0) = pair[0];
		let (i1, p1) = pair[1];
		let expected = ((i1 - i0) as u128 * TS_PACKET_SIZE as u128 * 8 * 27_000_000 / MUX_RATE as u128) as u64;
		let actual = p1 - p0;
		let diff = actual.abs_diff(expected);
		assert!(diff <= 2, "PCR not byte-locked: got {actual}, expected ~{expected}");
	}
}

#[tokio::test(start_paused = true)]
async fn preserve_mode_keeps_source_pcr_values() {
	let input = media_stream(200, 5_000_000, 7);
	let expected_first_pcr = input.iter().find_map(|p| p.pcr());
	let out = Arc::new(Mutex::new(Vec::new()));

	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(20))
		.with_pcr_mode(PcrMode::Preserve);
	pace(config, IterSource::new(input), CollectSink(out.clone()))
		.await
		.unwrap();

	let bytes = out.lock().unwrap().clone();
	let first_pcr = bytes.chunks_exact(TS_PACKET_SIZE).find_map(read_pcr);
	assert_eq!(
		first_pcr, expected_first_pcr,
		"preserve keeps source PCR values verbatim"
	);
}

#[tokio::test(start_paused = true)]
async fn auto_rate_matches_source_content_rate() {
	// A steady 5 Mb/s media stream. Auto should lock the output near the content
	// rate plus the default 15% headroom (~5.75 Mb/s), so only a little stuffing.
	let input = media_stream(700, 5_000_000, 7);
	let content_in = input.len() as u64;
	let out = Arc::new(Mutex::new(Vec::new()));

	let config = Config::auto().with_latency(Duration::from_millis(50));
	let stats = pace(config, IterSource::new(input), CollectSink(out.clone()))
		.await
		.unwrap();

	assert_eq!(stats.content_packets, content_in, "all content forwarded");
	assert_eq!(stats.dropped_packets, 0, "self-tuned rate must not overflow");
	// Near the content rate: far less stuffing than the 2x-overprovisioned
	// explicit-rate test, and nowhere near the 4 Mb/s no-PCR fallback.
	let ratio = stats.null_ratio();
	assert!(
		(0.02..0.30).contains(&ratio),
		"auto rate should track content + headroom, got {:.1}% stuffing",
		ratio * 100.0
	);
}

#[tokio::test(start_paused = true)]
async fn stalled_source_mutes_the_carrier() {
	// 700 packets arrive, then the source goes quiet without ever ending. Left to
	// itself the pacer would hold a byte-perfect carrier with no programme in it
	// for as long as it ran, which is indistinguishable downstream from health.
	let sent: Timeline = Arc::new(Mutex::new(Vec::new()));
	let log = Arc::new(Mutex::new(Vec::new()));
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_stall_timeout(Some(Duration::from_millis(500)));

	// Muting keeps the task alive, so the run is bounded rather than awaited.
	let run = tokio::time::timeout(
		Duration::from_secs(5),
		pace_with(
			config,
			StallingSource::silent_after(media_stream(700, 5_000_000, 7)),
			StampedSink::new(sent.clone()),
			recording_observer(log.clone()),
		),
	);
	assert!(run.await.is_err(), "a muted pacer stays alive instead of returning");

	let sent = sent.lock().unwrap().clone();
	let last = sent.last().expect("the live period produced output").0;
	assert!(
		(Duration::from_millis(450)..Duration::from_millis(600)).contains(&last),
		"carrier must stop one stall timeout after the source went quiet, not at {last:?}"
	);
	for (at, datagram) in &sent {
		for packet in datagram.chunks_exact(TS_PACKET_SIZE) {
			assert!(
				!carries_pcr(packet) || *at < Duration::from_millis(600),
				"no PCR may be minted after the source is declared gone"
			);
		}
	}

	let log = log.lock().unwrap().clone();
	let final_health = log.last().expect("health is reported");
	assert_eq!(final_health.source, SourceState::Stalled);
	assert_eq!(final_health.stats.stalls, 1);
	assert!(final_health.stats.muted_packets > 0, "the muted gap is counted");
	assert!(
		final_health.stats.content_gap_max_ms >= 4_000,
		"the gap is measured as it grows"
	);
	assert!(
		log.iter().any(|health| health.source == SourceState::Live),
		"the live period is reported too"
	);
}

#[tokio::test(start_paused = true)]
async fn the_first_stall_report_carries_the_stall_it_announces() {
	// The buffer can run dry *inside* an output slot, so the state read after
	// emitting differs from the one the slot acted on. Reporting that later
	// reading announced a stall against counters taken before it: an operator
	// watching the alarm read "stall #0, 0 ms" for a source gone for seconds.
	let sent: Timeline = Arc::new(Mutex::new(Vec::new()));
	let log = Arc::new(Mutex::new(Vec::new()));
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_stall_timeout(Some(Duration::from_millis(500)));

	let _ = tokio::time::timeout(
		Duration::from_secs(3),
		pace_with(
			config,
			StallingSource::silent_after(media_stream(700, 5_000_000, 7)),
			StampedSink::new(sent.clone()),
			recording_observer(log.clone()),
		),
	)
	.await;

	let log = log.lock().unwrap().clone();
	let first = log
		.iter()
		.find(|health| health.source == SourceState::Stalled)
		.expect("the stall is reported");
	assert_eq!(first.stats.stalls, 1, "the report names the stall it is announcing");
	assert!(
		first.stats.content_gap_max_ms >= 500,
		"and the silence that caused it, not {} ms",
		first.stats.content_gap_max_ms
	);
}

#[tokio::test(start_paused = true)]
async fn stall_detection_can_be_disabled() {
	// The inverse of the test above, and the escape hatch for a plant that wants
	// the pre-0.2 behaviour: no timeout, so silence is never a fault.
	let sent: Timeline = Arc::new(Mutex::new(Vec::new()));
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_stall_timeout(None);

	let run = tokio::time::timeout(
		Duration::from_secs(3),
		pace(
			config,
			StallingSource::silent_after(media_stream(700, 5_000_000, 7)),
			StampedSink::new(sent.clone()),
		),
	);
	assert!(run.await.is_err());

	let sent = sent.lock().unwrap().clone();
	let last = sent.last().expect("output was produced").0;
	assert!(
		last > Duration::from_millis(2_500),
		"no timeout means the carrier never stops"
	);
	assert!(
		sent.iter()
			.filter(|(at, _)| *at > Duration::from_millis(600))
			.flat_map(|(_, datagram)| datagram.chunks_exact(TS_PACKET_SIZE))
			.any(carries_pcr),
		"with detection off the pacer keeps holding the repetition limit"
	);
}

#[tokio::test(start_paused = true)]
async fn stalled_source_fails_under_the_fail_policy() {
	let out = Arc::new(Mutex::new(Vec::new()));
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_stall_timeout(Some(Duration::from_millis(500)))
		.with_stall_policy(StallPolicy::Fail);

	let error = pace(
		config,
		StallingSource::silent_after(media_stream(700, 5_000_000, 7)),
		CollectSink(out.clone()),
	)
	.await
	.expect_err("a stalled source must surface to the supervisor");

	match error {
		Error::SourceStalled { silent_for } => {
			assert!(
				silent_for >= Duration::from_millis(500),
				"reported silence {silent_for:?}"
			);
		}
		other => panic!("expected SourceStalled, got {other:?}"),
	}
}

#[tokio::test(start_paused = true)]
async fn carrier_resumes_when_content_returns() {
	// One continuous stream split across a 2 s outage, so the returning packets
	// carry a PCR timeline continuous with the first half.
	let stream = media_stream(1400, 5_000_000, 7);
	let (first, second) = stream.split_at(700);
	let sent: Timeline = Arc::new(Mutex::new(Vec::new()));
	let log = Arc::new(Mutex::new(Vec::new()));
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_stall_timeout(Some(Duration::from_millis(500)));

	let run = tokio::time::timeout(
		Duration::from_secs(6),
		pace_with(
			config,
			StallingSource::resuming(first.to_vec(), Duration::from_secs(2), second.to_vec()),
			StampedSink::new(sent.clone()),
			recording_observer(log.clone()),
		),
	);
	assert!(run.await.is_err());

	let sent = sent.lock().unwrap().clone();
	// The carrier has exactly one hole in it, and it is the outage.
	let gaps: Vec<Duration> = sent
		.windows(2)
		.map(|pair| pair[1].0 - pair[0].0)
		.filter(|gap| *gap > Duration::from_millis(10))
		.collect();
	assert_eq!(gaps.len(), 1, "expected a single carrier gap, got {gaps:?}");
	assert!(
		(Duration::from_millis(1_400)..Duration::from_millis(1_600)).contains(&gaps[0]),
		"the gap is the outage less the stall grace already spent: {:?}",
		gaps[0]
	);

	let content: usize = sent
		.iter()
		.flat_map(|(_, datagram)| datagram.chunks_exact(TS_PACKET_SIZE))
		.filter(|packet| pid(packet) == MEDIA_PID && !carries_pcr(packet))
		.count();
	assert!(
		content > 700,
		"content resumed after the outage, only {content} packets"
	);

	let states: Vec<SourceState> = log.lock().unwrap().iter().map(|health| health.source).collect();
	let stalled = states.iter().position(|state| *state == SourceState::Stalled);
	let recovered = states.iter().rposition(|state| *state == SourceState::Live);
	assert!(
		matches!((stalled, recovered), (Some(a), Some(b)) if b > a),
		"health must report the stall and then the recovery: {states:?}"
	);
}

#[tokio::test(start_paused = true)]
async fn continue_policy_holds_the_carrier_but_stops_claiming_a_clock() {
	// Some plants need the carrier up regardless. It still must not mint the PCR
	// that makes a programme-free stream look conformant.
	let sent: Timeline = Arc::new(Mutex::new(Vec::new()));
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(50))
		.with_stall_timeout(Some(Duration::from_millis(500)))
		.with_stall_policy(StallPolicy::Continue);

	let run = tokio::time::timeout(
		Duration::from_secs(3),
		pace(
			config,
			StallingSource::silent_after(media_stream(700, 5_000_000, 7)),
			StampedSink::new(sent.clone()),
		),
	);
	assert!(run.await.is_err());

	let sent = sent.lock().unwrap().clone();
	let last = sent.last().expect("output was produced").0;
	assert!(
		last > Duration::from_millis(2_500),
		"the carrier stays up under Continue"
	);
	for (at, datagram) in &sent {
		if *at < Duration::from_millis(600) {
			continue;
		}
		for packet in datagram.chunks_exact(TS_PACKET_SIZE) {
			assert_eq!(pid(packet), mpegts_pacer::NULL_PID, "only stuffing remains");
			assert!(!carries_pcr(packet), "a programme-free carrier claims no clock");
		}
	}
}

/// Pace `input` through the real engine down a path that delivers it in bursts,
/// and return each datagram with the RTP sequence number it went out under.
///
/// The bursts average out to the media rate — 200 packets of 5 Mb/s content is
/// 60 ms of programme. A path that sustained more than that would not be a path;
/// it would be a leg permanently behind the stream, which is a different test.
async fn leg(config: Config, input: Vec<Packet>) -> Vec<(u16, Vec<u8>)> {
	let sent: Numbered = Arc::new(Mutex::new(Vec::new()));
	pace_with(
		config,
		BurstySource::new(input, 200, Duration::from_millis(60)),
		FramedSink::new(sent.clone()),
		CallbackObserver::new(|_| {}),
	)
	.await
	.unwrap();
	let sent = sent.lock().unwrap();
	sent.clone()
}

#[tokio::test(start_paused = true)]
async fn a_stream_clocked_leg_joins_the_transport_its_partner_is_sending() {
	// The end-to-end form of what Arm D is for, through the real engine rather
	// than the scheduler alone. One leg has been running from the start; the
	// other picks the stream up part-way, as a leg restored after maintenance
	// does. Under stream clocking the newcomer emits the partner's bytes under
	// the partner's numbering without the two having been co-started, or having
	// exchanged anything at all.
	//
	// Emit-time jitter is the other half of the property, and cannot be shown
	// here: tokio's paused clock fires every timer exactly on time. The scheduler
	// tests model it directly, with an arrival-clocked control that diverges.
	let input = media_stream(1_200, 5_000_000, 20);
	let config = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(200))
		.with_clocking(Clocking::Stream);
	assert!(config.validate().is_ok());

	let running = leg(config, input.clone()).await;
	// Cut on a PCR, since that is where a run — and so a subscription — begins.
	let joined = leg(config, input[400..].to_vec()).await;

	assert!(running.len() > 200, "the leg emitted almost nothing");
	assert!(joined.len() > 100, "the late leg emitted almost nothing");
	assert!(
		joined.len() < running.len(),
		"the late leg cannot have emitted the whole stream"
	);

	let running: std::collections::HashMap<_, _> = running.into_iter().collect();
	// The datagram the leg joined inside is partial: it is short the content its
	// partner already had when it arrived. Everything after it must match.
	for (sequence, datagram) in joined.into_iter().skip(1) {
		match running.get(&sequence) {
			Some(theirs) => assert_eq!(&datagram, theirs, "sequence {sequence} differs between the legs"),
			None => panic!("the late leg sent sequence {sequence}, which its partner never used"),
		}
	}
}

// --- grooming either data plane ------------------------------------------------

/// The media rate of the two shapes below, and the packets one second of it takes.
///
/// Set 15 % under the mux rate, which is what a groomed feed looks like (and the
/// headroom `Config::auto` picks). The margin matters: the buffer bound is a
/// duration at the *mux* rate, so a feed running at half the mux rate gets twice
/// as much buffer as the figure suggests, and a comfortable ratio would hide the
/// overflow a real segmented egress hits.
const MEDIA_RATE: u64 = 8_500_000;
const MEDIA_PPS: usize = (MEDIA_RATE / (TS_PACKET_SIZE as u64 * 8)) as usize;
/// One output datagram, and so the tail of nulls a stream whose length is not a
/// multiple of it ends on.
const DATAGRAM: u64 = 7;

/// How long `packets` of media lasts, and so the silence a rate-matched delivery
/// of that many is followed by.
///
/// Both shapes below are built from this, because a delivery that is not
/// rate-matched is not a burst pattern — it is a feed running at the wrong rate,
/// which starves or overflows whatever the buffer does.
fn media_time(packets: usize) -> Duration {
	Duration::from_secs_f64(packets as f64 / MEDIA_PPS as f64)
}

/// A segmented-HTTP egress: two-second segments fetched at line rate, with every
/// fourth cycle collecting two at once after twice the wait. This is the shape a
/// lab capture of `tsp -I hls` has — mostly 2 s gaps, occasional 4 s ones, and a
/// median burst larger than one segment because of them.
fn segmented(segments: usize) -> BurstySource {
	let per_segment = 2 * MEDIA_PPS;
	BurstySource::uneven(
		media_stream(segments * per_segment, MEDIA_RATE, 20),
		per_segment,
		media_time(per_segment),
		4,
		2,
	)
}

/// An object-transport egress: ~12 kB bursts a few milliseconds apart, with an
/// occasional silence of ~140 ms, which is the worst a lab capture of a MoQ egress
/// showed. Two orders of magnitude off the shape above in both dimensions.
fn objects(seconds: usize) -> BurstySource {
	let per_burst = 66;
	let bursts = seconds * MEDIA_PPS / per_burst;
	BurstySource::uneven(
		media_stream(bursts * per_burst, MEDIA_RATE, 20),
		per_burst,
		media_time(per_burst),
		12,
		12,
	)
}

/// Pace a source to a discarding sink and return the stats.
async fn groom(config: Config, source: BurstySource) -> mpegts_pacer::Stats {
	let out = Arc::new(Mutex::new(Vec::new()));
	tokio::time::timeout(Duration::from_secs(600), pace(config, source, CollectSink(out)))
		.await
		.expect("the run must finish rather than mute its own tail")
		.unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_segmented_source_is_groomed_without_dropping_or_muting() {
	// The point of the whole exercise. Fed a feed that arrives a segment at a time,
	// with no flag saying so, the pacer must not drop programme out of it, underrun
	// between segments, or read an ordinary inter-segment gap as a dead source.
	let stats = groom(Config::new(MUX_RATE), segmented(10)).await;

	assert_eq!(stats.dropped_packets, 0, "dropped programme from a healthy feed");
	assert_eq!(stats.stalls, 0, "read an inter-segment gap as a dead source");
	assert_eq!(stats.muted_packets, 0, "and muted the carrier for it");
	assert_eq!(
		stats.content_packets, 10 * 2 * MEDIA_PPS as u64,
		"every content packet must reach the wire"
	);
	// The last datagram is padded out, since the stream does not end on a datagram
	// boundary. Anything past that would be a real starve between segments.
	assert!(
		stats.underruns < DATAGRAM,
		"starved between segments: {} underruns",
		stats.underruns
	);

	// And it sized itself from the arrival pattern rather than from a default.
	assert!(
		(1_500..4_500).contains(&stats.arrival_lead_ms),
		"expected a lead of one or two segments, got {} ms",
		stats.arrival_lead_ms
	);
	assert!(
		stats.latency_target_ms >= 4_000,
		"a {} ms cushion cannot ride out the 4 s gap this feed has",
		stats.latency_target_ms
	);
	assert!(
		stats.burst_max_packets >= 2 * MEDIA_PPS as u64,
		"the reported burst {} is smaller than the segment delivered",
		stats.burst_max_packets
	);
}

#[tokio::test(start_paused = true)]
async fn the_old_defaults_show_why_that_needed_fixing() {
	// The control arm: the same feed through the depths a groomer tuned for an
	// object transport used. All three failures are visible at once, which is why
	// they are not one mistuned timeout.
	let pinned = Config::new(MUX_RATE)
		.with_latency(Duration::from_millis(200))
		.with_max_latency(Duration::from_millis(2_000))
		.with_stall_timeout(Some(Duration::from_secs(1)));
	let stats = groom(pinned, segmented(10)).await;

	assert!(
		stats.dropped_packets > 0,
		"a 2 s bound cannot hold a 2 s segment plus a cushion"
	);
	assert!(stats.stalls > 0, "a 1 s timeout fires on every inter-segment gap");
	assert!(stats.muted_packets > 0, "and mutes the carrier for most of every period");
}

#[tokio::test(start_paused = true)]
async fn an_object_source_pays_nothing_for_it() {
	// The regression guard. Adaptive sizing is measured against the arrival pattern
	// rather than switched on by transport, so the object plane has to come out
	// where it always did: a cushion in the hundreds of milliseconds, not the
	// seconds a segmented feed needs.
	let stats = groom(Config::new(MUX_RATE), objects(20)).await;

	assert!(
		stats.latency_target_ms < 500,
		"an object feed's cushion inflated to {} ms (lead {} ms)",
		stats.latency_target_ms,
		stats.arrival_lead_ms
	);
	assert_eq!(stats.dropped_packets, 0, "no drops");
	assert_eq!(stats.stalls, 0, "no stalls");
	assert_eq!(stats.muted_packets, 0, "no muting");
	assert!(stats.underruns < DATAGRAM, "no starve past the padded final datagram");
	// The largest burst is the catch-up delivery after the ~140 ms silence, not the
	// typical one — and it is still a small fraction of a segment, which is the
	// whole reason the two planes need different depths.
	assert!(
		stats.burst_max_packets < MEDIA_PPS as u64 / 2,
		"expected bursts far short of a segment, got {} packets",
		stats.burst_max_packets
	);
}

#[tokio::test(start_paused = true)]
async fn a_stream_clocked_pair_still_agrees_on_a_segmented_feed() {
	// Stream clocking requires pinned depths precisely because an adaptive cushion
	// is measured from one leg's own arrival window. Pinned from the segment
	// duration, the pair property survives the burst shape: what a leg emits is a
	// function of the stream, so two of them agree without sharing a process.
	let input = media_stream(6 * 2 * MEDIA_PPS, MEDIA_RATE, 20);
	let config = Config::new(MUX_RATE)
		.with_segment_duration(Duration::from_secs(2))
		.with_clocking(Clocking::Stream);
	config.validate().expect("pinned depths are what stream clocking needs");

	let one = segmented_leg(config, input.clone()).await;
	let two = segmented_leg(config, input).await;
	assert!(one.len() > 100, "the leg emitted almost nothing");
	assert_eq!(one, two, "two legs fed the same stream must emit the same datagrams");
}

/// One stream-clocked leg of a pair, fed a segmented arrival pattern.
async fn segmented_leg(config: Config, input: Vec<Packet>) -> Vec<(u16, Vec<u8>)> {
	let sent: Numbered = Arc::new(Mutex::new(Vec::new()));
	let source = BurstySource::uneven(input, 2 * MEDIA_PPS, media_time(2 * MEDIA_PPS), 4, 2);
	tokio::time::timeout(
		Duration::from_secs(600),
		pace_with(config, source, FramedSink::new(sent.clone()), CallbackObserver::new(|_| {})),
	)
	.await
	.expect("the run must finish")
	.unwrap();
	let sent = sent.lock().unwrap();
	sent.clone()
}

#[tokio::test(start_paused = true)]
async fn an_adaptive_cushion_is_refused_where_two_legs_must_agree() {
	let config = Config::new(MUX_RATE).with_clocking(Clocking::Stream);
	assert!(
		config.validate().is_err(),
		"two legs sizing their own cushions would hold different depths"
	);
}

#[tokio::test(start_paused = true)]
async fn a_cushion_deeper_than_the_buffer_is_refused() {
	// A priming target the buffer cannot physically hold: the pacer would drop the
	// input to make room for itself, for ever, and report nothing unusual.
	let config = Config::new(MUX_RATE)
		.with_max_latency(Duration::from_millis(500))
		.with_latency(Duration::from_secs(2));
	assert!(config.validate().is_err(), "latency past max_latency must be rejected");
}

#[tokio::test(start_paused = true)]
async fn a_source_that_ends_before_it_primes_is_still_paced_out() {
	// An adaptive start holds output back until it holds a cushion. A short input
	// never gets there, and discarding it for being short would be worse than any
	// burst it failed to absorb.
	let input = media_stream(700, MEDIA_RATE, 7);
	let expected = input.len() as u64;
	let stats = groom(Config::new(MUX_RATE), BurstySource::new(input, 700, Duration::from_secs(1))).await;
	assert_eq!(stats.content_packets, expected, "the whole short input reaches the wire");
}

#[tokio::test(start_paused = true)]
async fn empty_source_produces_no_output() {
	let out = Arc::new(Mutex::new(Vec::new()));
	let stats = pace(
		Config::new(MUX_RATE),
		IterSource::new(Vec::<Packet>::new()),
		CollectSink(out.clone()),
	)
	.await
	.unwrap();
	assert_eq!(stats.output_packets, 0);
	assert!(out.lock().unwrap().is_empty());
}
