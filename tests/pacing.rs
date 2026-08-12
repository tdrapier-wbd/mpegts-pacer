//! End-to-end pacing tests over the async engine.
//!
//! These drive the real [`mpegts_pacer::pace`] loop under `tokio`'s paused clock, so
//! they are deterministic (virtual time auto-advances to each scheduled emit)
//! yet exercise the full source -> scheduler -> sink path.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mpegts_pacer::{
	CallbackObserver, Config, Error, Health, IterSource, Packet, PcrMode, Result, Sink, SourceState, StallPolicy,
	TS_PACKET_SIZE, pace, pace_with,
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
