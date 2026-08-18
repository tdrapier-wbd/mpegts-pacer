//! Replay a `.ts` file into the **live** pacer in segment-sized bursts, and write
//! the paced result to a file.
//!
//! ```text
//! cargo run --release -p mpegts-pacer --example burst_replay -- in.ts out.ts auto --segment-ms 2000
//! tsp -I file out.ts -P pcrverify --jitter-max 500 -O drop
//! ```
//!
//! The companion to [`cbr_file`](../cbr_file.rs), and the reason it is not enough.
//! `cbr_file` drives the [`Scheduler`](mpegts_pacer::Scheduler) directly on a
//! synthetic clock derived from the source PCR, which is what makes it
//! deterministic — and also means it has no arrival timing at all. Nothing it does
//! can exercise buffer sizing, the start gate, or stall detection, because those
//! are all functions of *when* packets turn up.
//!
//! So this replays the same file through the real [`pace`] loop on the wall clock,
//! delivering it the way a segment-fetching client does: a segment's worth of media
//! handed over at line rate, then silence until the next one is due, with every
//! `--double-every`th cycle waiting twice as long and then collecting two segments,
//! as a client that missed a publish cycle does. A run takes as long as the clip
//! lasts, since that is the point.
//!
//! Burst sizes are derived from the source's own PCR-implied media rate, so the
//! delivery is rate-matched however the file was encoded. A delivery that is not
//! rate-matched is not a burst pattern; it is a feed running at the wrong rate.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mpegts_pacer::{
	CallbackSink, Config, DEFAULT_AUTO_HEADROOM, Packet, Result, Source, TS_PACKET_SIZE, estimate_content_bitrate,
	pace,
};

const USAGE: &str = "usage: burst_replay <in.ts> <out.ts> <bitrate_bps|auto> [--segment-ms N] \
                     [--double-every N] [--segment-hint] [--latency-ms N] [--max-latency-ms N] [--stall-ms N]";

/// A file replayed as a segment-fetching client would deliver it.
struct SegmentedSource {
	packets: std::vec::IntoIter<Packet>,
	/// Packets one segment of media takes, at the source's own rate.
	per_segment: usize,
	segment: Duration,
	/// Every `double_every`th cycle waits twice as long and delivers twice as much.
	double_every: usize,
	cycles: usize,
	pending: usize,
	remaining: usize,
	next_burst: Option<tokio::time::Instant>,
}

impl SegmentedSource {
	fn new(packets: Vec<Packet>, per_segment: usize, segment: Duration, double_every: usize) -> Self {
		Self {
			packets: packets.into_iter(),
			per_segment,
			segment,
			double_every,
			cycles: 0,
			pending: per_segment,
			remaining: per_segment,
			next_burst: None,
		}
	}
}

impl Source for SegmentedSource {
	async fn recv(&mut self) -> Result<Option<Packet>> {
		if self.remaining == 0 {
			// The pacer drops and remakes this future on every output slot, so the
			// cycle is decided once and memoised rather than re-rolled per poll.
			let deadline = match self.next_burst {
				Some(deadline) => deadline,
				None => {
					self.cycles += 1;
					let times = if self.double_every > 0 && self.cycles % self.double_every == 0 {
						2
					} else {
						1
					};
					self.pending = self.per_segment * times;
					let deadline = tokio::time::Instant::now() + self.segment * times as u32;
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

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
	let mut args = std::env::args().skip(1);
	let input = args.next().expect(USAGE);
	let output = args.next().expect(USAGE);
	let rate = args.next().expect(USAGE);

	let mut segment = Duration::from_secs(2);
	let mut double_every = 4;
	let mut hint = false;
	let mut latency = None;
	let mut max_latency = None;
	let mut stall = None;
	let mut rest = args;
	while let Some(arg) = rest.next() {
		match arg.as_str() {
			"--segment-ms" => segment = Duration::from_millis(value(&mut rest, "--segment-ms")?),
			"--double-every" => double_every = value(&mut rest, "--double-every")? as usize,
			// Tell the pacer the segment duration instead of letting it work the
			// depths out, which is the control arm for whether it needs telling.
			"--segment-hint" => hint = true,
			"--latency-ms" => latency = Some(Duration::from_millis(value(&mut rest, "--latency-ms")?)),
			"--max-latency-ms" => max_latency = Some(Duration::from_millis(value(&mut rest, "--max-latency-ms")?)),
			"--stall-ms" => stall = Some(value(&mut rest, "--stall-ms")?),
			other => panic!("unknown argument {other:?}; {USAGE}"),
		}
	}

	let bytes = std::fs::read(&input)?;
	let packets: Vec<Packet> = bytes
		.chunks_exact(TS_PACKET_SIZE)
		.filter_map(|chunk| Packet::from_slice(chunk).ok())
		.collect();
	let content = estimate_content_bitrate(&packets).expect("need at least two PCRs to size the bursts");
	let bitrate = if rate == "auto" {
		((content as f64) * (1.0 + DEFAULT_AUTO_HEADROOM)) as u64
	} else {
		rate.parse().expect("bitrate must be an integer or \"auto\"")
	};

	// Rate-matched by construction: a segment carries exactly the media the
	// publisher produced during the silence before it.
	let media_pps = content as f64 / (TS_PACKET_SIZE as f64 * 8.0);
	let per_segment = (segment.as_secs_f64() * media_pps).round().max(1.0) as usize;

	let mut config = Config::new(bitrate);
	if hint {
		config = config.with_segment_duration(segment);
	}
	if let Some(latency) = latency {
		config = config.with_latency(latency);
	}
	if let Some(max) = max_latency {
		config = config.with_max_latency(max);
	}
	if let Some(ms) = stall {
		config = config.with_stall_timeout((ms > 0).then(|| Duration::from_millis(ms)));
	}
	config.validate()?;

	eprintln!(
		"burst_replay: {input} -> {output} | content={content} b/s mux={bitrate} b/s \
		 segment={} ms ({per_segment} packets, {:.2} MB) doubled every {double_every} cycles",
		segment.as_millis(),
		(per_segment * TS_PACKET_SIZE) as f64 / 1_000_000.0,
	);

	let out = Arc::new(Mutex::new(Vec::with_capacity(bytes.len())));
	let collected = out.clone();
	let sink = CallbackSink::new(move |datagram: &[u8]| {
		collected.lock().unwrap().extend_from_slice(datagram);
		Ok(())
	});
	let source = SegmentedSource::new(packets, per_segment, segment, double_every);
	let stats = pace(config, source, sink).await?;

	std::fs::write(&output, &*out.lock().unwrap())?;

	eprintln!(
		"burst_replay: done. in_packets={} out_packets={} content={} null={} ({:.1}% stuffing) \
		 dropped={} underruns={} stalls={} muted={} rebases={} max_content_gap={} ms",
		bytes.len() / TS_PACKET_SIZE,
		stats.output_packets,
		stats.content_packets,
		stats.null_packets,
		stats.null_ratio() * 100.0,
		stats.dropped_packets,
		stats.underruns,
		stats.stalls,
		stats.muted_packets,
		stats.pcr_rebases,
		stats.content_gap_max_ms,
	);
	eprintln!(
		"burst_replay: arrival. bursts={} max_burst={} packets ({:.2} MB) lead={} ms \
		 cushion={} ms buffer_high_water={} packets",
		stats.bursts,
		stats.burst_max_packets,
		(stats.burst_max_packets * TS_PACKET_SIZE as u64) as f64 / 1_000_000.0,
		stats.arrival_lead_ms,
		stats.latency_target_ms,
		stats.buffer_high_water,
	);

	// The three failures a groomer sized for an object transport shows on a
	// segmented one. Fail the run rather than leaving them to the analyzer, which
	// only sees the output and cannot tell absorbed burst from deleted programme.
	let mut failed = Vec::new();
	if stats.dropped_packets > 0 {
		failed.push(format!("{} packets dropped on arrival", stats.dropped_packets));
	}
	if stats.stalls > 0 {
		failed.push(format!("{} stalls on inter-segment gaps", stats.stalls));
	}
	if stats.content_packets != (bytes.len() / TS_PACKET_SIZE) as u64 - stats.input_nulls_stripped {
		failed.push(format!(
			"{} of {} content packets reached the wire",
			stats.content_packets,
			bytes.len() / TS_PACKET_SIZE
		));
	}
	if !failed.is_empty() {
		eprintln!("burst_replay: FAILED -- {}", failed.join("; "));
		std::process::exit(1);
	}
	Ok(())
}

/// Parse the next argument as an integer.
fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> std::result::Result<u64, std::num::ParseIntError> {
	args.next().unwrap_or_else(|| panic!("{flag} needs a value")).parse()
}
