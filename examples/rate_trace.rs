//! Replay a captured transport stream through the pacer's rate estimator and
//! print the accumulators.
//!
//! The estimator reads PCR values and counts packets between them. It consults
//! no clock, so its whole trajectory is a function of the input packet sequence
//! and a capture replays it exactly. A defect that needs nine minutes of live
//! lane to appear therefore becomes a file and a few seconds of CPU.
//!
//! Prints one line per window: packets consumed, the two accumulators
//! separately, the ratio they produce, the intervals admitted against the PCRs
//! seen, and — as an independent check on all of it — the rate implied by
//! simply dividing the packets seen so far by the source PCR elapsed so far.
//! That last column is the ground truth the estimator is supposed to track.
//!
//! Usage: `rate_trace <capture.ts> [packets-per-window]`

use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::Instant;

use mpegts_pacer::{Clocking, Config, Scheduler, TS_PACKET_SIZE};

fn main() {
	let mut args = env::args().skip(1);
	let path = args.next().expect("usage: rate_trace <capture.ts> [window]");
	let window: u64 = args.next().map(|w| w.parse().expect("window")).unwrap_or(50_000);

	let config = Config::new(11_000_000)
		.with_latency(std::time::Duration::from_millis(1000))
		.with_clocking(Clocking::Arrival);
	let mut sched = Scheduler::new(&config);

	let mut reader = BufReader::with_capacity(1 << 20, File::open(&path).expect("open"));
	let mut buf = [0u8; TS_PACKET_SIZE];
	// A synthetic clock. The estimator ignores it, and pinning it keeps the
	// release path from consuming the buffer and perturbing what we are reading.
	let t0 = Instant::now();

	let mut packets: u64 = 0;
	// Ground truth, accumulated independently of the estimator: total packets
	// against total source PCR elapsed, both taken from the same byte stream.
	let mut first_pcr: Option<u64> = None;
	let mut last_pcr: u64 = 0;
	let mut pcr_wraps: u64 = 0;

	println!(
		"packets\tdecayed_packets\tdecayed_secs\traw_pps\trelease_pps\tstalled\tintervals\tpcrs_seen\ttruth_pps\terr_x"
	);

	while let Ok(()) = reader.read_exact(&mut buf) {
		if buf[0] != 0x47 {
			// Resynchronise rather than abandon: a capture truncated mid-packet
			// would otherwise end the trace early and look like a short file.
			continue;
		}
		if let Some(pcr) = read_pcr(&buf) {
			if let Some(f) = first_pcr {
				if pcr < last_pcr && last_pcr - pcr > (1 << 32) * 300 {
					pcr_wraps += 1;
				}
				let _ = f;
			} else {
				first_pcr = Some(pcr);
			}
			last_pcr = pcr;
		}

		let Ok(packet) = mpegts_pacer::Packet::new(bytes::Bytes::copy_from_slice(&buf)) else {
			continue;
		};
		sched.enqueue(packet, t0);
		packets += 1;

		if packets % window == 0 {
			let s = sched.stats();
			let est = if s.rate_decayed_secs > 0.0 {
				s.rate_decayed_packets / s.rate_decayed_secs
			} else {
				0.0
			};
			let elapsed = first_pcr.map(|f| {
				let raw = last_pcr as f64 - f as f64;
				(raw + pcr_wraps as f64 * ((1u64 << 33) as f64 * 300.0)) / 27_000_000.0
			});
			let truth = elapsed.filter(|e| *e > 0.0).map(|e| packets as f64 / e).unwrap_or(0.0);
			// `raw_pps` is the accumulators' ratio; `release_pps` is what the
			// pacer will actually release at. They differ exactly when the
			// source's timebase has stopped being credible, which is the case
			// this trace exists to show.
			let release = s.media_rate_bps as f64 / 1504.0;
			println!(
				"{}\t{:.1}\t{:.6}\t{:.1}\t{:.1}\t{}\t{}\t{}\t{:.1}\t{:.3}",
				packets,
				s.rate_decayed_packets,
				s.rate_decayed_secs,
				est,
				release,
				s.rate_clock_stalled,
				s.rate_intervals,
				s.rate_pcrs_seen,
				truth,
				if truth > 0.0 { release / truth } else { 0.0 }
			);
		}
	}
	eprintln!("replayed {packets} packets from {path}");
}

/// PCR from a packet's adaptation field, in 27 MHz units, if present.
fn read_pcr(p: &[u8; TS_PACKET_SIZE]) -> Option<u64> {
	let afc = (p[3] >> 4) & 0x3;
	if afc != 2 && afc != 3 {
		return None;
	}
	let len = p[4] as usize;
	if len < 7 || 5 + len > TS_PACKET_SIZE {
		return None;
	}
	if p[5] & 0x10 == 0 {
		return None;
	}
	let base = ((p[6] as u64) << 25)
		| ((p[7] as u64) << 17)
		| ((p[8] as u64) << 9)
		| ((p[9] as u64) << 1)
		| ((p[10] as u64) >> 7);
	let ext = (((p[10] as u64) & 0x01) << 8) | p[11] as u64;
	Some(base * 300 + ext)
}
