//! ST 2022-7 sender pattern: groom once, emit the identical RTP datagram on two
//! paths.
//!
//! A hitless ST 2022-7 pair is reconstructed by matching RTP sequence numbers, so
//! the two legs must be packet-identical and sequence-aligned. Two independently
//! grooming pacers cannot guarantee that — their content/null interleave is gated
//! on the wall-clock instant each datagram is emitted. The standard sender pattern
//! sidesteps the problem: groom once, duplicate the bytes.
//!
//! ```text
//! moq ... export ts | dual_rtp 127.0.0.1:5100 127.0.0.1:5200 auto --ssrc 1 --seq 0
//! ```
//!
//! This protects the paths to the receiver, not the chain upstream of the groomer:
//! one process holds both legs, so anything that stops it stops the pair. To
//! protect the chain as well, run one pacer per leg under
//! [`Clocking::Stream`](mpegts_pacer::Clocking) — `mpegts-pacer --rtp
//! --stream-clock` — where each leg's bytes and numbering are a function of the
//! stream rather than of the process, so the two agree without sharing one.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mpegts_pacer::{
	CallbackObserver, Config, Health, PcrMode, RTP_PAYLOAD_TYPE_MP2T, ReadSource, Result, Sink, SourceState,
	StallPolicy, Stats, pace_with,
};
use tokio::net::UdpSocket;

const RTP_HEADER_SIZE: usize = 12;
const RTP_VERSION: u8 = 2;

/// Sends one RTP-encapsulated datagram to two destinations, byte for byte.
///
/// Sequence number, timestamp and SSRC are computed once per datagram and shared
/// by both legs, so the pair is aligned by construction. A receiver merging on
/// sequence number sees a single stream.
struct DualRtpSink {
	socket: UdpSocket,
	destinations: [SocketAddr; 2],
	sequence: u16,
	ssrc: u32,
	started_at: Instant,
	scratch: Vec<u8>,
}

impl DualRtpSink {
	fn new(socket: UdpSocket, destinations: [SocketAddr; 2], ssrc: u32, sequence: u16) -> Self {
		Self {
			socket,
			destinations,
			sequence,
			ssrc,
			started_at: Instant::now(),
			scratch: Vec::with_capacity(RTP_HEADER_SIZE + 1316),
		}
	}
}

impl Sink for DualRtpSink {
	async fn send(&mut self, datagram: &[u8]) -> Result<()> {
		let sequence = self.sequence;
		self.sequence = self.sequence.wrapping_add(1);
		let elapsed = self.started_at.elapsed();
		let timestamp = (elapsed.as_secs_f64() * 90_000.0) as u64 as u32;

		self.scratch.clear();
		self.scratch.extend_from_slice(&[0; RTP_HEADER_SIZE]);
		let header = &mut self.scratch[..RTP_HEADER_SIZE];
		header[0] = RTP_VERSION << 6;
		header[1] = RTP_PAYLOAD_TYPE_MP2T & 0x7f;
		header[2..4].copy_from_slice(&sequence.to_be_bytes());
		header[4..8].copy_from_slice(&timestamp.to_be_bytes());
		header[8..12].copy_from_slice(&self.ssrc.to_be_bytes());
		self.scratch.extend_from_slice(datagram);

		for destination in self.destinations {
			self.socket.send_to(&self.scratch, destination).await?;
		}
		Ok(())
	}
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
	let usage = "usage: dual_rtp <destA_ip:port> <destB_ip:port> <bitrate_bps|auto> \
	             [--ssrc N] [--seq N] [--preserve] [--latency-ms N] [--max-latency-ms N] \
	             [--stall-ms N] [--on-stall mute|continue|fail]";
	let mut args = std::env::args().skip(1);
	let dest_a: SocketAddr = args.next().expect(usage).parse()?;
	let dest_b: SocketAddr = args.next().expect(usage).parse()?;
	let rate = args.next().expect(usage);

	let mut ssrc: u32 = 0x2022_0007;
	let mut sequence: u16 = 0;
	let mut pcr = PcrMode::Regenerate;
	let mut latency = Duration::from_millis(200);
	let mut max_latency = None;
	let mut stall_timeout = None;
	let mut stall_policy = None;
	let mut rest = args.peekable();
	while let Some(arg) = rest.next() {
		match arg.as_str() {
			"--ssrc" => ssrc = rest.next().expect("--ssrc needs a value").parse()?,
			"--seq" => sequence = rest.next().expect("--seq needs a value").parse()?,
			"--preserve" => pcr = PcrMode::Preserve,
			"--latency-ms" => {
				let ms: u64 = rest.next().expect("--latency-ms needs a value").parse()?;
				latency = Duration::from_millis(ms);
			}
			"--max-latency-ms" => {
				let ms: u64 = rest.next().expect("--max-latency-ms needs a value").parse()?;
				max_latency = Some(Duration::from_millis(ms));
			}
			// 0 disables stall detection, holding the carrier however long the
			// source stays gone.
			"--stall-ms" => {
				let ms: u64 = rest.next().expect("--stall-ms needs a value").parse()?;
				stall_timeout = Some((ms > 0).then(|| Duration::from_millis(ms)));
			}
			"--on-stall" => {
				stall_policy = Some(match rest.next().expect("--on-stall needs a value").as_str() {
					"mute" => StallPolicy::Mute,
					"continue" => StallPolicy::Continue,
					"fail" => StallPolicy::Fail,
					other => panic!("unknown stall policy {other:?}; expected mute, continue or fail"),
				});
			}
			other => panic!("unknown argument {other:?}; {usage}"),
		}
	}

	let source = ReadSource::new(tokio::io::stdin());
	let mut config = if rate == "auto" {
		Config::auto()
	} else {
		Config::new(rate.parse().expect("bitrate must be an integer or \"auto\""))
	}
	.with_latency(latency)
	.with_pcr_mode(pcr);
	if let Some(max) = max_latency {
		config = config.with_max_latency(max);
	}
	if let Some(timeout) = stall_timeout {
		config = config.with_stall_timeout(timeout);
	}
	if let Some(policy) = stall_policy {
		config = config.with_stall_policy(policy);
	}

	let socket = UdpSocket::bind("0.0.0.0:0").await?;
	eprintln!(
		"mpegts-pacer: groom once -> rtp://{dest_a} + rtp://{dest_b} @ {rate} b/s, \
		 ssrc={ssrc:#010x}, seq0={sequence}, {} ms latency",
		latency.as_millis()
	);

	let sink = DualRtpSink::new(socket, [dest_a, dest_b], ssrc, sequence);
	let stats = pace_with(config, source, sink, liveness()).await?;
	report(&stats);
	Ok(())
}

/// Log liveness transitions to stderr.
///
/// Both legs are fed by one groomer here, so they stall together: the pair goes
/// quiet at the same instant rather than one leg holding a programme-free carrier
/// its partner has no way to distinguish from a live one.
fn liveness() -> CallbackObserver<impl FnMut(Health) + Send> {
	let mut last: Option<SourceState> = None;
	CallbackObserver::new(move |health: Health| {
		if last.replace(health.source) == Some(health.source) {
			return;
		}
		match health.source {
			SourceState::Stalled => eprintln!(
				"mpegts-pacer: SOURCE STALLED (stall #{}, {} ms without content)",
				health.stats.stalls, health.stats.content_gap_max_ms,
			),
			state => eprintln!("mpegts-pacer: source {state:?}"),
		}
	})
}

fn report(stats: &Stats) {
	eprintln!(
		"mpegts-pacer: done. output_packets={} content={} null={} ({:.1}% stuffing) \
		 pcr_inserted={} stripped_nulls={} dropped={} underruns={} \
		 stalls={} muted={} max_content_gap={} ms",
		stats.output_packets,
		stats.content_packets,
		stats.null_packets,
		stats.null_ratio() * 100.0,
		stats.pcr_inserted,
		stats.input_nulls_stripped,
		stats.dropped_packets,
		stats.underruns,
		stats.stalls,
		stats.muted_packets,
		stats.content_gap_max_ms,
	);
}
