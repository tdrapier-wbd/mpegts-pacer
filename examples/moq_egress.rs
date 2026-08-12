//! Thin MoQ egress adapter: pace a subscriber's MPEG-TS output to stdout, UDP,
//! or RTP.
//!
//! The MoQ subscriber is just a producer of TS packets, so the whole integration
//! is a pipe:
//!
//! ```text
//! # Pipe the paced stream straight on, like the subscriber itself:
//! moq ... export ts | cargo run -p mpegts-pacer --example moq_egress -- - auto | ffplay -i -
//!
//! # Or push it to a UDP/RTP multicast group:
//! moq ... export ts | cargo run -p mpegts-pacer --example moq_egress -- 239.0.0.1:5000 10000000 --rtp
//! ```
//!
//! Nothing here references MoQ: `export ts` writes transport packets to stdout,
//! [`ReadSource`] reads them, and the pacer shapes them to a broadcast-compliant
//! CBR stream. Swap the first stage for SRT, RIST, or a file and the rest is
//! unchanged.

use std::net::SocketAddr;
use std::time::Duration;

use mpegts_pacer::{
	CallbackObserver, Clocking, Config, Health, PcrMode, ReadSource, RtpSink, SourceState, StallPolicy, Stats, UdpSink,
	WriteSink, pace_with,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut args = std::env::args().skip(1);
	let usage = "usage: moq_egress <-|stdout|dest_ip:port> <bitrate_bps|auto> [--rtp] [--preserve] [--latency-ms N] [--max-latency-ms N] [--ssrc N] [--stall-ms N] [--on-stall mute|continue|fail] [--stream-clock] [--sequence-seed N]";
	let dest = args.next().expect(usage);
	let rate = args.next().expect(usage);

	let mut rtp = false;
	let mut pcr = PcrMode::Regenerate;
	let mut latency = Duration::from_millis(200);
	let mut max_latency = None;
	let mut ssrc = None;
	let mut stall_timeout = None;
	let mut stall_policy = None;
	let mut clocking = Clocking::Arrival;
	let mut sequence_seed = 0;
	let mut rest = args.peekable();
	while let Some(arg) = rest.next() {
		match arg.as_str() {
			"--rtp" => rtp = true,
			"--preserve" => pcr = PcrMode::Preserve,
			"--latency-ms" => {
				let ms: u64 = rest.next().expect("--latency-ms needs a value").parse()?;
				latency = Duration::from_millis(ms);
			}
			"--max-latency-ms" => {
				let ms: u64 = rest.next().expect("--max-latency-ms needs a value").parse()?;
				max_latency = Some(Duration::from_millis(ms));
			}
			"--ssrc" => ssrc = Some(rest.next().expect("--ssrc needs a value").parse::<u32>()?),
			// 0 disables stall detection: hold the carrier however long the source
			// is gone, which is what every pacer did before this was configurable.
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
			// Place packets by stream position rather than by arrival, so two
			// instances fed the same objects emit the same bytes in the same slots
			// and an ST 2022-7 receiver can merge them. Needs an explicit bitrate.
			"--stream-clock" => clocking = Clocking::Stream,
			"--sequence-seed" => {
				sequence_seed = rest.next().expect("--sequence-seed needs a value").parse::<u16>()?;
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
	.with_pcr_mode(pcr)
	.with_clocking(clocking)
	.with_sequence_seed(sequence_seed);
	if let Some(max) = max_latency {
		config = config.with_max_latency(max);
	}
	if let Some(timeout) = stall_timeout {
		config = config.with_stall_timeout(timeout);
	}
	if let Some(policy) = stall_policy {
		config = config.with_stall_policy(policy);
	}

	// A refused config is better than output that looks fine and does not merge.
	config.validate()?;

	let pcr_desc = if pcr == PcrMode::Regenerate {
		"regenerate PCR"
	} else {
		"preserve PCR"
	};
	let clock_desc = match clocking {
		Clocking::Stream => ", stream-clocked",
		Clocking::Arrival => "",
	};

	let stats = if dest == "-" || dest == "stdout" {
		eprintln!(
			"mpegts-pacer: -> stdout (raw ts) @ {rate} b/s, {pcr_desc}{clock_desc}, {} ms latency",
			latency.as_millis()
		);
		pace_with(config, source, WriteSink::new(tokio::io::stdout()), liveness()).await?
	} else {
		let destination: SocketAddr = dest.parse().expect("dest must be ip:port, - or stdout");
		let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
		eprintln!(
			"mpegts-pacer: -> {}{destination} @ {rate} b/s, {pcr_desc}{clock_desc}, {} ms latency",
			if rtp { "rtp://" } else { "udp://" },
			latency.as_millis(),
		);
		if rtp {
			let sink = match ssrc {
				Some(id) => RtpSink::with_ssrc(socket, destination, id),
				None => RtpSink::new(socket, destination),
			};
			pace_with(config, source, sink, liveness()).await?
		} else {
			pace_with(config, source, UdpSink::new(socket, destination), liveness()).await?
		}
	};

	report(&stats);
	Ok(())
}

/// Log liveness transitions to stderr.
///
/// Without this a dead source is invisible from the outside: the egress holds its
/// rate whether or not there is a programme in it, so an operator watching bitrate
/// or packet arrival sees a healthy leg either way.
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
		 pcr_inserted={} stripped_nulls={} dropped={} late_drops={} resyncs={} start_backlog={} underruns={} \
		 stalls={} muted={} max_content_gap={} ms",
		stats.output_packets,
		stats.content_packets,
		stats.null_packets,
		stats.null_ratio() * 100.0,
		stats.pcr_inserted,
		stats.input_nulls_stripped,
		stats.dropped_packets,
		stats.late_drops,
		stats.resyncs,
		stats.start_backlog,
		stats.underruns,
		stats.stalls,
		stats.muted_packets,
		stats.content_gap_max_ms,
	);
}
