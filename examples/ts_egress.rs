//! Thin egress adapter: pace a transport stream on stdin to stdout, UDP, or RTP.
//!
//! The receiver is just a producer of TS packets, so the whole integration is a
//! pipe — and the same pipe whichever data plane carried the bytes:
//!
//! ```text
//! # MoQ: objects reassembled to a transport stream, then groomed.
//! moq ... export ts | cargo run -p mpegts-pacer --example ts_egress -- - auto | ffplay -i -
//!
//! # Segmented HTTP: segments fetched and concatenated, then groomed.
//! tsp -I hls https://origin/live.m3u8 -O - \
//!   | cargo run -p mpegts-pacer --example ts_egress -- 239.0.0.1:5000 10000000 --rtp
//! ```
//!
//! Nothing here references either transport, and nothing needs to: both write
//! transport packets to stdout, [`ReadSource`] reads them, and the pacer shapes
//! them to a broadcast-compliant CBR stream. The two arrive very differently —
//! MoQ in kilobyte bursts milliseconds apart, segmented HTTP in megabyte bursts
//! seconds apart — and by default the pacer sizes its buffer from whichever it is
//! given rather than from a flag. Swap the first stage for SRT or RIST and the
//! rest is unchanged.
//!
//! Pass `--latency-ms` (or `--segment-ms`) to pin the depths instead, which is
//! what a plant that wants the run to be deterministic rather than self-tuning
//! should do, and what stream clocking requires.

use std::net::SocketAddr;
use std::time::Duration;

use mpegts_pacer::{
	CallbackObserver, Clocking, Config, DEFAULT_LATENCY, DEFAULT_LATENCY_FACTOR, Health, Latency, PcrMode, ReadSource,
	RtpSink, SourceState, StallPolicy, Stats, TS_PACKET_SIZE, UdpSink, WriteSink, pace_with,
};

const USAGE: &str = "usage: ts_egress <-|stdout|dest_ip:port> <bitrate_bps|auto> [--rtp] [--preserve] \
                     [--segment-ms N] [--latency-ms N] [--max-latency-ms N] [--latency-ceiling-ms N] \
                     [--ssrc N] [--stall-ms N] [--stall-grace-ms N] [--on-stall mute|continue|fail] \
                     [--stream-clock] [--sequence-seed N]";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut args = std::env::args().skip(1);
	let dest = args.next().expect(USAGE);
	let rate = args.next().expect(USAGE);

	let mut rtp = false;
	let mut pcr = PcrMode::Regenerate;
	let mut segment = None;
	let mut latency = None;
	let mut max_latency = None;
	let mut ceiling = None;
	let mut ssrc = None;
	let mut stall_timeout = None;
	let mut stall_grace = None;
	let mut stall_policy = None;
	let mut clocking = Clocking::Arrival;
	let mut sequence_seed = 0;
	let mut rest = args.peekable();
	while let Some(arg) = rest.next() {
		match arg.as_str() {
			"--rtp" => rtp = true,
			"--preserve" => pcr = PcrMode::Preserve,
			// The escape hatch for a segmented input whose segment duration is
			// known: it pins the depths adaptive sizing would converge on, without
			// spending two deliveries working them out.
			"--segment-ms" => segment = Some(millis(&mut rest, "--segment-ms")?),
			"--latency-ms" => latency = Some(millis(&mut rest, "--latency-ms")?),
			"--max-latency-ms" => max_latency = Some(millis(&mut rest, "--max-latency-ms")?),
			"--latency-ceiling-ms" => ceiling = Some(millis(&mut rest, "--latency-ceiling-ms")?),
			"--ssrc" => ssrc = Some(rest.next().expect("--ssrc needs a value").parse::<u32>()?),
			// 0 disables stall detection: hold the carrier however long the source
			// is gone, which is what every pacer did before this was configurable.
			"--stall-ms" => {
				let ms = millis(&mut rest, "--stall-ms")?;
				stall_timeout = Some((!ms.is_zero()).then_some(ms));
			}
			"--stall-grace-ms" => stall_grace = Some(millis(&mut rest, "--stall-grace-ms")?),
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
			// and an ST 2022-7 receiver can merge them. Needs an explicit bitrate
			// and an explicit latency.
			"--stream-clock" => clocking = Clocking::Stream,
			"--sequence-seed" => {
				sequence_seed = rest.next().expect("--sequence-seed needs a value").parse::<u16>()?;
			}
			other => panic!("unknown argument {other:?}; {USAGE}"),
		}
	}

	let source = ReadSource::new(tokio::io::stdin());
	let mut config = if rate == "auto" {
		Config::auto()
	} else {
		Config::new(rate.parse().expect("bitrate must be an integer or \"auto\""))
	}
	.with_pcr_mode(pcr)
	.with_clocking(clocking)
	.with_sequence_seed(sequence_seed);

	// Order matters: the segment duration sets both depths, so an explicit
	// latency or cap after it refines what it chose rather than being overwritten.
	if let Some(segment) = segment {
		config = config.with_segment_duration(segment);
	}
	if let Some(ceiling) = ceiling {
		config = config.with_adaptive_latency(DEFAULT_LATENCY, ceiling, DEFAULT_LATENCY_FACTOR);
	}
	if let Some(latency) = latency {
		config = config.with_latency(latency);
	}
	if let Some(max) = max_latency {
		config = config.with_max_latency(max);
	}
	if let Some(timeout) = stall_timeout {
		config = config.with_stall_timeout(timeout);
	}
	if let Some(grace) = stall_grace {
		config = config.with_stall_grace(grace);
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
	let depth = match config.latency {
		Latency::Fixed { target, max } => {
			format!("{} ms latency (cap {} ms)", target.as_millis(), max.as_millis())
		}
		Latency::Adaptive { floor, ceiling, .. } => format!(
			"latency sized from arrival, {}-{} ms",
			floor.as_millis(),
			ceiling.as_millis()
		),
	};

	let stats = if dest == "-" || dest == "stdout" {
		eprintln!("mpegts-pacer: -> stdout (raw ts) @ {rate} b/s, {pcr_desc}{clock_desc}, {depth}");
		pace_with(config, source, WriteSink::new(tokio::io::stdout()), liveness()).await?
	} else {
		let destination: SocketAddr = dest.parse().expect("dest must be ip:port, - or stdout");
		let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
		eprintln!(
			"mpegts-pacer: -> {}{destination} @ {rate} b/s, {pcr_desc}{clock_desc}, {depth}",
			if rtp { "rtp://" } else { "udp://" },
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

/// Parse the next argument as a count of milliseconds.
fn millis(
	args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
	flag: &str,
) -> Result<Duration, std::num::ParseIntError> {
	let raw = args.next().unwrap_or_else(|| panic!("{flag} needs a value"));
	Ok(Duration::from_millis(raw.parse()?))
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
			// The cushion it settled on is worth saying out loud the moment it
			// starts: on a segmented input it is the difference between a groomer
			// that works and one that mutes every two seconds.
			SourceState::Live => eprintln!(
				"mpegts-pacer: source Live (arrival lead {} ms, holding {} ms)",
				health.stats.arrival_lead_ms, health.stats.latency_target_ms,
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
	// The arrival shape the run actually saw, in the same terms an external
	// cadence instrument reports it, so a groomed leg can be compared with the
	// ungroomed measurement of the same path without a second tool.
	eprintln!(
		"mpegts-pacer: arrival. bursts={} max_burst={} packets ({:.2} MB) lead={} ms \
		 cushion={} ms buffer_high_water={} packets",
		stats.bursts,
		stats.burst_max_packets,
		(stats.burst_max_packets * TS_PACKET_SIZE as u64) as f64 / 1_000_000.0,
		stats.arrival_lead_ms,
		stats.latency_target_ms,
		stats.buffer_high_water,
	);
}
