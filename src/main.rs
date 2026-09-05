//! Pace a transport stream on stdin to stdout, UDP, or RTP.
//!
//! The producer is just a source of TS packets, so the whole integration is a
//! pipe — and the same pipe whichever data plane carried the bytes:
//!
//! ```text
//! # MoQ: objects reassembled to a transport stream, then groomed.
//! moq ... export ts | mpegts-pacer - auto | ffplay -i -
//!
//! # Segmented HTTP: segments fetched and concatenated, then groomed.
//! tsp -I hls https://origin/live.m3u8 --live -O file - | mpegts-pacer 239.0.0.1:5000 10000000 --rtp
//! ```
//!
//! Nothing here references either transport, and nothing needs to: both write
//! transport packets to stdout, `ReadSource` reads them, and the pacer shapes them
//! to a broadcast-compliant CBR stream. The two arrive very differently — MoQ in
//! kilobyte bursts milliseconds apart, segmented HTTP in megabyte bursts seconds
//! apart — and by default the pacer sizes its buffer from whichever it is given
//! rather than from a flag. Swap the first stage for SRT or RIST and the rest is
//! unchanged.
//!
//! Pass `--latency-ms` (or `--segment-ms`) to pin the depths instead, which is what
//! a plant that wants the run to be deterministic rather than self-tuning should do,
//! and what stream clocking requires.
//!
//! This is a thin front end over the library: an [`Invocation`] is a `Config`, a
//! destination and a sink shape, and everything after parsing is one `pace_with`
//! call. The two-destination ST 2022-7 pattern, the file-to-file case and the
//! burst-replay harness stay in `examples/`, because each shows a way of driving
//! the library that a single-destination CLI cannot express.

use std::fmt::Display;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use mpegts_pacer::{
	CallbackObserver, Clocking, Config, DEFAULT_LATENCY, DEFAULT_LATENCY_FACTOR, Health, Latency, PcrMode,
	PcrPositionPolicy, ReadSource, RtpSink, SourceState, StallPolicy, Stats, TS_PACKET_SIZE, UdpSink, WriteSink,
	pace_with,
};

const USAGE: &str = "\
usage: mpegts-pacer <-|stdout|dest_ip:port> <bitrate_bps|auto> [options]

Reads a transport stream on stdin and writes it out at a constant bitrate.

  <dest>                   `-` or `stdout` for raw TS on stdout, else ip:port
  <bitrate>                output mux rate in bits per second, or `auto` to
                           measure the content rate and add 15% headroom

  --rtp                    wrap datagrams in RTP (needs a UDP destination)
  --ssrc N                 RTP SSRC, as a decimal integer
  --preserve               pass source PCR through instead of byte-locking it

  --segment-ms N           pin the depths for a known segment duration
  --latency-ms N           pin the priming cushion, disabling adaptive sizing
  --max-latency-ms N       pin the hard cap on buffered media
  --latency-ceiling-ms N   raise the ceiling on an adaptively sized cushion

  --stall-ms N             silence after which the source counts as gone;
                           0 disables stall detection entirely
  --stall-grace-ms N       grace on top of an adaptively sized cushion
  --on-stall POLICY        mute, continue or fail

  --stream-clock           place packets by stream position, for an ST 2022-7 pair
  --sequence-seed N        first RTP sequence number, for a stream-clocked pair
  --require-pcr-position   fail if the source's PCR byte positions do not track
                           its PCR values by more than the buffer can absorb

  --stats-interval-ms N    emit a machine-readable counter line to stderr every
                           N ms, for a run long enough that the closing report
                           is not enough

  -h, --help               print this
  -V, --version            print the version";

#[tokio::main]
async fn main() -> ExitCode {
	// A mistyped flag exits 2 with one line rather than unwinding: this is a
	// program now, not an example, so a usage error should not read as a bug in
	// the pacer.
	match run().await {
		Ok(()) => ExitCode::SUCCESS,
		Err(Failure::Usage(message)) => {
			eprintln!("mpegts-pacer: {message}\n\n{USAGE}");
			ExitCode::from(2)
		}
		Err(Failure::Runtime(message)) => {
			eprintln!("mpegts-pacer: {message}");
			ExitCode::FAILURE
		}
	}
}

enum Failure {
	Usage(String),
	Runtime(String),
}

fn usage(message: impl Display) -> Failure {
	Failure::Usage(message.to_string())
}

fn failed(message: impl Display) -> Failure {
	Failure::Runtime(message.to_string())
}

/// Everything the command line says, parsed once.
struct Invocation {
	config: Config,
	/// `-`, `stdout`, or an `ip:port` to send to.
	dest: String,
	/// As typed, so the announcement can echo `auto` rather than a resolved rate.
	rate: String,
	rtp: bool,
	ssrc: Option<u32>,
	/// How often to emit a counter line, if at all.
	stats_interval: Option<Duration>,
}

async fn run() -> Result<(), Failure> {
	let args: Vec<String> = std::env::args().skip(1).collect();
	if args.iter().any(|arg| arg == "-h" || arg == "--help") {
		println!("{USAGE}");
		return Ok(());
	}
	if args.iter().any(|arg| arg == "-V" || arg == "--version") {
		println!("mpegts-pacer {}", env!("CARGO_PKG_VERSION"));
		return Ok(());
	}

	let invocation = parse(&args)?;
	// A refused config is better than output that looks fine and does not merge.
	invocation.config.validate().map_err(failed)?;
	announce(&invocation);

	let stats = egress(invocation).await?;
	report(&stats);
	Ok(())
}

fn parse(args: &[String]) -> Result<Invocation, Failure> {
	let dest = args.first().ok_or_else(|| usage("missing destination"))?.clone();
	let rate = args.get(1).ok_or_else(|| usage("missing bitrate"))?.clone();

	let mut rtp = false;
	let mut ssrc = None;
	let mut pcr = PcrMode::Regenerate;
	let mut segment = None;
	let mut latency = None;
	let mut max_latency = None;
	let mut ceiling = None;
	let mut stall_timeout = None;
	let mut stall_grace = None;
	let mut stall_policy = None;
	let mut clocking = Clocking::Arrival;
	let mut sequence_seed = 0;
	let mut pcr_position_policy = PcrPositionPolicy::default();
	let mut stats_interval = None;

	let mut at = 2;
	while let Some(arg) = args.get(at) {
		at += 1;
		match arg.as_str() {
			"--rtp" => rtp = true,
			"--preserve" => pcr = PcrMode::Preserve,
			"--ssrc" => ssrc = Some(number(args, &mut at, "--ssrc")?),
			// The escape hatch for a segmented input whose segment duration is
			// known: it pins the depths adaptive sizing would converge on, without
			// spending two deliveries working them out.
			"--segment-ms" => segment = Some(millis(args, &mut at, "--segment-ms")?),
			"--latency-ms" => latency = Some(millis(args, &mut at, "--latency-ms")?),
			"--max-latency-ms" => max_latency = Some(millis(args, &mut at, "--max-latency-ms")?),
			"--latency-ceiling-ms" => ceiling = Some(millis(args, &mut at, "--latency-ceiling-ms")?),
			// 0 disables stall detection: hold the carrier however long the source
			// is gone, which is what every pacer did before this was configurable.
			"--stall-ms" => {
				let ms = millis(args, &mut at, "--stall-ms")?;
				stall_timeout = Some((!ms.is_zero()).then_some(ms));
			}
			"--stall-grace-ms" => stall_grace = Some(millis(args, &mut at, "--stall-grace-ms")?),
			"--on-stall" => {
				stall_policy = Some(match value(args, &mut at, "--on-stall")?.as_str() {
					"mute" => StallPolicy::Mute,
					"continue" => StallPolicy::Continue,
					"fail" => StallPolicy::Fail,
					other => return Err(usage(format!("unknown stall policy {other:?}"))),
				});
			}
			// Place packets by stream position rather than by arrival, so two
			// instances fed the same objects emit the same bytes in the same slots
			// and an ST 2022-7 receiver can merge them. Needs an explicit bitrate
			// and an explicit latency.
			"--stream-clock" => clocking = Clocking::Stream,
			"--sequence-seed" => sequence_seed = number(args, &mut at, "--sequence-seed")?,
			// Refuse a source whose PCR positions do not track its PCR values,
			// rather than emitting a byte-locked clock over a stream the grid has
			// silently started dropping. Off by default: the same behaviour
			// absorbs a genuine rate peak, and a peak is legitimate.
			"--require-pcr-position" => pcr_position_policy = PcrPositionPolicy::Fail,
			// A permanent feed outlives any closing report, and the counters that
			// matter over hours are the ones that are supposed to be *stationary*
			// — buffer depth, recovered media rate, cushion. Emitting them on a
			// timer is what makes those a time series rather than one reading.
			"--stats-interval-ms" => {
				let ms = millis(args, &mut at, "--stats-interval-ms")?;
				stats_interval = (!ms.is_zero()).then_some(ms);
			}
			other => return Err(usage(format!("unknown argument {other:?}"))),
		}
	}

	let mut config = if rate == "auto" {
		Config::auto()
	} else {
		let bitrate = rate
			.parse()
			.map_err(|_| usage("bitrate must be a whole number of bits per second, or \"auto\""))?;
		Config::new(bitrate)
	}
	.with_pcr_mode(pcr)
	.with_clocking(clocking)
	.with_sequence_seed(sequence_seed)
	.with_pcr_position_policy(pcr_position_policy);

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

	Ok(Invocation {
		config,
		dest,
		rate,
		rtp,
		ssrc,
		stats_interval,
	})
}

/// The next argument, consuming it.
fn value<'a>(args: &'a [String], at: &mut usize, flag: &str) -> Result<&'a String, Failure> {
	let raw = args.get(*at).ok_or_else(|| usage(format!("{flag} needs a value")))?;
	*at += 1;
	Ok(raw)
}

/// The next argument as a whole number.
fn number<T: std::str::FromStr>(args: &[String], at: &mut usize, flag: &str) -> Result<T, Failure> {
	let raw = value(args, at, flag)?;
	raw.parse()
		.map_err(|_| usage(format!("{flag} needs a whole number, got {raw:?}")))
}

/// The next argument as a count of milliseconds.
fn millis(args: &[String], at: &mut usize, flag: &str) -> Result<Duration, Failure> {
	Ok(Duration::from_millis(number(args, at, flag)?))
}

async fn egress(invocation: Invocation) -> Result<Stats, Failure> {
	let Invocation {
		config,
		dest,
		rtp,
		ssrc,
		stats_interval,
		..
	} = invocation;
	let source = ReadSource::new(tokio::io::stdin());

	if dest == "-" || dest == "stdout" {
		return pace_with(
			config,
			source,
			WriteSink::new(tokio::io::stdout()),
			liveness(stats_interval),
		)
		.await
		.map_err(failed);
	}

	let destination: SocketAddr = dest
		.parse()
		.map_err(|_| usage("destination must be ip:port, - or stdout"))?;
	let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.map_err(failed)?;
	if rtp {
		let sink = match ssrc {
			Some(id) => RtpSink::with_ssrc(socket, destination, id),
			None => RtpSink::new(socket, destination),
		};
		pace_with(config, source, sink, liveness(stats_interval))
			.await
			.map_err(failed)
	} else {
		pace_with(
			config,
			source,
			UdpSink::new(socket, destination),
			liveness(stats_interval),
		)
		.await
		.map_err(failed)
	}
}

/// Say what the run is going to do, before it starts doing it.
fn announce(invocation: &Invocation) {
	let Invocation {
		config,
		dest,
		rate,
		rtp,
		..
	} = invocation;
	let pcr = if config.pcr == PcrMode::Regenerate {
		"regenerate PCR"
	} else {
		"preserve PCR"
	};
	let clock = match config.clocking {
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
	let target = match dest.as_str() {
		"-" | "stdout" => "stdout (raw ts)".to_string(),
		dest if *rtp => format!("rtp://{dest}"),
		dest => format!("udp://{dest}"),
	};
	eprintln!("mpegts-pacer: -> {target} @ {rate} b/s, {pcr}{clock}, {depth}");
}

/// Log liveness transitions to stderr, and optionally a counter line on a timer.
///
/// Without the first a dead source is invisible from the outside: the egress holds
/// its rate whether or not there is a programme in it, so an operator watching
/// bitrate or packet arrival sees a healthy leg either way.
///
/// The second exists because a feed that runs for months is not graded by the
/// report it prints when it stops. The counters that decide whether it is *still*
/// healthy — buffer depth against its set point, the recovered media rate, the
/// cushion in force — are the ones that should not be moving, and a single reading
/// cannot show that they are not.
fn liveness(interval: Option<Duration>) -> CallbackObserver<impl FnMut(Health) + Send> {
	let mut last: Option<SourceState> = None;
	let started = Instant::now();
	let mut next = interval.map(|every| (started + every, every));
	CallbackObserver::new(move |health: Health| {
		if let Some((due, every)) = next {
			let now = Instant::now();
			if now >= due {
				// Skip whole periods rather than catching up, so a stalled
				// emitter does not produce a burst of backdated lines.
				next = Some((now + every, every));
				sample(started.elapsed(), &health);
			}
		}
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

/// One counter line, `key=value` so it can be parsed without a schema.
///
/// Cumulative counters and standing levels are both here on purpose: the first
/// say what has gone wrong, the second whether it is still going wrong.
fn sample(elapsed: Duration, health: &Health) {
	let s = &health.stats;
	eprintln!(
		"mpegts-pacer: sample t={} state={:?} out={} content={} null={} stuffing={:.3} \
		 dropped={} late_drops={} underruns={} stalls={} muted={} resyncs={} pcr_inserted={} \
		 buffer={} buffer_high_water={} cushion_ms={} lead_ms={} media_rate_bps={} \
		 content_gap_max_ms={} pcr_displacement={}",
		elapsed.as_secs(),
		health.source,
		s.output_packets,
		s.content_packets,
		s.null_packets,
		s.null_ratio(),
		s.dropped_packets,
		s.late_drops,
		s.underruns,
		s.stalls,
		s.muted_packets,
		s.resyncs,
		s.pcr_inserted,
		s.buffer_packets,
		s.buffer_high_water,
		s.latency_target_ms,
		s.arrival_lead_ms,
		s.media_rate_bps,
		s.content_gap_max_ms,
		s.pcr_position_displacement,
	);
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
		 cushion={} ms buffer_high_water={} packets media_rate={} b/s",
		stats.bursts,
		stats.burst_max_packets,
		(stats.burst_max_packets * TS_PACKET_SIZE as u64) as f64 / 1_000_000.0,
		stats.arrival_lead_ms,
		stats.latency_target_ms,
		stats.buffer_high_water,
		stats.media_rate_bps,
	);
	// Reported separately and unconditionally, because the symptom otherwise
	// arrives as `resyncs` above and reads as a rate set too low. A non-zero
	// displacement says the source's PCR positions do not track its PCR values,
	// which no amount of extra rate fixes.
	if stats.pcr_position_overruns > 0 {
		eprintln!(
			"mpegts-pacer: pcr position. overrun_intervals={} displacement={} packets ({} ms at rate) \
			 -- source PCR positions do not track its values; size the buffer past the displacement \
			 or use --require-pcr-position",
			stats.pcr_position_overruns, stats.pcr_position_displacement, stats.pcr_position_displacement_ms,
		);
	}
}
