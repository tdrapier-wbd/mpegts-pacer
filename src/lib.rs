//! Transport-agnostic MPEG-TS constant-bitrate pacer for broadcast (IRD) egress.
//!
//! Modern IP transports (MoQ, SRT, RIST, segmented HTTP, file playback) deliver
//! MPEG-TS in bursts. Professional Integrated Receiver/Decoders (IRDs) expect a
//! smooth, constant-bitrate transport with byte-accurate PCR. This crate is the
//! missing adaptation layer between the two: feed it already-multiplexed transport
//! packets and it emits a deterministic CBR stream a hardware IRD will accept.
//!
//! # Buffer depth
//!
//! Burst granularity differs by two orders of magnitude between transports. An
//! object transport arrives in kilobyte bursts milliseconds apart; a
//! segment-fetching one arrives in megabyte bursts seconds apart. A pacer set for
//! the first and fed the second drops programme out of a healthy feed on every
//! segment, starts on a timer holding a fraction of what it needs, and reads every
//! ordinary inter-segment gap as a dead source.
//!
//! So by default the depths are measured rather than configured. The pacer tracks
//! how much media its input hands over *ahead of real time* — which is precisely
//! the occupancy the arrival pattern forces — and derives the cushion, the hard cap
//! and the stall timeout from it. A feed delivered at the media rate never gets
//! ahead, so it stays on the 200 ms floor and behaves exactly as it always has;
//! a feed fetched a segment at a time settles on a multiple of the segment
//! duration. See [`Latency`] for the mechanism and its costs, and pin any depth to
//! turn it off.
//!
//! It is **not** a muxer. It never demultiplexes, remultiplexes, rewrites PSI,
//! regenerates PAT/PMT, or touches continuity counters. PID structure and
//! PES/PSI payloads pass through untouched. It shapes *transmission timing* and,
//! optionally, byte-locks the PCR. The only bytes it ever rewrites are the six
//! PCR octets under [`PcrMode::Regenerate`].
//!
//! # Pipeline
//!
//! The engine is a [`Source`] -> pacer -> [`Sink`] pipeline, so any packet
//! producer is just one `Source` and any output is just one `Sink`. Nothing here
//! knows about MoQ, QUIC, objects, groups, tracks, or subscribers.
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use mpegts_pacer::{Config, ReadSource, UdpSink, pace};
//!
//! # async fn run() -> mpegts_pacer::Result<()> {
//! // A MoQ subscriber writing `export ts` to a pipe is just an AsyncRead source.
//! let source = ReadSource::new(tokio::io::stdin());
//! let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
//! let sink = UdpSink::new(socket, "239.0.0.1:5000".parse::<SocketAddr>().unwrap());
//! let stats = pace(Config::new(10_000_000), source, sink).await?;
//! eprintln!("null ratio {:.1}%", stats.null_ratio() * 100.0);
//! # Ok(())
//! # }
//! ```
//!
//! # Push API
//!
//! Prefer to push packets yourself? [`TsPacer`] wraps the same engine behind a
//! background task:
//!
//! ```no_run
//! use mpegts_pacer::{Config, Packet, TsPacer, CallbackSink};
//!
//! # async fn run(packet: Packet) -> mpegts_pacer::Result<()> {
//! let pacer = TsPacer::spawn(Config::new(10_000_000), CallbackSink::new(|_dg: &[u8]| Ok(())));
//! pacer.push_packet(packet).await?;
//! let _stats = pacer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Content liveness
//!
//! Stuffing null packets to hold a rate is the right answer to a *late* source and
//! the wrong answer to an *absent* one. A pacer left to it will emit a
//! byte-perfect carrier — valid transport, correct rate, PCR present and accurate
//! — with no programme in it, for as long as it is left running, and every signal
//! a monitor or a 1+1 receiver keys on (arrival, loss, continuity) reads healthy.
//!
//! So the pacer tracks when content last *arrived*, not just what it emitted. Past
//! what [`Config::stall`] allows the source is treated as gone: no PCR is inserted
//! into a programme-free stream, and [`Config::stall_policy`] decides what happens
//! to the carrier — by default [`StallPolicy::Mute`], which stops emitting while
//! holding the output byte clock, and resumes when content returns.
//! [`SourceState`] is observable while the pacer runs, via
//! [`TsPacer::watch_health`] or [`pace_with`].
//!
//! # Redundant pairs
//!
//! By default the pacer decides what goes in each output slot from its own emit
//! clock, which is correct for one output and wrong for two: an ST 2022-7
//! receiver merges a pair by matching RTP sequence numbers and expects the legs
//! to be packet-identical, and two pacers clocked on their own emit instants are
//! not. [`Clocking::Stream`] places every packet on the absolute slot its source
//! PCR implies, and numbers the output from that slot, so what a leg sends is a
//! function of the stream rather than of the leg — two of them agree without
//! sharing a process, and one can join, leave and rejoin the pair on its own.
//!
//! Rejoining has two conditions the mode has to meet, both of which are about
//! refusing to take a delivery accident as a fact about the stream. A leg that
//! subscribes to a running broadcast is handed whatever the relay has buffered,
//! oldest first, and keeps its output clock at the live edge of it rather than
//! at its head — when it starts, and again whenever the backlog arrives after
//! the clock is already running. A leg emitting at the mux rate cannot catch up,
//! so taking the buffer's depth as its phase would put it that far behind its
//! partner permanently, and a depth is an operator's tuning choice rather than
//! a fact about the stream. And a leg that
//! has been cut off for longer than a source discontinuity is allowed to last
//! reads the jump in its source PCR against the time it spent silent, so that
//! missing the middle of a stream is not mistaken for the stream being spliced.
//! Getting the second wrong is invisible: the leg comes back, its numbering is
//! right, and every packet it carries lands in a slot that has already gone.

mod arrival;
mod config;
mod error;
mod estimate;
mod jitter_buffer;
mod null_insertion;
mod observe;
mod output;
mod pacer;
mod packet;
mod pcr;
mod scheduler;
mod slot;
mod source;
mod stats;

pub use arrival::{BURST_SEPARATION, DELIVERY_GAP};
pub use config::{
	Bitrate, Clocking, Config, DEFAULT_AUTO_FALLBACK, DEFAULT_AUTO_HEADROOM, DEFAULT_LATENCY,
	DEFAULT_LATENCY_CEILING, DEFAULT_LATENCY_FACTOR, DEFAULT_MAX_LATENCY, DEFAULT_PACKETS_PER_DATAGRAM,
	DEFAULT_STALL_GRACE, Latency, PcrMode, Stall, StallPolicy,
};
pub use error::{Error, Result};
pub use estimate::estimate_content_bitrate;
pub use null_insertion::NULL_PID;
pub use observe::{CallbackObserver, Health, Observer, SourceState};
pub use output::{CallbackSink, Framing, RTP_PAYLOAD_TYPE_MP2T, RtpSink, Sink, UdpSink, WriteSink};
pub use pacer::{TsPacer, pace, pace_with};
pub use packet::{Packet, SYNC_BYTE, TS_PACKET_SIZE};
pub use scheduler::Scheduler;
pub use source::{IterSource, ReadSource, Source};
pub use stats::Stats;

/// Bridges between `tokio::time::Instant` (used for scheduling, so the pacer
/// honours `tokio::time::pause()` in tests) and the `std::time::Instant` the
/// synchronous [`Scheduler`] reasons about.
pub(crate) mod tokio_instant {
	use std::time::Instant;

	/// The current instant, as a `std::time::Instant` derived from tokio's clock.
	pub fn now_std() -> Instant {
		tokio::time::Instant::now().into_std()
	}

	/// Lift a `std::time::Instant` back onto tokio's clock for `sleep_until`.
	pub fn from_std(instant: Instant) -> tokio::time::Instant {
		tokio::time::Instant::from_std(instant)
	}
}
