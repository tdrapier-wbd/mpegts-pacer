//! Transport-agnostic MPEG-TS constant-bitrate pacer for broadcast (IRD) egress.
//!
//! Modern IP transports (MoQ, SRT, RIST, RTP, file playback) deliver MPEG-TS in
//! bursts. Professional Integrated Receiver/Decoders (IRDs) expect a smooth,
//! constant-bitrate transport with byte-accurate PCR. This crate is the missing
//! adaptation layer between the two: feed it already-multiplexed transport
//! packets and it emits a deterministic CBR stream a hardware IRD will accept.
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
//! [`Config::stall_timeout`] the source is treated as gone: no PCR is inserted
//! into a programme-free stream, and [`Config::stall_policy`] decides what happens
//! to the carrier — by default [`StallPolicy::Mute`], which stops emitting while
//! holding the output byte clock, and resumes when content returns.
//! [`SourceState`] is observable while the pacer runs, via
//! [`TsPacer::watch_health`] or [`pace_with`].

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
mod source;
mod stats;

pub use config::{
	Bitrate, Config, DEFAULT_AUTO_FALLBACK, DEFAULT_AUTO_HEADROOM, DEFAULT_LATENCY, DEFAULT_MAX_LATENCY,
	DEFAULT_PACKETS_PER_DATAGRAM, DEFAULT_STALL_TIMEOUT, PcrMode, StallPolicy,
};
pub use error::{Error, Result};
pub use estimate::estimate_content_bitrate;
pub use null_insertion::NULL_PID;
pub use observe::{CallbackObserver, Health, Observer, SourceState};
pub use output::{CallbackSink, RTP_PAYLOAD_TYPE_MP2T, RtpSink, Sink, UdpSink, WriteSink};
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
