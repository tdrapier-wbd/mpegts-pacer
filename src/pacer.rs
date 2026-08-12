//! The async pacing engine: a thin loop over [`Scheduler`] plus the ergonomic
//! push-style [`TsPacer`] handle.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use crate::config::{Bitrate, Config, DEFAULT_AUTO_FALLBACK, StallPolicy};
use crate::error::{Error, Result};
use crate::estimate::estimate_content_bitrate;
use crate::observe::{Health, Observer, SourceState};
use crate::packet::{Packet, SYNC_BYTE, TS_PACKET_SIZE};
use crate::scheduler::Scheduler;
use crate::source::Source;
use crate::stats::Stats;
use crate::{Sink, tokio_instant};

/// PCR samples to observe before locking a [`Bitrate::Auto`] rate. More samples
/// mean a longer warm-up but a steadier rate estimate.
const AUTO_WARMUP_PCRS: usize = 8;
/// Safety cap on the auto warm-up: resolve from whatever we have rather than
/// buffering unboundedly if the source has no (or very sparse) PCR.
const AUTO_WARMUP_MAX_PACKETS: usize = 20_000;
/// How often health is reported in the absence of a state transition, so a
/// caller polling counters sees fresh ones without a report per datagram.
const HEALTH_INTERVAL: Duration = Duration::from_millis(250);
/// Bound on a wait for input when no stall timeout is configured, so the engine
/// still wakes up to report health rather than blocking indefinitely.
const IDLE_POLL: Duration = Duration::from_secs(1);

/// Run the pacer to completion: pull packets from `source`, shape them to the
/// constant bitrate in `config`, and write paced datagrams to `sink`.
///
/// This is the fully generic pipeline entry point. Returns the final [`Stats`]
/// when the source is exhausted and the buffer has drained. Any source or sink
/// (a MoQ subscriber, SRT/RIST receiver, file, socket) composes here without the
/// engine knowing what they are.
///
/// A source that stops delivering *without* ending (a killed publisher behind a
/// pipe that stays open, a stalled subscriber) is governed by
/// [`Config::stall_policy`]; use [`pace_with`] to watch that happen.
pub async fn pace<Src, Snk>(config: Config, source: Src, sink: Snk) -> Result<Stats>
where
	Src: Source,
	Snk: Sink,
{
	pace_with(config, source, sink, ()).await
}

/// [`pace`], reporting liveness to an [`Observer`] as it runs.
///
/// The observer is called on every [`SourceState`] transition and periodically in
/// between, which is the only way to learn that a source has died: the output
/// carries on at the configured rate either way, so nothing downstream of the
/// sink can tell the difference.
pub async fn pace_with<Src, Snk, Obs>(
	config: Config,
	mut source: Src,
	mut sink: Snk,
	mut observer: Obs,
) -> Result<Stats>
where
	Src: Source,
	Snk: Sink,
	Obs: Observer,
{
	config.validate()?;

	// Resolve an auto-rate config against a short measurement window, then run
	// the fixed-rate scheduler. Warm-up packets prime the de-jitter buffer.
	let (config, warmup) = match config.bitrate {
		Bitrate::Constant(_) => (config, Vec::new()),
		Bitrate::Auto { headroom } => {
			let (bitrate, warmup) = resolve_auto(&mut source, headroom, &config).await?;
			(config.with_bitrate(Bitrate::Constant(bitrate)), warmup)
		}
	};

	let mut scheduler = Scheduler::new(&config);
	let mut closing = false;
	let mut reporter = Reporter::new();
	let started_at = tokio_instant::now_std();

	if !warmup.is_empty() {
		// Enqueue at a single instant so they fill the buffer as the initial
		// cushion rather than replaying as a startup burst.
		let now = tokio_instant::now_std();
		for packet in warmup {
			scheduler.enqueue(packet, now);
		}
	}

	loop {
		let Some(due) = scheduler.next_due() else {
			// Clock not armed yet: wait for the first packet, bounded so a source
			// that never speaks is a reported state rather than a silent hang.
			match recv_bounded(&mut source, wait_budget(&config)).await? {
				Arrival::Packet(packet) => scheduler.enqueue(packet, tokio_instant::now_std()),
				Arrival::Eof => return Ok(scheduler.stats()),
				Arrival::Silent => {
					let now = tokio_instant::now_std();
					reporter.report(&mut observer, &scheduler, scheduler.state(now), now);
					if stalls_hard(&config) {
						return Err(Error::SourceStalled {
							silent_for: now.saturating_duration_since(started_at),
						});
					}
				}
			}
			continue;
		};

		if closing && !scheduler.has_pending() {
			reporter.force(&mut observer, &scheduler, tokio_instant::now_std());
			return Ok(scheduler.stats());
		}

		let sleep = tokio::time::sleep_until(tokio_instant::from_std(due));
		tokio::select! {
			biased;
			// The byte clock wins: always keep the wire at the mux rate, even
			// under a backlog, so ingest can't starve emission.
			_ = sleep => {
				let now = tokio_instant::now_std();
				// One state read per slot: the buffer can empty *inside* this slot,
				// so re-reading it after emitting would report a stall the slot's
				// own bookkeeping has not counted yet.
				let state = scheduler.state(now);
				match (state, config.stall_policy) {
					// The source is gone, not late. Hold the byte clock but put
					// nothing on the wire, so downstream sees the carrier stop
					// instead of a programme-free stream it will read as healthy.
					(SourceState::Stalled, StallPolicy::Mute) => scheduler.advance_muted(now),
					(SourceState::Stalled, StallPolicy::Fail) => {
						reporter.force(&mut observer, &scheduler, now);
						return Err(Error::SourceStalled { silent_for: scheduler.silent_for(now) });
					}
					_ => {
						// Offered before the bytes, so a sink that numbers its
						// output numbers it from the stream rather than from its
						// own send count.
						if let Some(framing) = scheduler.framing() {
							sink.set_framing(framing);
						}
						let datagram = scheduler.emit_datagram(now);
						sink.send(datagram).await?;
					}
				}
				reporter.report(&mut observer, &scheduler, state, now);
			}
			maybe = source.recv(), if !closing => {
				match maybe? {
					Some(packet) => scheduler.enqueue(packet, tokio_instant::now_std()),
					None => {
						// Stream clocking holds the tail of the stream back waiting
						// for a closing PCR that is not coming.
						scheduler.flush();
						closing = true;
					}
				}
			}
		}
	}
}

/// The outcome of one bounded wait on a [`Source`].
enum Arrival {
	/// A packet arrived.
	Packet(Packet),
	/// The source ended cleanly.
	Eof,
	/// Nothing arrived inside the budget. Distinct from `Eof`: the source is
	/// still open, it just isn't delivering, which is the failure mode a pacer
	/// otherwise papers over.
	Silent,
}

/// Wait for one packet, giving up after `budget` so silence is observable.
async fn recv_bounded<Src: Source>(source: &mut Src, budget: Duration) -> Result<Arrival> {
	match tokio::time::timeout(budget, source.recv()).await {
		Ok(received) => Ok(match received? {
			Some(packet) => Arrival::Packet(packet),
			None => Arrival::Eof,
		}),
		Err(_elapsed) => Ok(Arrival::Silent),
	}
}

/// How long to wait on a silent source before coming back to report health.
fn wait_budget(config: &Config) -> Duration {
	config.stall_timeout.unwrap_or(IDLE_POLL)
}

/// Whether a stall should end the run with [`Error::SourceStalled`].
fn stalls_hard(config: &Config) -> bool {
	config.stall_timeout.is_some() && config.stall_policy == StallPolicy::Fail
}

/// Rate-limits health reporting to transitions plus a slow heartbeat.
struct Reporter {
	last: Option<(SourceState, Instant)>,
}

impl Reporter {
	fn new() -> Self {
		Self { last: None }
	}

	/// Report if the state changed or the heartbeat is due.
	///
	/// `state` is the state the caller acted on, not a fresh reading, so the
	/// counters in the report describe the same slot as the state it names.
	fn report<Obs: Observer>(&mut self, observer: &mut Obs, scheduler: &Scheduler, state: SourceState, now: Instant) {
		let due = match self.last {
			None => true,
			Some((last_state, at)) => last_state != state || now.saturating_duration_since(at) >= HEALTH_INTERVAL,
		};
		if due {
			self.emit(observer, scheduler, state, now);
		}
	}

	/// Report unconditionally (a terminal transition the caller must not miss).
	fn force<Obs: Observer>(&mut self, observer: &mut Obs, scheduler: &Scheduler, now: Instant) {
		let state = scheduler.state(now);
		self.emit(observer, scheduler, state, now);
	}

	fn emit<Obs: Observer>(&mut self, observer: &mut Obs, scheduler: &Scheduler, state: SourceState, now: Instant) {
		self.last = Some((state, now));
		observer.on_change(Health {
			source: state,
			stats: scheduler.stats(),
		});
	}
}

/// Pull packets until enough PCRs are seen to estimate the content rate, then
/// return the locked mux rate (measured content rate plus `headroom`) and the
/// buffered warm-up packets so the caller can prime the scheduler with them.
///
/// The rate is locked for the whole run, so a source that goes quiet mid-warm-up
/// is only resolved early once there is something real to measure: guessing from
/// one packet would pin the fallback rate to the entire session.
async fn resolve_auto<Src: Source>(source: &mut Src, headroom: f64, config: &Config) -> Result<(u64, Vec<Packet>)> {
	let mut warmup: Vec<Packet> = Vec::new();
	let mut pcrs = 0usize;
	let budget = wait_budget(config);
	let started_at = tokio_instant::now_std();

	loop {
		match recv_bounded(source, budget).await? {
			Arrival::Packet(packet) => {
				if packet.has_pcr() {
					pcrs += 1;
				}
				warmup.push(packet);
				if pcrs >= AUTO_WARMUP_PCRS || warmup.len() >= AUTO_WARMUP_MAX_PACKETS {
					break;
				}
			}
			// Exhausted: resolve from whatever we gathered.
			Arrival::Eof => break,
			Arrival::Silent => {
				if stalls_hard(config) {
					return Err(Error::SourceStalled {
						silent_for: tokio_instant::now_std().saturating_duration_since(started_at),
					});
				}
				// Two PCRs is a measurable window; below that, keep waiting rather
				// than lock a guess.
				if pcrs >= 2 {
					break;
				}
			}
		}
	}

	let content = estimate_content_bitrate(&warmup).unwrap_or(DEFAULT_AUTO_FALLBACK);
	let bitrate = ((content as f64) * (1.0 + headroom.max(0.0))) as u64;
	Ok((bitrate.max(1), warmup))
}

/// An ergonomic, push-style handle to a running pacer.
///
/// [`spawn`](TsPacer::spawn) starts the emitter on a background task; feed it
/// with [`push_packet`](TsPacer::push_packet) / [`push_packets`](TsPacer::push_packets)
/// / [`push_bytes`](TsPacer::push_bytes) and finish with [`close`](TsPacer::close).
/// Internally this is just [`pace`] driven by an in-memory channel source, so a
/// caller who prefers the pipeline form can use [`pace`] directly instead.
pub struct TsPacer {
	tx: mpsc::Sender<Packet>,
	handle: tokio::task::JoinHandle<Result<Stats>>,
	/// Partial trailing bytes buffered across [`push_bytes`](TsPacer::push_bytes) calls.
	partial: Mutex<Vec<u8>>,
	health: watch::Receiver<Health>,
}

impl TsPacer {
	/// Start pacing to `sink` on a background task, returning a handle to push
	/// packets into.
	pub fn spawn<S>(config: Config, sink: S) -> Self
	where
		S: Sink + Send + 'static,
	{
		// Bound the channel so a stalled sink applies backpressure to the
		// producer rather than growing an unbounded queue.
		let (tx, rx) = mpsc::channel(1024);
		let (health_tx, health) = watch::channel(Health::default());
		let handle = tokio::spawn(pace_with(config, ChannelSource { rx }, sink, WatchObserver(health_tx)));
		Self {
			tx,
			handle,
			partial: Mutex::new(Vec::new()),
			health,
		}
	}

	/// The pacer's current liveness state and counters.
	///
	/// Worth polling even when the output looks perfect: that is precisely the
	/// case a stalled source produces. See [`SourceState`].
	pub fn health(&self) -> Health {
		*self.health.borrow()
	}

	/// A [`watch`] receiver for awaiting liveness changes, for a supervisor that
	/// should react to a stall rather than discover it on the next poll.
	///
	/// ```no_run
	/// # async fn run(pacer: mpegts_pacer::TsPacer) -> mpegts_pacer::Result<()> {
	/// let mut health = pacer.watch_health();
	/// while health.changed().await.is_ok() {
	///     if health.borrow().source.is_stalled() {
	///         // fail the leg over, raise an alarm, drop the input
	///     }
	/// }
	/// # Ok(())
	/// # }
	/// ```
	pub fn watch_health(&self) -> watch::Receiver<Health> {
		self.health.clone()
	}

	/// Push a single validated packet.
	pub async fn push_packet(&self, packet: Packet) -> Result<()> {
		self.tx.send(packet).await.map_err(|_| Error::Closed)
	}

	/// Push several packets in order.
	pub async fn push_packets(&self, packets: &[Packet]) -> Result<()> {
		for packet in packets {
			self.push_packet(packet.clone()).await?;
		}
		Ok(())
	}

	/// Push a raw byte buffer of one or more (possibly partial) 188-byte packets.
	///
	/// Whole packets are forwarded; a trailing partial packet is buffered until
	/// the next call. Convenient when reading fixed-size reads off a socket or
	/// pipe that don't fall on packet boundaries.
	pub async fn push_bytes(&self, data: &[u8]) -> Result<()> {
		let packets = self.split_packets(data);
		for packet in packets {
			self.push_packet(packet).await?;
		}
		Ok(())
	}

	/// Drain whole packets out of the partial buffer under the lock, returning
	/// them so sends happen without the lock held.
	fn split_packets(&self, data: &[u8]) -> Vec<Packet> {
		let mut partial = self.partial.lock().expect("partial buffer poisoned");
		partial.extend_from_slice(data);

		let mut packets = Vec::with_capacity(partial.len() / TS_PACKET_SIZE);
		let mut offset = 0;
		while offset + TS_PACKET_SIZE <= partial.len() {
			if partial[offset] != SYNC_BYTE {
				offset += 1; // resync on the next candidate byte
				continue;
			}
			if let Ok(packet) = Packet::from_slice(&partial[offset..offset + TS_PACKET_SIZE]) {
				packets.push(packet);
			}
			offset += TS_PACKET_SIZE;
		}
		partial.drain(..offset);
		packets
	}

	/// Stop accepting input, flush the buffer, and wait for the emitter to
	/// finish, returning the final [`Stats`].
	pub async fn close(self) -> Result<Stats> {
		drop(self.tx);
		match self.handle.await {
			Ok(result) => result,
			Err(err) => Err(Error::Io(std::io::Error::other(err))),
		}
	}
}

/// An [`Observer`] that publishes health on a [`watch`] channel for [`TsPacer`].
struct WatchObserver(watch::Sender<Health>);

impl Observer for WatchObserver {
	fn on_change(&mut self, health: Health) {
		// A dropped receiver just means nobody is watching.
		let _ = self.0.send(health);
	}
}

/// A [`Source`] over the [`TsPacer`] push channel.
struct ChannelSource {
	rx: mpsc::Receiver<Packet>,
}

impl Source for ChannelSource {
	async fn recv(&mut self) -> Result<Option<Packet>> {
		Ok(self.rx.recv().await)
	}
}
