//! Liveness state and the [`Observer`] hook that reports it.
//!
//! A pacer's output tells you nothing about its input. It holds the wire at a
//! constant rate whether the source is delivering a programme, delivering
//! nothing, or gone entirely, because that is what constant-bitrate stuffing
//! does. So carrier liveness and *content* liveness are separate facts, and only
//! the pacer is in a position to report the second one.
//!
//! [`SourceState`] is that fact, and an `Observer` is how it leaves the engine:
//! feed it to a health endpoint, a GPI relay, or the supervisor deciding whether
//! to fail a redundancy leg over.

use crate::stats::Stats;

/// What the pacer's input is currently doing.
///
/// Derived from when content last *arrived*, not from what was last emitted:
/// emission is gated by the media clock, so a perfectly healthy below-rate feed
/// produces stuffing all day and says nothing about the source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SourceState {
	/// No content has arrived yet, or the de-jitter buffer is still priming
	/// towards the first emit.
	#[default]
	Priming,

	/// Content is arriving, or buffered media is still being released. A dead
	/// source draining its cushion counts as live: real programme is going to air.
	Live,

	/// The buffer has run dry and no content has arrived for longer than the
	/// configured de-jitter latency — running on fumes, but still inside the
	/// stall grace period. Ordinary on a lumpy transport; sustained, it is the
	/// warning ahead of [`SourceState::Stalled`].
	Starved,

	/// The buffer is empty and no content has arrived for longer than
	/// [`Config::stall`](crate::Config::stall). The source is
	/// treated as gone rather than late, and
	/// [`Config::stall_policy`](crate::Config::stall_policy) applies.
	Stalled,
}

impl SourceState {
	/// Whether the input is delivering content (`Priming` counts as not yet).
	pub fn is_live(&self) -> bool {
		matches!(self, SourceState::Live)
	}

	/// Whether the input has been silent past the stall timeout.
	pub fn is_stalled(&self) -> bool {
		matches!(self, SourceState::Stalled)
	}
}

/// A snapshot of what the pacer's input is doing and what it has emitted.
#[derive(Clone, Copy, Debug, Default)]
pub struct Health {
	/// The current input state.
	pub source: SourceState,
	/// Cumulative counters at the moment of the snapshot.
	pub stats: Stats,
}

/// A hook for observing the pacer's liveness while it runs.
///
/// Implemented for `()` as a no-op, so [`pace`](crate::pace) is just
/// [`pace_with`](crate::pace_with) without an observer. Wrap a closure with
/// [`CallbackObserver`] to watch from the outside.
///
/// Called on every [`SourceState`] transition and periodically in between so the
/// counters stay fresh. It runs on the emitter's own task, in the path of the
/// output byte clock, so it must not block.
pub trait Observer {
	/// Report the current health.
	fn on_change(&mut self, health: Health);
}

impl Observer for () {
	fn on_change(&mut self, _health: Health) {}
}

/// An [`Observer`] that hands each health snapshot to a closure, the counterpart
/// to [`CallbackSink`](crate::CallbackSink).
pub struct CallbackObserver<F> {
	callback: F,
}

impl<F> CallbackObserver<F>
where
	F: FnMut(Health) + Send,
{
	/// Wrap a closure as an observer.
	pub fn new(callback: F) -> Self {
		Self { callback }
	}
}

impl<F> Observer for CallbackObserver<F>
where
	F: FnMut(Health) + Send,
{
	fn on_change(&mut self, health: Health) {
		(self.callback)(health);
	}
}
