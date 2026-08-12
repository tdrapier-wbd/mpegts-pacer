//! The absolute output slot grid: where a packet goes, as a function of the
//! stream rather than of when it arrived.
//!
//! A pacer that decides *what* to transmit from its own emit clock cannot be run
//! twice with the same answer. Two groomers fed the same objects over independent
//! paths interleave content and stuffing differently, so their outputs are
//! different transports rather than one transport sent twice — which is why an
//! ST 2022-7 receiver cannot merge them.
//!
//! The fix is to make every output position a function of the delivered bytes. At
//! a locked mux rate, output packet `n` occupies a known instant on the transport
//! byte clock, so the mapping between a 27 MHz PCR value and an output packet
//! index is fixed:
//!
//! ```text
//! slot_of_pcr(P) = P * mux_rate / (188 * 8 * 27_000_000)
//! pcr_of_slot(n) = n * 188 * 8 * 27_000_000 / mux_rate
//! ```
//!
//! Both are evaluated in `u128` so the rate need not divide evenly. Nothing here
//! depends on when the pacer started, how long its buffer is, or how the OS
//! scheduled it: two pacers that see the same PCR compute the same slot, and a
//! pacer that joins late lands on the grid the running one is already using.
//!
//! `pcr_of_slot` is also the emitted PCR, so PCR-against-byte-position is exact by
//! construction rather than anchored on whichever PCR happened to arrive first.

use crate::pcr::{PACKET_BITS, PCR_CLOCK_HZ, PCR_WRAP_TICKS};

/// The fixed mapping between source PCR values and output packet indices at one
/// locked mux rate.
#[derive(Clone, Copy, Debug)]
pub struct SlotMap {
	mux_rate_bps: u64,
}

impl SlotMap {
	/// Build the mapping for a locked mux rate (bits per second).
	pub fn new(mux_rate_bps: u64) -> Self {
		Self {
			mux_rate_bps: mux_rate_bps.max(1),
		}
	}

	/// The output slot a packet carrying 27 MHz PCR value `pcr` belongs in.
	pub fn slot_of_pcr(&self, pcr: u64) -> u64 {
		let numerator = u128::from(pcr) * u128::from(self.mux_rate_bps);
		(numerator / (PACKET_BITS * u128::from(PCR_CLOCK_HZ))) as u64
	}

	/// The byte-locked 27 MHz PCR value for output slot `slot`.
	pub fn pcr_of_slot(&self, slot: u64) -> u64 {
		let ticks = u128::from(slot) * PACKET_BITS * u128::from(PCR_CLOCK_HZ) / u128::from(self.mux_rate_bps);
		(ticks % u128::from(PCR_WRAP_TICKS)) as u64
	}

	/// How many slots one full PCR wrap spans, so a stream that runs past the
	/// 33-bit wrap keeps a monotonic slot index.
	pub fn slots_per_wrap(&self) -> u64 {
		self.slot_of_pcr(PCR_WRAP_TICKS)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// 188 * 8 * 27_000_000 / 12_000_000 = 3384 ticks per slot, exactly.
	const MUX_RATE: u64 = 12_000_000;
	const TICKS_PER_SLOT: u64 = 3_384;

	#[test]
	fn slot_and_pcr_are_inverses_on_an_exact_rate() {
		let map = SlotMap::new(MUX_RATE);
		for slot in [0_u64, 1, 7, 1_000, 1_000_000] {
			assert_eq!(map.pcr_of_slot(slot), slot * TICKS_PER_SLOT);
			assert_eq!(map.slot_of_pcr(slot * TICKS_PER_SLOT), slot);
		}
	}

	#[test]
	fn slot_is_monotonic_and_quantises_within_one_slot() {
		// A rate that does not divide evenly is the normal case; the mapping must
		// still be monotonic, and land every PCR within one slot of the truth.
		let map = SlotMap::new(4_000_000);
		let mut previous = 0;
		for step in 0..2_000_u64 {
			let pcr = step * 7_919; // a prime step, to avoid landing on boundaries
			let slot = map.slot_of_pcr(pcr);
			assert!(slot >= previous, "slot must not go backwards");
			previous = slot;
			let error = pcr.abs_diff(map.pcr_of_slot(slot));
			let ticks_per_slot = map.pcr_of_slot(1);
			assert!(error <= ticks_per_slot, "PCR {pcr} quantised by {error} ticks");
		}
	}

	#[test]
	fn two_maps_at_the_same_rate_agree_exactly() {
		// The property the whole design rests on: the mapping carries no state
		// from the process that built it.
		let one = SlotMap::new(4_000_000);
		let other = SlotMap::new(4_000_000);
		for pcr in [0_u64, 1, 27_000_000, 1_234_567_891, PCR_WRAP_TICKS - 1] {
			assert_eq!(one.slot_of_pcr(pcr), other.slot_of_pcr(pcr));
		}
	}
}
