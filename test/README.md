# mpegts-pacer compliance harness

Validates the [`mpegts-pacer`](..) crate's constant-bitrate grooming in
isolation: it shapes a transport stream through the crate's offline CBR example
(`cargo run -p mpegts-pacer --example cbr_file`) and checks that the paced output
is something a professional IRD would accept. No relay, no network, so it is fast
and deterministic.

## What it checks

1. **`pcrverify` (headline gate).** A TSDuck `tsp -P pcrverify` pass at a tight
   PCR-accuracy tolerance (default 500 us). Byte-locked CBR output passes; a
   bursty re-mux fails. Any PCR outside tolerance fails the harness.
2. **Full compliance report.** Runs the vendored [`compliance.py`](compliance.py)
   with the source as the duration-fidelity reference: PAT/PMT, PSI CRC,
   continuity, PCR presence/monotonicity, PCR repetition/jitter, null ratio,
   T-STD, and that the paced clip's duration matches the source (a self-consistent
   PCR on the wrong rate is caught here).

`[hard]` checks fail the run; `[shape]` checks warn unless `--strict`.

## The bursty arm (`--bursty`)

Everything above is blind to *arrival timing*, and structurally so: `cbr_file`
drives the scheduler on a synthetic clock derived from the source PCR, which is
what makes it deterministic and also means it has no arrival pattern at all.
Buffer sizing, the start gate and stall detection are all functions of when
packets turn up, so none of them are exercised.

`--bursty` adds an arm that replays the same clip through the **live** pacer the
way a segment-fetching client delivers it: a segment's worth of media at line
rate, then silence until the next is due, with every fourth cycle waiting twice as
long and collecting two segments (`--segment-ms` sets the period, default 2000).
It then runs the same `pcrverify` gate and compliance report on the result, so the
assertion is that burst-delivered input comes out to the same PCR accuracy as
file-delivered input.

`burst_replay` also fails the run itself on the three things the analyzer cannot
see, because they are properties of the input rather than the output: packets
dropped on arrival, stalls declared on inter-segment gaps, and content that never
reached the wire. An analyzer looking only at the output cannot distinguish burst
absorbed from programme deleted.

This arm runs in real time, so it costs the clip's own duration.

## Running

```bash
./run.sh                     # generate a clip, pace it, analyze
./run.sh --source cap.ts     # pace a real capture instead
./run.sh --bitrate 12000000  # force the target mux rate
./run.sh --pcr preserve      # preserve source PCR (no byte-lock/re-insert)
./run.sh --strict            # also fail on broadcast-shape warnings
./run.sh --bursty            # also run the live segment-burst arm
./run.sh --bursty --segment-ms 6000
```

Requires TSDuck (`tsp`, `tsanalyze`), `python3`, `cargo`, and (when generating a
clip) `ffmpeg`. The default target mux rate is 1.2x the source bitrate so content
never out-runs the byte clock.

## Notes on PCR

- **Regenerate mode** (default) byte-locks every PCR to its output position and
  re-inserts extra PCR-only packets on the PCR PID when the source's PCR is
  sparser than the 40 ms repetition limit. This is what a hardware IRD's PLL and
  PCR-accuracy checks require.
- **Preserve mode** keeps source PCR values verbatim and only paces
  transmission, so it inherits the source's PCR cadence (fine for soft IRDs that
  re-buffer, not for a CBR/ASI hardware IRD).
