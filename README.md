# mpegts-pacer

Transport-agnostic MPEG-TS constant-bitrate pacer for broadcast (IRD) egress.

Modern IP transports (MoQ, SRT, RIST, RTP, file playback) deliver MPEG-TS in
bursts. Professional Integrated Receiver/Decoders (IRDs) expect a smooth,
constant-bitrate transport with byte-accurate PCR. This crate is the missing
adaptation layer between the two: feed it already-multiplexed transport packets
and it emits a deterministic CBR stream a hardware IRD will accept.

It is **not** a muxer. It never demultiplexes, remultiplexes, rewrites PSI,
regenerates PAT/PMT, or touches continuity counters. PID structure and PES/PSI
payloads pass through untouched. It shapes *transmission timing* and, optionally,
byte-locks the PCR. The only bytes it ever rewrites are the six PCR octets (under
`PcrMode::Regenerate`).

Deliberately free of any `moq-*` / QUIC dependency: a MoQ subscriber is just one
possible source. Point it at SRT, RIST, a file, or a socket and nothing changes.

## Relationship to MoQ

This crate began life inside the [moq-dev](https://github.com/moq-dev/moq)
monorepo, where the motivating case was grooming a MoQ subscriber's `export ts`
output for a broadcast IRD. It carries no MoQ code, so it lives here as a
standalone crate: reusable by any MPEG-TS transport (SRT, RIST, RTP, file
playback), citable from a paper, and buildable without the whole monorepo. The
`moq-egress` example still shows the MoQ pipe wiring, but only because MoQ is the
source that prompted the crate, not because the engine depends on it.

It is intentionally *not* published to crates.io yet. Depend on it by git for now:

```toml
[dependencies]
mpegts-pacer = { git = "https://github.com/tdrapier-wbd/mpegts-pacer" }
```

## How it works

Two clocks run side by side:

- a **transport byte clock** at the target mux rate. Output packet `n` leaves at
  `anchor + n * 188 * 8 / bitrate`, so the wire rate is exactly constant. This
  clock also byte-locks the regenerated PCR.
- a **media clock** recovered from the source PCR. Content packets are released
  at the source's own media rate, delayed by the configured latency, so the
  output preserves the source duration instead of draining a burst faster than
  real time.

Every output slot emits a content packet when the media clock says one is due and
the buffer has it, otherwise a null packet. So a burst is absorbed by a bounded
jitter buffer and released at media rate, while the wire stays CBR.

A third thing is tracked alongside those clocks: whether content is still
*arriving*. See [Content liveness](#content-liveness) -- stuffing is the right
answer to a late source and the wrong answer to an absent one.

## Quick start

The engine is a `Source` -> pacer -> `Sink` pipeline. Any producer is one
`Source`, any output is one `Sink`.

```rust
use std::net::SocketAddr;
use mpegts_pacer::{Config, ReadSource, UdpSink, pace};

// A MoQ subscriber writing `export ts` to a pipe is just an AsyncRead source.
let source = ReadSource::new(tokio::io::stdin());
let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
let sink = UdpSink::new(socket, "239.0.0.1:5000".parse::<SocketAddr>()?);

let stats = pace(Config::new(10_000_000), source, sink).await?;
eprintln!("null ratio {:.1}%", stats.null_ratio() * 100.0);
```

Prefer to push packets yourself? `TsPacer` wraps the same engine behind a
background task:

```rust
use mpegts_pacer::{Config, CallbackSink, TsPacer};

let pacer = TsPacer::spawn(Config::new(10_000_000), CallbackSink::new(|dg: &[u8]| {
    // forward the datagram somewhere
    Ok(())
}));
pacer.push_bytes(&some_ts_bytes).await?;   // whole or partial 188-byte packets
let stats = pacer.close().await?;
```

## Configuration

Build a `Config` with `Config::new(bitrate)` (explicit rate) or `Config::auto()`
(derive the rate from the source), then chain the `with_*` setters. The struct is
`#[non_exhaustive]`, so use the constructors, not a struct literal.

| Setter | Type | Default | Meaning |
|---|---|---|---|
| `Config::new(bitrate)` | `u64` bits/s | required | Constant output rate over full 188-byte packets (content + stuffing). Must be at least the peak instantaneous input rate. |
| `Config::auto()` | -- | -- | Derive the rate from the source's measured content rate (see [Auto bitrate](#auto-bitrate)). |
| `.with_bitrate(Bitrate)` | `Bitrate` | -- | Set the rate target directly (`Bitrate::Constant(bps)` or `Bitrate::Auto { headroom }`). |
| `.with_latency(d)` | `Duration` | 200 ms | De-jitter priming: how long to fill the buffer before the first emit, and the steady-state cushion. Larger absorbs more burst at the cost of latency. |
| `.with_max_latency(d)` | `Duration` | 2000 ms | Hard cap on buffered media. Input past this depth is dropped oldest-first to keep latency and memory bounded. |
| `.with_pcr_mode(m)` | `PcrMode` | `Regenerate` | How the PCR is handled (see [PCR modes](#pcr-modes)). |
| `.with_packets_per_datagram(n)` | `usize` | 7 | Packets coalesced per output datagram. 7 * 188 = 1316 bytes fits a 1500-byte MTU over UDP/RTP. |
| `.with_pcr_max_interval(d)` | `Duration` | 40 ms | Max PCR repetition to hold on the output (TR 101 290 P1 limit). Under `Regenerate`, extra byte-locked PCR-only packets are inserted into stuffing slots when the source PCR is sparser than this. Ignored under `Preserve`. |
| `.with_stall_timeout(o)` | `Option<Duration>` | 1000 ms | How long the input may carry no content before it counts as gone rather than late. Grace *on top of* `latency`. `None` disables detection (carrier forever). |
| `.with_stall_policy(p)` | `StallPolicy` | `Mute` | What to do once stalled (see [Content liveness](#content-liveness)). |

### PCR modes

- **`PcrMode::Regenerate`** (default) rewrites each PCR-bearing packet's PCR to
  the value implied by its output byte offset at the target rate, so
  `PCR == byte_offset * 8 * 27_000_000 / bitrate` by construction. This is what a
  CBR/ASI hardware IRD's PCR-accuracy and repetition checks require. Only the six
  PCR octets (and the discontinuity indicator across a genuine source
  discontinuity) are touched.
- **`PcrMode::Preserve`** leaves every PCR value byte-for-byte untouched and only
  paces transmission. A soft IRD / player that recovers the clock from PCR
  *values* and re-buffers plays this cleanly. Note that once nulls are stuffed to
  hit the rate, a preserved PCR no longer sits at the byte position a constant-rate
  demuxer expects, so a hardware IRD that checks PCR-vs-byte (`tsp -P pcrverify`)
  will flag it. Use `Preserve` only for soft players.

### Auto bitrate

You asked whether the output rate can just match the source instead of being
stated explicitly. Yes, with one caveat worth understanding:

**By the time TS comes out of a MoQ subscriber it's a re-mux.** MoQ carries only
media objects, so the source's original null padding is gone. If the original was
padded CBR (say 6 Mb/s = 1 Mb/s content + 5 Mb/s null), that 6 Mb/s *mux rate* is
not recoverable from MoQ, because the padding no longer exists. What **is**
recoverable, exactly, is the true **content bitrate**, measured from the PCR
clock, and that's almost always what you actually want for egress (reproducing
the dead padding just wastes bandwidth).

`Config::auto()` measures the incoming content rate over a short warm-up window (a
few PCR samples) and locks the output to that rate plus a headroom margin (default
15%, for VBR peaks and the pacer's own stuffing). The output is still true CBR;
only the *choice* of rate is automatic.

```rust
// Self-tuning: no explicit rate. Output CBR ~= source content rate + 15%.
let stats = pace(Config::auto(), source, sink).await?;

// Tune the headroom (e.g. 25% for peaky VBR):
use mpegts_pacer::Bitrate;
let config = Config::auto().with_bitrate(Bitrate::Auto { headroom: 0.25 });
```

Trade-offs: auto adds a little startup latency (the measurement window), and too
little headroom risks buffer overflow on VBR peaks while too much wastes
bandwidth on nulls. When you know the rate and want minimal latency, pass it
explicitly. When the source has no usable PCR to measure, auto falls back to
`DEFAULT_AUTO_FALLBACK` (4 Mb/s). `estimate_content_bitrate(&[Packet])` is exposed
if you want to measure a run of packets yourself.

## Content liveness

A pacer holds the wire at a constant rate by stuffing null packets. That is
exactly right when the source is *late* and exactly wrong when the source is
*gone*: left alone, a pacer whose upstream dies keeps emitting a byte-perfect
carrier -- valid transport, correct rate, PCR present and accurate -- with no
programme in it, for as long as it is left running. Every signal a monitor or a
1+1 receiver normally keys on reads healthy: no loss, no continuity errors, no
silence. An input-failover policy performs zero switches, because there is never
any silence to detect. Grooming decouples *carrier* liveness from *content*
liveness, and only the pacer is positioned to tell them apart.

So the engine tracks when content last **arrived**, not just what it emitted, and
past `stall_timeout` it treats the source as gone:

- It stops inserting its own PCR. Re-insertion exists to hold the repetition limit
  for a stream being carried; there is no clock to hold in a stream with no
  content, and minting one is what makes a dead feed look conformant.
- It applies `StallPolicy`:
  - **`Mute`** (default) -- stop emitting, keep the task alive, resume when content
    returns. The output byte clock keeps running through the gap, so the
    regenerated PCR is still wall-clock-aligned on the far side and the output
    position (which a sink may number from) does not lose the outage. Downstream
    sees the carrier stop, which is what an IRD input failover or an ST 2022-7
    receiver can actually detect -- and a groomed leg's normal inter-datagram gap
    is microseconds, so a 50 ms failover threshold is unambiguous.
  - **`Continue`** -- hold the carrier through the stall, for a plant where the
    input must not drop and something else supervises content. Still no minted PCR.
  - **`Fail`** -- return `Error::SourceStalled` and let a supervisor decide.

On resume the media clock is re-anchored (so the refilled buffer is released at
media rate instead of dumped at line rate) and the first regenerated PCR carries
the discontinuity indicator, since the media timeline has a hole the output clock
does not.

`SourceState` is observable while the pacer runs -- the counters alone cannot say
"starving right now":

```rust
use mpegts_pacer::{CallbackObserver, Config, Health, pace_with};

let stats = pace_with(Config::new(10_000_000), source, sink,
    CallbackObserver::new(|health: Health| {
        if health.source.is_stalled() {
            eprintln!("source gone: {} ms without content", health.stats.content_gap_max_ms);
        }
    }),
).await?;
```

`TsPacer` exposes the same thing as a snapshot (`health()`) or a channel to await
on (`watch_health()`).

## Clocking: whose clock decides what goes in each slot

Both modes hold the wire at the configured rate; they differ in what decides
which packet occupies each output slot.

- **`Clocking::Arrival`** (default) releases content at the media rate recovered
  from the source PCR, measured against this process's own emit clock. Correct
  for a single output, and the mode every measurement in this crate was made in.
- **`Clocking::Stream`** places every packet on the absolute slot its source PCR
  implies at the locked mux rate. Where a packet goes -- and what number it goes
  out under -- is a function of the delivered bytes and nothing else: not the
  start time, not the buffer depth, not how the OS scheduled the emit timer.

The difference only matters when there are two of you. An ST 2022-7 receiver
merges a redundant pair by matching RTP sequence numbers and expects the legs to
be packet-identical, so two arrival-clocked pacers fed the same source over
independent paths produce two individually valid streams that are not a pair:
their content/stuffing interleave follows their own emit instants. The usual
answer is to groom once and duplicate the bytes (the `dual_rtp` example), which
protects the paths but leaves the groomer a single point of failure. Stream
clocking is the other answer: run a pacer per leg and let the stream, rather than
a shared process, be what they agree on.

Run one per leg, sharing the pair's rate, SSRC and sequence seed:

```bash
# leg A
moq ... export ts | moq_egress 239.0.0.1:5000 10000000 --rtp \
                      --ssrc 538968071 --stream-clock --sequence-seed 0
# leg B, on its own chain
moq ... export ts | moq_egress 239.0.0.2:5000 10000000 --rtp \
                      --ssrc 538968071 --stream-clock --sequence-seed 0
```

It follows from the same property that a leg can be started, stopped and started
again independently: a pacer that joins a stream already in flight lands on the
grid its partner is using, and one that mutes through an outage returns on its
partner's numbering rather than however far behind its own send count left it.
The datagram the leg arrives in is partial; every one after it is identical.

Requires an explicit constant bitrate (an auto rate is measured from one
process's arrival window, so two legs would lock different grids) and
`PcrMode::Regenerate` (the emitted PCR *is* the slot position, exact by
construction rather than anchored on whichever PCR arrived first). The source
must carry PCR; without it there is no grid. `Config::validate()` rejects the
rest rather than letting them produce output that merges badly.

A packet that arrives after its slot has gone is dropped and counted in
`late_drops`, never re-placed: moving it would make its position depend on how
late it was, reintroducing exactly the per-process variation the mode removes. A
climbing count means the release latency is short for the path's jitter -- and
the partner leg, which got the packet in time, covers for it.

## Sources and sinks

Built-in `Source`s:

- `ReadSource<R>` -- any `tokio::io::AsyncRead` (a pipe, socket, file, or process
  stdin such as `moq ... export ts`). Resynchronises on the `0x47` sync byte.
- `IterSource<I>` -- any `Iterator<Item = Packet>`, for in-memory / test vectors.

Built-in `Sink`s:

- `WriteSink<W>` -- any `tokio::io::AsyncWrite` (a pipe, file, or `stdout`).
  Writes the raw transport bytes so you can pipe the paced stream onward exactly
  like the subscriber itself (`... | pacer | ffplay -i -`).
- `UdpSink` -- raw MPEG-TS over UDP to a unicast or multicast destination.
- `RtpSink` -- RTP-encapsulated MPEG-TS (RFC 2250, payload type 33).
- `CallbackSink` -- hand each datagram to a closure for embedding.

Implement `Source` / `Sink` for anything else (a MoQ subscriber handle, an SRT
receiver, an ST 2022-7 pair, an FEC path) without touching the engine.

## Stats

`pace` returns, and `TsPacer::close` yields, a `Stats` snapshot:

`output_packets`, `content_packets`, `null_packets`, `dropped_packets` (buffer at
`max_latency`), `late_drops` (packets that missed their slot under
`Clocking::Stream`), `input_nulls_stripped` (source padding replaced by our own),
`underruns` (buffer starved while the input was still live), `pcr_rebases` (source
discontinuities), `pcr_inserted` (byte-locked PCR-only packets added to hold the
repetition limit), `stalls` (times the input went away), `muted_packets` (output
slots skipped while stalled, i.e. the carrier gap), `content_gap_max_ms` (longest
silence), and `null_ratio()`.

`stalls` is the one counter no amount of output inspection can substitute for: a
run that reports zero loss, zero drops and a healthy bitrate may still have spent
most of its life carrying nothing.

## Examples

### Live MoQ subscriber -> paced stdout / UDP / RTP

The `moq_egress` example is a thin MoQ egress adapter: stdin -> pacer -> stdout,
UDP, or RTP. The MoQ subscriber is just a producer, so the whole thing is a pipe.
Take your working subscriber command and splice the pacer in before the player.

The simplest form pipes the paced stream straight on, just like the subscriber:

```bash
# Before: subscriber straight to a soft player.
./moq --client-tls-disable-verify \
      --client-connect https://<relay-host>:443/anon \
      --broadcast cnn.international.emea.loop.hang \
      export ts --latency-max 5s \
  | ffplay -probesize 10M -analyzeduration 5M -vf bwdif -sync video -framedrop -i -

# After: subscriber -> mpegts-pacer -> ffplay, auto-rate, over a stdout pipe.
./moq --client-tls-disable-verify \
      --client-connect https://<relay-host>:443/anon \
      --broadcast cnn.international.emea.loop.hang \
      export ts --latency-max 5s \
  | cargo run --release -p mpegts-pacer --example moq_egress -- - auto \
  | ffplay -probesize 10M -analyzeduration 5M -vf bwdif -sync video -framedrop -i -
```

Or push it to a multicast group for a hardware IRD:

```bash
./moq ... export ts --latency-max 5s \
  | cargo run --release -p mpegts-pacer --example moq_egress -- 239.0.0.1:5000 auto

ffplay -i 'udp://@239.0.0.1:5000'   # or point your IRD at the group
```

`moq_egress` arguments:

```text
moq_egress <-|stdout|dest_ip:port> <bitrate_bps|auto> [--rtp] [--preserve] [--latency-ms N]
           [--max-latency-ms N] [--ssrc N] [--stall-ms N] [--on-stall mute|continue|fail]
           [--stream-clock] [--sequence-seed N]
```

- `<-|stdout|dest_ip:port>` -- `-` or `stdout` to write raw TS to a pipe, or a
  UDP/RTP destination (unicast or multicast group).
- `<bitrate_bps|auto>` -- explicit rate (e.g. `10000000`) or `auto` to derive it.
- `--rtp` -- RTP encapsulation instead of raw UDP (ignored for stdout).
- `--preserve` -- keep source PCR values (`PcrMode::Preserve`) instead of
  regenerating them. Use for soft players, not hardware IRDs.
- `--latency-ms N` -- de-jitter priming latency (default 200).
- `--max-latency-ms N` -- buffer depth (default 2000); input past it is dropped
  oldest-first.
- `--ssrc N` -- RTP SSRC, as a decimal 32-bit integer. Both legs of a redundant
  pair must carry the same one.
- `--stall-ms N` -- input-silence grace before the source counts as gone (default
  1000; `0` disables detection).
- `--on-stall mute|continue|fail` -- what to do then (default `mute`). Transitions
  are logged to stderr either way.
- `--stream-clock` -- place packets by stream position rather than by arrival, so
  two legs are a mergeable pair (see [Clocking](#clocking-whose-clock-decides-what-goes-in-each-slot)).
  Requires an explicit bitrate.
- `--sequence-seed N` -- RTP sequence offset (default 0), identical on both legs
  of a pair.

### ffplay and "RTP: dropping old packet received too late"

That warning is a receiver-side quirk, not stream corruption. ffmpeg's RTP
reorder buffer compares each packet's 16-bit sequence number to the last one it
emitted (`diff = seq - s->seq`) and drops anything that looks older; on a
restart, or under reordering/loss, many builds get stuck dropping every packet.
Raw MPEG-TS over UDP and a stdout pipe carry no RTP sequence numbers, so they
can't hit it: prefer `-` (stdout) or plain UDP (no `--rtp`) for soft players like
ffplay/VLC. If you do need RTP into ffplay, disable its reorder buffer:

```bash
ffplay -reorder_queue_size 0 -fflags nobuffer -i 'rtp://239.0.0.1:5000'
```

RTP is still the right choice for a hardware IRD that expects it; the reorder
buffer is specific to ffmpeg's software receiver.

### Offline CBR shaping (file in, CBR file out)

The `cbr_file` example paces a `.ts` file deterministically (no sockets, no wall
clock), which is what the compliance harness runs:

```bash
# Explicit rate:
cargo run -p mpegts-pacer --example cbr_file -- in.ts out.ts 12000000 regenerate
# Or derive it from the file:
cargo run -p mpegts-pacer --example cbr_file -- in.ts out.ts auto

# Verify byte-locked PCR accuracy:
tsp -I file out.ts -P pcrverify --jitter-max 500 -O drop
```

```text
cbr_file <in.ts> <out.ts> <bitrate_bps|auto> [preserve|regenerate]
```

## Compliance

`test/` runs a self-contained TSDuck-based compliance harness (PCR accuracy,
bitrate stability, continuity, PAT/PMT integrity, TR 101 290). It generates or
takes a `.ts`, paces it through the `cbr_file` example, and asserts the output is
something a hardware IRD accepts. Run it via:

```bash
./test/run.sh                     # generate a clip, pace it, analyze
./test/run.sh --source cap.ts     # pace a real capture instead
./test/run.sh --strict            # also fail on broadcast-shape warnings
```

TSDuck (`tsp`, `tsanalyze`) and, for the generated-clip mode, `ffmpeg` must be on
`PATH`. See [`test/README.md`](test/README.md) for details.

## Roadmap

Stream clocking now has its receiver-side proof against a software ST 2022-7
receiver: two pacers on independent MoQ chains, sharing no process, clock or
messages, emit byte-identical datagrams under identical RTP sequence numbers, and
the merged output loses nothing across leg blackout, 1 % and 3 % path loss, 50 ms
differential delay, and the death of either chain's publisher, relay or
subscriber. The remaining proof is a hardware IRD's own merge engine.

Beyond it, the `Source` / `Sink` split keeps the door open for FEC, SRT/RIST
output adapters, SCTE-35 splice monitoring, and NOC telemetry, none of which the
core pacer needs to know about.
