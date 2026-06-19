# Event-time bucketing, lossless late frames, and per-frame receive-time

**Date:** 2026-06-20
**Status:** Design — pending review
**Affects:** `collector`, `common`; downstream readers (`sealer`, `verify`) for the new line format
**Amends:** master spec §8.c, §8.e, invariant 3.a, manifest schema §10.d (see §7 below)

## 1. Motivation

The skeleton Collector buckets every captured frame by **local receive time** and stores
each WebSocket frame as bare wire bytes. Both choices block correct downstream use:

1. **Receive-time bucketing mis-files events.** The same logical event can land in
   different minute files across two parallel sockets during a rotation overlap, and an
   event's file no longer reflects when it happened on the exchange.
2. **No receive-time is recorded for WS frames.** A trading model needs to know not only
   *when an event happened* (the exchange timestamp) but *when we actually had it in hand*.
   Without receive-time, a backtest cannot tell which records a live strategy could have
   acted on, so it silently assumes instant delivery and overstates its edge.
3. **The skeleton's plan was to discard late frames.** Discarded records are not random —
   late arrivals cluster during the exact volatile bursts a strategy most needs to learn
   from. Throwing them away punches holes in the data precisely where it matters.

This design changes capture to: bucket by **server event time**, **never discard** a frame,
and record a **per-frame receive timestamp** by wrapping each WS frame in a capture envelope.

## 2. Guiding principle

The minute files are scratch storage; the **hour file is the unit that matters** and is the
point at which we make a "this period is complete" commitment. Three facts drive the design:

- Every record carries its own exchange timestamp, so the true order is always
  reconstructable regardless of which file a record physically sits in.
- A late record is **self-evident in the raw data**: its exchange timestamp jumps backwards
  relative to the records written just before it. No inline flag is required for correctness.
- Therefore placement is **best-effort**, never lossy. We place each frame in the minute file
  matching its event time when we can; if its target minute is already sealed we still keep
  the frame (it merges into the same hour and stays self-identifying via its own timestamp).
  Lateness only causes real trouble when a record crosses an *already-finalized* boundary —
  the hour boundary — which a later grace-window phase tightens (§8).

## 3. Scope

**In scope (this increment):**
- a. Per-frame WS **capture envelope** carrying `received_at` + the verbatim raw frame.
- b. **Server-event-time bucketing** (`E`, or `T` for `trade`/`aggTrade`).
- c. **Lossless late frames** — never discard; count late frames for observability.

**Out of scope (deferred, named so the boundary is explicit):**
- Frame-buffer + seal-grace windows (master spec §8.e) — the mechanism that *prevents* most
  late frames by holding a minute open past its boundary. This design makes late frames
  *lossless*; the grace window later makes them *rare*. The two compose.
- Per-minute late-overflow files that re-file a straggler into its own correct minute.
- Hot-swap / WS rotation overlap merge (separate design, 2026-06-09).

## 4. Design

### 4.a WS capture envelope

Each WebSocket text frame is wrapped in a one-line JSON envelope before it is written. The
raw frame is stored **verbatim as a string** so the exact wire bytes survive (parsing and
re-serializing would lose key order and whitespace). An inner `raw_sha256` lets a consumer
prove the inner frame is untampered independent of the envelope.

```json
{
  "envelope": "ws_frame",
  "received_at": "2026-06-20T14:23:15.182Z",
  "raw_sha256": "9f2c…",
  "raw": "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":1750000000182,\"T\":1750000000180,\"t\":145003,...}}"
}
```

- `envelope` — discriminator, mirrors the existing REST `"rest_response"` envelope.
- `received_at` — ISO-8601 UTC instant captured **at the moment the frame arrives**, inside
  the WS read loop, before any queueing, so it reflects true receive time.
- `raw_sha256` — lowercase hex SHA-256 over the UTF-8 bytes of the `raw` string.
- `raw` — the exact WS text frame, unmodified, as a JSON string value.

REST poll responses keep their existing `rest_response` envelope unchanged (it already
carries `received_at`).

### 4.b Server-event-time bucketing

A frame's minute file is chosen by its **exchange event timestamp**, extracted from the
parsed wire frame:

| Stream | Bucket field |
|---|---|
| `trade`, `aggTrade` | `data.T` (trade time) |
| everything else (`depth`, `bookTicker`, `kline_*`, `ticker`, `markPrice`, `miniTicker`) | `data.E` (event time) |
| `forceOrder` broadcast (`!forceOrder@arr`) | `data.E`; routing symbol still `data.o.s` |

A new `common` helper isolates this rule:

```
EventTime.bucketInstant(String streamType, JsonNode wrapper) -> Instant
```

REST responses continue to bucket by **poll-issue wall-clock time** (single-source, no
overlap concern) — unchanged from today.

### 4.c Lossless late frames

`MinuteSegmentWriter` keeps one open minute buffer and decides placement from the supplied
bucket instant rather than the wall clock:

- target minute **>** current open minute → seal current, open target (normal advance).
- target minute **==** current → append.
- target minute **<** current (a straggler whose minute is already sealed) → **append to the
  current open minute and increment a `lateFrames` counter.** The frame is never discarded;
  its own event timestamp + `received_at` make the lateness fully recoverable downstream.

This holds losslessly within an hour (all minutes merge into the same hour file). A straggler
that arrives after its *hour* has rolled over lands in the next hour's file — still kept, not
lost, recoverable via its timestamp — and the deferred grace window (§8) removes that case.

### 4.d Component changes

- **`common/EventTime`** (new) — pure `(streamType, wrapper) -> Instant` extractor. Unit-testable in isolation.
- **`common/CaptureEnvelope`** (new) — builds the `ws_frame` envelope line from raw text +
  `received_at`, computes `raw_sha256`, uses the shared `EnvelopeCodec` mapper.
- **`MinuteSegmentWriter`** — `accept(byte[] line, Instant bucketInstant)`; bucketing no longer
  reads an internal clock; adds the straggler branch + `lateFrames` counter exposed via a getter.
- **`BinanceWsClient`** — frame consumer becomes `(rawText, receivedAt)` so receive-time is
  captured at the read loop, not reconstructed later.
- **`Main.onFrame`** — capture `received_at`, parse the wire frame once for routing (`stream`,
  and `data.o.s` for `forceOrder`) and for `EventTime.bucketInstant`, build the envelope via
  `CaptureEnvelope`, then `writer.accept(envelopeLine, eventInstant)`.
- **REST sink** — `RestPoller`'s envelope is unchanged; `Main` calls
  `writer.accept(restEnvelopeBytes, pollIssuedInstant)`.
- **Downstream readers** (`sealer` gap-detection, `verify`) — sequence IDs now live at
  `raw.data.<id>` inside the envelope rather than `data.<id>`; extraction paths updated.

## 5. Data flow

```
WS text frame ──▶ BinanceWsClient (capture received_at) ──▶ Main.onFrame
   parse wire frame once:
     • stream            → which writer
     • data.o.s          → forceOrder symbol routing
     • EventTime.bucket  → which minute
   CaptureEnvelope.build(rawText, received_at) → envelope line
   writer.accept(envelopeLine, eventInstant)
        └─ place by eventInstant (lossless; late → current minute + lateFrames++)
```

## 6. Error handling & edge cases

- **Unparseable wire frame** — cannot extract `E`/`T`. Fall back to bucketing by `received_at`
  (wall clock), still wrap and **keep** the frame, and count it (`unparseableFrames`). Never discard.
- **Missing `E`/`T` field** — same fallback as unparseable: bucket by `received_at`, keep, count.
- **`forceOrder` missing `data.o.s`** — existing behavior (log + drop) is retained only because
  the symbol is unroutable; revisit if it ever fires in practice.
- **Cross-hour straggler** — kept in the next hour's file, recoverable via its event timestamp;
  fully removed once the grace window (§8) lands.
- **Clock for `received_at`** — node wall clock (UTC, `TZ=UTC`); bucketing uses the frame's own
  timestamp, so an NTP step affects only `received_at`, not placement.

## 7. Spec amendments (apply when this design is accepted)

- **§8.c (Raw capture)** — WS frames are wrapped in a `ws_frame` capture envelope. Reword from
  "written as-is (raw bytes, not re-serialized)" to "the exact wire bytes are preserved
  **verbatim inside** the envelope's `raw` field, with `received_at` and `raw_sha256` alongside."
- **§8.e (Minute-segment rotation)** — replace "Any frame arriving after seal is **dropped** and
  recorded in `late_frames_dropped`" with "late frames are **kept** (placed in the current open
  minute) and counted in `late_frames`." The grace window remains the deferred mechanism that
  keeps `late_frames` near zero.
- **Invariant 3.a (raw fidelity)** — from "the stored line *is* the wire bytes" to "the stored
  line *contains* the wire bytes verbatim (recoverable byte-for-byte via `raw`/`raw_sha256`)."
- **Manifest §10.d** — `late_frames_dropped` → `late_frames` (kept, not dropped); add
  `unparseable_frames`.

## 8. Deferred: how the grace window completes this

This increment makes late frames **lossless but possibly mis-placed by minute**. The deferred
grace window holds each minute open ~10s past its boundary so near-boundary stragglers land in
their correct minute *before* seal, driving `late_frames` toward zero and eliminating the
cross-hour case. Order matters: lossless-first (here), rare-second (grace window). The capture
envelope and event-time bucketing built here are prerequisites for the rotation overlap merge.

## 9. Testing (TDD — test-first per CLAUDE.md §14.k)

- `EventTimeTest` — `T` for trade/aggTrade, `E` otherwise, `E` for forceOrder; missing-field path.
- `CaptureEnvelopeTest` — `raw` is byte-verbatim; `raw_sha256` matches; round-trips through the codec.
- `MinuteSegmentWriterTest` — bucket by supplied instant (not wall clock); straggler appended to
  current minute + `lateFrames` increments; existing rotation test migrated to the new signature.
- `BinanceWsClient*Test` — consumer receives `(rawText, receivedAt)`; receive-time populated.
- Collector integration — a frame whose event time precedes the open minute is kept and counted;
  unparseable frame falls back to `received_at` and is kept.

## 10. Open questions

- Whether `raw_sha256` is worth its per-frame cost, or should be sealer-time only. (Leaning: keep
  it — cheap, and it preserves inner-frame provability that the envelope otherwise dilutes.)
- Exact manifest surfacing of `late_frames` / `unparseable_frames` (counts only, or with sample ids).
