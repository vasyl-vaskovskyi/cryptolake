# Gap Filter via Backup Coverage Design

**Date:** 2026-04-08
**Status:** Draft

## Overview

Stop writing collector-emitted gap envelopes to the archive when the other collector already delivered data for the same window. Only persist a gap when **neither** collector had data — a true bilateral outage.

## Core Principle

> A gap belongs in the archive if and only if neither collector delivered data for that window. Collector gap reasons (`ws_disconnect`, `snapshot_poll_miss`, `pu_chain_break`, etc.) are hints about what went wrong upstream, not the archive's ground truth.

## Goals

- Zero false-positive gaps in the archive when the surviving collector covered the window.
- Symmetric: primary gap envelopes filtered against backup data **and** vice versa.
- Coalesce the stacked `ws_disconnect` records emitted per reconnect retry into a single pending entry.
- Chaos test 10 proves the invariant: chaos lands on primary, backup unaffected, archive has 0 collector gaps and continuous data.

## Non-Goals

- Writer-emitted gaps (`checkpoint_lost`, `restart_gap`, `write_error`, corrupt envelopes). These are observed by the writer itself, not claimed by a collector, and have no "other source" to check.
- REST backfill of true bilateral outages (separate feature).
- Sub-second gap resolution — grace period is 10s by default.

## Current State

Eight collector-side gap emission sites, all routed through `src/collector/producer.py:181` `emit_gap()`:

| File:line | Reason |
|---|---|
| `src/collector/connection.py:298` | `ws_disconnect` (fires once per reconnect-loop iteration → stacks) |
| `src/collector/connection.py:208` | `pu_chain_break` (resync producer unhealthy) |
| `src/collector/connection.py:270` | `pu_chain_break` (resync retries exhausted) |
| `src/collector/snapshot.py:132` | `snapshot_poll_miss` (depth snapshot) |
| `src/collector/streams/open_interest.py:117` | `snapshot_poll_miss` (open interest) |
| `src/collector/streams/depth.py:117` | `pu_chain_break` (pu mid-stream break) |
| `src/collector/streams/depth.py:142` | `pu_chain_break` (stale update_id) |
| `src/collector/streams/base.py:39` | `session_seq_skip` (internal seq tracker) |

These produce envelopes with `type=gap` onto the collector's topic. The writer currently consumes them and persists them verbatim, ignoring what the other collector may have delivered for the same window.

Writer-emitted gaps (6 sites in `src/writer/`) are out of scope.

## Design

### Coverage Tracker

A new component in `src/writer/failover.py` tracks the latest `received_at` seen per source, per stream:

```
_last_received_by_source: dict[tuple[str, str, str], dict[str, int]]
  key: (exchange, symbol, stream)
  value: {"primary": <ns>, "backup": <ns>}
```

Updated on every **data** envelope arrival, by source. Gap envelopes never update it.

This replaces `FailoverManager._last_received` (a single per-stream value, currently read only by `FailoverManager.activate()` to compute seek timestamps — see `src/writer/failover.py:151-157`). The seek-time computation should use `max(primary, backup)` over the new dict to preserve existing behavior.

### Pending Gap Queue

A second structure holds incoming gap envelopes until their fate is decided:

```
_pending_gaps: dict[tuple[str, tuple[str, str, str], int], dict]
  key: (source, stream_key, gap_start_ts)
  value: the gap envelope (with latest gap_end_ts)
```

Keyed on `gap_start_ts` so that a new gap arriving with the same start (from a stacked `_emit_disconnect_gaps` call) simply updates `gap_end_ts` in place. This is the natural coalescing.

### Decision Flow

On every incoming envelope (from either source):

**Data envelope:**
1. Update `_last_received_by_source[stream_key][source] = received_at`.
2. Sweep `_pending_gaps` for that stream_key: drop any gap (from *either* source) whose `gap_end_ts <= received_at`. The newly-seen data covers those windows.
3. Write the data envelope as usual.

**Gap envelope:**
1. Let `other_source = "backup" if source == "primary" else "primary"`.
2. Lookup `other_received_at = _last_received_by_source[stream_key].get(other_source, 0)`.
3. If `other_received_at >= gap_end_ts` → drop the gap immediately (already covered).
4. Else, insert into `_pending_gaps`. If a pending entry exists for the same `(source, stream_key, gap_start_ts)`, update its `gap_end_ts` to `max(old, new)`.

**Periodic sweep** (amortized — no hard per-iteration requirement, but at least once per flush interval):
For each entry in `_pending_gaps` whose `(now - first_seen) >= grace_period`:
- Write it to the archive (the data-envelope path has already had `grace_period` seconds to drop it; anything still in the queue is a real bilateral outage).
- Remove it from pending.

### Interaction with Failover

The coverage tracker works regardless of failover state — it's fed by every envelope the writer consumes. In normal operation the writer only consumes primary, so only `primary` entries advance; backup gap envelopes can't arrive (writer isn't polling backup) so the filter has nothing to do.

During failover both sources feed the tracker (primary via probe polls, backup via active consumption). This is when the filter becomes load-bearing.

### Supporting Fix: Silence Timer

`src/writer/consumer.py:827` currently calls `_failover.reset_silence_timer()` on every primary envelope. Change: skip the reset when `envelope["type"] == "gap"`. Rationale — a primary emitting gap envelopes is *not* a healthy primary; keeping the silence timer running is what gets failover activated, which is what gets backup data flowing into the coverage tracker.

Without this fix, the filter can still run, but during a long primary outage where primary emits periodic gap envelopes, failover may never activate, the `backup` side of the coverage tracker never advances, pending gaps age out, and all the gaps get written — defeating the filter.

### Supporting Fix: Collector Coalescing (cleanup, non-blocking)

`src/collector/connection.py:130-143` — `_emit_disconnect_gaps()` is called on every iteration of the reconnect loop, producing a new gap record per failed retry. Change: emit the gap once on disconnect; on every subsequent retry, extend the existing pending disconnect's end time instead of emitting a fresh envelope. Emit a final closing envelope only when reconnection succeeds (with the true `gap_end_ts`).

This reduces topic noise but is not load-bearing — the writer's pending queue coalesces stacked gaps correctly via the `gap_start_ts`-keyed dict.

## Configuration

New writer config setting:

```yaml
writer:
  gap_filter:
    grace_period_seconds: 10
```

Default 10s. Must be ≥ 2× `FailoverManager.silence_timeout` (currently 5s, so minimum 10s) — below that, a pending gap may age out before failover has had time to activate and start feeding backup data into the coverage tracker. Setting `grace_period_seconds: 0` disables the filter entirely (writes all gaps verbatim, current behavior) — useful as a feature flag.

## Observability

New Prometheus metrics (writer):

- `writer_gap_envelopes_suppressed_total{source, reason}` — counter, gap envelopes dropped as covered.
- `writer_gap_envelopes_written_total{source, reason}` — counter, gap envelopes that survived to disk (bilateral outage).
- `writer_gap_pending_size` — gauge, current pending-queue length.
- `writer_gap_coalesced_total{source}` — counter, gap envelopes merged into an existing pending entry.

## Chaos Test 10 Changes

Current test only asserts `snapshot_poll_miss` gaps exist. After this change, the test inverts to prove the filter worked:

1. **Chaos landed on primary**: scrape primary's `/metrics` delta; assert `collector_ws_reconnects_total` ↑ ≥ 1 and `collector_gaps_detected_total{reason="ws_disconnect"}` ↑ ≥ 1.
2. **Backup unaffected**: scrape backup's `/metrics` delta; assert both counters above unchanged.
3. **Archive is clean**: `count_gaps "ws_disconnect" == 0` and `count_gaps "snapshot_poll_miss" == 0`.
4. **Data is continuous**: for each continuous stream (bookticker, trades, depth), assert no per-stream inter-record interval > 5s inside `[event_start_ns, event_end_ns]`.

Assertion 4 is critical — it catches the regression where the filter suppresses a gap but the writer also fails to ingest the backup data.

## Rollout

- Feature-flag behavior by `grace_period_seconds`: setting it to 0 reverts to current (write-all-gaps) behavior. Useful for A/B comparison in production.
- No migration — purely additive state in the writer.
- Chaos tests cover the happy path. Prod monitoring on `writer_gap_envelopes_suppressed_total` and `writer_gap_envelopes_written_total` will show the filter's effect on real traffic.
