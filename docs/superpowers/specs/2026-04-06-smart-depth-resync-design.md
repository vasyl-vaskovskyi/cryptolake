# Smart Depth Resync Design

**Date:** 2026-04-06
**Status:** Draft

## Overview

When the collector starts or reconnects its WebSocket, it currently always fetches a depth snapshot from Binance REST API and resets the pu chain. This is unnecessary when the backup collector kept the depth diff stream alive — the data is continuous, and the new snapshot creates a sync point misalignment that `cryptolake verify` flags as an error.

Skip the resync snapshot when the other collector's depth topic has recent diffs. Seed the pu chain from the backup's last update ID instead.

## Goals

- Eliminate false depth sync point errors in `cryptolake verify` after failover
- Reduce unnecessary Binance REST API calls on collector restart
- Maintain depth data integrity — pu chain validation still catches real breaks

## Non-Goals

- Changing periodic snapshots (every 5m) — these are archive checkpoints, unchanged
- Changing the writer or failover logic — this is collector-only
- Generic state topic in Redpanda — solve the specific problem first

## Current Behavior

Every call to `_depth_resync(symbol)` in `connection.py`:

1. Wait for producer health
2. Reset depth detector (`_synced=False`)
3. Fetch snapshot from Binance REST (3 retries)
4. Parse `lastUpdateId` from snapshot
5. Call `set_sync_point(lastUpdateId)` — replays buffered diffs
6. Publish snapshot to `depth_snapshot` topic

This happens on:
- WebSocket connect (every startup)
- WebSocket disconnect/reconnect
- pu chain break detected

## New Behavior

Before step 3 (fetch snapshot), add a check:

1. Read the last few messages from the **other** collector's depth topic, filtered by symbol
2. If a recent diff exists (received_at within 30 seconds of now):
   - Extract `u` (final update ID) from the diff
   - Call `set_sync_point(u)` using the backup's last update ID
   - Replay buffered diffs against it
   - Log `depth_resync_skipped_snapshot` and return (no REST call, no snapshot published)
3. If no recent diff found: fall through to existing snapshot fetch logic (unchanged)

### Topic Selection

The collector checks the **other** collector's topic:
- Primary collector (`TOPIC_PREFIX=""`) reads from `backup.binance.depth`
- Backup collector (`TOPIC_PREFIX="backup."`) reads from `binance.depth`

Derived as: if own prefix is empty, check `backup.{exchange}.depth`; if own prefix is `backup.`, check `{exchange}.depth` (strip prefix).

### Reading the Backup Topic

Create a temporary Kafka consumer, seek to end minus 30 seconds (by timestamp), poll for messages matching the target symbol. Extract `u` from the last matching message's `raw_text` JSON.

Close the consumer immediately after — this is a one-shot read, not a persistent subscription.

## Edge Cases

**Backup not running:** No recent messages found → normal snapshot fetch. No harm.

**Both collectors starting simultaneously:** Neither has recent data in the other's topic → both do normal snapshot fetch.

**Backup data stale (> 30s):** Fall through to normal snapshot. The stream was genuinely interrupted.

**Seeded chain doesn't align with incoming diffs:** The pu chain validation catches it — first diff's `pu` won't match seeded `last_u` → chain break → triggers another resync → falls through to snapshot (since backup check already failed once).

**Periodic snapshots:** Unchanged. They don't call `set_sync_point()` — they're archive checkpoints only. The `NOTE` comment in `snapshot.py` lines 156-159 documents this explicitly.

## Code Changes

### Modify

- `src/collector/connection.py`: `_depth_resync()` method — add backup topic check before snapshot fetch

### Add

- `src/collector/backup_chain_reader.py`: Small module with `read_last_depth_update_id(brokers, backup_topic, symbol, max_age_seconds=30) -> int | None` — reads backup topic, returns last `u` or None

### Keep Unchanged

- `src/collector/streams/depth.py` — DepthHandler, set_sync_point, pu validation
- `src/collector/snapshot.py` — periodic snapshot scheduler
- `src/collector/gap_detector.py` — DepthGapDetector validation logic
- All writer code

## Testing

### Unit Tests

- `read_last_depth_update_id` returns correct `u` from mocked Kafka messages
- `read_last_depth_update_id` returns None when no recent messages
- `read_last_depth_update_id` returns None when topic doesn't exist
- `read_last_depth_update_id` filters by symbol correctly
- `_depth_resync` skips snapshot when backup has recent data
- `_depth_resync` falls through to snapshot when backup has no data
- pu chain works correctly when seeded from backup's `u`

### Chaos Test Update

- Update `tests/chaos/17_collector_failover_to_backup.sh`: after switchback, `cryptolake verify --full` should pass (no depth sync point errors)
- Update `tests/chaos/1_collector_unclean_exit.sh`: verify the restarted collector skips snapshot when backup is running
