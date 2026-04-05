# Writer Real-Time Failover Design

**Date:** 2026-04-06
**Status:** Draft

## Overview

Replace the retrospective backup recovery (read old backup data after gap detected) with real-time failover: when the primary collector stops publishing, the writer switches to consuming from the backup topic until the primary resumes. No mixing of data sources, no dedup, no ordering issues.

## Goals

- Zero-duplicate data during failover and switchback
- Seamless continuation — records flow in natural key order from one source at a time
- No changes to either collector — only the writer changes
- Analyze and integrity scripts show identical results

## Non-Goals

- Handling both collectors down simultaneously (real gap, REST backfill later)
- Sub-second failover (5-second timeout is acceptable)

## Architecture

### Normal Operation

```
Primary collector → WebSocket → binance.* topics → Writer consumes
Backup collector  → WebSocket → backup.* topics  → (not consumed)
```

### During Failover

```
Primary collector → (down)
Backup collector  → WebSocket → backup.* topics → Writer consumes
```

### After Switchback

```
Primary collector → WebSocket → binance.* topics → Writer consumes
Backup collector  → WebSocket → backup.* topics  → (not consumed)
```

The writer reads from ONE source at a time. Never both simultaneously for the same stream.

## Failover Detection

**Trigger:** If `poll()` on the primary consumer returns no messages for 5 consecutive seconds, activate failover.

**Implementation:** Track `_no_message_since` as a monotonic timer. Reset on every successful poll. When `monotonic() - _no_message_since > 5.0`, create backup consumer and switch.

## Backup Consumer

**Creation:** On first failover, create a Kafka consumer subscribed to all `backup.*` topics. Keep alive until primary recovers.

**Seeking:** For each stream, seek to the backup topic at timestamp = `_last_received[stream] - 10_seconds`. The 10-second margin handles clock skew between collectors.

**Filtering:** After seeking, skip backup records with natural key ≤ `_last_key[stream]`. This ensures exact continuity from where the primary left off.

**Natural key per stream:**
- trades: aggregate trade ID `a`
- depth: update ID `u`
- bookticker: update ID `u`
- funding_rate: `exchange_ts`
- liquidations: `exchange_ts`
- open_interest: `exchange_ts`
- depth_snapshot: `exchange_ts`

## Switchback

**Detection:** While reading from backup, also poll the primary consumer every 1 second. When primary returns a message, switchback begins.

**Filtering:** The first few primary records after switchback might overlap with backup records just consumed. Filter primary records with natural key ≤ `_last_key[stream]` until a higher key appears. Then stop filtering and resume normal operation.

**Cleanup:** Close backup consumer after switchback completes.

## Key Tracking

Per-stream state maintained during consumption:

```python
_last_key: dict[tuple[str, str, str], int]      # (exchange, symbol, stream) -> last natural key
_last_received: dict[tuple[str, str, str], int]  # (exchange, symbol, stream) -> last received_at ns
```

Updated on every consumed record regardless of source (primary or backup).

## Gap Envelope Emission

- If backup covers the gap seamlessly (natural keys continuous from primary to backup), no gap envelope emitted
- If there is a gap between last primary key and first available backup key, emit a gap envelope covering that period
- If both collectors are down, emit a gap envelope for the entire outage period

## Consume Loop

```
while running:
    if not failover_active:
        msg = primary_consumer.poll(1.0)
        if msg:
            reset no_message_timer
            extract and track natural key
            process(msg)
        else:
            if time since last message > 5 seconds:
                activate_failover()
    else:
        # Read from backup
        msg = backup_consumer.poll(0.5)
        if msg and natural_key(msg) > _last_key[stream]:
            process(msg)

        # Check if primary is back
        primary_msg = primary_consumer.poll(0.1)
        if primary_msg:
            if natural_key(primary_msg) > _last_key[stream]:
                process(primary_msg)
            deactivate_failover()
```

## Edge Cases

**Both collectors down:** Primary times out → switch to backup → backup also empty. Writer keeps polling both. When either comes back, writer resumes. Gap is real — REST backfill later.

**Low-frequency streams (open_interest, depth_snapshot):** 5-second timeout triggers but backup has nothing new either. Writer polls backup, gets nothing, keeps trying. When primary resumes, switches back. Gap envelope emitted if needed.

**Rapid primary flapping:** Primary restarts repeatedly. Writer switches between primary and backup. Natural key filter ensures no duplicates regardless of switching frequency.

**Writer restart during failover:** `_last_key` is lost. On startup, reads last record from archive files (existing `_check_recovery_gap` logic) to reconstruct `_last_key`. Then normal failover detection kicks in.

**Clock skew between collectors:** The 10-second seek-back margin on backup topic handles this. Natural key filtering ensures exact cutoff.

## Code Changes

### Remove

- `src/writer/backup_recovery.py` — entire file
- `_try_backup_recovery()` method in `consumer.py`
- Backup recovery hooks in both gap emission paths (recovery gap + session change)
- `backup_brokers` / `backup_topic_prefix` params from WriterConsumer.__init__
- `backup_brokers` / `backup_topic_prefix` from Writer.main.py
- Old backup recovery Prometheus metrics (attempts/success/partial/miss)

### Add to `src/writer/consumer.py`

- `_last_key: dict` — last natural key per stream, updated on every record
- `_last_received: dict` — last received_at per stream
- `_backup_consumer` — Kafka consumer for backup topics, created on failover
- `_failover_active: bool` — current state
- `_no_message_since: float` — monotonic timer
- `_extract_natural_key(envelope) -> int` — extracts stream-appropriate key
- `_activate_failover()` — creates backup consumer, seeks by timestamp
- `_deactivate_failover()` — closes backup consumer, resets state
- Modified consume loop with dual-poll logic

### Keep Unchanged

- Backup collector — still publishes to `backup.*` topics continuously
- `backup.*` topic retention (30 min)
- Docker network setup (`backup_egress`)
- Backup collector Prometheus metrics and health dashboard BKUP indicator

## Prometheus Metrics

### Add

| Metric | Type | Description |
|--------|------|-------------|
| `writer_failover_active` | Gauge | 1 if reading from backup, 0 if primary |
| `writer_failover_total` | Counter | Total failover activations |
| `writer_failover_duration_seconds` | Histogram | Duration of each failover |
| `writer_failover_records_total` | Counter | Records consumed from backup |
| `writer_switchback_total` | Counter | Successful switchbacks to primary |

### Remove

| Metric | Reason |
|--------|--------|
| `writer_backup_recovery_attempts_total` | Replaced by failover metrics |
| `writer_backup_recovery_success_total` | No longer applicable |
| `writer_backup_recovery_partial_total` | No longer applicable |
| `writer_backup_recovery_miss_total` | No longer applicable |

### Update Health Dashboard

Replace "Backup Recovery /12h" textbox with "Failover Status" showing active state, total failovers, and records from backup.
