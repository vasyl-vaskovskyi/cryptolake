# Backup Collector with Gap Recovery Design

**Date:** 2026-04-03
**Status:** Draft

## Overview

A redundant WebSocket collector that maintains independent connections to Binance, writing to short-retention backup Redpanda topics. When the primary collector disconnects, the writer recovers missing data from the backup topics before emitting gap envelopes. This eliminates most data loss for non-backfillable streams (depth, bookticker) where REST API recovery is impossible.

## Goals

- Eliminate data loss from single-connection WebSocket drops (the #1 cause of gaps)
- Recover depth and bookticker data that cannot be backfilled from REST
- Zero changes to the primary collector's behavior — backup is purely additive
- Minimal resource overhead on an 8GB VPS

## Non-Goals

- Geographic redundancy (single VPS deployment)
- Protection against full VPS network outages (both connections share the same physical interface)
- Replacing REST backfill for trades (backup is for real-time recovery, REST is for historical)

## Architecture

### Backup Collector

A second Docker container running the same collector code with different configuration:

- **collector_id:** `binance-collector-backup`
- **topic_prefix:** `backup.` — all Redpanda topics prefixed (e.g., `backup.binance.trades`, `backup.binance.depth`)
- **Docker network:** `backup_egress` (new, separate bridge) + `cryptolake_internal` (shared, for Redpanda access)
- **Prometheus metrics:** port 8004
- **Full collector instance** — independent WebSocket connections, depth resync, open_interest polling, health checks

The only code change to the collector: the producer needs a configurable `topic_prefix` field. Currently topics are hardcoded as `{exchange}.{stream}`. With prefix, they become `{topic_prefix}{exchange}.{stream}`. Default prefix is empty string (primary behavior unchanged).

### Backup Redpanda Topics

- **Naming:** `backup.binance.trades`, `backup.binance.depth`, `backup.binance.bookticker`, etc.
- **Retention:** 30 minutes — covers any realistic reconnect + resync time (longest observed gap was 28 seconds)
- **Auto-created** by the producer (Redpanda has `auto_create_topics_enabled=true`)
- **Retention configuration:** Set via `rpk` command after Redpanda starts

### Gap Recovery in the Writer

When the writer detects a gap (about to emit a gap envelope), it attempts recovery from backup topics first.

**Recovery flow:**

1. Writer detects gap (ws_disconnect, session_seq_skip, pu_chain_break, etc.) with `gap_start_ts` and `gap_end_ts`
2. Before writing the gap envelope, calls `recover_from_backup(stream, symbol, gap_start_ts, gap_end_ts)`
3. Creates a temporary Redpanda consumer for the corresponding `backup.*` topic
4. Seeks to the offset nearest `gap_start_ts`, reads records until `gap_end_ts`
5. Filters to only records within the gap window
6. Deduplicates by natural key:
   - trades: aggregate trade ID `a`
   - depth: update ID `u`
   - bookticker: update ID `u`
   - others: `exchange_ts`
7. Based on coverage:
   - **Full recovery:** backup records cover the entire gap → writes records, no gap envelope
   - **Partial recovery:** backup covers part of the gap → writes available records, emits narrowed gap envelope for the unrecovered portion
   - **No recovery:** backup has nothing → emits original gap envelope unchanged

**Edge cases:**

- Backup topic doesn't exist → skip recovery, emit gap
- Backup topic has no data for the window → skip recovery, emit gap
- Backup has partial data → write what's available, emit narrowed gap envelope with adjusted `gap_start_ts` set to last backup record's timestamp + 1

### Partial Recovery and Narrowed Gaps

When the backup has data for only part of the gap window:

- Primary gap: 12:21:34 → 12:22:02 (28 seconds)
- Backup has records: 12:21:34 → 12:21:56 (22 seconds)
- Writer writes backup records, emits narrowed gap: 12:21:56 → 12:22:02 (6 seconds)

## Infrastructure

### Docker Network

New network for backup collector egress:

```yaml
backup_egress:
  driver: bridge
```

Separate from `collector_egress` — different Docker bridge, different source port range. Both route through the same physical NIC but have maximum isolation Docker can provide on a single host.

### Docker Service

```yaml
collector-backup:
  build:
    context: .
    dockerfile: Dockerfile.collector
  depends_on:
    redpanda:
      condition: service_healthy
  networks:
    - cryptolake_internal
    - backup_egress
  environment:
    - CONFIG_PATH=/app/config/config.yaml
    - COLLECTOR_ID=binance-collector-backup
    - TOPIC_PREFIX=backup.
  ports:
    - "127.0.0.1:8004:8000"
  healthcheck: (same as primary collector)
  restart: unless-stopped
```

### Prometheus

New scrape target:

```yaml
- job_name: collector-backup
  static_configs:
    - targets: ["collector-backup:8000"]
```

### Redpanda Topic Retention

Set 30-minute retention on backup topics after creation:

```bash
rpk topic alter-config backup.binance.trades --set retention.ms=1800000
rpk topic alter-config backup.binance.depth --set retention.ms=1800000
# ... for all backup topics
```

This can be done via an init script or as part of the backup collector startup.

## Configuration Changes

### Producer Config

New optional field in producer/config:

- `topic_prefix: str = ""` — prepended to all topic names. Primary collector uses default (empty). Backup collector sets `"backup."`.

### Writer Config

New optional field:

- `backup_topic_prefix: str = "backup."` — tells the writer where to look for backup data during gap recovery.

## Prometheus Metrics

New writer metrics for backup recovery:

| Metric | Type | Description |
|--------|------|-------------|
| `writer_backup_recovery_attempts_total` | Counter | Total recovery attempts |
| `writer_backup_recovery_success_total` | Counter | Full recoveries (no gap envelope needed) |
| `writer_backup_recovery_partial_total` | Counter | Partial recoveries (narrowed gap) |
| `writer_backup_recovery_miss_total` | Counter | Backup had no data |

## Module Changes

### Collector (minimal)

- `src/collector/producer.py` or `src/common/config.py` — add `topic_prefix` field
- Producer topic construction: `f"{self.topic_prefix}{exchange}.{stream}"`

### Writer (main changes)

- New function: `recover_from_backup(stream, symbol, gap_start_ts, gap_end_ts)` in consumer module
- Modify gap emission code path to call recovery first
- New temporary Redpanda consumer creation for backup topics
- Deduplication logic by natural key per stream type

### Infrastructure

- `docker-compose.yml` — new `collector-backup` service, new `backup_egress` network
- `infra/prometheus/prometheus.yml` — new scrape target
- Topic retention init script or startup hook

## Resource Impact

On an 8GB VPS:

- **Memory:** ~100-200MB additional (second collector process + second set of WebSocket buffers)
- **CPU:** minimal (WebSocket receive is I/O-bound)
- **Network:** 2x WebSocket bandwidth (~10-50 Mbps depending on market activity)
- **Redpanda storage:** ~500MB for 30 min of backup across all streams (auto-cleaned by retention)
- **Total overhead:** ~300-700MB memory, negligible CPU and disk
