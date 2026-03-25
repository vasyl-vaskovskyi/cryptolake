# Replace Grafana with Sampler CLI

## Problem

Grafana requires port 3000 to be exposed on the VPS, which is undesirable for a production server accessed only via SSH. A terminal-native monitoring tool eliminates the need for any web UI port.

## Solution

Replace Grafana with [sampler](https://github.com/sqshq/sampler) (v1.1.0) — a pre-built Go binary that renders real-time terminal dashboards from YAML configuration. Each sampler widget runs a shell command (`curl` + `jq`) to query the existing Prometheus HTTP API and displays the result as a sparkline, gauge, or bar chart directly in the terminal.

**Note:** sampler's last release (v1.1.0) was in 2019. The tool is stable and functional but not actively maintained. If issues arise, grafterm or a custom Python TUI are fallback options.

## Changes

### 1. Remove Grafana from Docker Compose

Delete the `grafana` service block from `docker-compose.yml`. This removes:
- The `grafana/grafana:11.0.0` container
- Port `3000` binding
- `GF_ADMIN_PASSWORD` environment variable requirement
- Volume mounts for provisioning and dashboards

### 2. Clean up environment variables

Remove from `.env` and `.env.example`:
- `GF_ADMIN_PASSWORD`
- `GRAFANA_BIND`

### 3. Expose Prometheus on localhost

Add to the `prometheus` service in `docker-compose.yml`:

```yaml
prometheus:
  ...
  ports:
    - "127.0.0.1:9090:9090"
  networks:
    - cryptolake_internal
    - host_access    # added — required for port binding on Docker Desktop
```

This allows sampler (running on the host) to reach Prometheus. The `host_access` network follows the same pattern used by `redpanda`, `writer`, and the now-removed `grafana`.

### 4. Create `infra/sampler/sampler.yml`

15 sampler widgets organized in 3 tiers by operational importance. This is an upgrade over the original 8-panel Grafana dashboard — 5 critical metrics that were never visualized are now included.

#### Tier 1: Critical (must have)

| # | Panel | Widget | PromQL Query | Notes |
|---|-------|--------|-------------|-------|
| 1 | Consumer Lag | sparkline | `max(writer_consumer_lag)` | Primary health indicator, 2 alert thresholds |
| 2 | Gaps Detected/5m | sparkline | `sum(increase(collector_gaps_detected_total[5m]))` | Any gap = data loss |
| 3 | Messages Dropped/5m | sparkline | `sum(increase(collector_messages_dropped_total[5m]))` | **NEW** — was missing from Grafana despite having critical alert |
| 4 | Disk Usage % | gauge | `writer_disk_usage_pct` | Prevents storage exhaustion |
| 5 | Active Connections | sparkline | `sum(collector_ws_connections_active)` | Connectivity loss halts collection |
| 6 | Write Errors | sparkline | `sum(increase(writer_write_errors_total[5m]))` | **NEW** — disk failures, never visualized |

#### Tier 2: High (strongly recommended)

| # | Panel | Widget | PromQL Query | Notes |
|---|-------|--------|-------------|-------|
| 7 | Message Throughput | sparkline | `sum(rate(collector_messages_produced_total[1m]))` | Ingestion rate baseline |
| 8 | Exchange Latency p99 | sparkline | `histogram_quantile(0.99, sum by (le)(rate(collector_exchange_latency_ms_bucket[5m])))` | Alert at >500ms |
| 9 | Reconnects/5m | sparkline | `sum(increase(collector_ws_reconnects_total[5m]))` | Connection instability signal |
| 10 | Snapshots Taken/15m | sparkline | `sum(increase(collector_snapshots_taken_total[15m]))` | Snapshot health (success) |
| 11 | Snapshots Failed/15m | sparkline | `sum(increase(collector_snapshots_failed_total[15m]))` | Snapshot health (failure), has alert |
| 12 | DB Commit Failures | sparkline | `sum(increase(writer_pg_commit_failures_total[5m]))` | **NEW** — state persistence failures |
| 13 | Kafka Commit Failures | sparkline | `sum(increase(writer_kafka_commit_failures_total[5m]))` | **NEW** — offset tracking failures |

#### Tier 3: Operational (nice to have)

| # | Panel | Widget | PromQL Query | Notes |
|---|-------|--------|-------------|-------|
| 14 | Compression Ratio | sparkline | `avg(writer_compression_ratio)` | I/O efficiency |
| 15 | Flush Duration p99 | sparkline | `histogram_quantile(0.99, sum by (le)(rate(writer_flush_duration_ms_bucket[5m])))` | **NEW** — I/O bottleneck detection |

**Design notes:**
- The Grafana heatmap (Exchange Latency) is converted to a p99 sparkline since terminal UIs cannot render heatmaps.
- Connection Status (Grafana panel with two queries) is split: Active Connections (Tier 1) and Reconnects (Tier 2).
- Snapshot Health (Grafana panel with two queries) is split: Taken and Failed (both Tier 2).
- Queries that used `sum by (symbol, stream)` in Grafana are aggregated to a single value in sampler, since sampler sparklines display one series per widget. Per-symbol debugging is done via ad-hoc PromQL queries.
- **5 metrics marked NEW** were implemented in the codebase but never shown in Grafana. This dashboard closes that visibility gap.

Each widget polls every 5000ms using a shell command like:
```yaml
sparklines:
  - title: Consumer Lag
    rate-ms: 5000
    sample: >
      curl -s 'http://localhost:9090/api/v1/query?query=max(writer_consumer_lag)' |
      jq '.data.result[0].value[1] // 0' -r
```

### 5. Add missing Prometheus alert rules

Three critical metrics lack alerts. Add to `infra/prometheus/alert_rules.yml`:

```yaml
- alert: WriteErrors
  expr: increase(writer_write_errors_total[5m]) > 0
  labels:
    severity: critical
  annotations:
    summary: "Writer disk write errors detected"
    description: "{{ $value | printf \"%.0f\" }} write error(s) in the last 5 minutes on {{ $labels.exchange }}/{{ $labels.stream }}."

- alert: PostgresCommitFailing
  expr: increase(writer_pg_commit_failures_total[5m]) > 0
  labels:
    severity: critical
  annotations:
    summary: "PostgreSQL state commit failures"
    description: "{{ $value | printf \"%.0f\" }} PostgreSQL commit failure(s) in the last 5 minutes. State persistence is degraded."

- alert: KafkaCommitFailing
  expr: increase(writer_kafka_commit_failures_total[5m]) > 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "Kafka offset commit failures"
    description: "{{ $value | printf \"%.0f\" }} Kafka offset commit failure(s) in the last 5 minutes. Offset tracking is degraded."
```

### 6. Delete Grafana infrastructure files

Remove the entire `infra/grafana/` directory:
- `infra/grafana/dashboards/cryptolake.json`
- `infra/grafana/provisioning/datasources/datasources.yml`
- `infra/grafana/provisioning/dashboards/dashboards.yml`

### 7. No changes to

- Prometheus scrape configuration (prometheus.yml)
- AlertManager and WhatsApp bridge
- Collector and writer metrics code
- Any application code

## How to use

### Install sampler on VPS

```bash
# Linux amd64 — pinned to v1.1.0
wget https://github.com/sqshq/sampler/releases/download/v1.1.0/sampler-1.1.0-linux-amd64 -O /usr/local/bin/sampler
chmod +x /usr/local/bin/sampler
```

### Run

```bash
ssh user@vps
cd /path/to/cryptolake
# Run in tmux/screen so it persists after disconnect
tmux new -s monitor
sampler -c infra/sampler/sampler.yml
# Detach: Ctrl+b, d — reattach: tmux attach -t monitor
```

### Key bindings

- `q` — quit
- Arrow keys — navigate between panels

### Ad-hoc per-symbol queries

For per-symbol/stream breakdown (not shown in the aggregate dashboard):
```bash
curl -s 'http://localhost:9090/api/v1/query?query=sum+by+(symbol,stream)(rate(collector_messages_produced_total[1m]))' | jq '.data.result[]'
```

## Dependencies

- `sampler` v1.1.0 binary installed on the VPS host
- `curl` and `jq` available on the VPS host (standard on most Linux distros)
- `tmux` or `screen` recommended for persistent sessions
- Prometheus accessible at `localhost:9090`
