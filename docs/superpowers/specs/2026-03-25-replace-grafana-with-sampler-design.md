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

A YAML config file covering all 8 Grafana dashboard panels, expanded to 10 sampler widgets (two multi-query Grafana panels are split into separate widgets):

| # | Panel | Widget | PromQL Query |
|---|-------|--------|-------------|
| 1 | Message Throughput | sparkline | `sum(rate(collector_messages_produced_total[1m]))` |
| 2 | Exchange Latency p99 | sparkline | `histogram_quantile(0.99, sum by (le)(rate(collector_exchange_latency_ms_bucket[5m])))` |
| 3 | Consumer Lag | sparkline | `max(writer_consumer_lag)` |
| 4 | Active Connections | sparkline | `sum(collector_ws_connections_active)` |
| 5 | Reconnects/5m | sparkline | `sum(increase(collector_ws_reconnects_total[5m]))` |
| 6 | Gaps Detected/5m | sparkline | `sum(increase(collector_gaps_detected_total[5m]))` |
| 7 | Disk Usage % | gauge | `writer_disk_usage_pct` |
| 8 | Snapshots Taken/15m | sparkline | `sum(increase(collector_snapshots_taken_total[15m]))` |
| 9 | Snapshots Failed/15m | sparkline | `sum(increase(collector_snapshots_failed_total[15m]))` |
| 10 | Compression Ratio | sparkline | `avg(writer_compression_ratio)` |

**Design notes:**
- The Grafana heatmap (panel #2) is converted to a p99 latency sparkline since terminal UIs cannot render heatmaps.
- Connection Status (Grafana panel #4, two queries) is split into two sparklines: Active Connections and Reconnects/5m.
- Snapshot Health (Grafana panel #7, two queries) is split into two sparklines: Taken and Failed.
- Queries that used `sum by (symbol, stream)` in Grafana are aggregated to a single value in sampler, since sampler sparklines display one series per widget. This is an intentional trade-off: the terminal dashboard shows aggregate health at a glance, while per-symbol debugging is done via direct PromQL queries (`curl localhost:9090/api/v1/query?query=...`).

Each widget polls every 5000ms using a shell command like:
```yaml
sparklines:
  - title: Message Throughput
    rate-ms: 5000
    sample: >
      curl -s 'http://localhost:9090/api/v1/query?query=sum(rate(collector_messages_produced_total[1m]))' |
      jq '.data.result[0].value[1] // 0' -r
```

### 5. Delete Grafana infrastructure files

Remove the entire `infra/grafana/` directory:
- `infra/grafana/dashboards/cryptolake.json`
- `infra/grafana/provisioning/datasources/datasources.yml`
- `infra/grafana/provisioning/dashboards/dashboards.yml`

### 6. No changes to

- Prometheus configuration and alert rules
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
