# Replace Grafana with Sampler CLI

## Problem

Grafana requires port 3000 to be exposed on the VPS, which is undesirable for a production server accessed only via SSH. A terminal-native monitoring tool eliminates the need for any web UI port.

## Solution

Replace Grafana with [sampler](https://github.com/sqshq/sampler) — a pre-built Go binary that renders real-time terminal dashboards from YAML configuration. Sampler queries the existing Prometheus HTTP API via `curl` and displays results as sparklines, gauges, and bar charts directly in the terminal.

## Changes

### 1. Remove Grafana from Docker Compose

Delete the `grafana` service block from `docker-compose.yml`. This removes:
- The `grafana/grafana:11.0.0` container
- Port `3000` binding
- `GF_ADMIN_PASSWORD` environment variable requirement
- Volume mounts for provisioning and dashboards

### 2. Expose Prometheus on localhost

Add to the `prometheus` service in `docker-compose.yml`:
- `ports: ["127.0.0.1:9090:9090"]` — binds only to localhost, no public exposure
- Add `host_access` network so the port binding works

This allows sampler (running on the host) to reach Prometheus.

### 3. Create `infra/sampler/sampler.yml`

A YAML config file that mirrors all 8 Grafana dashboard panels:

| # | Panel | Widget | PromQL Query |
|---|-------|--------|-------------|
| 1 | Message Throughput | sparkline | `sum(rate(collector_messages_produced_total[1m]))` |
| 2 | Exchange Latency p99 | sparkline | `histogram_quantile(0.99, sum by (le)(rate(collector_exchange_latency_ms_bucket[5m])))` |
| 3 | Consumer Lag | sparkline | `max by (stream)(writer_consumer_lag)` |
| 4 | Active Connections | sparkline | `sum(collector_ws_connections_active)` |
| 5 | Reconnects/5m | sparkline | `sum(increase(collector_ws_reconnects_total[5m]))` |
| 6 | Gaps Detected/5m | sparkline | `sum(increase(collector_gaps_detected_total[5m]))` |
| 7 | Disk Usage % | gauge | `writer_disk_usage_pct` |
| 8 | Snapshots Taken/15m | sparkline | `sum(increase(collector_snapshots_taken_total[15m]))` |
| 9 | Snapshots Failed/15m | sparkline | `sum(increase(collector_snapshots_failed_total[15m]))` |
| 10 | Compression Ratio | sparkline | `avg by (stream)(writer_compression_ratio)` |

Note: The Grafana heatmap panel (#2) is converted to a p99 latency sparkline since terminal UIs cannot render heatmaps. Connection Status is split into two sparklines (active + reconnects) for clarity.

All queries poll every 5 seconds via `curl` against `http://localhost:9090/api/v1/query`.

### 4. Delete Grafana infrastructure files

Remove the entire `infra/grafana/` directory:
- `infra/grafana/dashboards/cryptolake.json`
- `infra/grafana/provisioning/datasources/datasources.yml`
- `infra/grafana/provisioning/dashboards/dashboards.yml`

### 5. No changes to

- Prometheus configuration and alert rules
- AlertManager and WhatsApp bridge
- Collector and writer metrics code
- Any application code

## How to use

### Install sampler on VPS

```bash
# Linux amd64
wget https://github.com/sqshq/sampler/releases/latest/download/sampler-linux-amd64 -O /usr/local/bin/sampler
chmod +x /usr/local/bin/sampler
```

### Run

```bash
ssh user@vps
cd /path/to/cryptolake
sampler -c infra/sampler/sampler.yml
```

### Key bindings

- `q` — quit
- Arrow keys — navigate between panels

## Dependencies

- `sampler` binary installed on the VPS host
- `curl` and `jq` available on the VPS host (standard on most Linux distros)
- Prometheus accessible at `localhost:9090`
