# Replace Grafana with Sampler CLI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Grafana web UI with sampler terminal dashboard, add 3 missing Prometheus alert rules, and expose Prometheus on localhost for host-side querying.

**Architecture:** Remove Grafana Docker service and its config files. Expose Prometheus on `127.0.0.1:9090`. Create a sampler YAML config with 15 widgets (sparklines + gauge) that poll Prometheus via `curl` + `jq`. Add 3 new alert rules for write errors, postgres commit failures, and kafka commit failures.

**Tech Stack:** sampler v1.1.0 (Go binary), Prometheus, curl, jq

**Spec:** `docs/superpowers/specs/2026-03-25-replace-grafana-with-sampler-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `docker-compose.yml:124-163` | Remove grafana service, add prometheus port + network |
| Modify | `.env` | Remove `GF_ADMIN_PASSWORD`, `GRAFANA_BIND` |
| Modify | `.env.example` | Remove `GF_ADMIN_PASSWORD`, `GRAFANA_BIND` |
| Modify | `infra/prometheus/alert_rules.yml` | Add 3 new alert rules |
| Create | `infra/sampler/sampler.yml` | Terminal dashboard config (15 widgets) |
| Delete | `infra/grafana/dashboards/cryptolake.json` | Grafana dashboard |
| Delete | `infra/grafana/provisioning/datasources/datasources.yml` | Grafana datasource config |
| Delete | `infra/grafana/provisioning/dashboards/dashboards.yml` | Grafana dashboard provider |

---

### Task 1: Remove Grafana from Docker Compose

**Files:**
- Modify: `docker-compose.yml:145-163` (delete grafana service block)

- [ ] **Step 1: Remove the grafana service block**

Delete lines 145–163 from `docker-compose.yml` (the entire `grafana:` service definition including image, ports, networks, environment, volumes, healthcheck, and depends_on).

- [ ] **Step 2: Validate docker-compose syntax**

Run: `docker compose config --quiet`
Expected: exits 0 with no output (valid YAML)

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: remove grafana service from docker-compose"
```

---

### Task 2: Expose Prometheus on localhost

**Files:**
- Modify: `docker-compose.yml:124-143` (prometheus service)

- [ ] **Step 1: Add ports and host_access network to prometheus service**

Add `ports` and update `networks` in the prometheus service block so it looks like:

```yaml
  prometheus:
    image: prom/prometheus:v2.51.0
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
    ports:
      - "127.0.0.1:9090:9090"
    networks:
      - cryptolake_internal
      - host_access
    volumes:
      - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./infra/prometheus/alert_rules.yml:/etc/prometheus/alert_rules.yml:ro
      - prometheus_data:/prometheus
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:9090/-/healthy || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
    depends_on:
      - collector
      - writer
      - alertmanager
```

- [ ] **Step 2: Validate docker-compose syntax**

Run: `docker compose config --quiet`
Expected: exits 0 with no output

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: expose prometheus on 127.0.0.1:9090 for host-side queries"
```

---

### Task 3: Clean up Grafana environment variables

**Files:**
- Modify: `.env`
- Modify: `.env.example`

- [ ] **Step 1: Remove Grafana variables from `.env.example`**

Remove these two lines from `.env.example`:
```
GF_ADMIN_PASSWORD=
GRAFANA_BIND=127.0.0.1
```

The file should end up as:
```
POSTGRES_PASSWORD=
WEBHOOK_URL=http://whatsapp-bridge:9095
CALLMEBOT_PHONE=
CALLMEBOT_APIKEY=
TEST_DURATION_SECONDS=600
HOST_DATA_DIR=/data
TEST_DATA_DIR=/data/test_data
```

- [ ] **Step 2: Remove Grafana variables from `.env`**

Remove the lines containing `GF_ADMIN_PASSWORD=` and `GRAFANA_BIND=` from `.env`.

- [ ] **Step 3: Commit**

```bash
git add .env.example
git commit -m "chore: remove grafana env vars from .env.example"
```

Note: `.env` is gitignored — do not `git add .env`. The change to `.env` is a local-only cleanup.

---

### Task 4: Add missing Prometheus alert rules

**Files:**
- Modify: `infra/prometheus/alert_rules.yml`

- [ ] **Step 1: Append 3 new alert rules**

Add the following rules at the end of the `rules:` list in `infra/prometheus/alert_rules.yml` (after the existing `MessagesDropped` rule):

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

- [ ] **Step 2: Validate YAML syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('infra/prometheus/alert_rules.yml'))"`
Expected: exits 0 (valid YAML)

- [ ] **Step 3: Commit**

```bash
git add infra/prometheus/alert_rules.yml
git commit -m "feat: add alert rules for write errors, postgres and kafka commit failures"
```

---

### Task 5: Create sampler dashboard config

**Files:**
- Create: `infra/sampler/sampler.yml`

- [ ] **Step 1: Create `infra/sampler/` directory**

Run: `mkdir -p infra/sampler`

- [ ] **Step 2: Create `infra/sampler/sampler.yml`**

```yaml
# CryptoLake Terminal Dashboard
# Run: sampler -c infra/sampler/sampler.yml
# Requires: curl, jq, prometheus on localhost:9090

variables:
  PROM: http://localhost:9090

# ── Tier 1: Critical ──────────────────────────────────────────────

sparklines:
  - title: Consumer Lag
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=max(writer_consumer_lag)' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Gaps Detected /5m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(collector_gaps_detected_total[5m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Messages Dropped /5m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(collector_messages_dropped_total[5m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Active Connections
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(collector_ws_connections_active)' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Write Errors /5m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(writer_write_errors_total[5m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

# ── Tier 2: High ──────────────────────────────────────────────────

  - title: Message Throughput msg/s
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(rate(collector_messages_produced_total[1m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.1f'

  - title: Exchange Latency p99 ms
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=histogram_quantile(0.99,sum+by(le)(rate(collector_exchange_latency_ms_bucket[5m])))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Reconnects /5m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(collector_ws_reconnects_total[5m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Snapshots Taken /15m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(collector_snapshots_taken_total[15m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Snapshots Failed /15m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(collector_snapshots_failed_total[15m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: DB Commit Failures /5m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(writer_pg_commit_failures_total[5m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

  - title: Kafka Commit Failures /5m
    rate-ms: 5000
    sample: >
      curl -s '$PROM/api/v1/query?query=sum(increase(writer_kafka_commit_failures_total[5m]))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

# ── Tier 3: Operational ──────────────────────────────────────────

  - title: Compression Ratio
    rate-ms: 10000
    sample: >
      curl -s '$PROM/api/v1/query?query=avg(writer_compression_ratio)' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.2f'

  - title: Flush Duration p99 ms
    rate-ms: 10000
    sample: >
      curl -s '$PROM/api/v1/query?query=histogram_quantile(0.99,sum+by(le)(rate(writer_flush_duration_ms_bucket[5m])))' |
      jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'

# ── Gauge ─────────────────────────────────────────────────────────

gauges:
  - title: Disk Usage %
    rate-ms: 10000
    cur:
      sample: >
        curl -s '$PROM/api/v1/query?query=writer_disk_usage_pct' |
        jq '.data.result[0].value[1] // "0"' -r
    max:
      sample: echo 100
    min:
      sample: echo 0
```

- [ ] **Step 3: Validate YAML syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('infra/sampler/sampler.yml'))"`
Expected: exits 0 (valid YAML)

- [ ] **Step 4: Commit**

```bash
git add infra/sampler/sampler.yml
git commit -m "feat: add sampler CLI dashboard config with 15 monitoring widgets"
```

---

### Task 6: Delete Grafana infrastructure files

**Files:**
- Delete: `infra/grafana/dashboards/cryptolake.json`
- Delete: `infra/grafana/provisioning/datasources/datasources.yml`
- Delete: `infra/grafana/provisioning/dashboards/dashboards.yml`
- Delete: `infra/grafana/` (entire directory)

- [ ] **Step 1: Remove the entire infra/grafana directory**

Run: `rm -rf infra/grafana`

- [ ] **Step 2: Verify deletion**

Run: `test -d infra/grafana && echo "EXISTS" || echo "GONE"`
Expected: `GONE`

- [ ] **Step 3: Commit**

```bash
git add -A infra/grafana
git commit -m "chore: delete grafana dashboard and provisioning files"
```

---

### Task 7: Validate the complete stack

- [ ] **Step 1: Validate docker-compose**

Run: `docker compose config --quiet`
Expected: exits 0

- [ ] **Step 2: Validate prometheus alert rules YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('infra/prometheus/alert_rules.yml'))"`
Expected: exits 0

- [ ] **Step 3: Validate sampler config YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('infra/sampler/sampler.yml'))"`
Expected: exits 0

- [ ] **Step 4: Verify no references to grafana remain in docker-compose.yml**

Run: `grep -i grafana docker-compose.yml`
Expected: no output (exit code 1)

- [ ] **Step 5: Verify .env.example has no grafana references**

Run: `grep -i grafana .env.example`
Expected: no output (exit code 1)

- [ ] **Step 6: Verify infra/grafana is gone**

Run: `test -d infra/grafana && echo "EXISTS" || echo "GONE"`
Expected: `GONE`
