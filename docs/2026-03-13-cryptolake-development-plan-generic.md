# CryptoLake Implementation Plan

---

## Section 1: Project Bootstrap, Foundation & Shared Core

**Tasks:**
- Repository scaffolding and dependency pinning (uv)
- Common configuration loader, Pydantic model, and env overrides
- Message envelope schema and raw hash (`raw_sha256`) utilities
- Structured JSON logging setup
- Shared HTTP utilities for health and readiness probes
- Base Dockerfiles and `docker-compose.yml` skeleton

**Tests:**
- Unit tests for configuration parsing and validation edge cases
- Unit tests for envelope schema serialization/deserialization and hash utilities

**Manual Testing Instructions:**
1. Run `docker compose up -d redpanda collector writer`
2. Send HTTP requests to `http://127.0.0.1:8000/health` and `http://127.0.0.1:8001/health`
3. **Verify:** Both services return a `200` healthy JSON payload (not an error page or timeout). Confirm the containers stay up for several minutes without restart loops. Check startup logs to ensure configuration loading, logger initialization, and service boot complete without validation errors.

---

## Section 2: Binance Collector Core & Gap Detection

**Tasks:**
- Binance exchange adapter (URL building, payload parsing, `raw_text` extraction)
- WebSocket connection manager (combined streams, reconnect with backoff)
- Stream handlers (trades, depth, bookticker, funding_rate, liquidations)
- REST pollers with staggered scheduling (depth snapshots, open interest)
- Gap detector (depth `pu` chain validator, `session_seq` tracking)
- Depth resync flow and gap record emission
- Redpanda producer integration with partitioned memory buffer
- Collector metrics and alert hooks

**Tests:**
- Unit tests for gap detector state machine
- Integration tests for Binance WebSocket connection (using recorded fixtures)
- Integration tests for Redpanda producer/consumer round-trip via testcontainers

**Manual Testing Instructions:**
1. Run `docker compose up -d redpanda collector`
2. Run `docker compose exec redpanda rpk topic list` to ensure topics are created only for enabled streams with 48h retention.
3. Run `docker compose exec redpanda rpk topic consume binance.trades -n 5` to confirm messages contain common envelope fields and preserve the unparsed `raw_text`.
4. Trigger chaos: Run `bash tests/chaos/kill_ws_connection.sh`.
5. **Verify:** Automatic reconnect occurs. Inspect logs/topics to confirm a `type: "gap"` record is emitted with the correct `reason` and time window. Ensure data flow and depth resync behavior resume seamlessly.

---

## Section 3: Writer Durability & Archive Pipeline

**Tasks:**
- Redpanda consumer and partition ownership assertion
- Buffer manager (routing by per-file targets and flush thresholds)
- Zstd streaming compressor (frame writer)
- Hourly file rotator, `.sha256` sidecar generation, and late-file spillover handling
- PostgreSQL offset and file-size state management for deduplication
- Unsealed file recovery on startup
- Writer shutdown sequence and restart recovery

**Tests:**
- Unit tests for file rotation logic, path generation, and compression round-trip
- Integration tests for writer restart recovery and offset deduplication guarantees

**Manual Testing Instructions:**
1. Run `docker compose up -d redpanda collector writer`
2. Wait for data, then verify files appear matching the layout: `/data/{exchange}/{symbol}/{stream}/{date}/hour-{HH}.jsonl.zst`.
3. Run `zstd -d < path/to/hour-XX.jsonl.zst | head` to confirm valid JSON lines with writer-stamped broker coordinates.
4. Trigger chaos: Run `docker kill writer && sleep 120 && docker compose up -d writer`.
5. **Verify:** Writing resumes using PostgreSQL offsets, already-written records are not duplicated, offsets continue forward, and sealed files receive `.sha256` sidecars.

---

## Section 4: Verification CLI & Operator Tooling

**Tasks:**
- CLI entry point (`cryptolake verify`)
- File-level checksum and decompression validation
- Envelope schema and `raw_sha256` payload validation
- Duplicate broker offset validation (dedup verification)
- Depth snapshot and `pu` chain replay validation
- Gap reporting tool
- Daily `manifest.json` generation

**Tests:**
- Unit tests using fixture archives (both clean and corrupted)
- Integration tests for replay, manifest generation, and dedup validation

**Manual Testing Instructions:**
1. Let the full stack run for at least 1 hour, then execute `docker compose exec writer python -m cli.verify --full --date $(date -u +%Y-%m-%d)`.
2. **Verify:** Command outputs zero checksum mismatches, zero duplicate offsets, and depth replay passes cleanly.
3. If gap chaos was triggered earlier, ensure the CLI explicitly reports the gap events pointing to the exact symbol, stream, and time window.
4. Open the generated `manifest.json` in the date directory and confirm it accurately summarizes the available hours, total record counts, and gaps.

---

## Section 5: Observability Stack & Full-System Validation

**Tasks:**
- Prometheus scrape configuration and targets
- Grafana dashboard provisioning (datasources, dashboard JSON)
- Alertmanager configuration with env-expanded webhook routing
- Docker Compose runtime wiring and network isolation (internal vs. egress)
- End-to-end test stack containerization
- Chaos test scripts integration

**Tests:**
- Full E2E pipeline run via `docker compose -f docker-compose.test.yml up --build`
- Manual execution of all chaos scripts against a running stack

**Manual Testing Instructions:**
1. Start the stack with secrets: `GF_ADMIN_PASSWORD=changeme WEBHOOK_URL=<webhook_url> docker compose up -d`.
2. Open Grafana at `http://localhost:3000`. **Verify:** Dashboards populate with real-time throughput, consumer lag, latency, and system metrics.
3. Open Prometheus at `http://localhost:9090/targets`. **Verify:** Collector, writer, and Redpanda targets are `UP`.
4. Trigger a simulated failure (e.g., `bash tests/chaos/kill_writer.sh` or `bash tests/chaos/fill_disk.sh`).
5. **Verify:** Dashboards reflect the incident (e.g., consumer lag spikes), and Alertmanager successfully fires a notification to the configured webhook within the expected delay. The system must recover fully once the failure is resolved.