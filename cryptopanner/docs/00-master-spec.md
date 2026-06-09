# CryptoPanner — Master Specification

**Status:** Draft. Sections are added incrementally, one at a time, with review and approval before proceeding.

**Review progress:**
- Sections 1–4: **APPROVED** (reviewed 2026-05-25)
- Sections 5–16: **DRAFT — NOT REVIEWED.** Written as initial proposals based on design discussions. Each section needs user review and approval before it is final. Review them one by one starting from Section 5.

**Key design decisions made during Sections 1–4 review (context for future sessions):**
- Minute-segment files on local disk → merged into hourly files at hour boundary → uploaded to IONOS S3
- Gap detection only via sequence-ID validation during hourly merge, not inline in capture path
- No gap envelope classification, no runtime failure tracking — manifest records what's missing
- Backfill via REST only for ID-bearing streams; non-ID streams record gaps and move on
- Cross-region comparison/backfill/deduplication is out of scope (separate local compactor tool)
- Four systemd services per node: Collector, Sealer, Uploader, Node Agent
- Dedicated Monitor on separate VPS (different provider), pulls /status every 5s, can restart components
- Inter-node communication over encrypted mesh VPN (Tailscale or WireGuard)
- Components are "dumb" — they log and touch heartbeat files; Node Agent derives state from heartbeat mtimes
- IONOS chosen for object storage (existing vendor relationship)

---

## 1. Naming & identity

a. CryptoPanner is a raw-first, multi-region capture pipeline for Binance USD-M Futures market data.
b. The end product is an append-only, byte-faithful collection of per-node hourly sealed files in S3-compatible object storage. For each (node, symbol, stream, day, hour) the storage holds one zstd-compressed JSONL file, a `.sha256` integrity sidecar, and a `.manifest.json` recording sequence ranges and capture metadata. Consumers fetch files via standard S3-compatible HTTP and can verify integrity locally. Cross-region merging into a single canonical archive is performed by a separate local tool — out of scope of this project.
c. The project is a clean-room successor to CryptoLake (v1) with no shared code, configuration, or data.

## 2. Goals & non-goals

- **a. Goals**
    1. Capture all configured WebSocket streams and reference-data REST endpoints.
    2. Preserve raw-payload fidelity — no re-serialization.
    3. Write minute-segment files to local disk.
    4. After the hour is finished, merge minute segments into per-node hourly files. During the merge validate sequence-ID continuity for streams that carry IDs (trades, depth, etc.) and backfill detected gaps via REST. Non-ID streams are concatenated as-is. Record missing segments and any remaining gaps in the hourly manifest for both ID and non-ID files.
    5. Upload sealed hourly files to IONOS S3-compatible object storage.
    6. Run unattended on single-node VPS deployments.

- **b. Non-goals**
    1. Cross-region comparison, backfill, or deduplication (separate local tool).
    2. Real-time serving or query access.
    3. Support for exchanges other than Binance USD-M Futures.
    4. Historical bulk import of data predating the pipeline's first run.
    5. Inline gap detection in the capture hot path.

## 3. Invariants

- a. **Raw-payload fidelity.** Bytes received from the WebSocket or REST response are stored verbatim. No parsing, re-serialization, or field extraction occurs before writing to the minute-segment file.
- b. **Durability before acknowledgement.** A minute segment is considered sealed only after the file is fsynced to disk and the `.sha256` sidecar is written. The upload step begins only after the hourly merge completes and its integrity sidecar is verified.
- c. **Manifest is the source of truth.** Every hourly file has a `.manifest.json` that records which minute segments are present, which are missing, and — for ID-bearing streams — any sequence gaps that could not be backfilled. If the manifest says a gap exists, it exists. If the manifest says the file is complete, it is complete.
- d. **Per-node independence.** A node is a single VPS instance running its own capture pipeline. Each node operates in isolation — no node reads from, writes to, or coordinates with another node. Cross-region logic is external to this system.
- e. **Idempotent upload.** Uploading the same hourly file twice produces the same object in storage. The object key encodes (node, symbol, stream, day, hour) — a given key is written once and never mutated.
- f. **No silent data loss.** The system does not track or classify runtime failures (crashes, disconnects, restarts). Gap detection happens only by sequence-ID validation and only during the hourly merge step. If a gap is found and cannot be backfilled via REST, it is recorded in the manifest. Missing minute segments are likewise recorded. No other mechanism claims or infers completeness.

## 4. Architecture overview

- a. Each node is a VPS instance running four systemd services:
    1. **Collector** — maintains WebSocket connections to Binance USD-M Futures and polls REST endpoints. Writes raw bytes to per-stream minute-segment files on local disk. Touches a heartbeat file every 5s.
    2. **Sealer** — at the turn of each hour, merges the previous hour's minute segments into a single hourly JSONL file per (symbol, stream). For ID-bearing streams, validates sequence continuity and backfills gaps via REST. Produces the `.sha256` sidecar and `.manifest.json`. Touches a heartbeat file every 5s.
    3. **Uploader** — takes sealed hourly files and uploads them to IONOS S3-compatible object storage. Confirms upload integrity, then cleans up local minute segments. Touches a heartbeat file every 5s.
    4. **Node Agent** — lightweight HTTP server bound to the encrypted mesh VPN interface only. Reads component heartbeat file mtimes and `/proc` for VPS metrics. Exposes `GET /status` (aggregated JSON) and `POST /restart/{component}` (calls `systemctl restart`). Protected by systemd WatchdogSec.

- b. All four services share a local filesystem. No message broker, no database, no coordination service between components on the same node.

- c. Multiple nodes are deployed in different regions for redundancy. Each node is a complete, independent instance of this pipeline.

- d. A dedicated **Monitor** runs on a separate VPS (different provider than the nodes) with a built-in HTTP server:
    1. Pulls `/status` from each node every 5s. Three consecutive failures (15s) marks a node as down.
    2. Sends `POST /restart/{component}` to recover failed components, with exponential backoff and a circuit breaker (3 failures in 5 min → stop restarting, alert operator).
    3. Alerts via Telegram/WhatsApp when a component fails or a circuit breaker trips.
    4. Pushes its own heartbeat to an external dead-man's switch (e.g., Healthchecks.io) — if the Monitor itself goes down, the external service alerts.
    5. Exposes `GET /dashboard` (HTML), `GET /api/nodes` (JSON), and `POST /api/restart/{node}/{component}`.

- e. Inter-node communication runs over an encrypted mesh VPN (Tailscale or raw WireGuard). All endpoints are bound to VPN interfaces only — invisible to the public internet.

- f. Security for the restart endpoint: bearer token in header, timestamp validation (reject requests older than 30s), and VPN-level ACLs restricting access to the Monitor's IP only. Read-only endpoints (`/status`, `/dashboard`) require no additional auth.

## 5. Topology & deployment ⚠️ DRAFT — NOT REVIEWED

a. **Node placement.** At least two VPS nodes in geographically separate regions (e.g., Tokyo and Frankfurt). Each node captures the full set of configured symbols and streams independently. Region selection prioritizes low latency to Binance API servers and provider diversity.

b. **Monitor placement.** A single Monitor VPS hosted on a different provider than the nodes (e.g., nodes on IONOS, Monitor on Hetzner or OVH). Cheapest tier sufficient — 1 vCPU, 1GB RAM.

c. **Object storage.** IONOS S3-compatible object storage. Each node uploads to a node-specific prefix: `s3://<bucket>/<node-id>/<symbol>/<stream>/<date>/hour-HH.jsonl.zst`. Nodes never read from or write to another node's prefix.

d. **VPN mesh.** All nodes and the Monitor join a single encrypted mesh VPN (Tailscale or WireGuard). All inter-instance communication is routed through VPN interfaces. No public ports are exposed for internal APIs.

e. **Deployment method.** Each component is packaged as a fat JAR (or installDist output) and deployed via systemd unit files. Updates are deployed by copying the new artifact, then `systemctl restart <unit>`. No container runtime required on nodes.

## 6. Node anatomy ⚠️ DRAFT — NOT REVIEWED

- a. **Directory layout** on each node:
    1. `/opt/cryptopanner/` — application binaries (collector, sealer, uploader, node-agent JARs)
    2. `/etc/cryptopanner/` — configuration files (`config.yaml`, `agent.token`)
    3. `/data/cryptopanner/segments/` — minute-segment files, organized as `<symbol>/<stream>/<date>/minute-<HH-MM>.jsonl.zst`
    4. `/data/cryptopanner/sealed/` — sealed hourly files awaiting upload, organized as `<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst` + `.sha256` + `.manifest.json`
    5. `/data/cryptopanner/logs/` — structured JSON log files per component
    6. `/tmp/cryptopanner-*.heartbeat` — heartbeat files touched by each component

- b. **systemd units.** Four units per node: `cryptopanner-collector.service`, `cryptopanner-sealer.service`, `cryptopanner-uploader.service`, `cryptopanner-agent.service`. All set to `Restart=always`. The agent unit has `WatchdogSec=30`.

- c. **Boot order.** The agent starts after the three pipeline components: `After=cryptopanner-collector.service cryptopanner-sealer.service cryptopanner-uploader.service`.

- d. **Resource expectations.** The pipeline is I/O-bound, not CPU-bound. A 2-vCPU, 4GB RAM VPS with 80GB SSD should be sufficient for the initial symbol set. Disk usage is transient — minute segments are cleaned up after hourly upload.

## 7. Data inventory ⚠️ DRAFT — NOT REVIEWED

- a. **WebSocket streams per symbol:**
    1. `trade` — individual trade events (carries trade ID → ID-sequenced)
    2. `depth@100ms` — order book diff updates (carries `lastUpdateId` → ID-sequenced)
    3. `aggTrade` — aggregated trade events (carries agg trade ID → ID-sequenced)
    4. `kline_1m` — 1-minute candlestick updates (no sequence ID)
    5. `ticker` — 24h rolling ticker statistics (no sequence ID)
    6. `bookTicker` — best bid/ask updates (no sequence ID)
    7. `markPrice` — mark price and funding rate (no sequence ID)
    8. `forceOrder` — liquidation events (no sequence ID)

- b. **REST endpoints polled periodically:**
    1. `GET /fapi/v1/depth?symbol=X&limit=1000` — full order book snapshot for depth anchor validation
    2. `GET /fapi/v1/openInterest?symbol=X` — open interest (polled every 1–5 min)
    3. `GET /fapi/v1/exchangeInfo` — symbol metadata and trading rules (polled infrequently, e.g., daily)

- c. **Symbol set.** Configured via `config.yaml`. Initial deployment targets the top symbols by volume (e.g., BTCUSDT, ETHUSDT, etc.). The full list is a configuration decision, not an architectural one.

- d. **Backfill sources (REST, used during hourly merge):**
    1. `GET /fapi/v1/aggTrades?symbol=X&fromId=N` — paginated aggTrades by ID for gap backfill
    2. `GET /fapi/v1/historicalTrades?symbol=X&fromId=N` — paginated trades by ID for gap backfill

## 8. Ingest ⚠️ DRAFT — NOT REVIEWED

a. **WebSocket connection.** The Collector maintains a single consolidated WebSocket connection to Binance USD-M Futures per configured endpoint. All subscribed streams are multiplexed over this connection via combined stream names (e.g., `btcusdt@trade/ethusdt@depth@100ms/...`).

b. **Reconnection.** On disconnect, the Collector reconnects immediately with exponential backoff (1s, 2s, 4s, ..., capped at 60s). After reconnection, it re-subscribes to all configured streams.

c. **Raw capture.** Each incoming WebSocket frame is written as-is (raw bytes, not re-serialized) to the appropriate minute-segment file. The Collector determines the target file from the stream name and the current wall-clock minute.

d. **REST polling.** REST endpoints are polled on independent timers. Responses are written as raw JSON to the same minute-segment file structure, with a wrapper envelope identifying the source endpoint and poll timestamp.

e. **Minute-segment rotation.** At each minute boundary, the Collector closes the current segment file (fsync + SHA-256 sidecar) and opens a new one. The closed segment is immutable from this point forward.

## 9. WAL & local sealing & upload ⚠️ DRAFT — NOT REVIEWED

- a. **Minute segments as WAL.** The per-minute segment files serve as the write-ahead log. Each segment is small (typically seconds of data), fsynced on close, and accompanied by a `.sha256` sidecar. This is the durability boundary — data is safe once the segment is sealed.

- b. **Hourly merge (Sealer).** At the turn of each hour (e.g., at 15:00:00 UTC the Sealer processes hour 14):
    1. Collects all minute segments for the previous hour, per (symbol, stream).
    2. Concatenates them in chronological order into a single JSONL file.
    3. For ID-bearing streams: validates sequence-ID continuity across the concatenated data. If gaps are detected, attempts backfill via REST (see 7.d). Backfilled records are inserted in sequence order.
    4. Compresses the result with zstd.
    5. Writes the `.sha256` integrity sidecar.
    6. Writes the `.manifest.json` recording: list of minute segments present, list of minute segments missing, sequence-ID range (first, last), any remaining gaps after backfill, backfill attempts and outcomes.

- c. **Upload (Uploader).** The Uploader watches the sealed directory for completed hourly files:
    1. Uploads the `.jsonl.zst`, `.sha256`, and `.manifest.json` to IONOS S3.
    2. Verifies the upload by comparing the remote ETag or checksum with the local `.sha256`.
    3. On success, deletes the local minute segments for that (symbol, stream, hour) and moves the sealed files to a `done/` staging area (or deletes them, depending on disk pressure).
    4. On failure, retries with exponential backoff. Does not delete local files until upload is confirmed.

- d. **Ordering guarantee.** The Sealer does not begin processing hour N until all minute segments for hour N are closed (i.e., the clock has passed the hour boundary). The Uploader does not upload until the Sealer marks the hour as sealed.

## 10. Per-node file format & manifest ⚠️ DRAFT — NOT REVIEWED

- a. **Minute-segment file.** Path: `segments/<symbol>/<stream>/<date>/minute-<HH-MM>.jsonl.zst`. Each line is one raw WebSocket frame or REST response, stored as received. Compressed with zstd. Accompanied by `minute-<HH-MM>.jsonl.zst.sha256`.

- b. **Sealed hourly file.** Path: `sealed/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst`. Concatenation of all minute segments for that hour, with backfilled records inserted in sequence order for ID-bearing streams. Accompanied by `hour-<HH>.jsonl.zst.sha256`.

- c. **S3 object key.** `<node-id>/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst` (and `.sha256`, `.manifest.json`).

- d. **Manifest format** (`hour-<HH>.manifest.json`):

    ```json
    {
      "node": "vps-fra-1",
      "symbol": "btcusdt",
      "stream": "trade",
      "date": "2026-05-25",
      "hour": 14,
      "sealed_at": "2026-05-25T15:00:12Z",
      "minutes_present": [0, 1, 2, ..., 58, 59],
      "minutes_missing": [],
      "sequence_id_range": { "first": 123456, "last": 234567 },
      "sequence_gaps": [],
      "backfill_attempts": [],
      "file_sha256": "abcdef...",
      "file_size_bytes": 1048576,
      "record_count": 42000
    }
    ```

- e. **Non-ID streams.** For streams without sequence IDs, the manifest omits `sequence_id_range`, `sequence_gaps`, and `backfill_attempts`. Only `minutes_present` and `minutes_missing` are populated.

## 11. Health & observability ⚠️ DRAFT — NOT REVIEWED

a. **Structured logging.** Every component writes JSON Lines logs to `/data/cryptopanner/logs/<component>.jsonl`. Each log entry contains: `ts` (ISO-8601), `component`, `event` (machine-readable), `level` (INFO/WARN/ERROR), and domain-specific fields.

b. **Heartbeat files.** Each component touches its heartbeat file (`/tmp/cryptopanner-<component>.heartbeat`) every 5s on its main loop iteration. The Node Agent derives component state from mtime: `running` (mtime < 15s), `stuck` (mtime > 60s), `down` (systemd reports inactive).

c. **Node Agent `/status` response.** Returns JSON with per-component state (running/stuck/down, PID, heartbeat age, uptime) and VPS metrics (CPU%, memory%, disk%, load average). Scraped by the Monitor every 5s.

d. **Monitor dashboard.** The Monitor serves a simple HTML page at `GET /dashboard` showing all nodes and their component states, with auto-refresh. Backed by `GET /api/nodes` (JSON).

e. **Log events of interest.** Key events the Monitor should track from component logs (if log tailing is added in the future): WebSocket connect/disconnect, minute-segment sealed, hourly merge started/completed, backfill attempted/succeeded/failed, upload started/completed/failed.

## 12. Failure model ⚠️ DRAFT — NOT REVIEWED

a. **Collector crash.** The current minute segment may be incomplete or corrupt. The Sealer treats it as the last segment for that minute — whatever was fsynced is kept. The gap is detected during hourly merge via sequence-ID validation and backfilled if possible. Non-ID streams record missing minutes in the manifest.

b. **Collector WebSocket disconnect.** The Collector reconnects with exponential backoff. Data during the disconnection window is missing from the minute segments. Detected and handled during merge (same as crash).

c. **Sealer crash during merge.** The merge must be idempotent. On restart, the Sealer checks if a sealed file already exists for the hour; if not, it re-runs the merge from the minute segments (which are still on disk). If a partial sealed file exists, it is deleted and the merge re-runs.

d. **Uploader crash.** On restart, the Uploader scans the sealed directory for files not yet uploaded. It re-attempts upload. Idempotent upload (invariant 3.e) ensures no corruption from duplicate uploads.

e. **VPS reboot.** systemd restarts all four services. The Collector begins capturing from the current moment. The Sealer checks for any un-merged past hours and processes them. The Uploader checks for any un-uploaded sealed files. Data during the downtime is missing — detected during merge.

f. **IONOS S3 outage.** The Uploader retries with backoff. Sealed files accumulate on local disk. When S3 recovers, the backlog is drained. Disk pressure is monitored by the Node Agent and surfaced via `/status`.

g. **Monitor VPS down.** Nodes continue operating independently — the Monitor is not in the data path. The external dead-man's switch (Healthchecks.io) detects the Monitor's absence and alerts the operator.

h. **VPN mesh failure.** Nodes continue capturing and uploading — VPN is only used for Monitor↔Node communication. Data flow (node → S3) goes over the public internet (HTTPS to IONOS). Monitor loses visibility but no data is lost.

## 13. Reliability & alerting ⚠️ DRAFT — NOT REVIEWED

- a. **Alert channels.** The Monitor sends alerts via Telegram (or WhatsApp) webhook. Two severity levels:
    1. **Warning** — component stuck (heartbeat age > 60s), upload retry count > 3, disk usage > 80%.
    2. **Critical** — component down (3 consecutive scrape failures), circuit breaker tripped (3 restart failures in 5 min), node unreachable, Monitor's own health degraded.

- b. **Restart policy.** The Monitor attempts `POST /restart/{component}` with exponential backoff: 5s → 15s → 60s → 300s. After 3 failures within 5 minutes, the circuit breaker trips: no more restarts, critical alert to operator.

- c. **Dead-man's switch.** The Monitor pushes a heartbeat to Healthchecks.io every 60s. If pushes stop for 5 minutes, Healthchecks.io alerts the operator via email/SMS. This covers the "Monitor itself is down" and "entire VPS is dead" scenarios.

- d. **No false-positive restarts.** The Monitor uses a two-tier check before restarting: (1) systemd reports the unit as failed or inactive, AND (2) heartbeat age exceeds the threshold. A slow but alive component is not restarted — only stuck or dead ones.

- e. **Alert deduplication.** The Monitor suppresses repeated alerts for the same condition. A new alert is sent only when the condition changes (e.g., component recovers, then fails again).

## 14. Testing strategy ⚠️ DRAFT — NOT REVIEWED

a. **Unit tests.** Each component is tested in isolation. Key areas: minute-segment writing, hourly merge logic, sequence-ID validation, backfill insertion, manifest generation, SHA-256 computation, S3 upload/verify.

b. **Integration tests.** End-to-end tests that run the full pipeline locally: Collector captures from a mock WebSocket server, Sealer merges, Uploader uploads to a local MinIO instance. Verify the sealed files and manifests are correct.

c. **Fault injection tests.** Simulate failure scenarios: kill Collector mid-minute, kill Sealer mid-merge, make S3 unreachable, corrupt a minute segment. Verify that recovery produces correct results and manifests accurately reflect what happened.

d. **Sequence-ID gap tests.** Feed the Sealer minute segments with known sequence gaps. Verify backfill is attempted, and the manifest correctly records gaps that could not be filled.

e. **Monitor tests.** Test the Monitor's restart logic, backoff, circuit breaker, and alerting against a mock Node Agent that simulates various failure modes.

f. **VPN-independent testing.** All tests run locally without requiring a VPN mesh. The Node Agent binds to `127.0.0.1` in test mode.

## 15. Configuration ⚠️ DRAFT — NOT REVIEWED

- a. **Config file.** Each node reads `/etc/cryptopanner/config.yaml`. The Monitor has its own config file.

- b. **Node config contents:**
    1. `node_id` — unique identifier for this node (e.g., `vps-fra-1`)
    2. `symbols` — list of symbols to capture (e.g., `[btcusdt, ethusdt]`)
    3. `streams` — list of streams per symbol (e.g., `[trade, depth@100ms, aggTrade, kline_1m]`)
    4. `rest_endpoints` — list of REST endpoints to poll with intervals
    5. `storage.endpoint` — IONOS S3 endpoint URL
    6. `storage.bucket` — S3 bucket name
    7. `storage.credentials_file` — path to S3 credentials
    8. `agent.listen_address` — VPN IP and port for the Node Agent (e.g., `100.x.y.z:9100`)
    9. `agent.token_file` — path to bearer token file
    10. `paths.segments` — local path for minute segments
    11. `paths.sealed` — local path for sealed hourly files
    12. `paths.logs` — local path for log files

- c. **Monitor config contents:**
    1. `nodes` — list of node endpoints (VPN IPs and ports)
    2. `scrape_interval_s` — how often to pull `/status` (default: 5)
    3. `restart.backoff` — backoff schedule for restart attempts
    4. `restart.circuit_breaker` — failure count and window for circuit breaker
    5. `alerting.telegram_webhook` — Telegram bot webhook URL
    6. `alerting.healthchecks_url` — Healthchecks.io ping URL
    7. `dashboard.listen_address` — VPN IP and port for the dashboard

- d. **Environment variables.** Secrets (S3 credentials, bearer token, Telegram webhook) may be provided via environment variables as an alternative to file paths in the config.

## 16. Open questions ⚠️ DRAFT — NOT REVIEWED

a. **Minute-segment compression.** Should minute segments be zstd-compressed on write, or stored as plain JSONL and only compressed during the hourly merge? Compression on write saves disk but adds CPU overhead to every write. Compression at merge time is simpler but uses more transient disk.

b. **Backfill retry limits.** How many REST backfill attempts should the Sealer make before giving up and recording the gap in the manifest? What backoff strategy? Should it retry in a future merge cycle or only once?

c. **Disk pressure management.** If the Uploader falls behind and sealed files accumulate, at what disk usage threshold should the Node Agent alert? Should the pipeline pause capture to avoid filling the disk, or keep capturing and let the operator decide?

d. **Log retention.** How long should structured log files be kept on each node? Should they be rotated by size, by time, or both?

e. **Clock synchronization.** Minute-segment boundaries depend on wall-clock time. What NTP configuration is assumed? How much clock skew between nodes is tolerable?

f. **Multi-symbol WebSocket strategy.** Binance limits combined stream connections. If the symbol set exceeds the limit, should the Collector open multiple WebSocket connections? How are streams distributed across connections?

g. **Sealer scheduling.** The Sealer runs at the hour boundary, but what if the merge + backfill takes longer than expected? Should it queue hours, or is there a timeout after which it gives up and moves on?

h. **Node identity in S3.** The `node-id` is used as the S3 key prefix. What happens if a node is replaced (new VPS, same region)? Should it reuse the old node ID or get a new one?

i. **VPN provider decision.** Tailscale (managed, free tier) vs raw WireGuard (self-managed, no external dependency). Decision deferred to deployment phase.
