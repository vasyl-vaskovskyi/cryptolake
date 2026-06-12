# CryptoPanner — Master Specification

**Status:** Draft. Sections are added incrementally, one at a time, with review and approval before proceeding.

**Review progress:**
- Sections 1–4: **APPROVED** (reviewed 2026-05-25)
- Sections 5–10: **APPROVED** (reviewed 2026-06-09; adds Collector hot-swap referencing [superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md))
- Sections 11–14: **APPROVED** (reviewed 2026-06-12)
- Sections 15–16: **DRAFT — NOT REVIEWED.** Written as initial proposals based on design discussions. Each section needs user review and approval before it is final. Review them one by one starting from Section 15.

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

## 5. Topology & deployment

a. **Node placement.** At least two VPS nodes in geographically separate regions (e.g., Tokyo and Frankfurt). Each node captures the full set of configured symbols and streams independently. Region selection prioritizes low latency to Binance API servers and provider diversity.

b. **Monitor placement.** A single Monitor VPS hosted on a different provider than the nodes (e.g., nodes on IONOS, Monitor on Hetzner or OVH). Cheapest tier sufficient — 1 vCPU, 1GB RAM.

c. **Object storage.** IONOS S3-compatible object storage. Each node uploads to a node-specific prefix: `s3://<bucket>/<node-id>/<symbol>/<stream>/<date>/hour-HH.jsonl.zst`. Nodes never read from or write to another node's prefix.

d. **VPN mesh.** All nodes and the Monitor join a single encrypted mesh VPN (Tailscale or WireGuard). All inter-instance communication is routed through VPN interfaces. No public ports are exposed for internal APIs.

e. **Deployment method.** Each component is packaged as a fat JAR (or installDist output) and deployed via systemd unit files. No container runtime required on nodes.

f. **Collector hot-swap.** The Collector is deployed via a make-before-break protocol — a new candidate JVM runs alongside the old one for at least one minute boundary, equivalence is verified on the overlapping minute files, then the active slot is flipped and overlap minutes are merged. This avoids data loss on non-ID streams (which cannot be backfilled via REST). The Sealer, Uploader, and Node Agent restart freely (they work on data already on disk). See [Collector hot-swap and WS rotation design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) for the full mechanism. (The same overlap library is reused at runtime by the Collector's internal daily WS rotation — see §8.b.)

   **Scheduling.** Hot-swap deploys are forbidden in the `HH:50 → HH:15` window of any hour — that window overlaps the hourly Sealer merge (§9.d) and the tail risk of an overlap-minute spanning the hour boundary. The recommended operator window is `HH:15 → HH:45`. The deploy tooling acquires the node-level `/data/cryptopanner/.fs-heavy.lock` at the promote step (§9.d); if it cannot acquire it (a rotation or merge is in progress), the promote defers and returns to the operator.

## 6. Node anatomy

- a. **Directory layout** on each node:
    1. `/opt/cryptopanner/`
        - `current/` — symlink to the active version directory
        - `candidate/` — symlink to a staged version (only present during a hot-swap deploy)
        - `versions/<ver>/` — per-version JARs (collector, sealer, uploader, node-agent)
    2. `/etc/cryptopanner/` — configuration files (`config.yaml`, `agent.token`)
    3. `/data/cryptopanner/segments/` — minute-segment files, organized as `<symbol>/<stream>/<date>/minute-<HH-MM>.jsonl.zst`. May transiently contain sibling `*.shadow.jsonl.zst` files during a WS rotation (see [design doc](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §5.3).
    4. `/data/cryptopanner/sealed/` — sealed hourly files awaiting upload, organized as `<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst` + `.sha256` + `.manifest.json`
    5. `/data/cryptopanner/staging/<deploy-id>/` — candidate Collector's write tree during a hot-swap deploy (mirrors `segments/` layout). Removed at deploy cleanup.
    6. `/data/cryptopanner/deploy/` — deploy state: `.lock` (deploy state-machine guard), `history.jsonl`, `active-slot`, `superseded/<deploy-id>/`, `verify-<deploy-id>.report.json`. See [design doc](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §4.1.
    7. `/data/cryptopanner/.fs-heavy.lock` — node-level mutex that serializes the three filesystem-heavy operations (hourly merge by the Sealer, JAR hot-swap promote, WS rotation cutover). Distinct from the deploy lock above: the deploy lock guards the deploy state machine; this lock guards disk-I/O contention across components.
    8. `/data/cryptopanner/logs/` — structured JSON log files per component and per slot: `cryptopanner-collector@<slot>.jsonl`, `cryptopanner-sealer.jsonl`, `cryptopanner-uploader.jsonl`, `cryptopanner-agent.jsonl`.
    9. `/tmp/cryptopanner-*.heartbeat` — heartbeat files touched by each running component. The collector writes per-slot heartbeats: `cryptopanner-collector@a.heartbeat`, `cryptopanner-collector@b.heartbeat`. During a deploy overlap both files are fresh; otherwise only the active slot's file is fresh.

- b. **systemd units.** Five units per node — four logical services, but the Collector is templated across two slots:
    - `cryptopanner-collector@a.service` and `cryptopanner-collector@b.service` — Collector slots (see [design doc](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §3.3). At any moment exactly one is the production slot; the other is empty or running a candidate during a deploy.
    - `cryptopanner-sealer.service`, `cryptopanner-uploader.service`, `cryptopanner-agent.service`.

    All set to `Restart=always` while active. The agent unit has `WatchdogSec=30`. The Node Agent's `POST /restart/collector` targets the currently active slot (read from `/data/cryptopanner/deploy/active-slot`); for granular control, `POST /restart/collector/{slot}` operates on a named slot.

- c. **Boot order.** The pipeline components do not require explicit `After=` ordering between each other — components discover state via the filesystem (existing minute segments, sealed files, S3 manifests) and the Node Agent derives liveness from heartbeats rather than systemd start-completion. Each unit comes up independently when systemd reaches it.

- d. **Resource expectations.** The pipeline is I/O-bound, not CPU-bound. A 2-vCPU, 4GB RAM VPS with 80GB SSD should be sufficient for the initial symbol set. Disk usage is transient — minute segments are cleaned up after hourly upload.

    Two operational events temporarily double Collector memory or connection count:
    - **JAR hot-swap deploy** (rare): two Collector JVMs run concurrently for 1–2 minutes (overlap window).
    - **Daily WS rotation** (automatic, ~daily per node): one Collector JVM with two WS connections for 1–2 minutes.

    `/data` is recommended as a separate volume from `/` so that capture backlog (sealed files on S3 outage), staging trees, and rotation superseded files cannot fill the OS partition.

## 7. Data inventory

- a. **WebSocket streams per symbol.** Per-symbol stream forms are used by default (e.g., `btcusdt@trade`); exceptions are called out inline. Each entry lists the sequence-ID property (gap-detectable / gap-fillable / neither) and the server-side timestamp field used for minute bucketing (see [hot-swap design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §3.4).
    1. `trade` — individual trade events. Sequence ID: `t` (trade ID, strictly increasing). **Gap-detectable and gap-fillable** via REST. Bucketing: `T` (trade time).
    2. `depth@100ms` — order book diff updates. Sequence IDs: `U` (first updateId), `u` (final updateId), `pu` (previous final updateId for the continuity check). **Gap-detectable** via the `pu`-chain; recovery is by fresh snapshot from `/fapi/v1/depth` (snapshot resync, **not** historical replay — mechanism described in §8). Bucketing: `E` (event time).
    3. `aggTrade` — aggregated trade events. Sequence ID: `a` (agg trade ID). **Gap-detectable and gap-fillable** via REST. Bucketing: `T` (trade time).
    4. `kline_1m` — 1-minute candlestick updates. **No sequence ID; not gap-detectable; not gap-fillable.** Fires continuously during a minute (multiple snapshots of the in-progress kline) plus a final closed event with `x: true`. A captured minute holds roughly 60 events per symbol, not 1. Bucketing: `E`.
    5. `ticker` — 24h rolling ticker statistics, 1-second cadence. No sequence ID. Bucketing: `E`.
    6. `bookTicker` — best bid/ask updates. Each event carries a `u` field (order book updateId from the depth update-ID space), but `u` values are sparse and non-contiguous on this stream — the event fires only when the best level changes, skipping any update that doesn't move the top of book. `u` is therefore **not usable for gap detection** despite being present. Treated as no-sequence-ID. Bucketing: `E`.
    7. `markPrice` — mark price and funding rate, 1-second cadence (per-symbol form `<symbol>@markPrice@1s`). No sequence ID. Bucketing: `E`.
    8. `forceOrder` — liquidation events. **All-symbol form** `!forceOrder@arr` rather than per-symbol — liquidations are sporadic, and per-symbol subscriptions waste connection slots on streams that rarely fire. The wrapper identifies the symbol; the Collector partitions to the appropriate per-symbol file. No sequence ID. Bucketing: `E`.

- b. **REST endpoints polled periodically.** Cadences are canonical defaults; overridable via `config.yaml` (§15).
    1. `GET /fapi/v1/depth?symbol=X&limit=1000` — full order book snapshot. Polled every 5 minutes per symbol as a baseline archive anchor; additionally fetched on demand by the depth-handler resync logic on a `pu`-chain break (mechanism in §8).
    2. `GET /fapi/v1/openInterest?symbol=X` — open interest. Polled every 60 seconds per symbol.
    3. `GET /fapi/v1/exchangeInfo` — symbol metadata and trading rules for all symbols. Polled once per day at 00:05 UTC (the same wall-clock time on every node so cross-region comparisons see the same snapshot).

- c. **Symbol set.** Initial deployment captures the **top 20 USD-M Futures perpetuals by 30-day notional volume**, configured in `config.yaml.symbols`. The list is refreshed quarterly by operator decision. The 20-symbol target keeps the combined-streams subscription well below Binance's per-connection stream limit and fits the resource envelope in §6.d. The handling of larger symbol sets that exceed the per-connection limit remains an open question (§16.f).

- d. **Backfill sources (REST, used at hourly merge for gap-fillable streams):**
    1. `GET /fapi/v1/aggTrades?symbol=X&fromId=N` — paginated aggTrades by ID. Source for `aggTrade` gap fill.
    2. `GET /fapi/v1/historicalTrades?symbol=X&fromId=N` — paginated trades by ID. Source for `trade` gap fill.

    Depth gaps are handled at **capture** time by snapshot resync (§7.b.1), not at merge time. Streams that are neither gap-detectable nor gap-fillable (`kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`) have no backfill source — any gap is recorded in the manifest and remains permanent on that node. This is the central motivation for the Collector hot-swap and daily WS rotation (§5.f, §8.b).

## 8. Ingest

a. **WebSocket connection.** In steady state the Collector maintains a single consolidated WebSocket connection to Binance USD-M Futures (`wss://fstream.binance.com/ws`); during a planned WS rotation overlap (§8.b) up to two connections run in parallel briefly. Streams are subscribed via a `{"method":"SUBSCRIBE","params":[...]}` message after connect, not encoded into the URL (the URL form gets unwieldy with this many streams and is harder to mock for tests). For the initial 20-symbol set (§7.c), the subscription totals 141 streams (7 per-symbol × 20 + 1 all-symbol `!forceOrder@arr`), well below Binance's documented 200-streams-per-connection limit; larger symbol sets that exceed the limit remain an open question (§16.f). Public market-data streams require no authentication. Binance pings each connection every 3 minutes and disconnects clients that fail to pong within 10 minutes; the Collector responds to pings automatically as part of its read loop.

b. **Reconnection.** Two distinct cases:
   1. **Unplanned faults** (disconnect, ping timeout, half-open detection): the Collector reconnects immediately with exponential backoff (1s, 2s, 4s, ..., capped at 60s). After reconnection, it re-subscribes to all configured streams. The resulting gap is recorded normally — ID streams may be filled by REST backfill at hourly merge; non-ID streams record the gap in the manifest.
   2. **Planned rotation** (approaching Binance's 24h connection limit): the Collector opens a shadow connection and runs the make-before-break overlap protocol. No gap. The rotation start is constrained to the per-hour window `HH:10 → HH:50` — avoiding the hourly Sealer merge at the start of the hour (§9.d) and ensuring the overlap minute does not span the hour boundary. Within that window, each node's exact rotation minute is deterministic: `(hash(node_id) % 40) + 10` past the hour, so no two nodes in a region rotate at the same minute and Binance never sees a correlated reconnect storm. At cutover the Collector acquires the node-level `/data/cryptopanner/.fs-heavy.lock`; if it cannot (a deploy or merge is in progress), it defers to the next eligible minute within the window. See the [hot-swap and WS rotation design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §5.

c. **Raw capture.** Each incoming WebSocket frame is written as-is (raw bytes, not re-serialized) to the appropriate minute-segment file. The combined-streams wrapper `{"stream": "<name>", "data": {...}}` is part of the captured bytes and serves as the routing key — the Collector parses the wrapper's `stream` field to select the `<symbol>/<stream>/...` subdirectory, but leaves the wrapper intact in the written line (raw-fidelity, invariant 3.a). Frames are bucketed into minute segments by their **server-side event timestamp** (`E` for most streams, `T` for `trade`/`aggTrade`), not by local receive time. This keeps the same logical event in the same minute file across two parallel connections during an overlap. REST poll responses are bucketed by poll-issue wall-clock time (single-source, no overlap concern). Rationale in the [design doc](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §3.4.

d. **REST polling.** REST endpoints are polled on independent timers (cadences pinned in §7.b). Responses are written as raw JSON to the same minute-segment file structure, with a wrapper envelope identifying the source endpoint, request parameters, and poll-issue wall-clock timestamp. Failed polls (HTTP 429 rate-limit, 5xx, timeout, connection error) are themselves recorded as envelopes — same shape but with an `error` field carrying the HTTP status or error class — so that raw-fidelity is preserved for failures and downstream consumers can distinguish "no response captured" from "response captured but Binance returned an error." Retries are scheduled per-endpoint with exponential backoff; both the failed envelope and the eventual successful envelope are kept in the minute file.

e. **Minute-segment rotation.** At each minute boundary, the Collector closes the current segment file (fsync + SHA-256 sidecar) and opens a new one. A short late-frame grace window (default 10s past the minute boundary) allows frames whose server event time falls in the just-closed minute to still be routed there before the file is sealed. The closed segment is immutable after sealing. Minute boundaries and the grace window are evaluated against the **local wall clock** (the server-event-time bucketing in §8.c handles per-frame placement; the local clock controls when files close and seal). Tolerable clock skew between the node and Binance servers is an open question (§16.e).

## 9. WAL & local sealing & upload

- a. **Minute segments as WAL.** The per-minute segment files serve as the write-ahead log. Each segment is small (typically seconds of data) and accompanied by a `.sha256` sidecar. This is the durability boundary — data is safe once the segment is sealed. Sealing order is **write data → fsync(data) → compute SHA-256 → write sidecar → fsync(sidecar) → fsync(containing directory)**, so a power loss at any point either leaves both files durable on disk or rolls back to a pre-write state. The sidecar is the durability acknowledgement that satisfies invariant 3.b.

- b. **Hourly merge (Sealer).** At the turn of each hour (e.g., at 15:00:00 UTC the Sealer processes hour 14):
    1. Collects all minute segments for the previous hour, per (symbol, stream). The Sealer always sees a single `minute-<HH-MM>.jsonl.zst` file per minute — `.shadow.jsonl.zst` overlap files produced by the Collector during a hot-swap or WS rotation have already been merged away before the Sealer runs.
    2. Concatenates them in **minute-filename order** (lexicographic on `minute-HH-MM`). Within each minute, frames retain their capture-arrival order — the Sealer does not re-sort within a minute. (Frames are server-event-timestamp-bucketed at capture time per §8.c, so the per-minute partition is by `E`/`T`; intra-minute ordering reflects how Binance delivered events on the WS connection, which is what raw-fidelity demands.)
    3. For gap-fillable streams (`trade`, `aggTrade` — see §7.d): validates sequence-ID continuity across the concatenated data. If gaps are detected, attempts backfill via REST. Backfilled records are inserted in sequence order, which means the entire hourly file is rebuilt: decompress → merge new records at the right positions → recompress. This is not a streaming append.
    4. Compresses the result with zstd (or finalizes the recompressed form from step 3 if backfill ran).
    5. Writes the `.sha256` integrity sidecar (same durability order as §9.a).
    6. Writes the `.manifest.json` recording: list of minute segments present, list of minute segments missing, sequence-ID range (first, last), any remaining gaps after backfill, backfill attempts and outcomes, and any `deploy_events[]` or `connection_rotation_events[]` that fall within the hour (see 10.f). Deploy events are read from `/data/cryptopanner/deploy/history.jsonl`; rotation events are read from per-Collector rotation records written at promotion time. Both sources are filtered to events whose `promoted_at` falls inside the hour being sealed.

- c. **Upload (Uploader).** The Uploader watches `/data/cryptopanner/sealed/` for completed hourly files and uploads them to IONOS S3. It never reads from or writes to `segments/`, `staging/`, or `deploy/` — those are owned by the Collector and the deploy tooling respectively.
    1. Uploads the three objects per (symbol, stream, hour) in this order: `.jsonl.zst` first, `.sha256` second, `.manifest.json` last. The manifest is uploaded last so its presence in S3 is the consumer-side completeness signal that all three objects are durably stored. An interrupted upload leaves no `.manifest.json`; consumers treat the (symbol, stream, hour) as not yet available and re-check later.
    2. Verifies the upload by comparing the remote ETag or checksum with the local `.sha256`.
    3. On success, deletes the local minute segments for that (symbol, stream, hour) and deletes the corresponding local sealed files (`.jsonl.zst`, `.sha256`, `.manifest.json`).
    4. On failure, retries with exponential backoff capped at one attempt per 5 minutes. There is no try-count limit — S3 outages can last hours and we'd rather accumulate disk than abandon data. Disk-pressure alerts (§13) cover the case where retries cannot drain fast enough.

- d. **Ordering guarantee.** The Sealer does not begin processing hour N until **both** (a) the clock has passed `HH:00:00 + sealer.hour_grace_window` (default `120s`) — giving late-arriving WS frames, in-flight REST polls issued in the closing seconds of hour N, and any wall-clock skew time to settle — **and** (b) the last minute file of hour N has been sealed past its per-minute late-frame grace (§8.e). The Uploader does not upload until the Sealer marks the hour as sealed.

   While processing hour N, the Sealer holds the node-level `/data/cryptopanner/.fs-heavy.lock` (§6.a.7) so that JAR hot-swap promotes (§5.f) and WS rotation cutovers (§8.b.2) do not contend for disk I/O. If a future hour's Sealer is ready to start while a previous hour is still merging, it waits on the lock — hours queue rather than run concurrently.

## 10. Per-node file format & manifest

- a. **Minute-segment file.** Path: `segments/<symbol>/<stream>/<date>/minute-<HH-MM>.jsonl.zst`. Each line is one raw WebSocket frame or REST response envelope, stored as received. Frames are placed into minute files by server-side event timestamp (§8.c), not local receive time. Compressed with zstd. Accompanied by `minute-<HH-MM>.jsonl.zst.sha256`.

- b. **Sealed hourly file.** Path: `sealed/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst`. Concatenation of all minute segments for that hour (lexicographic minute order; arrival order preserved within each minute — §9.b.2), with backfilled records inserted in sequence order for gap-fillable streams (§7.d). Compressed with zstd. Accompanied by `hour-<HH>.jsonl.zst.sha256`.

- c. **S3 object keys.** Three objects per (symbol, stream, hour), all sharing the same prefix:

    ```
    <node-id>/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst
    <node-id>/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst.sha256
    <node-id>/<symbol>/<stream>/<date>/hour-<HH>.manifest.json
    ```

    Where `<date>` is `YYYY-MM-DD` (matches the manifest's `date` field) and `<HH>` is `00`–`23` zero-padded. The manifest object is uploaded last per §9.c.1, so its presence in S3 is the consumer-side completeness signal.

- d. **Manifest format** (`hour-<HH>.manifest.json`). Representative example for an ID-bearing gap-fillable stream (`trade`) with a missing minute, a backfilled gap, and a WS rotation event during the hour:

    ```json
    {
      "manifest_schema_version": 1,
      "node": "vps-fra-1",
      "symbol": "btcusdt",
      "stream": "trade",
      "date": "2026-05-25",
      "hour": 14,
      "sealed_at": "2026-05-25T15:02:08Z",
      "minutes_present": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59],
      "minutes_missing": [15],
      "partial_minutes": [],
      "sequence_id_range": { "first": 123456, "last": 234567 },
      "sequence_gaps": [
        { "from_id": 145000, "to_id": 145042, "count": 43, "minute": 15, "backfill_outcome": "FILLED" }
      ],
      "backfill_attempts": [
        {
          "stream": "trade",
          "from_id": 145000,
          "to_id": 145042,
          "source_endpoint": "/fapi/v1/historicalTrades",
          "requests_made": 1,
          "outcome": "FILLED",
          "completed_at": "2026-05-25T15:01:47Z"
        }
      ],
      "late_frames_dropped": 0,
      "file_sha256": "abcdef0123456789...",
      "file_size_bytes": 1048576,
      "uncompressed_size_bytes": 9437184,
      "record_count": 42000,
      "deploy_events": [],
      "connection_rotation_events": [
        {
          "rotation_id": "rot-2026-05-25T14:23:00Z",
          "reason": "SCHEDULED",
          "old_connection_age_hours": 22.91,
          "promoted_at": "2026-05-25T14:24:00Z",
          "minutes_merged": [23],
          "verify_result": "PASS",
          "diff_summary": { "id_streams": "0 symmetric diff", "non_id_streams": "0 diff" }
        }
      ]
    }
    ```

    Field semantics:
    - `manifest_schema_version` — integer; downstream consumers parse based on this. Bumped when fields are added/removed in a non-additive way.
    - `file_sha256`, `file_size_bytes` — describe the **compressed** `.jsonl.zst` file as written to disk and uploaded to S3 (`file_sha256` matches the `.sha256` sidecar contents).
    - `uncompressed_size_bytes` — the JSONL payload size before zstd, for sizing intuition.
    - `record_count` — number of JSONL lines in the **final** file (post-backfill).
    - `late_frames_dropped` — count of frames received after the §8.e seal grace and discarded. Aggregated across all minutes of the hour. Non-zero values inform tuning of §16.j.
    - `partial_minutes` — minutes whose file exists but is known to be truncated (Collector crashed mid-minute, file fsynced up to the crash point). The minute still counts as `minutes_present`; the internal gap is also recorded in `sequence_gaps`.
    - `sequence_gaps[].from_id`/`.to_id` — inclusive bounds of the missing range. `count` = `to_id - from_id + 1` for trade/aggTrade; for depth, `from_id`/`to_id` are the surrounding `u` values bracketing a `pu`-chain break.
    - `backfill_outcome` — `FILLED`, `PARTIAL`, `FAILED`, or `SKIPPED_NO_SOURCE` (the last for non-fillable streams).

- e. **Per-stream-type schema variations.**
    - **ID-bearing gap-fillable** (`trade`, `aggTrade`): all fields populated as in the example.
    - **ID-bearing gap-detectable-only** (`depth@100ms`): `sequence_id_range` reports the first `U` and the last `u` over the hour; `sequence_gaps[]` entries describe `pu`-chain breaks; `backfill_attempts[]` entries record snapshot-resync attempts rather than historical-replay calls; an additional `depth_anchor_snapshots` field lists the minutes during the hour at which a baseline `/fapi/v1/depth` snapshot was captured (§7.b.1).
    - **Non-ID** (`kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`): the manifest omits `sequence_id_range`, `sequence_gaps`, and `backfill_attempts` entirely. Only `minutes_present`, `minutes_missing`, `partial_minutes`, and the file/size/count fields are populated. Any `minutes_missing` are permanent (no backfill source — see §7.d).

- f. **Optional event arrays.** Two optional arrays are populated only when the corresponding event crossed the hour:
    - `deploy_events[]` — one entry per Collector hot-swap deploy that promoted during this hour. Fields: `deploy_id`, `old_version`, `new_version`, `promoted_at`, `minutes_merged`, `verify_result`, `verify_report_sha256`.
    - `connection_rotation_events[]` — one entry per scheduled WS rotation that promoted during this hour. Fields: `rotation_id`, `reason`, `old_connection_age_hours`, `promoted_at`, `minutes_merged`, `verify_result`, `diff_summary` (see example in §10.d).

    Schemas and merge semantics in the [hot-swap and WS rotation design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §6.

## 11. Health & observability

a. **Structured logging.** Every component writes JSON Lines logs to `/data/cryptopanner/logs/`. Per-component paths follow §6.a.8: the collector writes per-slot files (`cryptopanner-collector@a.jsonl`, `cryptopanner-collector@b.jsonl`); sealer, uploader, and agent each write one file. Each log entry has the shape:

    ```json
    {
      "ts": "2026-06-12T14:23:47.512Z",
      "component": "cryptopanner-collector",
      "slot": "a",
      "event": "ws_connect",
      "level": "INFO",
      "stream_count": 141,
      "endpoint": "wss://fstream.binance.com/ws"
    }
    ```

    Required fields: `ts` (ISO-8601 with millisecond precision), `component`, `event` (machine-readable identifier from §11.e), `level`. `slot` is required for collector logs and omitted for other components. Domain-specific fields (`stream_count`, `endpoint`, `rotation_id`, etc.) are added per event.

    Log levels: `DEBUG`, `INFO`, `WARN`, `ERROR`. Production default is `INFO`; `DEBUG` is enabled via config reload (no process restart needed). Log rotation and retention are deferred to §16.d.

b. **Heartbeat files.** Each running component touches its heartbeat file every 5s on its main loop iteration. The collector touches a per-slot heartbeat (`/tmp/cryptopanner-collector@<slot>.heartbeat` per §6.a.9). During a deploy or WS rotation overlap, both slots' files are fresh; in steady state only the active slot's file is fresh.

    The Node Agent derives component state from mtime (5s touch cadence → 3 missed heartbeats = stuck warning, 12 missed = effectively down):
    - `running` (mtime ≤ 15s)
    - `degraded` (15s < mtime ≤ 60s) — main loop slow, possibly blocking on I/O; early warning
    - `stuck` (mtime > 60s, systemd reports active) — alive but not progressing
    - `down` (systemd reports inactive or failed)

c. **Node Agent endpoints.**

    `GET /status` — point-in-time JSON snapshot scraped by the Monitor every 5s:

    ```json
    {
      "node": "vps-fra-1",
      "scraped_at": "2026-06-12T14:23:50Z",
      "components": {
        "cryptopanner-collector@a": { "state": "running",  "pid": 1234, "heartbeat_age_s": 2.1, "uptime_s": 83412 },
        "cryptopanner-collector@b": { "state": "down",     "pid": null, "heartbeat_age_s": null, "uptime_s": 0 },
        "cryptopanner-sealer":      { "state": "running",  "pid": 1235, "heartbeat_age_s": 3.4, "uptime_s": 83410 },
        "cryptopanner-uploader":    { "state": "running",  "pid": 1236, "heartbeat_age_s": 1.9, "uptime_s": 83410 },
        "cryptopanner-agent":       { "state": "running",  "pid": 1237, "heartbeat_age_s": 0.5, "uptime_s": 83410 }
      },
      "active_slot": "a",
      "deploy":   { "state": "IDLE" },
      "rotation": { "state": "IDLE", "current_connection_age_s": 76800 },
      "vps": {
        "cpu_percent": 14.2,
        "memory_percent": 38.0,
        "load_average_1m": 0.42,
        "disk": {
          "/":     { "percent": 22.3, "free_bytes": 30400000000 },
          "/data": { "percent": 41.8, "free_bytes": 41200000000 }
        }
      }
    }
    ```

    Disk usage is reported per mount; at minimum `/` and `/data` (§6.d). The §13 disk-pressure alert thresholds against `/data`.

    During an active deploy, `deploy.state` reflects the state-machine value (`STAGED`, `OVERLAP_READY`, `VERIFIED`, `PROMOTING`, `PROMOTED`) with a `deploy_id` field; the Monitor renders a banner on the dashboard. Similarly `rotation.state` becomes `OVERLAP_VERIFYING` or `CUTOVER_PENDING` during a rotation, with `rotation_id` and `old_connection_age_s`.

    `GET /metrics` — Prometheus-style exposition for historical metrics. Counters: `cryptopanner_late_frames_dropped_total`, `cryptopanner_backfill_attempts_total`, `cryptopanner_uploads_total`, `cryptopanner_rotation_events_total`. Gauges: `cryptopanner_heartbeat_age_seconds`, `cryptopanner_current_connection_age_seconds`, `cryptopanner_sealed_files_pending_upload`, plus VPS metrics. Scraped by an external Prometheus or aggregator if the operator chooses to deploy one; the Monitor does not consume it. Same VPN-bound port as `/status`, no additional auth required (read-only).

d. **Monitor dashboard.** The Monitor serves a simple HTML page at `GET /dashboard` showing all nodes and their component states, with auto-refresh every 5s (matching the scrape interval in §13). Backed by `GET /api/nodes` (JSON). A deploy or rotation in progress on any node surfaces as a banner on that node's card; circuit-breaker trips and unreachable nodes are highlighted at the top of the dashboard.

e. **Log events of interest** — machine-readable event identifiers (snake_case) that the Monitor, the audit tooling, and the Sealer (when populating manifest event arrays per §10.f) depend on. Stable identifiers; do not rename without a migration:
    - WS connection lifecycle: `ws_connect`, `ws_disconnect`, `ws_ping_missed`, `late_frame_after_seal`
    - Minute / hour sealing: `minute_sealed`, `hour_merge_started`, `hour_sealed`
    - Backfill: `backfill_attempted`, `backfill_succeeded`, `backfill_failed`
    - Upload: `upload_started`, `upload_succeeded`, `upload_failed`
    - WS rotation: `rotation_started`, `rotation_verify_passed`, `rotation_verify_failed`, `rotation_promoted`, `rotation_forced_cutover`
    - Deploy: `deploy_staged`, `deploy_verified`, `deploy_promoted`, `deploy_aborted`, `deploy_resumed`
    - Heartbeat-state transitions (emitted by the Node Agent, not the components themselves): `component_running`, `component_degraded`, `component_stuck`, `component_down`

    Each event includes its domain-specific fields — e.g., `rotation_started` carries `rotation_id`, `old_connection_age_s`, `streams_subscribed`; `late_frame_after_seal` carries `symbol`, `stream`, `late_by_ms`, `server_event_time`.

## 12. Failure model

a. **Collector crash.** With slot templating (§6.b), the response depends on which slot crashed:
   - **Active slot crashes** → systemd restarts the unit (`Restart=always`). Frames are not captured during the downtime; the gap is recorded normally. The last minute file may be truncated mid-zstd-frame: the Sealer truncates to the last successfully-decompressed frame, emits a `partial_minute` event (§11.e), and records the affected minute in the manifest's `partial_minutes[]` (§10.e). For ID-bearing gap-fillable streams, REST backfill at hourly merge fills what it can; non-ID streams lose the window permanently (§7.d).
   - **Candidate slot crashes during a deploy** → no production impact (the active slot keeps running). The deploy state machine moves to a failed STAGED state; the operator runs `cryptopanner-deploy abort` (design doc §4.5).
   - **One of two slots crashes during a WS rotation overlap** → the surviving connection continues. The rotation falls into early cutover (design doc §5.4): the surviving connection becomes primary, the failed slot is restarted clean.

b. **Collector WebSocket disconnect.** Handling follows the §8.b split:
   - **Unplanned** (network blip, ping timeout): immediate reconnect with exponential backoff (1s, 2s, 4s, ..., 60s). Frames during the disconnect window are missing. For ID-bearing gap-fillable streams (trade, aggTrade), REST backfill at hourly merge fills the gap. For non-ID streams (kline_1m, ticker, bookTicker, markPrice, forceOrder), the data is **permanently lost** on this node and recorded as gaps in the manifest (no REST source — §7.d).
   - **Planned** (24h-approaching rotation): no disconnect window — the rotation overlap covers it (§8.b.2).

c. **Sealer crash during merge.** The Sealer holds `/data/cryptopanner/.fs-heavy.lock` while merging (§9.d). On process death, flock releases automatically (process-scoped). On restart, the Sealer scans `sealed/` for any partially-written hourly files (data without sidecar, or sidecar with mismatched SHA); these are deleted and the merge re-runs from the still-present minute segments. If a fully sealed file exists for the hour, the Sealer skips it (idempotent).

d. **Uploader crash.** On restart, the Uploader scans `sealed/` for files whose corresponding minute segments still exist locally (the deletion in §9.c.3 is the durable "uploaded" marker). It re-attempts upload. If the prior attempt had completed data + sidecar but not the manifest, all three are re-uploaded; S3 keys are deterministic (§10.c) so the result is byte-identical. Per the manifest-last upload order (§9.c.1), consumers see a partial set as "not yet uploaded" and only treat the hour as durable when the manifest object appears.

e. **VPS reboot.** systemd restarts services per §6.c (no explicit `After=` between pipeline components; each starts independently). On collector startup:
   1. Read `/data/cryptopanner/deploy/active-slot` to determine which slot to start in `primary` role. The other slot stays empty.
   2. Run startup recovery (design doc §5.5): scan `segments/` for any `*.shadow.jsonl.zst` files left over from an in-flight rotation; for each pair (`*.jsonl.zst` + `*.shadow.jsonl.zst`), run `OverlapMerger` to produce a single merged file. Idempotent.
   3. Open WS connection and resume capture.

   The Sealer then catches up on any un-merged past hours (subject to §9.d). The Uploader scans for un-uploaded sealed files. Data during the downtime is recorded normally (same handling as 12.a / 12.b).

f. **IONOS S3 outage.** Uploader retries with exponential backoff capped at 5 min between attempts; no try-count limit (§9.c.4). Sealed files accumulate on local disk. The §13 disk-pressure alert (against `/data` per §11.c) fires once the warn threshold is exceeded; if `/data` usage approaches full, the operator must intervene (manual cleanup or expanded volume). When S3 recovers the Uploader drains the backlog automatically.

g. **Monitor VPS down.** Nodes continue operating independently — the Monitor is not in the data path. The external dead-man's switch (Healthchecks.io) detects the Monitor's absence and alerts the operator.

h. **VPN mesh failure.** Nodes continue capturing and uploading — VPN is only used for Monitor↔Node communication. Data flow (node → S3) goes over the public internet (HTTPS to IONOS). Monitor loses visibility but no data is lost.

i. **Hot-swap deploy stuck.** Operator left a deploy in STAGED or OVERLAP_READY without promoting or aborting. The candidate JVM keeps running — consuming a second WS connection and writing to the staging tree, which grows linearly with time. Detection: §13 alerts Warning when the deploy state machine has not progressed for >1h. Resolution: operator runs `cryptopanner-deploy abort` (terminates candidate JVM, deletes staging tree) or proceeds with verify + promote.

j. **Rotation equivalence repeatedly fails.** Per design doc §5.4: first FAIL logs Warning and re-verifies after the next minute; 3 consecutive FAILs escalate Critical and force the cutover anyway (better one connection than approaching the 24h cliff). The forced cutover writes `verify_result: "FORCED"` and the equivalence diff into the manifest event so the divergence is auditable. No silent acceptance.

k. **Extended Binance outage (hours).** Collector continues attempting reconnect with backoff capped at 60s. Until Binance recovers, no minute files are written. The Sealer at each hour boundary records every minute as missing. For ID-bearing gap-fillable streams, REST backfill at hourly merge fills what it can once the REST endpoints are reachable again. Non-ID streams lose the entire outage window permanently. Monitor alerts Critical on the prolonged WS disconnect; archive completeness is correctly reflected in manifests.

l. **REST rate-limit (429) storm.** Failed polls are recorded as failure envelopes (§8.d); the Collector backs off and retries the affected endpoint. Sustained 429s degrade backfill success at hourly merge — unfilled gaps are recorded in `sequence_gaps[].backfill_outcome: "PARTIAL"` or `"FAILED"`. Monitor alerts Warning on elevated failed-poll rate; Critical if rate-limit pressure persists across multiple hourly merges.

m. **`active-slot` file corruption or mismatch.** If the file is unreadable, missing, or contradicts running systemd states (e.g., reports `a` but only slot `b` has a live PID), the Node Agent does not auto-resolve. Detection: discrepancy is flagged in `/status` as `active_slot: "MISMATCH"` and surfaces as Critical alert. Resolution: operator inspects systemd states and the per-slot heartbeat mtimes, then rewrites the file manually (the live slot is whichever has the recently-touched heartbeat).

n. **Clock skew detected.** Broken NTP causes frames to be mis-bucketed by server-event-time vs local time comparisons (§8.c, §8.e). Detection: heuristic comparing recent frames' server-event-time against local time; emit `clock_skew_detected` log event (threshold deferred to §16.e). Mitigation: Warning alert; operator restarts the node's NTP service. Mis-bucketed frames are not auto-corrected — the manifests will show anomalies that the operator must investigate.

## 13. Reliability & alerting

- a. **Alert channels.** **Telegram** is the primary alert channel (operator-group webhook); **WhatsApp** is an optional secondary configured per-operator deployment. Both receive every alert. Two severity levels:
    1. **Warning** — early-warning conditions the operator should address within hours:
        - Component `degraded` (heartbeat 15s < age ≤ 60s — §11.b) persisting for >2 min
        - Component `stuck` (heartbeat age > 60s, systemd still active)
        - Upload backlog age > 30 min (oldest sealed file unuploaded — §9.c.4)
        - Disk usage on `/data` > 80%
        - Deploy state machine stuck (no progression) for > 1h (§12.i)
        - Single failed WS rotation attempt (§12.j)
        - Elevated REST failed-poll rate (>10% of polls returning 429/5xx over a 10-min window — §12.l)
        - Clock skew detected on a node (§12.n)
    2. **Critical** — immediate-intervention conditions:
        - Component `down` (3 consecutive scrape failures from `/status`)
        - Circuit breaker tripped (3 restart failures in 5 min — see §13.b)
        - Node unreachable from Monitor (VPN, network, or VPS dead)
        - Monitor's own health degraded
        - Disk usage on `/data` > 95%
        - Extended Binance outage: WS disconnect persisting > 5 min on a Collector slot (§12.k)
        - 3 consecutive failed WS rotation attempts, or a forced cutover after 3 consecutive equivalence FAILs (§12.j; design doc §5.4)
        - REST rate-limit pressure persisting across > 2 consecutive hourly merges (§12.l)
        - `active-slot` MISMATCH between the file and running systemd states (§12.m)

- b. **Restart policy.** The Monitor attempts `POST /restart/<component>` with exponential backoff: 5s → 15s → 60s → 300s. For the Collector, restart targets the currently **active** slot only (read from `/status.active_slot` — §11.c); the empty slot is never auto-restarted. After 3 failures within 5 minutes, the circuit breaker trips for that (node, component) pair: no more restart attempts, Critical alert to operator.

- c. **Dead-man's switch.** The Monitor pushes a heartbeat to Healthchecks.io every 60s. If pushes stop for 5 minutes, Healthchecks.io alerts the operator via email/SMS. This covers the "Monitor itself is down" and "entire VPS is dead" scenarios.

    Additionally, the Monitor fires a **daily self-test alert at 02:00 UTC** ("alert path healthy") on the Telegram/WhatsApp channels. If the operator stops seeing this message, the alert channel has silently failed (revoked bot token, changed webhook URL, etc.) even though the Monitor and Healthchecks.io are alive.

- d. **No false-positive restarts.** The Monitor uses a two-tier check before triggering a restart: (1) systemd reports the unit as failed or inactive, AND (2) heartbeat-derived state is `stuck` or `down` (§11.b). A `degraded` component (heartbeat 15s < age ≤ 60s) is not restarted — it's observation-only, surfaced via the persisting-degraded Warning so the operator can decide whether to intervene. A `running` component is never restarted.

- e. **Alert deduplication, correlation, and latency.**
    - **Dedup key**: `(node, component, alert-type)`. A repeat firing of the same key within 1 hour of the previous alert is suppressed.
    - **Recovery signal**: when a condition clears, the Monitor emits a one-time `recovered` message on the same channels so the operator isn't left wondering whether the alert is still active.
    - **Correlation/grouping**: if 3 or more nodes fire the same alert-type within 1 minute, the Monitor sends one combined message listing the affected nodes instead of N separate pages. Avoids alert storms during region-wide events (S3 outage, Binance outage).
    - **End-to-end latency**: condition occurrence to operator notification is typically 10–30 seconds (5s scrape interval + ~5s alert pipeline + ~1–2s webhook delivery). Operators should calibrate response expectations accordingly.

## 14. Testing strategy

- a. **Test layers**, fastest to most expensive:
    1. **Unit** — module-level isolation; no I/O, no docker. Key areas: `MinuteSegmentWriter` server-event-time bucketing (§8.c), `EquivalenceChecker` over synthetic primary/shadow pairs, `OverlapMerger` for ID and non-ID streams (§7.d), sequence-ID continuity validation (including `pu`-chain for `depth@100ms`), manifest generation against schema_version=1 (§10.d), SHA-256 computation, S3 key derivation, slot state-machine transitions (§6.b), late-frame-after-seal accounting (§8.e, §10.d).
    2. **Integration** — in-process pair-wise tests without the full docker stack. WsConnectionManager + MinuteSegmentWriter; Sealer + Uploader against a local filesystem and a single MinIO container; deploy state machine against a fake systemd interface.
    3. **Chaos / end-to-end** — full docker-compose stack with injected faults. See §14.e for the catalogue.

- b. **Local end-to-end stack.** A `docker-compose.yml` at the repo root brings up the complete pipeline locally with one command (e.g. `make dev-up`):
    1. `mock-binance-ws` — replays a captured frame fixture; supports fault injection (§14.c).
    2. `mock-binance-rest` — serves REST fixtures for `/historicalTrades`, `/aggTrades`, `/depth`, `/openInterest`, `/exchangeInfo`; supports 429 / 5xx / timeout injection.
    3. `minio` — S3-compatible storage standing in for IONOS S3.
    4. `mock-healthchecks` — accepts heartbeat pings; exposes a query endpoint so tests can assert push timing.
    5. `cryptopanner-collector-a`, `cryptopanner-collector-b` — both slot units run as containers.
    6. `cryptopanner-sealer`, `cryptopanner-uploader`, `cryptopanner-agent` — the other three node services.
    7. `cryptopanner-monitor` — the Monitor with dashboard.

    All services share a single bridge network (no VPN locally). The Node Agent binds to `0.0.0.0` in test mode rather than the VPN interface (§4.f). Successful bring-up = `GET /dashboard` shows all nodes `running`, MinIO contains the expected sealed objects within a few minutes.

- c. **Mock Binance servers** — capabilities required:
    1. **WS mock**: replay a captured frame fixture; inject clean disconnect; inject ping timeout (no pong response); inject 24h forced close; inject half-open WS (TCP alive, no frames); inject frame reordering and late frames; serve identical fan-out to two parallel connections for rotation overlap tests.
    2. **REST mock**: serve fixture responses; inject 429 (rate limit), 5xx, and timeouts; vary response latency; serve out-of-order `fromId` paginations for adversarial backfill tests.

- d. **Test fixtures and replay determinism.** Captured raw-frame fixtures live under `tests/fixtures/binance/` — one minute of frames per stream type (`trade`, `depth@100ms`, `aggTrade`, `kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`), plus a matching `/depth` snapshot. Feeding the same fixture into the same Collector configuration twice **MUST produce byte-identical sealed `.jsonl.zst` files** (modulo the `sealed_at` timestamp in the manifest). This is the critical invariant that makes the make-before-break overlap protocol reviewable — without it, equivalence checks become non-deterministic.

- e. **Chaos test catalogue.** Each scenario is one script at `tests/chaos/NN_<name>.sh` that spins up an isolated docker-compose project (`cryptopanner-chaos-NN`), injects a specific fault, then asserts (via `cryptopanner-verify`) that the archive ends up in the expected state. One-to-one mapping with §12 plus hot-swap-specific scenarios:

    | # | Scenario | Maps to | Expected outcome |
    |---|---|---|---|
    | 01 | `collector_active_crash` | §12.a | Active slot SIGKILL mid-minute; systemd restarts; `partial_minute` recorded; ID gaps backfilled; non-ID gap permanent in manifest |
    | 02 | `collector_candidate_crash_during_deploy` | §12.a | Candidate JVM SIGKILL during overlap; deploy aborts cleanly; no production impact |
    | 03 | `ws_disconnect_unplanned` | §12.b | Network blip mid-stream; immediate reconnect; ID gap backfilled; non-ID gap recorded permanent |
    | 04 | `sealer_crash_mid_merge` | §12.c | SIGKILL during merge; on restart, partial sealed file deleted; merge re-runs; `.fs-heavy.lock` auto-released |
    | 05 | `uploader_crash_mid_upload` | §12.d | SIGKILL between data and manifest upload; on restart all three re-uploaded idempotently; consumers see manifest only on completion |
    | 06 | `vps_reboot_with_rotation_in_flight` | §12.e | Compose host restart while a `.shadow.jsonl.zst` exists; startup recovery merges it; capture resumes |
    | 07 | `s3_outage_extended` | §12.f | MinIO killed for 30+ min; Uploader retries indefinitely; on recovery the full backlog drains; `/data` disk-pressure Warning fires at 80% |
    | 08 | `monitor_down` | §12.g | Monitor container stopped; nodes continue capturing; mock-healthchecks records absence of push |
    | 09 | `vpn_partition` | §12.h | Monitor↔Node network broken; data flow to S3 continues; Monitor reports node unreachable Critical |
    | 10 | `hot_swap_deploy_stuck` | §12.i | Deploy left in STAGED for >1h (simulated time); Warning fires; abort path verified |
    | 11 | `rotation_equivalence_fail_forced_cutover` | §12.j | Mock injects divergence; 3 consecutive FAILs; forced cutover; `verify_result: "FORCED"` in manifest |
    | 12 | `binance_outage_hours` | §12.k | WS mock unreachable for 2 hours of simulated time; minutes recorded as missing; partial backfill on recovery for ID streams |
    | 13 | `rest_429_storm` | §12.l | REST mock returns sustained 429s; failed-poll envelopes recorded; backfill outcomes show `PARTIAL` / `FAILED`; alert escalation |
    | 14 | `active_slot_corruption` | §12.m | `active-slot` file rewritten to a value not matching running PIDs; MISMATCH flag surfaces in `/status`; operator-rewrite path verified |
    | 15 | `clock_skew` | §12.n | Time injection drifts node clock by minutes; `clock_skew_detected` log event emitted; frames continue bucketing by server-event-time |
    | 16 | `hot_swap_happy_path` | §5.f | Full deploy: stage → verify (PASS) → promote → cleanup; overlap minutes merged; `active-slot` flipped |
    | 17 | `rotation_happy_path` | §8.b.2 | Connection-age trigger; rotation completes; `connection_rotation_events[]` recorded in manifest |
    | 18 | `lock_contention_sealer_vs_rotation` | §9.d | Rotation tries to cutover while Sealer holds `.fs-heavy.lock`; rotation defers to next minute; both complete |

    Pass criterion for every scenario: `cryptopanner-verify --base-dir <test-dir> --date <test-date>` exits 0 with `ERRORS=0` and the expected gap/event annotations appear in the manifest. Failures are preserved under the scenario's compose project name for postmortem.

- f. **`cryptopanner-verify` CLI tests.** The audit/integrity CLI is unit + integration tested. Subcommands: `verify` (archive integrity), `manifest` (inspect), `gaps` (gap report), `integrity` (SHA-256 check). Coverage includes schema_version=1 manifests, all per-stream-type variants (§10.e), and degraded-input handling (truncated zstd, missing sidecar, mismatched SHA).

- g. **Performance / load tests.** Run the local stack at the §7.c target load (20 symbols × 8 streams = 141 subscriptions, §8.a) for at least 2 hours of wall-clock or 24 hours of simulated time. Assert: no missed minutes outside injected faults, `late_frames_dropped` < 0.1% of total frames, CPU and memory headroom on the §6.d 2vCPU/4GB envelope, hourly merge completing well before the next hour starts.

- h. **Multi-node independence test.** Two node containers run in parallel against the same mock-binance-ws (each acquires its own WS connection — same as production). Assert: each produces an independent complete archive in its own S3 prefix (invariant 3.d); failure of one does not affect the other.

- i. **Monitor tests.** Test the Monitor's scrape loop, restart logic with active-slot targeting (§13.b), exponential backoff, circuit breaker, alert deduplication, correlation grouping (§13.e), and recovery messages against a mock Node Agent that simulates the §11.b heartbeat-state transitions.

- j. **CI integration.** Unit and integration tests run on every PR via the standard build (`./gradlew build`). The chaos suite runs on every PR via a JUnit wrapper (e.g., `:consolidation:test --tests "*ChaosVerifyIT*"`) that iterates the §14.e catalogue; individual scripts can be invoked standalone (`bash tests/chaos/NN_*.sh`). Chaos scenarios are parallelized where their compose projects don't share ports.

- k. **VPN-independent testing.** All tests run locally on a single host without requiring a VPN mesh. The Node Agent binds to `0.0.0.0` in test mode; production deployment binds to the VPN interface (§4.f).

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
    13. `collector.connection_max_age` — proactive WS rotation trigger (default `23h`; see [hot-swap design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §7)
    14. `collector.frame_buffer_window` — late-frame buffering before minute flush (default `5s`)
    15. `collector.seal_grace_window` — grace period past the minute boundary before sealing (default `10s`)
    16. `collector.rotation_window` — allowed per-hour window for daily WS rotation start (default `HH:10-HH:50`; see §8.b.2)
    17. `sealer.hour_grace_window` — delay past the hour boundary before the Sealer begins processing the just-closed hour (default `120s`; see §9.d)
    18. `deploy.staging_root` — staging tree for hot-swap candidate JVM (default `/data/cryptopanner/staging`)
    19. `deploy.forbidden_window` — per-hour window in which JAR hot-swap promotes are refused (default `HH:50-HH:15`; see §5.f)
    20. `deploy.recommended_window` — operator-runbook target window for deploys (default `HH:15-HH:45`)
    21. `deploy.superseded_retention_days` — rollback window for rotation merges (default `7`)
    22. `deploy.versions_kept` — JAR version rollback reserve (default `2`)
    23. `paths.fs_heavy_lock` — path of the node-level filesystem-heavy mutex (default `/data/cryptopanner/.fs-heavy.lock`; see §6.a.7)

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

g. **Sealer scheduling under prolonged merge.** The Sealer is now scheduled at `HH:00 + 120s` and serializes hours via the `.fs-heavy.lock` (§9.d), so if one hour's merge runs long, the next hour queues behind it rather than running concurrently. **Open question:** is there an upper-bound timeout after which the Sealer should give up on a stuck merge and move on (recording the hour as incomplete in the manifest), or should it block indefinitely? The current default is "block indefinitely" — needs validation under load.

h. **Node identity in S3.** The `node-id` is used as the S3 key prefix. What happens if a node is replaced (new VPS, same region)? Should it reuse the old node ID or get a new one?

i. **VPN provider decision.** Tailscale (managed, free tier) vs raw WireGuard (self-managed, no external dependency). Decision deferred to deployment phase.

j. **Late-frame grace defaults.** The defaults for `collector.frame_buffer_window` (5s) and `collector.seal_grace_window` (10s) — see 15.b — are guesses pending observed worst-case `E`-to-local-receive delay across Binance regions. To be tuned based on `late_frame_after_seal` event counts in early deployment.

k. ~~WS rotation jitter across nodes.~~ **Resolved 2026-06-09:** rotation start is constrained to `HH:10 → HH:50` and within that window each node uses a deterministic offset `(hash(node_id) % 40) + 10` minutes past the hour, so no two nodes in a region rotate at the same minute. Documented in §8.b.2.
