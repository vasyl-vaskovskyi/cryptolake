# CryptoPanner — Master Specification

**Status:** Reviewed end-to-end.

**Review progress:**
- Sections 1–4: **APPROVED** (reviewed 2026-05-25)
- Sections 5–10: **APPROVED** (reviewed 2026-06-09; adds Collector hot-swap referencing [superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md))
- Sections 11–16: **APPROVED** (reviewed 2026-06-12)

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
    6. Backup, cross-region replication, or other disaster recovery for the S3 archive — managed by the operator at the S3-bucket level (versioning, lifecycle rules, replication to a second bucket). The pipeline guarantees per-node, write-once durability into S3; everything beyond that is operator-managed.

## 3. Invariants

- a. **Raw-payload fidelity.** Bytes received from the WebSocket or REST response are preserved verbatim inside a one-line capture envelope (the `ws_frame` envelope for WebSocket frames, the `rest_response` envelope for REST responses). The envelope adds capture metadata — `received_at`, and a `raw_sha256` over the payload — and stores the original payload byte-for-byte in its `raw` field. The Collector may parse a copy of the payload to route and bucket it (§8.c), but the stored `raw` is never the product of parse-then-re-serialize: it is always recoverable exactly as received.
- b. **Durability before acknowledgement.** A minute segment is considered sealed only after the file is fsynced to disk and the `.sha256` sidecar is written. The upload step begins only after the hourly merge completes and its integrity sidecar is verified.
- c. **Manifest is the source of truth.** Every hourly file has a `.manifest.json` that records which minute segments are present, which are missing, and — for ID-bearing streams — any sequence gaps that could not be backfilled. If the manifest says a gap exists, it exists. If the manifest says the file is complete, it is complete.
- d. **Per-node independence.** A node is a single VPS instance running its own capture pipeline. Each node operates in isolation — no node reads from, writes to, or coordinates with another node. Cross-region logic is external to this system.
- e. **Idempotent upload.** Uploading the same hourly file twice produces the same object in storage. The object key encodes (node, symbol, stream, day, hour) — a given key is written once and never mutated.
- f. **No silent data loss.** The system does not track or classify runtime failures (crashes, disconnects, restarts). Gap detection happens only by sequence-ID validation and only during the hourly merge step. If a gap is found and cannot be backfilled via REST, it is recorded in the manifest. Missing minute segments are likewise recorded. No other mechanism claims or infers completeness.

- g. **Encoding and wire formats.** Cross-cutting invariants on how data is represented on disk and on the wire — pinning prevents two implementations from producing divergent but "equivalent" outputs:
    1. **Character encoding.** All text files (`.jsonl`, `.jsonl.zst` payload, `.sha256`, `.manifest.json`, configs, logs) are **UTF-8** without BOM.
    2. **JSONL line terminator.** Lines are separated by a single LF (`\n`, U+000A). CRLF is not produced and not accepted on parse.
    3. **JSON parsing.** Strict JSON per RFC 8259 (no comments, no trailing commas, no JSON5).
    4. **Symbol identifiers.** Lowercase ASCII (`btcusdt`, not `BTCUSDT`) anywhere a symbol appears in a path, an S3 key, a manifest field, or a stream subscription name. Binance's payload `s` field is uppercase by their convention — the Collector preserves the field exactly as received in the raw frame, but lowercases when deriving paths.
    5. **Sequence-ID integer width.** Binance sequence IDs (`t`, `a`, `U`, `u`, `pu`) are 64-bit. Parsers must read these as Java `long` / equivalent unsigned-safe 64-bit type. Parsing as `int` truncates; parsing as `double` loses precision.
    6. **Binance schema is extensible — raw-fidelity protects us.** Binance may add fields to event payloads without notice (observed: `X` execution-type field on `trade`, not documented in older API references). The pipeline never declares an exhaustive set of expected fields; raw bytes are stored verbatim per invariant 3.a, and any new field is captured automatically. Sequence-ID validation and equivalence checks only consult specific named fields (e.g., `t`, `u`); they ignore unknown fields rather than failing.

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

- f. Security for the restart endpoint: bearer token in header, timestamp validation (reject requests older than 30s), and VPN-level ACLs restricting access to the Monitor's IP only. Read-only endpoints (`/status`, `/dashboard`, `/metrics`) require no additional auth. The bearer token is an **opaque random string of at least 32 bytes**, base64url-encoded (no padding), generated by the install script per node and stored at `/etc/cryptopanner/agent.token` with mode 0400 owned by the agent UID. The token is not a JWT — there is no claim parsing, no key rotation infrastructure, no expiry. Token rotation is a deploy-time concern: the operator regenerates the file, restarts the agent, and updates the Monitor's `token_file` mapping.

## 5. Topology & deployment

a. **Node placement.** At least two VPS nodes in geographically separate regions (e.g., Tokyo and Frankfurt). Each node captures the full set of configured symbols and streams independently. Region selection prioritizes low latency to Binance API servers and provider diversity.

b. **Monitor placement.** A single Monitor VPS hosted on a different provider than the nodes (e.g., nodes on IONOS, Monitor on Hetzner or OVH). Cheapest tier sufficient — 1 vCPU, 1GB RAM.

c. **Object storage.** IONOS S3-compatible object storage. Each node uploads to a node-specific prefix: `s3://<bucket>/<node-id>/<symbol>/<stream>/<date>/hour-HH.jsonl.zst`. Nodes never read from or write to another node's prefix. When a VPS is replaced (hardware refresh, region change, etc.), the new instance gets a **new** node ID (e.g., `vps-fra-2` follows `vps-fra-1`); the old prefix is preserved in S3 as historical archive. This prevents data duplication if the old VPS briefly comes back online and gives the cross-region merge tool (out of scope per §1.b) a clean per-instance partition.

d. **VPN mesh.** All nodes and the Monitor join a single encrypted **Tailscale** mesh VPN (managed, free tier sufficient for the operator scale targeted in v1). All inter-instance communication is routed through Tailscale interfaces; no public ports are exposed for internal APIs. The Tailscale dependency is in the control path (Monitor↔Node) only — the data path (Node → S3) goes over public HTTPS to IONOS, so a Tailscale outage doesn't lose data. v2 can switch to raw WireGuard if the free tier becomes limiting or a self-managed control plane is required.

e. **Deployment method.** Each component is packaged as a fat JAR (or installDist output) and deployed via systemd unit files. No container runtime required on nodes.

f. **Collector hot-swap.** The Collector is deployed via a make-before-break protocol — a new candidate JVM runs alongside the old one for at least one minute boundary, equivalence is verified on the overlapping minute files, then the active slot is flipped and overlap minutes are merged. This avoids data loss on non-ID streams (which cannot be backfilled via REST). The Sealer, Uploader, and Node Agent restart freely (they work on data already on disk). See [Collector hot-swap and WS rotation design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) for the full mechanism. (The same overlap library is reused at runtime by the Collector's internal daily WS rotation — see §8.b.2.)

   **Scheduling.** Hot-swap deploys are forbidden in the `HH:50 → HH:15` window of any hour — that window overlaps the hourly Sealer merge (§9.d) and the tail risk of an overlap-minute spanning the hour boundary. The recommended operator window is `HH:15 → HH:45`. The deploy tooling acquires the node-level `/data/cryptopanner/.fs-heavy.lock` at the promote step (§9.d); if it cannot acquire it (a rotation or merge is in progress), the promote defers and returns to the operator.

## 6. Node anatomy

- a. **Directory layout** on each node. All `<date>`, `<HH-MM>`, and `<HH>` values in the paths below are **UTC** (`YYYY-MM-DD` for `<date>`, `00`–`23` zero-padded for hours, `00`–`59` zero-padded for minutes). This is independent of the node's geographic timezone — every node, every region uses UTC in filenames so that the eventual cross-region merge tool (out of scope per §1.b) and any operator dashboard can join `(symbol, stream, date, hour)` directly without timezone math:
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

    **All units run with `Environment=TZ=UTC`** so the JVM's wall-clock methods, log timestamps, file-rotation calculations, and any operator-visible output are in UTC regardless of the host's geographic locale. This is a deployment requirement, not a runtime option — the install script verifies the unit files set it.

- c. **Boot order.** The pipeline components do not require explicit `After=` ordering between each other — components discover state via the filesystem (existing minute segments, sealed files, S3 manifests) and the Node Agent derives liveness from heartbeats rather than systemd start-completion. Each unit comes up independently when systemd reaches it.

- d. **Resource expectations.** The pipeline is I/O-bound, not CPU-bound. A 2-vCPU, 4GB RAM VPS with 80GB SSD should be sufficient for the initial symbol set. Disk usage is transient — minute segments are cleaned up after hourly upload.

    Two operational events temporarily double Collector memory or connection count:
    - **JAR hot-swap deploy** (rare): two Collector JVMs run concurrently for 1–2 minutes (overlap window).
    - **Daily WS rotation** (automatic, ~daily per node): one Collector JVM with two WS connections for 1–2 minutes.

    `/data` is recommended as a separate volume from `/` so that capture backlog (sealed files on S3 outage), staging trees, and rotation superseded files cannot fill the OS partition.

    **Operating system.** Production targets **Linux** (kernel ≥ 5.10, ext4 or XFS filesystem). The fsync-based durability ordering (invariant 3.b, §9.a) assumes Linux POSIX semantics. Tests on macOS dev machines use `F_FULLFSYNC` (via `FSync(fd, F_FULLFSYNC)` or equivalent) to match production durability — without it, macOS `fsync` returns before the data hits the disk platter. Windows is not supported.

    **Concurrency model.** Every component is **Java 21** with virtual threads. I/O loops use `Executors.newVirtualThreadPerTaskExecutor()`. The only platform-thread requirement is SIGTERM / signal-handler registration (a JVM constraint) — that one exception is intentional and applies uniformly across components.

    **Filesystem ownership.** All `/data/cryptopanner/**` files are owned by a dedicated `cryptopanner` system user (UID/GID assigned by the install script, ≥ 1000 by convention). Mode `0640` for data files (`.jsonl.zst`, `.sha256`, `.manifest.json`); `0750` for directories; `0600` for secrets (`agent.token`, `s3.credentials`, role-config files in `deploy/`). The Collector, Sealer, Uploader, and Agent all run as the same `cryptopanner` user so they can read each other's outputs.

## 7. Data inventory

- a. **WebSocket streams per symbol.** Per-symbol stream forms are used by default (e.g., `btcusdt@trade`); exceptions are called out inline. Each entry lists the sequence-ID property (gap-detectable / gap-fillable / neither), the server-side timestamp field used for minute bucketing (see [hot-swap design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §3.4), and the **routed socket** the stream is delivered on per §8.a (`/public` or `/market`).
    1. `trade` — individual trade events. Socket: **/public**. Sequence ID: `t` (trade ID, strictly increasing). **Gap-detectable and gap-fillable** via REST. Bucketing: `T` (trade time).
    2. `depth@100ms` — order book diff updates. Socket: **/public**. Sequence IDs: `U` (first updateId), `u` (final updateId), `pu` (previous final updateId for the continuity check). **Gap-detectable** via the `pu`-chain; recovery is by fresh snapshot from `/fapi/v1/depth` (snapshot resync, **not** historical replay — mechanism described in §8). Bucketing: `E` (event time).
    3. `aggTrade` — aggregated trade events. Socket: **/market**. Sequence ID: `a` (agg trade ID). **Gap-detectable and gap-fillable** via REST. Bucketing: `T` (trade time).
    4. `kline_1m` — 1-minute candlestick updates. Socket: **/market**. **No sequence ID; not gap-detectable; not gap-fillable.** Fires continuously during a minute (multiple snapshots of the in-progress kline) plus a final closed event with `x: true`. A captured minute holds roughly 60 events per symbol, not 1. Bucketing: `E`.
    5. `ticker` — 24h rolling ticker statistics, 1-second cadence. Socket: **/market**. No sequence ID. Bucketing: `E`.
    6. `bookTicker` — best bid/ask updates. Socket: **/public**. Each event carries a `u` field (order book updateId from the depth update-ID space), but `u` values are sparse and non-contiguous on this stream — the event fires only when the best level changes, skipping any update that doesn't move the top of book. `u` is therefore **not usable for gap detection** despite being present. Treated as no-sequence-ID. Bucketing: `E`.
    7. `markPrice` — mark price and funding rate, 1-second cadence (per-symbol form `<symbol>@markPrice@1s`). Socket: **/market**. No sequence ID. Bucketing: `E`.
    8. `forceOrder` — liquidation events. Socket: **/market**. **All-symbol form** `!forceOrder@arr` rather than per-symbol — liquidations are sporadic, and per-symbol subscriptions waste connection slots on streams that rarely fire. The wrapper identifies the symbol; the Collector partitions to the appropriate per-symbol file. No sequence ID. Bucketing: `E`.

- b. **REST endpoints polled periodically.** Base URL: `https://fapi.binance.com`. Cadences below are canonical defaults; overridable via `config.yaml` (§15).
    1. `GET /fapi/v1/depth?symbol=X&limit=1000` — full order book snapshot. Polled every 5 minutes per symbol as a baseline archive anchor; additionally fetched on demand by the depth-handler resync logic on a `pu`-chain break (mechanism in §8).
    2. `GET /fapi/v1/openInterest?symbol=X` — open interest. Polled every 60 seconds per symbol.
    3. `GET /fapi/v1/exchangeInfo` — symbol metadata and trading rules for all symbols. Polled once per day at 00:05 UTC (the same wall-clock time on every node so cross-region comparisons see the same snapshot).

- c. **Symbol set.** Initial deployment captures the **top 20 USD-M Futures perpetuals by 30-day notional volume**, configured in `config.yaml.symbols`. The list is refreshed quarterly by operator decision. The 20-symbol target keeps the combined-streams subscription well below Binance's per-connection stream limit and fits the resource envelope in §6.d. The handling of larger symbol sets that exceed the per-connection limit remains an open question (§16.a).

- d. **Backfill sources (REST, used at hourly merge for gap-fillable streams):**
    1. `GET /fapi/v1/aggTrades?symbol=X&fromId=N` — paginated aggTrades by ID. Source for `aggTrade` gap fill.
    2. `GET /fapi/v1/historicalTrades?symbol=X&fromId=N` — paginated trades by ID. Source for `trade` gap fill.

    Depth gaps are handled at **capture** time by snapshot resync (§7.b.1), not at merge time. Streams that are neither gap-detectable nor gap-fillable (`kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`) have no backfill source — any gap is recorded in the manifest and remains permanent on that node. This is the central motivation for the Collector hot-swap and daily WS rotation (§5.f, §8.b.2).

## 8. Ingest

a. **WebSocket connections (dual-socket routing).** Since the May 2026 Binance routing change, USD-M Futures WebSocket streams are split across **two routed endpoints**. An unrouted `/stream` silently delivers only the `/public` subset and drops the rest, so a single-socket build will silently miss data. The Collector therefore opens **up to two parallel connections**, one per endpoint:

   - **`wss://fstream.binance.com/public/stream`** carries: `<symbol>@trade`, `<symbol>@depth@100ms` (and other depth variants), `<symbol>@bookTicker`.
   - **`wss://fstream.binance.com/market/stream`** carries: `<symbol>@aggTrade`, `<symbol>@kline_<interval>`, `<symbol>@ticker`, `<symbol>@markPrice` (and `@1s`), `!forceOrder@arr`.

   Each connection is independent: its own SUBSCRIBE, its own ACK gate, its own reconnect schedule, its own ping/pong. Either socket can drop without affecting the other. During a planned WS rotation overlap (§8.b) up to **four** connections run briefly (the shadow opens its own pair). The Collector only opens a socket if the configured subscription set has at least one stream routed to it — a node capturing only `trade` and `depth` never opens `/market/stream`.

   Both endpoints use the same SUBSCRIBE protocol. Streams are subscribed via a `{"method":"SUBSCRIBE","params":[...],"id":<int>}` message after connect, not encoded into the URL (the URL form gets unwieldy with this many streams and is harder to mock for tests). Both endpoints wrap every event in `{"stream": "<name>", "data": {...}}` — the wrapper is the routing key inside the Collector for multi-stream layouts (§8.c). The Collector **blocks on the SUBSCRIBE acknowledgement** — Binance replies with `{"result":null,"id":<int>}` matching the request `id` — before treating the subscription as active. A SUBSCRIBE that times out (no ACK within 10 seconds) or returns an error response is treated as a connection failure: close, reconnect, retry subscription. Without this gate, a silently-dropped subscription would look identical to a quiet stream.

   Binance sends only WebSocket **text frames** (UTF-8 JSON). Binary frames are not expected; if one ever arrives the Collector logs a `ws_binary_frame_unexpected` event at WARN and ignores the frame.

   For the initial 20-symbol set (§7.c), the subscription totals 141 streams (7 per-symbol × 20 + 1 all-symbol `!forceOrder@arr`), well below Binance's documented 200-streams-per-connection limit; larger symbol sets that exceed the limit remain an open question (§16.a). Public market-data streams require no authentication. Binance pings each connection every 3 minutes and disconnects clients that fail to pong within 10 minutes; the Collector responds to pings automatically as part of its read loop.

b. **Reconnection.** Two distinct cases:
   1. **Unplanned faults** (disconnect, ping timeout, half-open detection): the Collector reconnects immediately with exponential backoff (1s, 2s, 4s, ..., capped at 60s) with **±25% uniform random jitter** applied to each delay (a "2s" base delay is drawn from `[1.5s, 2.5s]`). The jitter prevents multi-node fleets from producing correlated reconnect bursts after a Binance-side incident. After reconnection, it re-subscribes to all configured streams (waiting on the ACK per §8.a). The resulting gap is recorded normally — ID streams may be filled by REST backfill at hourly merge; non-ID streams record the gap in the manifest.
   2. **Planned rotation** (approaching Binance's 24h connection limit): the Collector opens a shadow connection and runs the make-before-break overlap protocol. No gap. The rotation start is constrained to the per-hour window `HH:10 → HH:50` — avoiding the hourly Sealer merge at the start of the hour (§9.d) and ensuring the overlap minute does not span the hour boundary. Within that window, each node's exact rotation minute is deterministic: `(hash(node_id) % 40) + 10` past the hour, so no two nodes in a region rotate at the same minute and Binance never sees a correlated reconnect storm. At cutover the Collector acquires the node-level `/data/cryptopanner/.fs-heavy.lock`; if it cannot (a deploy or merge is in progress), it defers to the next eligible minute within the window. See the [hot-swap and WS rotation design](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §5.

c. **Raw capture.** Each incoming WebSocket frame is wrapped in a one-line **`ws_frame` capture envelope** and written to the appropriate minute-segment file:

   ```json
   {
     "envelope": "ws_frame",
     "received_at": "2026-06-20T14:23:15.182Z",
     "raw_sha256": "9f2c…",
     "raw": "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":1750000000182,\"T\":1750000000180,\"t\":145003,...}}"
   }
   ```

   The envelope stores the original frame **verbatim** in `raw` (a JSON string — never parsed-and-re-serialized), with `received_at` (the UTC instant the frame arrived, captured in the read loop, so a backtest knows when the record was actually in hand) and `raw_sha256` (SHA-256 over the `raw` bytes, keeping the inner frame provable independent of the envelope). The Collector parses a copy of the frame to read the combined-streams wrapper `{"stream": "<name>", "data": {...}}` — the `stream` field selects the `<symbol>/<stream>/...` subdirectory — but the stored `raw` is byte-for-byte the wire frame (raw-fidelity, invariant 3.a). Frames are bucketed into minute segments by their **server-side event timestamp** (`E` for most streams, `T` for `trade`/`aggTrade`), not by local receive time. This keeps the same logical event in the same minute file across two parallel connections during an overlap. REST poll responses use the analogous `rest_response` envelope (§8.d) and are bucketed by poll-issue wall-clock time (single-source, no overlap concern). Rationale in the [design doc](superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) §3.4.

d. **REST polling.** REST endpoints are polled on independent timers (cadences pinned in §7.b). HTTP client: **connect timeout 5s, request (response-body-read) timeout 30s, HTTP/1.1 with connection keep-alive, no automatic retries at the HTTP layer** (retries are scheduled by the poller after the failure envelope is written). Responses are written as raw JSON to the same minute-segment file structure, with a wrapper envelope identifying the source endpoint, request parameters, and poll-issue wall-clock timestamp:

    ```json
    {
      "envelope": "rest_response",
      "endpoint": "/fapi/v1/openInterest",
      "params": { "symbol": "btcusdt" },
      "poll_issued_at": "2026-06-14T14:23:15.014Z",
      "received_at":    "2026-06-14T14:23:15.182Z",
      "http_status":    200,
      "response":       { "symbol": "BTCUSDT", "openInterest": "12345.678", "time": 1750000000000 }
    }
    ```

    Failed polls (HTTP 429 rate-limit, 5xx, timeout, connection error) are recorded with the same envelope shape but `response` is replaced by `error: { class: "TIMEOUT", message: "...", http_status: null|<code> }` — raw-fidelity preserved for failures so downstream consumers can distinguish "no response captured" from "Binance returned an error." Retries are scheduled per-endpoint with exponential backoff (same ±25% jitter as §8.b); both the failure envelope and the eventual success envelope are kept in the minute file.

e. **Minute-segment rotation.** The Collector keeps several minutes open at once and seals a minute only after a single **seal-grace window** (`seal_grace_window`, default 10s, configurable per §15) has elapsed past that minute's close. Until a minute is sealed it stays open, so a frame near the boundary routes to its **correct** minute file rather than the next one; the latest minute is always kept open as a straggler target. A frame whose event-minute has *already been sealed* is **kept** — appended to the current open minute and counted in `late_frames` (§10.d) — never discarded; its own event timestamp and `received_at` make the lateness recoverable downstream. So `late_frames` counts only stragglers later than the grace window allows: tuning the window down toward zero is a count-vs-latency trade-off, not a kept-vs-lost one. The closed segment is immutable after sealing (fsync + SHA-256 sidecar). Which minute a frame belongs to is decided by its server event time (§8.c) against the node's **UTC** wall clock (`TZ=UTC` per §6.b), so all nodes assign the same Binance minute regardless of region; but the grace delay itself is measured with **`CLOCK_MONOTONIC`** (design doc §3.4) anchored when the minute's end is first observed, so an NTP step during the window does not prematurely seal a minute. Tolerable clock skew is pinned in §12.n (±100ms accepted, ±1s Warning, ±5s Critical).

## 9. WAL & local sealing & upload

- a. **Minute segments as WAL.** The per-minute segment files serve as the write-ahead log. Each segment is small (typically seconds of data) and accompanied by a `.sha256` sidecar. This is the durability boundary — data is safe once the segment is sealed. Sealing order is **write data → fsync(data) → compute SHA-256 → write sidecar → fsync(sidecar) → fsync(containing directory)**, so a power loss at any point either leaves both files durable on disk or rolls back to a pre-write state. The sidecar is the durability acknowledgement that satisfies invariant 3.b.

- b. **Hourly merge (Sealer).** At the turn of each hour (e.g., at 15:00:00 UTC the Sealer processes hour 14):
    1. Collects all minute segments for the previous hour, per (symbol, stream). The Sealer always sees a single `minute-<HH-MM>.jsonl.zst` file per minute — `.shadow.jsonl.zst` overlap files produced by the Collector during a hot-swap or WS rotation have already been merged away before the Sealer runs.
    2. Concatenates them in **minute-filename order** (lexicographic on `minute-HH-MM`). Within each minute, frames retain their capture-arrival order — the Sealer does not re-sort within a minute. (Frames are server-event-timestamp-bucketed at capture time per §8.c, so the per-minute partition is by `E`/`T`; intra-minute ordering reflects how Binance delivered events on the WS connection, which is what raw-fidelity demands.)
    3. For gap-fillable streams (`trade`, `aggTrade` — see §7.d): validates sequence-ID continuity across the concatenated data. If gaps are detected, attempts backfill via REST. Each gap is attempted up to **3 times with exponential backoff** (1s, 5s, 30s; configurable per §15.b); on final failure the gap is recorded as `FAILED` in `sequence_gaps[].backfill_outcome` and the merge proceeds. There are no cross-merge retries — Binance's historical REST APIs are deterministic, so a failed gap will not resolve in a later attempt unless the underlying data changes. Backfilled records are inserted in sequence order, which means the entire hourly file is rebuilt: decompress → merge new records at the right positions → recompress. This is not a streaming append.
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

- a. **Minute-segment file.** Path: `segments/<symbol>/<stream>/<date>/minute-<HH-MM>.jsonl.zst`. Each line is one capture envelope — a `ws_frame` envelope wrapping a raw WebSocket frame (§8.c), or a `rest_response` envelope wrapping a REST response (§8.d) — terminated by a single LF (per invariant 3.g.2). Frames are placed into minute files by server-side event timestamp (§8.c), not local receive time.

    **zstd parameters** (pinned for reproducibility):
    - Compression level **3** (the zstd default; balances speed vs ratio for our payloads).
    - **Single-frame** format — one zstd frame per minute file (`-` versus per-line frames; allows efficient sequential decompression).
    - **Content checksum on** (zstd `--check` / `ZSTD_CCtx_setParameter(ZSTD_c_contentChecksum, 1)`). Detects on-disk corruption even before the SHA-256 sidecar is consulted.
    - **No dictionary.**

    **`.sha256` sidecar format.** Single text line: 64 lowercase hex characters, two spaces, the bare filename of the data file, LF terminator — i.e. the canonical output of `sha256sum <file>`. This makes `sha256sum -c minute-<HH-MM>.jsonl.zst.sha256` a one-line integrity check from any shell, no custom tooling required. The hash is over the compressed bytes (what's on disk and in S3), not the uncompressed payload.

- b. **Sealed hourly file.** Path: `sealed/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst`. Concatenation of all minute segments for that hour (lexicographic minute order; arrival order preserved within each minute — §9.b.2), with backfilled records inserted in sequence order for gap-fillable streams (§7.d). Compressed with zstd. Accompanied by `hour-<HH>.jsonl.zst.sha256`.

    **Line format is a discriminated envelope union.** Every line is a JSON object with an `envelope` field identifying its kind; consumers dispatch on it rather than assuming a single shape. Backfilled records are *not* re-shaped into synthetic WS frames (that would fabricate data and violate raw fidelity, invariant 3.a) — they are kept as the REST records they are, with provenance:

    | `envelope` | source | payload location | sequence ID |
    |---|---|---|---|
    | `ws_frame` | captured WS frame (§8.c) | `raw` (verbatim wire text) | `raw`→`data.<t\|a>` |
    | `rest_response` | REST poll (§8.d) | `response` | n/a (non-ID streams) |
    | `backfill_record` | REST gap backfill (§9.b.3) | `record` | `record.<id\|a>` |

    A reader doing sequence-continuity analysis over a sealed file resolves the ID per the table above (WS trades use `t`, REST-backfilled trades use `id`; aggTrades use `a` on both); a backfilled hour therefore re-analyzes as continuous.

- c. **S3 object keys.** Three objects per (symbol, stream, hour), all sharing the same prefix:

    ```
    <node-id>/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst
    <node-id>/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst.sha256
    <node-id>/<symbol>/<stream>/<date>/hour-<HH>.manifest.json
    ```

    Where `<date>` is `YYYY-MM-DD` and `<HH>` is `00`–`23` zero-padded, both **in UTC** (matches the manifest's `date` and `hour` fields). A key like `vps-fra-1/btcusdt/trade/2026-06-13/hour-14.jsonl.zst` denotes the UTC hour 14:00–14:59 of 2026-06-13 on node `vps-fra-1`; the same UTC hour on `vps-tyo-1` uses the identical date/hour suffix, giving the cross-region merge tool (out of scope per §1.b) a trivial join key. The manifest object is uploaded last per §9.c.1, so its presence in S3 is the consumer-side completeness signal.

- d. **Manifest format** (`hour-<HH>.manifest.json`). Encoded as **UTF-8, pretty-printed, 2-space indent, LF line endings, no trailing newline after the closing `}`** — these formatting choices are part of the manifest's byte-level identity, so the same logical manifest produces identical bytes on every node, which makes `file_sha256`-of-the-manifest reproducible. **Key order is the order shown in the example below** (insertion order matches the schema documentation); implementations using Jackson should configure `MapperFeature.SORT_PROPERTIES_ALPHABETICALLY = false` and use `@JsonPropertyOrder` on the manifest DTO. Representative example for an ID-bearing gap-fillable stream (`trade`) with a missing minute, a backfilled gap, and a WS rotation event during the hour:

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
      "late_frames": 0,
      "unparseable_frames": 0,
      "file_sha256": "abcdef0123456789...",
      "file_size_bytes": 1048576,
      "uncompressed_size_bytes": 9437184,
      "record_count": 42000,
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
    - `manifest_schema_version` — integer; downstream consumers parse based on this. **Evolution policy:** additive-only changes (new optional fields, new entry types in existing arrays) do **not** bump the version; downstream consumers ignore unknown fields gracefully. Breaking changes (field renames, type changes, semantic shifts) bump the version; the `cryptopanner-verify` CLI supports reading the current and previous version, and a migration tool reads old manifests and writes new ones.
    - `file_sha256`, `file_size_bytes` — describe the **compressed** `.jsonl.zst` file as written to disk and uploaded to S3 (`file_sha256` matches the `.sha256` sidecar contents).
    - `uncompressed_size_bytes` — the JSONL payload size before zstd, for sizing intuition.
    - `record_count` — number of JSONL lines in the **final** file (post-backfill).
    - `late_frames` — count of frames whose event-minute was already sealed when they arrived, so they were appended to the then-current open minute rather than their own (kept, never discarded — §8.e). Aggregated across all minutes of the hour. Non-zero values inform tuning of the seal-grace window (§16.c).
    - `unparseable_frames` — count of frames that could not be parsed to extract an event timestamp and were therefore bucketed by `received_at` (still kept, never discarded — §8.c). Aggregated across the hour.
    - `partial_minutes` — minutes whose file exists but is known to be truncated (Collector crashed mid-minute, file fsynced up to the crash point). The minute still counts as `minutes_present`; the internal gap is also recorded in `sequence_gaps`.
    - `sequence_gaps[].from_id`/`.to_id` — inclusive bounds of the missing range. `count` = `to_id - from_id + 1` for trade/aggTrade; for depth, `from_id`/`to_id` are the surrounding `u` values bracketing a `pu`-chain break.
    - `backfill_outcome` — `FILLED`, `PARTIAL`, `FAILED`, or `SKIPPED_NO_SOURCE` (the last for non-fillable streams).

- e. **Per-stream-type schema variations.**
    - **ID-bearing gap-fillable** (`trade`, `aggTrade`): all fields populated as in the example.
    - **ID-bearing gap-detectable-only** (`depth@100ms`): `sequence_id_range` reports the first `U` and the last `u` over the hour; `sequence_gaps[]` entries describe `pu`-chain breaks; `backfill_attempts[]` entries record snapshot-resync attempts rather than historical-replay calls; an additional `depth_anchor_snapshots` field lists the minutes during the hour at which a baseline `/fapi/v1/depth` snapshot was captured (§7.b.1).
    - **Non-ID** (`kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`): the manifest omits `sequence_id_range`, `sequence_gaps`, and `backfill_attempts` entirely. Only `minutes_present`, `minutes_missing`, `partial_minutes`, and the file/size/count fields are populated. Any `minutes_missing` are permanent (no backfill source — see §7.d).

- f. **Optional event arrays.** Two optional arrays. **Absent** (not present in the JSON) on hours with no events of that type; populated as a non-empty array when one or more events crossed the hour.
    - `deploy_events[]` — one entry per Collector hot-swap deploy that promoted during this hour. Fields: `deploy_id`, `old_version`, `new_version`, `promoted_at`, `minutes_merged`, `verify_result`, `verify_report_sha256`.
    - `connection_rotation_events[]` — one entry per WS rotation that promoted during this hour. Fields: `rotation_id`, `reason`, `old_connection_age_hours`, `promoted_at`, `minutes_merged`, `verify_result`, `diff_summary` (see example in §10.d).

    Enums:
    - `verify_result` ∈ { `PASS`, `FORCED`, `RECOVERED_AT_STARTUP`, `ABORTED` }. `PASS` is the steady-state value; `FORCED` is a cutover after 3 consecutive equivalence FAILs (§12.j); `RECOVERED_AT_STARTUP` is set by the Collector's startup recovery when it merges leftover `.shadow` files (design doc §5.6); `ABORTED` records a rotation that started but couldn't complete (both connections dropped, etc.).
    - `reason` (rotation events) ∈ { `SCHEDULED`, `OPERATOR_TRIGGERED`, `EMERGENCY` }. `SCHEDULED` is the normal age-based daily rotation; `OPERATOR_TRIGGERED` is via the `/rotation/trigger` endpoint (e.g., for a symbol-set change per §15.f); `EMERGENCY` is the cliff-approach bypass when connection age > `connection_max_age + 45 min` (design doc §5.1).

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

    Log levels: `DEBUG`, `INFO`, `WARN`, `ERROR`. Production default is `INFO`; `DEBUG` is enabled via config reload (no process restart needed).

    **Rotation and retention.** Logs are rotated when a file exceeds 100MB OR weekly, whichever comes first (`logrotate` or component-internal equivalent). Four weeks of compressed history are kept; files older than 30 days are deleted. ERROR-level entries are additionally mirrored to a `critical-events.jsonl` sidecar that is **never** rotated — retention managed by the operator separately (typically shipped off-node for incident analysis). Rotation parameters are configurable per §15.b.

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
      "fs_heavy_lock": { "held_by": null },
      "deploy":       { "state": "IDLE" },
      "rotation":     { "state": "IDLE", "current_connection_age_s": 76800 },
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

    `fs_heavy_lock.held_by` is the component name currently holding `/data/cryptopanner/.fs-heavy.lock` (§6.a.7) or `null` if free. The Monitor uses this for the cliff-approach restart exception (§13.d).

    During an active deploy, `deploy.state` reflects the state-machine value (`STAGED`, `OVERLAP_READY`, `VERIFIED`, `PROMOTING`, `PROMOTED`) with a `deploy_id` field; the Monitor renders a banner on the dashboard. Similarly `rotation.state` becomes `OVERLAP_VERIFYING` or `CUTOVER_PENDING` during a rotation, with `rotation_id` and `old_connection_age_s`.

    `GET /metrics` — **OpenMetrics text format** (Prometheus-compatible) for historical metrics. Counters: `cryptopanner_late_frames_total`, `cryptopanner_unparseable_frames_total`, `cryptopanner_backfill_attempts_total`, `cryptopanner_uploads_total`, `cryptopanner_rotation_events_total`. Gauges: `cryptopanner_heartbeat_age_seconds`, `cryptopanner_current_connection_age_seconds`, `cryptopanner_sealed_files_pending_upload`, plus VPS metrics. Scraped by an external Prometheus or aggregator if the operator chooses to deploy one; the Monitor does not consume it. Same VPN-bound port as `/status`, no additional auth required (read-only).

d. **Monitor dashboard.** The Monitor serves a simple HTML page at `GET /dashboard` showing all nodes and their component states, with auto-refresh every 5s (matching the scrape interval in §11.c). Backed by `GET /api/nodes` (JSON). A deploy or rotation in progress on any node surfaces as a banner on that node's card; circuit-breaker trips and unreachable nodes are highlighted at the top of the dashboard.

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
   - **One of two slots crashes during a WS rotation overlap** → the surviving connection continues. The rotation falls into early cutover (design doc §5.5): the surviving connection becomes primary, the failed slot is restarted clean.

b. **Collector WebSocket disconnect.** Handling follows the §8.b split:
   - **Unplanned** (network blip, ping timeout): immediate reconnect with exponential backoff (1s, 2s, 4s, ..., 60s). Frames during the disconnect window are missing. For ID-bearing gap-fillable streams (trade, aggTrade), REST backfill at hourly merge fills the gap. For non-ID streams (kline_1m, ticker, bookTicker, markPrice, forceOrder), the data is **permanently lost** on this node and recorded as gaps in the manifest (no REST source — §7.d).
   - **Planned** (24h-approaching rotation): no disconnect window — the rotation overlap covers it (§8.b.2).

c. **Sealer crash during merge.** The Sealer holds `/data/cryptopanner/.fs-heavy.lock` while merging (§9.d). On process death, flock releases automatically (process-scoped). On restart, the Sealer scans `sealed/` for any partially-written hourly files (data without sidecar, or sidecar with mismatched SHA); these are deleted and the merge re-runs from the still-present minute segments. If a fully sealed file exists for the hour, the Sealer skips it (idempotent).

d. **Uploader crash.** On restart, the Uploader scans `sealed/` for files whose corresponding minute segments still exist locally (the deletion in §9.c.3 is the durable "uploaded" marker). It re-attempts upload. If the prior attempt had completed data + sidecar but not the manifest, all three are re-uploaded; S3 keys are deterministic (§10.c) so the result is byte-identical. Per the manifest-last upload order (§9.c.1), consumers see a partial set as "not yet uploaded" and only treat the hour as durable when the manifest object appears.

e. **VPS reboot.** systemd restarts services per §6.c (no explicit `After=` between pipeline components; each starts independently). On collector startup:
   1. Read `/data/cryptopanner/deploy/active-slot` to determine which slot to start in `primary` role. The other slot stays empty.
   2. Run startup recovery (design doc §5.6): scan `segments/` for any `*.shadow.jsonl.zst` files left over from an in-flight rotation; for each pair (`*.jsonl.zst` + `*.shadow.jsonl.zst`), run `OverlapMerger` to produce a single merged file. Idempotent.
   3. Open WS connection and resume capture.

   The Sealer then catches up on any un-merged past hours (subject to §9.d). The Uploader scans for un-uploaded sealed files. Data during the downtime is recorded normally (same handling as 12.a / 12.b).

f. **IONOS S3 outage.** Uploader retries with exponential backoff capped at 5 min between attempts; no try-count limit (§9.c.4). Sealed files accumulate on local disk. The §13 disk-pressure alert (against `/data` per §11.c) fires once the warn threshold is exceeded; if `/data` usage approaches full, the operator must intervene (manual cleanup or expanded volume). When S3 recovers the Uploader drains the backlog automatically.

g. **Monitor VPS down.** Nodes continue operating independently — the Monitor is not in the data path. The external dead-man's switch (Healthchecks.io) detects the Monitor's absence and alerts the operator.

h. **VPN mesh failure.** Nodes continue capturing and uploading — VPN is only used for Monitor↔Node communication. Data flow (node → S3) goes over the public internet (HTTPS to IONOS). Monitor loses visibility but no data is lost.

i. **Hot-swap deploy stuck.** Operator left a deploy in STAGED or OVERLAP_READY without promoting or aborting. The candidate JVM keeps running — consuming a second WS connection and writing to the staging tree, which grows linearly with time. Detection: §13 alerts Warning when the deploy state machine has not progressed for >1h. Resolution: operator runs `cryptopanner-deploy abort` (terminates candidate JVM, deletes staging tree) or proceeds with verify + promote.

j. **Rotation equivalence repeatedly fails.** Per design doc §5.5: first FAIL logs Warning and re-verifies after the next minute; 3 consecutive FAILs escalate Critical and force the cutover anyway (better one connection than approaching the 24h cliff). The forced cutover writes `verify_result: "FORCED"` and the equivalence diff into the manifest event so the divergence is auditable. No silent acceptance.

k. **Extended Binance outage (hours).** Collector continues attempting reconnect with backoff capped at 60s. Until Binance recovers, no minute files are written. The Sealer at each hour boundary records every minute as missing. For ID-bearing gap-fillable streams, REST backfill at hourly merge fills what it can once the REST endpoints are reachable again. Non-ID streams lose the entire outage window permanently. Monitor alerts Critical on the prolonged WS disconnect; archive completeness is correctly reflected in manifests.

l. **REST rate-limit (429) storm.** Failed polls are recorded as failure envelopes (§8.d); the Collector backs off and retries the affected endpoint. Sustained 429s degrade backfill success at hourly merge — unfilled gaps are recorded in `sequence_gaps[].backfill_outcome: "PARTIAL"` or `"FAILED"`. Monitor alerts Warning on elevated failed-poll rate; Critical if rate-limit pressure persists across multiple hourly merges.

m. **`active-slot` file corruption or mismatch.** If the file is unreadable, missing, or contradicts running systemd states (e.g., reports `a` but only slot `b` has a live PID), the Node Agent does not auto-resolve. Detection: discrepancy is flagged in `/status` as `active_slot: "MISMATCH"` and surfaces as Critical alert. Resolution: operator inspects systemd states and the per-slot heartbeat mtimes, then rewrites the file manually (the live slot is whichever has the recently-touched heartbeat).

n. **Clock skew detected.** Broken NTP causes frames to be mis-bucketed by server-event-time vs local time comparisons (§8.c, §8.e). Detection: heuristic comparing recent frames' server-event-time against local time over a 60s window. Skew tolerance: ±100ms accepted silently; ±1s emits `clock_skew_detected` (Warning, §13.a); ±5s escalates to Critical. Mitigation: operator restarts the node's NTP service (`chrony` or `systemd-timesyncd` against any reputable pool — `pool.ntp.org` default acceptable). Mis-bucketed frames are not auto-corrected — manifests will show anomalies that the operator must investigate.

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
        - Clock skew on a node > 1s (§12.n)
    2. **Critical** — immediate-intervention conditions:
        - Component `down` (3 consecutive scrape failures from `/status`)
        - Circuit breaker tripped (3 restart failures in 5 min — see §13.b)
        - Node unreachable from Monitor (VPN, network, or VPS dead)
        - Monitor's own health degraded
        - Disk usage on `/data` > 95%
        - Extended Binance outage: WS disconnect persisting > 5 min on a Collector slot (§12.k)
        - 3 consecutive failed WS rotation attempts, or a forced cutover after 3 consecutive equivalence FAILs (§12.j; design doc §5.2 step 6)
        - REST rate-limit pressure persisting across > 2 consecutive hourly merges (§12.l)
        - `active-slot` MISMATCH between the file and running systemd states (§12.m)
        - Clock skew on a node > 5s (§12.n)
        - WS connection age > `connection_max_age + 30 min` (default 23h30m, approaching Binance's 24h forced-close — see design doc §5.1)

- b. **Restart policy.** The Monitor attempts `POST /restart/<component>` with exponential backoff: 5s → 15s → 60s → 300s. For the Collector, restart targets the currently **active** slot only (read from `/status.active_slot` — §11.c); the empty slot is never auto-restarted. After 3 failures within 5 minutes, the circuit breaker trips for that (node, component) pair: no more restart attempts, Critical alert to operator.

- c. **Dead-man's switch.** The Monitor pushes a heartbeat to Healthchecks.io every 60s. If pushes stop for 5 minutes, Healthchecks.io alerts the operator via email/SMS. This covers the "Monitor itself is down" and "entire VPS is dead" scenarios.

    Additionally, the Monitor fires a **daily self-test alert at 02:00 UTC** ("alert path healthy") on the Telegram/WhatsApp channels. If the operator stops seeing this message, the alert channel has silently failed (revoked bot token, changed webhook URL, etc.) even though the Monitor and Healthchecks.io are alive.

- d. **No false-positive restarts.** The Monitor uses a two-tier check before triggering a restart: (1) systemd reports the unit as failed or inactive, AND (2) heartbeat-derived state is `stuck` or `down` (§11.b). A `degraded` component (heartbeat 15s < age ≤ 60s) is not restarted — it's observation-only, surfaced via the persisting-degraded Warning so the operator can decide whether to intervene. A `running` component is never restarted.

    **Exception (cliff-approach lock release).** If `/status` reports a `.fs-heavy.lock` holder that is `degraded`, AND there is a Collector slot whose WS connection age has crossed `connection_max_age + 30 min` (§13.a Critical), the Monitor restarts the lock-holding component (typically the Sealer) to release the lock and unblock the rotation. This overrides the `degraded`-is-observation-only rule because the alternative is reaching Binance's 24h forced-close. Restart attempt counts toward §13.b's circuit breaker normally.

- e. **Alert deduplication, correlation, and latency.**
    - **Dedup key**: `(node, component, alert-type)`. A repeat firing of the same key within 1 hour of the previous alert is suppressed.
    - **Recovery signal**: when a condition clears, the Monitor emits a one-time `recovered` message on the same channels so the operator isn't left wondering whether the alert is still active.
    - **Correlation/grouping**: if 3 or more nodes fire the same alert-type within 1 minute, the Monitor sends one combined message listing the affected nodes instead of N separate pages. Avoids alert storms during region-wide events (S3 outage, Binance outage).
    - **End-to-end latency**: condition occurrence to operator notification is typically 10–30 seconds (5s scrape interval + ~5s alert pipeline + ~1–2s webhook delivery). Operators should calibrate response expectations accordingly.

## 14. Testing strategy

- a. **Test layers**, fastest to most expensive:
    1. **Unit** — module-level isolation; no I/O, no docker. Key areas: `MinuteSegmentWriter` server-event-time bucketing (§8.c), `EquivalenceChecker` over synthetic primary/shadow pairs, `OverlapMerger` for ID and non-ID streams (§7.d), sequence-ID continuity validation (including `pu`-chain for `depth@100ms`), manifest generation against schema_version=1 (§10.d), SHA-256 computation, S3 key derivation, slot state-machine transitions (§6.b), late-frame-after-seal accounting (§8.e, §10.d). **§12 failure modes whose verification is pure logic — state machines, classifiers, policy decisions, and recovery from a manually-prepared dirty filesystem state — are exercised at this layer** (e.g. recovery for §12.a/c/e, reconnect backoff for §12.b, 429 handling for §12.l, clock-skew detection for §12.n, active-slot mismatch for §12.m, STAGED-too-long for §12.i, monitor-down isolation for §12.g, VPN-partition isolation for §12.h).
    2. **Integration** — in-process pair-wise tests without the full docker stack. Uses TinyWsServer + captured fixtures from `tests/fixtures/binance/` + an in-memory S3 stub (or Testcontainers MinIO where real S3 semantics matter, or `ProcessBuilder`-spawned child JVMs where cross-process semantics matter — `flock(2)` contention, SIGKILL during `write(2)`). **Covers §12 failure modes whose verification needs wiring** — WS disconnect end-to-end (§12.b), uploader idempotency against real S3 (§12.d), sealer crash + `.fs-heavy.lock` auto-release via child-JVM SIGKILL (§12.c), rotation happy path with paired TinyWsServers (§8.b.2), forced cutover after EquivalenceChecker FAIL (§12.j), backfill end-to-end against the mock REST server (§12.k), candidate-JVM crash during deploy overlap (§12.a candidate-side), S3 outage with retry and backlog drain (§12.f). Convention: **test class names reference the §12 entry they cover** (e.g. `WsDisconnectIT` for §12.b, `Rest429StormUnitTest` for §12.l), so the spec↔test mapping is greppable without spec enumeration.
    3. **Soak** — one long-running real-environment test. See §14.e. The only test in the suite that pays the docker-compose startup cost.

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

- e. **Real-environment soak.** The only test in the suite that pays the docker-compose startup cost. One script: `tests/soak/run.sh` brings up the full local stack (`make dev-up`) with **two collector nodes in parallel** against the shared mock-binance-ws, runs at the §7.c target load for ≥ 5 min wall-clock, and triggers one synthetic WS rotation mid-run.

    Pass criterion:
    - `cryptopanner-verify --base-dir <test-dir> --date <test-date>` exits 0 with `ERRORS=0` for each node;
    - both nodes produce an independent complete archive in their own S3 prefix (invariant 3.d); failure or restart of one does not affect the other;
    - no missed minutes outside the rotation overlap window;
    - `late_frames` < 0.1% of total frames;
    - the rotation event is recorded in the manifest under `connection_rotation_events[]`;
    - CPU and memory within the §6.d 2vCPU/4GB envelope per node.

    Naturally covers §8.b.2 (rotation happy path) and the multi-node independence invariant (§3.d) at real-env. **All other §12 failure modes are exercised at unit / integration level per §14.a.1 / §14.a.2** — the soak is intentionally the only test that pays the docker-compose startup cost. The longer 2 h wall-clock / 24 h simulated variant in §14.g remains the pre-release gate.

- f. **`cryptopanner-verify` CLI tests.** The audit/integrity CLI is unit + integration tested. Subcommands: `verify` (archive integrity), `manifest` (inspect), `gaps` (gap report), `integrity` (SHA-256 check). Coverage includes schema_version=1 manifests, all per-stream-type variants (§10.e), and degraded-input handling (truncated zstd, missing sidecar, mismatched SHA).

- g. **Performance / load tests.** Run the local stack at the §7.c target load (20 symbols × 8 streams = 141 subscriptions, §8.a) for at least 2 hours of wall-clock or 24 hours of simulated time. Assert: no missed minutes outside injected faults, `late_frames` < 0.1% of total frames, CPU and memory headroom on the §6.d 2vCPU/4GB envelope, hourly merge completing well before the next hour starts.

- h. **Monitor tests.** Test the Monitor's scrape loop, restart logic with active-slot targeting (§13.b), exponential backoff, circuit breaker, alert deduplication, correlation grouping (§13.e), and recovery messages against a mock Node Agent that simulates the §11.b heartbeat-state transitions.

- i. **CI integration.** `mvn verify` runs the full unit + integration suite on every PR — including all §12 failure-mode coverage per §14.a.1 / §14.a.2. The real-environment soak (§14.e) runs nightly as `bash tests/soak/run.sh`. There is no separate "chaos" CI target — the §12 ↔ test mapping is enforced by class-name convention, not a spec-enumerated catalogue.

- j. **VPN-independent testing.** All tests run locally on a single host without requiring a VPN mesh. The Node Agent binds to `0.0.0.0` in test mode; production deployment binds to the VPN interface (§4.f).

- k. **TDD discipline.** New features land **test-first**: write a failing test that expresses the required behaviour, run it to confirm it fails, write the smallest implementation that makes it pass, run again to confirm green, then commit. Strict for new feature work. **Pragmatic for bug fixes**: write a regression test that reproduces the bug first, then fix; this prevents the bug from silently returning. **Refactors** keep all existing tests green throughout; the diff signal is "behaviour unchanged, structure improved." The walking-skeleton plan (`docs/superpowers/plans/2026-06-14-walking-skeleton.md`) is the reference shape for how a TDD plan task is structured (each task = failing test → run (fails) → implementation → run (passes) → commit); future plans MUST follow the same shape. **Exemptions** — TDD does not apply to: the soak (§14.e, no meaningful fail-first surface), YAML configs, build scripts (`pom.xml`, Maven config), documentation, fixture captures. For these, "verified by inspection / by running the soak" is acceptable.

## 15. Configuration

- a. **Config files.** Each node reads `/etc/cryptopanner/config.yaml`; the Monitor reads `/etc/cryptopanner-monitor/monitor.yaml`. Both are YAML. Nested groups map to the dotted key notation used elsewhere in this spec (e.g., `collector.seal_grace_window` = `collector:\n  seal_grace_window: ...`).

    **Validation.** Components fail-fast at startup if the config is malformed, missing required keys, or contains contradictions (e.g., overlapping `forbidden_window` and `rotation_window`). The error message identifies the offending key and the expected shape; there is no silent acceptance of bad config.

    **Hot-reload.** Keys marked `# HOT-RELOADABLE` in the examples below can be changed without restarting the process (signal-based reload via `SIGHUP`). All other keys require a process restart to take effect. The hot-reload set is intentionally narrow (currently: log level) to keep operational behavior predictable.

    **Timezone is fixed at UTC.** All wall-clock decisions in CryptoPanner (minute/hour boundaries, file naming, scheduled REST polls, rotation windows, deploy forbidden window, alert daily self-test time) are interpreted as UTC. The deployment requirement `TZ=UTC` on every component's systemd unit (§6.b) makes the JVM's local-time methods return UTC values, removing any ambiguity for log readers and for components that compare timestamps. There is no `timezone` configuration key — UTC is not an opinion.

- b. **Node config** (`/etc/cryptopanner/config.yaml`):

    ```yaml
    node_id: vps-fra-1

    symbols:                 # see §7.c — initial set is top 20 USD-M perpetuals
      - btcusdt
      - ethusdt
      # ...

    streams:                 # see §7.a — split by subscription form
      per_symbol:            # subscribed individually per symbol
        - trade
        - depth@100ms
        - aggTrade
        - kline_1m
        - ticker
        - bookTicker
        - markPrice
      all_symbol:            # subscribed once across all symbols
        - "!forceOrder@arr"

    paths:
      segments:      /data/cryptopanner/segments         # §6.a.3
      sealed:        /data/cryptopanner/sealed           # §6.a.4
      staging:       /data/cryptopanner/staging          # §6.a.5
      deploy:        /data/cryptopanner/deploy           # §6.a.6
      logs:          /data/cryptopanner/logs             # §6.a.8
      fs_heavy_lock: /data/cryptopanner/.fs-heavy.lock   # §6.a.7

    logging:
      level: INFO            # DEBUG | INFO | WARN | ERROR  — HOT-RELOADABLE (§11.a)
      rotation:
        max_file_size: 100MB                # rotate when file exceeds this (§11.a)
        rotation_period:  weekly            # OR weekly, whichever first
        compressed_history_weeks: 4         # keep this many rotated files
        delete_after_days: 30
        critical_events_sidecar: /data/cryptopanner/logs/critical-events.jsonl  # never rotated (§11.a)

    storage:
      endpoint:         https://s3.eu-central-1.ionoscloud.com
      bucket:           cryptopanner-prod
      credentials_file: /etc/cryptopanner/s3.credentials

    collector:
      ws_endpoint_url:                   wss://fstream.binance.com/ws    # mock points elsewhere in test (§14.b)
      rest_base_url:                     https://fapi.binance.com        # §7.b
      rest_connect_timeout_s:            5                               # §8.d
      rest_request_timeout_s:            30                              # §8.d
      unplanned_reconnect_backoff_max_s: 60                              # §8.b
      unplanned_reconnect_jitter_pct:    25                              # §8.b ±%
      subscribe_ack_timeout_s:           10                              # §8.a
      seal_grace_window:                 10s                             # §8.e (single grace window)
      connection_max_age:                23h                             # §8.b.2
      rotation_window:                   "HH:10-HH:50"                   # §8.b.2
      rest:                                                              # §7.b
        depth:
          url_template:           "/fapi/v1/depth?symbol={symbol}&limit=1000"
          baseline_poll_interval: 5m
          on_demand_resync:       true                                   # also fetch on pu-chain break
        open_interest:
          url_template:  "/fapi/v1/openInterest?symbol={symbol}"
          poll_interval: 60s
        exchange_info:
          url_template:  "/fapi/v1/exchangeInfo"
          poll_time_utc: "00:05"

    sealer:
      hour_grace_window: 120s                                            # §9.d
      backfill:
        attempts_per_gap: 3                                              # §9.b.3
        backoff:           [1s, 5s, 30s]                                 # §9.b.3
        cross_merge_retry: false                                         # §9.b.3

    uploader:
      retry_backoff_max_s: 300                                           # §9.c.4

    deploy:
      forbidden_window:          "HH:50-HH:15"                           # §5.f
      recommended_window:        "HH:15-HH:45"                           # §5.f
      superseded_retention_days: 7                                       # §6.a.6
      versions_kept:             2                                       # §6.a.1

    agent:
      listen_address:  100.x.y.z:9100                                    # VPN IP:port in production
      token_file:      /etc/cryptopanner/agent.token
      test_mode:       false                                             # true → bind to 0.0.0.0 (§14.b, §14.j)
      metrics_enabled: true                                              # /metrics endpoint (§11.c)
      disk_mounts:                                                       # mounts reported in /status (§11.c)
        - /
        - /data
      heartbeat:
        degraded_threshold_s: 15                                         # §11.b
        stuck_threshold_s:    60                                         # §11.b
    ```

- c. **Monitor config** (`/etc/cryptopanner-monitor/monitor.yaml`):

    ```yaml
    nodes:
      - id:         vps-fra-1
        endpoint:   100.x.y.z:9100
        token_file: /etc/cryptopanner-monitor/tokens/vps-fra-1
      - id:         vps-tyo-1
        endpoint:   100.a.b.c:9100
        token_file: /etc/cryptopanner-monitor/tokens/vps-tyo-1

    scrape_interval_s: 5                                                 # §11.c

    dashboard:
      listen_address:     100.m.n.o:9200
      refresh_interval_s: 5                                              # §11.d

    restart:                                                             # §13.b
      backoff: [5s, 15s, 60s, 300s]
      circuit_breaker:
        failure_count: 3
        window:        5m

    alert:                                                               # §13.a, §13.e
      dedup_ttl: 1h
      correlation:
        threshold_nodes: 3
        window:          1m
      warning:
        degraded_persisting:       2m
        upload_backlog_age:        30m
        deploy_stuck:              1h
        rest_failed_poll_rate_pct: 10
        rest_failed_poll_window:   10m
        disk_data_pct:             80
        clock_skew_s:              1                                     # §12.n
      critical:
        extended_ws_disconnect:            5m
        rest_rate_limit_persistence_hours: 2
        disk_data_pct:                     95
        clock_skew_s:                      5                             # §12.n

    alerting:
      telegram:
        webhook_url: ""                                                  # required; populate via env var
      whatsapp:
        webhook_url: ""                                                  # optional secondary (§13.a)
      healthchecks_url: ""                                               # populate via env var

    dead_man:                                                            # §13.c
      healthchecks_push_interval: 60s
      self_test_time_utc:         "02:00"
    ```

- d. **Environment variables and secrets.** Secrets (S3 credentials, agent bearer tokens, webhook URLs) should not live in the config file. Any config key can be overridden by `CRYPTOPANNER_<KEY_PATH>` (uppercase, dots replaced with underscores). Examples:

    - `CRYPTOPANNER_STORAGE_CREDENTIALS_FILE=/run/secrets/s3.credentials`
    - `CRYPTOPANNER_AGENT_TOKEN=...`
    - `CRYPTOPANNER_MONITOR_ALERTING_TELEGRAM_WEBHOOK_URL=https://api.telegram.org/bot.../sendMessage`
    - `CRYPTOPANNER_MONITOR_ALERTING_HEALTHCHECKS_URL=https://hc-ping.com/...`

    When both the config file and an env var set the same key, the env var wins. This lets operators ship a generic config in the image/package and inject secrets at deploy time without rewriting files.

- e. **Role override during hot-swap.** Both collector slot JVMs read the same `/etc/cryptopanner/config.yaml`. The role is supplied at the systemd unit command line as `--role=primary` or `--role=candidate` and drives the write-root selection (`paths.segments` for primary vs `paths.staging/<deploy-id>/` for candidate — design doc §3.3, §4.1). The config file is **not** edited during a deploy; the active slot identity lives in `paths.deploy/active-slot` (§6.b).

- f. **Symbol-set changes.** Adding or removing a symbol requires a new WS `SUBSCRIBE` message, which the Collector only issues when opening a connection. The natural application moment is the next planned WS rotation (§8.b.2) — the shadow connection picks up the new subscription list as it opens, and after promote the production process is on the new set. Operators update `config.yaml`; the change is applied automatically within ≤24h. To force immediate application, operators trigger an out-of-cycle rotation via the Node Agent (e.g., `POST /rotation/trigger`). Symbol-set changes are **not** hot-reloadable in place.

## 16. Open questions

a. **Multi-symbol WebSocket partitioning (v2).** When the configured symbol set would produce ≥ 200 subscriptions (e.g., ≥ 28 symbols × 7 per-symbol streams + 1 all-symbol), the Collector must open multiple parallel WS connections. v1 ships single-connection because §7.c's initial 20-symbol set produces only 141 subscriptions (§8.a). The partitioning algorithm (round-robin, by symbol, by stream type) is deferred to v2; design doc §5 rotation mechanics apply per-connection so the per-connection lifecycle is already specified.

b. **Sealer prolonged-merge hard timeout (v2).** The Sealer currently blocks indefinitely if an hourly merge runs long (§9.d), with the Monitor's upload-backlog-age Warning (§13.a) as the operator signal. A hard timeout (abandon merge, record hour as incomplete in the manifest) is a potential v2 addition — to be revisited only if operational data shows real-world cases of permanent merge hangs. A blind timeout risks silently abandoning a near-completion merge, which is worse than waiting.

c. **Late-frame grace default.** `collector.seal_grace_window` (10s) is a guess pending observed `E`-to-local-receive delay across Binance regions (§8.e). Tune from `late_frames` counts in early production deployment — these are kept-but-mis-minuted stragglers, not dropped data, so the window is a count-vs-sealing-latency trade-off. Target: `late_frames` ≤ 0.1% of frames (per §14.g acceptance criterion). No resolution before operational data.
