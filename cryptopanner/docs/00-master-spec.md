# CryptoPanner — Master Specification

**Status:** Draft. Sections are added incrementally, one at a time, with review and approval before proceeding.

---

## 1. Naming & identity

a. CryptoPanner is a raw-first, multi-region capture pipeline for Binance USD-M Futures market data.
b. The end product is an append-only, byte-faithful collection of per-node hourly sealed files in S3-compatible object storage. For each (node, symbol, stream, day, hour) the storage holds one zstd-compressed JSONL file, a `.sha256` integrity sidecar, and a `.manifest.json` recording sequence ranges and capture metadata. Consumers fetch files via standard S3-compatible HTTP and can verify integrity locally. Cross-region merging into a single canonical archive is performed by a separate local tool — out of scope of this project.
c. The project is a clean-room successor to CryptoLake (v1) with no shared code, configuration, or data.

## 2. Goals & non-goals
    
a. **Goals**
    1. Capture all configured WebSocket streams and reference-data REST endpoints.
    2. Preserve raw-payload fidelity — no re-serialization.
    3. Write minute-segment files to local disk.
    4. After the hour is finished, merge minute segments into per-node hourly files. During the merge validate sequence-ID continuity for streams that carry IDs (trades, depth, etc.) and backfill detected gaps via REST. Non-ID streams are concatenated as-is. Record missing segments and any remaining gaps in the hourly manifest for both ID and non-ID files.
    5. Upload sealed hourly files to IONOS S3-compatible object storage.
    6. Run unattended on single-node VPS deployments.

b. **Non-goals**
    1. Cross-region comparison, backfill, or deduplication (separate local tool).
    2. Real-time serving or query access.
    3. Support for exchanges other than Binance USD-M Futures.
    4. Historical bulk import of data predating the pipeline's first run.
    5. Inline gap detection in the capture hot path.

## 3. Invariants

a. **Raw-payload fidelity.** Bytes received from the WebSocket or REST response are stored verbatim. No parsing, re-serialization, or field extraction occurs before writing to the minute-segment file.
b. **Durability before acknowledgement.** A minute segment is considered sealed only after the file is fsynced to disk and the `.sha256` sidecar is written. The upload step begins only after the hourly merge completes and its integrity sidecar is verified.
c. **Manifest is the source of truth.** Every hourly file has a `.manifest.json` that records which minute segments are present, which are missing, and — for ID-bearing streams — any sequence gaps that could not be backfilled. If the manifest says a gap exists, it exists. If the manifest says the file is complete, it is complete.
d. **Per-node independence.** A node is a single VPS instance running its own capture pipeline. Each node operates in isolation — no node reads from, writes to, or coordinates with another node. Cross-region logic is external to this system.
e. **Idempotent upload.** Uploading the same hourly file twice produces the same object in storage. The object key encodes (node, symbol, stream, day, hour) — a given key is written once and never mutated.
f. **No silent data loss.** The system does not track or classify runtime failures (crashes, disconnects, restarts). Gap detection happens only by sequence-ID validation and only during the hourly merge step. If a gap is found and cannot be backfilled via REST, it is recorded in the manifest. Missing minute segments are likewise recorded. No other mechanism claims or infers completeness.

## 4. Architecture overview

a. Each node is a VPS instance running four systemd services:
    1. **Collector** — maintains WebSocket connections to Binance USD-M Futures and polls REST endpoints. Writes raw bytes to per-stream minute-segment files on local disk. Touches a heartbeat file every 5s.
    2. **Sealer** — at the turn of each hour, merges the previous hour's minute segments into a single hourly JSONL file per (symbol, stream). For ID-bearing streams, validates sequence continuity and backfills gaps via REST. Produces the `.sha256` sidecar and `.manifest.json`. Touches a heartbeat file every 5s.
    3. **Uploader** — takes sealed hourly files and uploads them to IONOS S3-compatible object storage. Confirms upload integrity, then cleans up local minute segments. Touches a heartbeat file every 5s.
    4. **Node Agent** — lightweight HTTP server bound to the encrypted mesh VPN interface only. Reads component heartbeat file mtimes and `/proc` for VPS metrics. Exposes `GET /status` (aggregated JSON) and `POST /restart/{component}` (calls `systemctl restart`). Protected by systemd WatchdogSec.

b. All four services share a local filesystem. No message broker, no database, no coordination service between components on the same node.

c. Multiple nodes are deployed in different regions for redundancy. Each node is a complete, independent instance of this pipeline.

d. A dedicated **Monitor** runs on a separate VPS (different provider than the nodes) with a built-in HTTP server:
    1. Pulls `/status` from each node every 5s. Three consecutive failures (15s) marks a node as down.
    2. Sends `POST /restart/{component}` to recover failed components, with exponential backoff and a circuit breaker (3 failures in 5 min → stop restarting, alert operator).
    3. Alerts via Telegram/WhatsApp when a component fails or a circuit breaker trips.
    4. Pushes its own heartbeat to an external dead-man's switch (e.g., Healthchecks.io) — if the Monitor itself goes down, the external service alerts.
    5. Exposes `GET /dashboard` (HTML), `GET /api/nodes` (JSON), and `POST /api/restart/{node}/{component}`.

e. Inter-node communication runs over an encrypted mesh VPN (Tailscale or raw WireGuard). All endpoints are bound to VPN interfaces only — invisible to the public internet.

f. Security for the restart endpoint: bearer token in header, timestamp validation (reject requests older than 30s), and VPN-level ACLs restricting access to the Monitor's IP only. Read-only endpoints (`/status`, `/dashboard`) require no additional auth.
