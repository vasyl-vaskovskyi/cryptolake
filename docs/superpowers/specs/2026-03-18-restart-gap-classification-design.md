# Restart Gap Classification Design

## Goal

Archive restart-related gaps correctly for both planned and unplanned downtime, including:

- scheduled service restarts
- full stack restarts
- VPS or host reboot after failure

The archive must preserve a small `reason` enum and store detailed classification in separate fields.

## Constraints

- Classification mode is `strict`.
- The system may only emit a specific restart cause when it has durable evidence.
- If evidence is insufficient, the archived gap must fall back to:
  - `reason: "restart_gap"`
  - `component: "system"`
  - `cause: "unknown"`
  - `planned: false`
- Restart gap detection must survive writer and collector restarts.
- Runtime disconnect gaps such as `ws_disconnect` remain valid and should not be replaced by restart-gap logic.

## Envelope Contract

Restart-related gap records use:

- `reason`: `restart_gap`

Additional gap fields:

- `component`: `collector|writer|redpanda|postgres|host|system`
- `cause`: `operator_shutdown|unclean_exit|host_reboot|unknown`
- `planned`: boolean
- `classifier`: short identifier for the subsystem that produced the classification
- `evidence`: array of short proof strings
- `maintenance_id`: optional string

Existing gap reasons remain unchanged:

- `ws_disconnect`
- `pu_chain_break`
- `session_seq_skip`
- `buffer_overflow`
- `snapshot_poll_miss`

`collector_restart` is superseded by `restart_gap` and should be removed after migration.

### Migration Compatibility

- Existing archived files containing `reason: "collector_restart"` are immutable and remain valid.
- The verify CLI must accept both `collector_restart` (legacy) and `restart_gap` (new) when reading archives.
- New gap records always use `restart_gap`. The writer must not emit `collector_restart` after migration.
- `collector_restart` should be removed from `VALID_GAP_REASONS` only after the migration is complete and all consumers are updated. During the transition, both reasons are valid.

## Persistent State

### Phase 1

Durable state lives in PostgreSQL and is written by application code.

**Collector PG dependency:** The collector currently has no PostgreSQL connection. Phase 1 adds a lightweight PG connection to the collector solely for lifecycle state writes (startup, heartbeat, clean shutdown). This is a deliberate architectural addition — the collector needs to persist evidence that survives its own crash so the writer can read it on recovery. The connection is write-only, uses the same `db_url` from config, and is non-critical: if PG is unreachable, the collector logs a warning and continues operating (data collection is not blocked), but restart classification will fall back to `unknown`.

Tables:

1. `writer_file_state`
   Existing exactly-once archive state.

2. `stream_checkpoint`
   Per `(exchange, symbol, stream)` durable checkpoint.
   Stores:
   - `last_received_at`
   - `last_collector_session_id`
   - `last_gap_reason`
   - `updated_at`

3. `component_runtime_state`
   Latest known lifecycle state per component instance.
   Stores:
   - `component`
   - `instance_id`
   - `host_boot_id`
   - `started_at`
   - `last_heartbeat_at`
   - `clean_shutdown_at`
   - `planned_shutdown`
   - `maintenance_id`
   - `updated_at`

4. `maintenance_intent`
   Durable operator intent for scheduled maintenance.
   Stores:
   - `maintenance_id`
   - `scope`
   - `planned_by`
   - `reason`
   - `created_at`
   - `expires_at`
   - `consumed_at`

### Phase 2

Strict component-specific restart classification for `redpanda`, `postgres`, and `writer` requires host-side evidence that survives full-box failure.

Add a host lifecycle ledger persisted on disk at `/data/.cryptolake/lifecycle/events.jsonl`, written by a small host-side agent:

- **Format:** JSONL (one JSON object per line). This is crash-resilient — a partial write corrupts at most the last line, which the reader discards. Do not use a single JSON array.
- **Records:**
  - host boot ID (written on agent startup)
  - maintenance intent snapshots (copied from PG or CLI)
  - Docker container start/stop/die events for: `collector`, `writer`, `redpanda`, `postgres`
  - exit codes from Docker (distinguishes graceful exit 0 from OOM/SIGKILL)
- **Pruning:** The agent prunes entries older than 7 days on startup to prevent unbounded growth.
- **Docker socket:** The agent requires read access to `/var/run/docker.sock`. This is a host-level privilege — document in deployment instructions.
- **Volume mount:** The ledger directory must be on a host-mounted volume (not inside a container's ephemeral filesystem) so it survives container recreation.

The writer reads this ledger on recovery and upgrades classification from generic `host|system` to specific component causes when evidence is sufficient. The reader only considers events within the restart window (last checkpoint timestamp to now) — it does not scan the entire ledger history.

## Classification Rules

### Phase 1

The writer synthesizes restart gaps on the first post-outage message for each stream.

Classification matrix:

1. Planned system restart
   Evidence:
   - valid `maintenance_intent`
   - prior component rows show `clean_shutdown_at`
   Result:
   - `reason=restart_gap`
   - `component=system`
   - `cause=operator_shutdown`
   - `planned=true`

2. Host reboot
   Evidence:
   - previous durable `host_boot_id` differs from current boot ID
   - no matching clean shutdown evidence
   Result:
   - `component=host`
   - `cause=host_reboot`
   - `planned=false`

3. Planned collector restart (collector-only restart under maintenance)
   Evidence:
   - same `host_boot_id`
   - collector session ID changes
   - clean collector shutdown evidence present
   - valid `maintenance_intent` covering the shutdown window
   Result:
   - `component=collector`
   - `cause=operator_shutdown`
   - `planned=true`

4. Collector crash or forced collector restart while writer survives
   Evidence:
   - same `host_boot_id`
   - collector session ID changes
   - no clean collector shutdown evidence
   Result:
   - `component=collector`
   - `cause=unclean_exit`
   - `planned=false`

5. Host reboot with maintenance intent
   Evidence:
   - previous durable `host_boot_id` differs from current boot ID
   - valid `maintenance_intent` covering the shutdown window
   Result:
   - `component=host`
   - `cause=host_reboot`
   - `planned=true`

6. Unknown restart gap
   Evidence:
   - none of the above proved
   Result:
   - `component=system`
   - `cause=unknown`
   - `planned=false`

### REST-Polled Streams

REST-polled streams (e.g., `open_interest`) do not have persistent WebSocket connections, so they do not experience session ID changes in the same way. Gap detection for these streams relies on the durable `stream_checkpoint.last_received_at` timestamp. If the time delta between the checkpoint and the first post-recovery poll exceeds the expected poll interval, a restart gap is emitted. The classifier uses the same classification matrix — the gap cause is determined by boot ID and component state, not by the stream type.

### Phase 2

The host supervisor promotes classification when it can prove:

- `component=redpanda`
- `component=postgres`
- `component=writer`

Examples:

- `writer` process died while host boot ID stayed constant and Docker recorded an unexpected exit
- `postgres` was intentionally restarted under a maintenance intent
- `redpanda` restarted under maintenance while the host did not reboot

If the host supervisor cannot prove the component-specific cause, classification stays at the Phase 1 fallback.

## Gap Window Semantics

- `gap_start_ts`: last durable `received_at` checkpoint for the stream before outage
- `gap_end_ts`: first post-recovery `received_at` for the same stream

This rule applies to:

- WebSocket streams
- REST-polled streams such as `open_interest`
- synthetic restart gaps on collector session change

The system must not use file-rotation boundaries or broker offset commits as restart-gap timestamps.

## Operational Model

Planned restart flow:

1. Operator marks maintenance intent through CLI or wrapper.
2. Services perform graceful shutdown and persist clean shutdown markers.
3. Services restart.
4. Writer emits `restart_gap` with `planned=true` when the first new message per stream arrives.

Unplanned outage flow:

1. Host or process dies without clean shutdown markers.
2. On recovery, services persist new runtime state.
3. Writer compares prior checkpoints and runtime evidence.
4. Writer emits `restart_gap` with the strict fallback classification.

### Maintenance Intent TTL Expiry

A maintenance intent has an `expires_at` timestamp. If the writer recovers **after** the intent has expired, the intent is treated as invalid and classification falls back to `planned=false`. This is intentional safety behavior — an expired TTL suggests the restart took longer than expected, which could indicate a failure rather than a clean maintenance window.

Operators should set TTL generously (e.g., 30–60 minutes) to account for slow restarts. If a restart legitimately takes longer, the operator can issue a new maintenance intent before the old one expires.

### Postgres Restart Chicken-and-Egg (Phase 2)

When the maintenance target is PostgreSQL itself, the `mark-maintenance` CLI cannot write the intent to PG because PG may already be stopping. Phase 2 solves this: the maintenance wrapper writes the intent to both PG (best-effort) and the host lifecycle ledger (always available). The writer checks both sources on recovery. In Phase 1, a planned Postgres restart without Phase 2 infrastructure will classify as `component=system, cause=unknown, planned=false` — this is the expected strict fallback.

## Verification Requirements

The final implementation must prove:

- graceful full-stack restart archives planned restart gaps
- abrupt collector-only restart archives `component=collector`, `cause=unclean_exit`
- simulated host boot ID change archives `component=host`, `cause=host_reboot`
- REST-only streams receive restart gaps on first post-recovery poll
- verify CLI accepts and reports extended gap metadata
- old runtime gap reasons still work unchanged
