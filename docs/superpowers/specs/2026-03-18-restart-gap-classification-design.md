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

## Persistent State

### Phase 1

Durable state lives in PostgreSQL and is written by application code.

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

Add a host lifecycle ledger persisted on disk, written by a small supervisor/wrapper:

- host boot ID
- maintenance intent snapshots
- container stop/start events
- last known clean shutdown markers

The writer reads this ledger on recovery and upgrades classification from generic `host|system` to specific component causes when evidence is sufficient.

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

3. Collector crash or forced collector restart while writer survives
   Evidence:
   - same `host_boot_id`
   - collector session ID changes
   - no clean collector shutdown evidence
   Result:
   - `component=collector`
   - `cause=unclean_exit`
   - `planned=false`

4. Unknown restart gap
   Evidence:
   - none of the above proved
   Result:
   - `component=system`
   - `cause=unknown`
   - `planned=false`

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

## Verification Requirements

The final implementation must prove:

- graceful full-stack restart archives planned restart gaps
- abrupt collector-only restart archives `component=collector`, `cause=unclean_exit`
- simulated host boot ID change archives `component=host`, `cause=host_reboot`
- REST-only streams receive restart gaps on first post-recovery poll
- verify CLI accepts and reports extended gap metadata
- old runtime gap reasons still work unchanged
