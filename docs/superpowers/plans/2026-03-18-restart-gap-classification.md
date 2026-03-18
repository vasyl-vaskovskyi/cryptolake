# Restart Gap Classification Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist and archive restart-related gaps correctly for planned restarts, full-stack restarts, and host reboots, using strict classification with a small `reason` plus separate metadata fields.

**Architecture:** Phase 1 adds durable PostgreSQL-backed stream checkpoints and component lifecycle state so the writer can synthesize `restart_gap` envelopes after recovery with strict `collector|host|system` classification. Phase 2 adds a host-side lifecycle ledger and maintenance wrappers so the writer can upgrade strict classification for `redpanda|postgres|writer` when the evidence survives full-box failure.

**Tech Stack:** Python 3.12, PostgreSQL via psycopg, Docker Compose, Prometheus-style health checks, pytest

**Spec:** `docs/superpowers/specs/2026-03-18-restart-gap-classification-design.md`

---

## File Structure

### Phase 1

- Modify: `src/common/envelope.py`
  Adds `restart_gap` as a valid reason and supports optional structured restart metadata on gap envelopes.
- Create: `src/common/system_identity.py`
  Provides host boot ID lookup with deterministic test fallback.
- Create: `src/writer/restart_gap_classifier.py`
  Pure classification logic for strict `collector|host|system` restart gaps.
- Modify: `src/writer/state_manager.py`
  Adds `stream_checkpoint`, `component_runtime_state`, and `maintenance_intent` tables plus typed load/save helpers.
- Modify: `src/writer/buffer_manager.py`
  Carries the original flushed envelopes through `FlushResult` so checkpoint writes and restart-gap synthesis can happen at the durable flush boundary.
- Modify: `src/writer/consumer.py`
  Replaces in-memory-only `collector_restart` logic with durable `restart_gap` synthesis, checkpoint updates, and strict classification.
- Modify: `src/writer/main.py`
  Records writer lifecycle state and wires classifier/state dependencies.
- Modify: `src/collector/main.py`
  Records collector lifecycle state, host boot ID, heartbeats, and clean shutdown markers.
- Modify: `src/cli/verify.py`
  Accepts extended gap fields, reports restart metadata, and adds a maintenance-intent CLI command.
- Test: `tests/unit/test_envelope.py`
- Test: `tests/unit/test_verify.py`
- Test: `tests/unit/test_state_manager.py`
- Create: `tests/unit/test_restart_gap_classifier.py`
- Create: `tests/unit/test_writer_restart_gap_recovery.py`
- Create: `tests/unit/test_system_identity.py`

### Phase 2

- Create: `scripts/host_lifecycle_agent.py`
  Host-side Docker event recorder and boot ledger writer.
- Create: `scripts/cryptolake-maintenance.sh`
  Wrapper that writes strict maintenance intent before planned stop/restart.
- Create: `infra/aws/systemd/cryptolake-lifecycle-agent.service`
  Systemd unit for the host lifecycle agent on the EC2/VPS host.
- Create: `src/writer/host_lifecycle_reader.py`
  Reads host lifecycle ledger and upgrades restart classification when strict evidence exists.
- Modify: `src/writer/restart_gap_classifier.py`
  Adds `redpanda|postgres|writer` component-specific strict rules.
- Modify: `src/writer/main.py`
  Injects the host lifecycle reader.
- Modify: `infra/aws/README.md`
  Documents lifecycle agent installation and planned maintenance workflow.
- Create: `tests/unit/test_host_lifecycle_reader.py`
- Create: `tests/unit/test_host_lifecycle_agent.py`

---

## Chunk 1: Phase 1 Foundation

### Task 1: Extend the gap schema for restart classification

**Files:**
- Modify: `src/common/envelope.py`
- Test: `tests/unit/test_envelope.py`
- Test: `tests/unit/test_verify.py`

- [ ] **Step 1: Write failing envelope tests for structured restart metadata**

Add tests that require:

- `reason="restart_gap"` to be valid
- `create_gap_envelope()` to accept optional fields:
  - `component`
  - `cause`
  - `planned`
  - `classifier`
  - `evidence`
  - `maintenance_id`
- non-restart gaps to continue working without those fields

- [ ] **Step 2: Run tests to verify the new assertions fail**

Run: `./.venv/bin/pytest tests/unit/test_envelope.py -q`

Expected: FAIL because `restart_gap` is not in `VALID_GAP_REASONS` and the helper does not emit the new fields.

- [ ] **Step 3: Implement the minimal schema change**

Update `src/common/envelope.py` to:

- add `restart_gap` to `VALID_GAP_REASONS`
- extend `create_gap_envelope()` with optional keyword-only restart metadata
- include only provided optional fields in the returned envelope

- [ ] **Step 4: Add verify CLI coverage for extended gap metadata**

Add tests in `tests/unit/test_verify.py` that prove:

- extra restart fields do not break `verify_envelopes()`
- `report_gaps()` preserves those fields
- manifest generation includes restart metadata when present

- [ ] **Step 5: Update `src/cli/verify.py` to preserve extra gap fields**

Keep required fields unchanged, but update manifest generation to include:

```python
{
    "reason": env.get("reason"),
    "component": env.get("component"),
    "cause": env.get("cause"),
    "planned": env.get("planned"),
    "maintenance_id": env.get("maintenance_id"),
}
```

- [ ] **Step 6: Run tests to verify the schema change passes**

Run: `./.venv/bin/pytest tests/unit/test_envelope.py tests/unit/test_verify.py -q`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/common/envelope.py src/cli/verify.py tests/unit/test_envelope.py tests/unit/test_verify.py
git commit -m "feat: add structured restart gap envelope metadata"
```

### Task 2: Add durable restart-gap persistence primitives

**Files:**
- Create: `src/common/system_identity.py`
- Modify: `src/writer/state_manager.py`
- Test: `tests/unit/test_state_manager.py`
- Create: `tests/unit/test_system_identity.py`

- [ ] **Step 1: Write failing tests for boot ID lookup and new SQL tables**

Add tests that require:

- `get_host_boot_id()` to read `/proc/sys/kernel/random/boot_id`
- env override fallback such as `CRYPTOLAKE_TEST_BOOT_ID`
- `StateManager` SQL to contain:
  - `stream_checkpoint`
  - `component_runtime_state`
  - `maintenance_intent`

- [ ] **Step 2: Run tests to verify they fail**

Run: `./.venv/bin/pytest tests/unit/test_state_manager.py tests/unit/test_system_identity.py -q`

Expected: FAIL because the helper file and new SQL do not exist.

- [ ] **Step 3: Implement `src/common/system_identity.py`**

Use a small helper API:

```python
def get_host_boot_id() -> str:
    ...
```

Rules:

- prefer `/proc/sys/kernel/random/boot_id`
- allow deterministic env override for tests
- fall back to a stable `"unknown"` sentinel only when strict evidence is unavailable

- [ ] **Step 4: Extend `StateManager` with new typed records**

Add dataclasses in `src/writer/state_manager.py`:

- `StreamCheckpoint`
- `ComponentRuntimeState`
- `MaintenanceIntent`

Add SQL constants and methods:

- `load_stream_checkpoints()`
- `save_stream_checkpoints()`
- `upsert_component_runtime()`
- `mark_component_clean_shutdown()`
- `create_maintenance_intent()`
- `consume_maintenance_intent()`
- `load_latest_component_states()`

- [ ] **Step 5: Keep existing file-state behavior untouched**

Do not refactor existing `writer_file_state` behavior in this task. The new tables should be additive.

- [ ] **Step 6: Run tests to verify the persistence primitives pass**

Run: `./.venv/bin/pytest tests/unit/test_state_manager.py tests/unit/test_system_identity.py -q`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/common/system_identity.py src/writer/state_manager.py tests/unit/test_state_manager.py tests/unit/test_system_identity.py
git commit -m "feat: add durable restart gap persistence tables"
```

---

## Chunk 2: Phase 1 Recovery Classification

### Task 3: Implement strict Phase 1 restart classification

**Files:**
- Create: `src/writer/restart_gap_classifier.py`
- Create: `tests/unit/test_restart_gap_classifier.py`

- [ ] **Step 1: Write failing classifier tests**

Cover the strict matrix:

- planned system restart
- host reboot
- collector unclean exit with same boot ID
- unknown fallback

Expected classifier output shape:

```python
{
    "reason": "restart_gap",
    "component": "collector",
    "cause": "unclean_exit",
    "planned": False,
    "classifier": "writer_recovery_v1",
    "evidence": ["collector_session_changed", "host_boot_id_unchanged"],
}
```

- [ ] **Step 2: Run the classifier tests to verify they fail**

Run: `./.venv/bin/pytest tests/unit/test_restart_gap_classifier.py -q`

Expected: FAIL because the classifier module does not exist.

- [ ] **Step 3: Implement a pure classifier**

Create a pure function API:

```python
def classify_restart_gap(
    *,
    previous_boot_id: str | None,
    current_boot_id: str,
    previous_session_id: str | None,
    current_session_id: str | None,
    collector_clean_shutdown: bool,
    system_clean_shutdown: bool,
    maintenance_intent: MaintenanceIntent | None,
) -> dict[str, object]:
    ...
```

Rules:

- specific result only when durable evidence proves it
- otherwise return `component="system", cause="unknown", planned=False`

- [ ] **Step 4: Run the classifier tests to verify they pass**

Run: `./.venv/bin/pytest tests/unit/test_restart_gap_classifier.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/writer/restart_gap_classifier.py tests/unit/test_restart_gap_classifier.py
git commit -m "feat: add strict phase1 restart gap classifier"
```

### Task 4: Move writer restart-gap recovery to the durable flush boundary

**Files:**
- Modify: `src/writer/buffer_manager.py`
- Modify: `src/writer/consumer.py`
- Modify: `src/writer/main.py`
- Test: `tests/unit/test_writer_restart_gap_recovery.py`

- [ ] **Step 1: Write failing recovery tests**

Add tests that prove:

- a collector session change after writer restart emits `restart_gap`, not `collector_restart`
- boot ID change emits `component=host`, `cause=host_reboot`
- a valid maintenance intent emits `planned=true`, `cause=operator_shutdown`
- first post-recovery record sets `gap_end_ts`
- checkpoint updates happen only after durable write/commit

- [ ] **Step 2: Run the recovery tests to verify they fail**

Run: `./.venv/bin/pytest tests/unit/test_writer_restart_gap_recovery.py -q`

Expected: FAIL because `FlushResult` does not expose flushed envelopes and the consumer still uses in-memory `collector_restart`.

- [ ] **Step 3: Extend `FlushResult` to carry flushed envelopes**

Modify `src/writer/buffer_manager.py`:

- add `envelopes: list[dict]` to `FlushResult`
- preserve the original flushed envelope objects alongside `lines`

This avoids reparsing bytes when creating durable checkpoints.

- [ ] **Step 4: Replace in-memory-only restart detection in `WriterConsumer`**

In `src/writer/consumer.py`:

- remove `_last_session` as the source of truth for restart recovery
- load durable stream checkpoints from `StateManager`
- on first post-recovery envelope per stream:
  - compare durable checkpoint vs current envelope
  - classify via `restart_gap_classifier`
  - inject a synthetic `restart_gap` envelope before the data envelope

- [ ] **Step 5: Persist stream checkpoints only after durable commit**

At the same boundary where file bytes and Kafka offsets become durable:

- derive checkpoint from flushed envelopes:
  - last `received_at`
  - last `collector_session_id`
- save checkpoint in the same transaction window as file state
- only update in-memory checkpoint cache after commit succeeds

- [ ] **Step 6: Record writer runtime metadata**

Modify `src/writer/main.py` to:

- read current host boot ID
- upsert writer component runtime on start
- write clean shutdown marker on graceful stop

- [ ] **Step 7: Run the writer recovery tests**

Run: `./.venv/bin/pytest tests/unit/test_writer_restart_gap_recovery.py -q`

Expected: PASS

- [ ] **Step 8: Run focused regression tests**

Run: `./.venv/bin/pytest tests/unit/test_stream_handlers.py tests/unit/test_verify.py tests/unit/test_envelope.py -q`

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/writer/buffer_manager.py src/writer/consumer.py src/writer/main.py tests/unit/test_writer_restart_gap_recovery.py
git commit -m "feat: synthesize durable restart gaps in writer recovery"
```

### Task 5: Record collector lifecycle and planned maintenance intent

**Files:**
- Modify: `src/collector/main.py`
- Modify: `src/cli/verify.py`
- Test: `tests/unit/test_verify.py`
- Create: `tests/unit/test_collector_lifecycle.py`

- [ ] **Step 1: Write failing tests for planned maintenance and collector lifecycle**

Cover:

- CLI command to create maintenance intent
- collector startup writes runtime state with host boot ID
- collector shutdown marks clean shutdown
- absence of a clean shutdown leaves restart classification unplanned

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./.venv/bin/pytest tests/unit/test_verify.py tests/unit/test_collector_lifecycle.py -q`

Expected: FAIL because the CLI command and lifecycle writes do not exist.

- [ ] **Step 3: Add a maintenance-intent CLI command**

Extend the existing `cryptolake` CLI in `src/cli/verify.py` with a command such as:

```bash
cryptolake mark-maintenance \
  --scope system \
  --maintenance-id deploy-2026-03-18T21-00Z \
  --reason "scheduled deploy" \
  --ttl-minutes 30
```

Behavior:

- writes a durable `maintenance_intent` row
- does not shut anything down itself

- [ ] **Step 4: Persist collector lifecycle state**

Modify `src/collector/main.py` to:

- upsert collector runtime row on start
- periodically update `last_heartbeat_at`
- mark clean shutdown on graceful stop
- attach any active maintenance intent to the clean shutdown marker

- [ ] **Step 5: Run the lifecycle tests**

Run: `./.venv/bin/pytest tests/unit/test_verify.py tests/unit/test_collector_lifecycle.py -q`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/collector/main.py src/cli/verify.py tests/unit/test_verify.py tests/unit/test_collector_lifecycle.py
git commit -m "feat: record collector lifecycle and maintenance intent"
```

### Task 6: Verify Phase 1 end-to-end and update restart-gap docs

**Files:**
- Modify: `docs/2026-03-13-cryptolake-design.md`
- Modify: `tests/chaos/1_kill_ws_connection.sh`
- Create: `tests/chaos/7_full_stack_restart_gap.sh`

- [ ] **Step 1: Update the docs to replace `collector_restart` with structured `restart_gap`**

Revise design language so restart gaps are documented as:

- `reason="restart_gap"`
- structured metadata in separate fields

- [ ] **Step 2: Update existing chaos coverage**

Adjust `tests/chaos/1_kill_ws_connection.sh` to count `restart_gap` records and validate:

- `component=collector`
- `cause=unclean_exit`

- [ ] **Step 3: Add a full-stack restart chaos test**

Create `tests/chaos/7_full_stack_restart_gap.sh` that:

- starts the stack
- allows data to flow
- performs `docker compose down`
- records a maintenance intent before restart
- brings the stack back up
- waits for fresh data
- verifies archived `restart_gap` records with `planned=true`

- [ ] **Step 4: Run focused unit coverage**

Run:

```bash
./.venv/bin/pytest \
  tests/unit/test_envelope.py \
  tests/unit/test_verify.py \
  tests/unit/test_state_manager.py \
  tests/unit/test_restart_gap_classifier.py \
  tests/unit/test_writer_restart_gap_recovery.py \
  tests/unit/test_collector_lifecycle.py -q
```

Expected: PASS

- [ ] **Step 5: Run the two restart chaos tests**

Run:

```bash
tests/chaos/1_kill_ws_connection.sh
tests/chaos/7_full_stack_restart_gap.sh
```

Expected:

- both scripts exit 0
- archive contains structured `restart_gap` records

- [ ] **Step 6: Commit**

```bash
git add docs/2026-03-13-cryptolake-design.md tests/chaos/1_kill_ws_connection.sh tests/chaos/7_full_stack_restart_gap.sh
git commit -m "test: cover planned and unplanned restart gaps"
```

---

## Chunk 3: Phase 2 Host-Level Strict Classification

### Task 7: Add a host lifecycle ledger that survives full-box failure

**Files:**
- Create: `scripts/host_lifecycle_agent.py`
- Create: `tests/unit/test_host_lifecycle_agent.py`

- [ ] **Step 1: Write failing tests for the lifecycle ledger**

Require a local ledger format persisted under `/data/.cryptolake/lifecycle/ledger.json` or similar, with records for:

- current host boot ID
- maintenance intents
- Docker container start/stop events for:
  - `collector`
  - `writer`
  - `redpanda`
  - `postgres`

- [ ] **Step 2: Run tests to verify they fail**

Run: `./.venv/bin/pytest tests/unit/test_host_lifecycle_agent.py -q`

Expected: FAIL because the host agent does not exist.

- [ ] **Step 3: Implement the host agent**

The agent should:

- subscribe to Docker events
- persist append-only lifecycle facts to the local ledger
- record clean shutdown vs unexpected exit when Docker provides it
- record the current host boot ID on startup

- [ ] **Step 4: Run the host-agent tests**

Run: `./.venv/bin/pytest tests/unit/test_host_lifecycle_agent.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/host_lifecycle_agent.py tests/unit/test_host_lifecycle_agent.py
git commit -m "feat: add host lifecycle event ledger"
```

### Task 8: Read host evidence in the writer and promote strict classification

**Files:**
- Create: `src/writer/host_lifecycle_reader.py`
- Modify: `src/writer/restart_gap_classifier.py`
- Modify: `src/writer/main.py`
- Create: `tests/unit/test_host_lifecycle_reader.py`

- [ ] **Step 1: Write failing tests for component-specific promotion**

Cover:

- `writer` unexpected exit with unchanged host boot ID
- `postgres` planned restart under maintenance intent
- `redpanda` unexpected restart with unchanged host boot ID
- fallback to Phase 1 classification when host ledger evidence is missing

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./.venv/bin/pytest tests/unit/test_host_lifecycle_reader.py tests/unit/test_restart_gap_classifier.py -q`

Expected: FAIL because no host reader exists and the classifier has no component-specific rules.

- [ ] **Step 3: Implement the host lifecycle reader**

`src/writer/host_lifecycle_reader.py` should:

- load the local ledger
- expose strict evidence lookup by component and restart window
- return no evidence when the ledger is incomplete

- [ ] **Step 4: Extend the classifier conservatively**

Only promote to `redpanda|postgres|writer` when the host ledger proves the component-specific event. Otherwise keep the Phase 1 result.

- [ ] **Step 5: Wire the reader into writer startup**

Modify `src/writer/main.py` so the writer loads the ledger once at startup and passes it to the classifier.

- [ ] **Step 6: Run the host classification tests**

Run: `./.venv/bin/pytest tests/unit/test_host_lifecycle_reader.py tests/unit/test_restart_gap_classifier.py -q`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/writer/host_lifecycle_reader.py src/writer/restart_gap_classifier.py src/writer/main.py tests/unit/test_host_lifecycle_reader.py tests/unit/test_restart_gap_classifier.py
git commit -m "feat: promote restart gap classification from host evidence"
```

### Task 9: Add host maintenance wrappers and AWS/VPS operations docs

**Files:**
- Create: `scripts/cryptolake-maintenance.sh`
- Create: `infra/aws/systemd/cryptolake-lifecycle-agent.service`
- Modify: `infra/aws/README.md`

- [ ] **Step 1: Write the maintenance wrapper**

The wrapper should:

- call `cryptolake mark-maintenance`
- stop or restart services only after the maintenance intent is durable
- store a deterministic maintenance ID

- [ ] **Step 2: Add the systemd unit**

The unit should:

- start after Docker
- restart on failure
- write the local ledger to persistent storage

- [ ] **Step 3: Update AWS/VPS documentation**

Document:

- how to install the lifecycle agent
- how to use the maintenance wrapper
- what strict guarantees Phase 2 adds beyond Phase 1

- [ ] **Step 4: Commit**

```bash
git add scripts/cryptolake-maintenance.sh infra/aws/systemd/cryptolake-lifecycle-agent.service infra/aws/README.md
git commit -m "docs: add host lifecycle agent deployment workflow"
```

### Task 10: Verify Phase 2 end-to-end

**Files:**
- Create: `tests/chaos/8_host_reboot_restart_gap.sh`

- [ ] **Step 1: Add a host-ledger-driven reboot simulation**

Create a chaos/integration test that:

- seeds a previous-boot lifecycle ledger
- restarts the stack under a new boot ID
- verifies archived restart gaps classify as:
  - `component=host`, `cause=host_reboot`
  - or promoted component-specific values when strict ledger evidence exists

- [ ] **Step 2: Run focused unit coverage**

Run:

```bash
./.venv/bin/pytest \
  tests/unit/test_host_lifecycle_agent.py \
  tests/unit/test_host_lifecycle_reader.py \
  tests/unit/test_restart_gap_classifier.py -q
```

Expected: PASS

- [ ] **Step 3: Run the reboot simulation**

Run: `tests/chaos/8_host_reboot_restart_gap.sh`

Expected: exit 0 and archived `restart_gap` metadata matches the seeded evidence.

- [ ] **Step 4: Commit**

```bash
git add tests/chaos/8_host_reboot_restart_gap.sh
git commit -m "test: verify strict host reboot restart gap classification"
```

---

## Deployment Checklist

### Phase 1 release criteria

- [ ] `restart_gap` envelopes are archived with structured metadata
- [ ] planned full-stack restart produces `planned=true`
- [ ] abrupt collector restart produces `component=collector`, `cause=unclean_exit`
- [ ] host boot ID change produces `component=host`, `cause=host_reboot`
- [ ] REST-only streams get restart gaps on first post-recovery poll
- [ ] verify CLI reports structured restart gap metadata

### Phase 2 release criteria

- [ ] host lifecycle agent writes durable evidence to persistent storage
- [ ] maintenance wrapper writes intent before stopping services
- [ ] writer upgrades strict classification for `writer|redpanda|postgres` only when proven
- [ ] fallback remains `component=system`, `cause=unknown` when evidence is incomplete

Plan complete and saved to `docs/superpowers/plans/2026-03-18-restart-gap-classification.md`. Ready to execute?
