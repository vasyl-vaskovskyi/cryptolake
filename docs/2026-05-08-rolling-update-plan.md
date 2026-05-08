# Rolling Update Plan

How to update the live VPS without losing collected data. The architecture already provides the guarantees we need; this doc names them, picks the safe sequence, and pins the script (`scripts/rolling-update.sh`) to that sequence.

## What "no data loss" actually means here

Three properties combine to give zero-data-loss updates:

1. **Two independent collectors.** `collector` produces into `binance.<stream>`; `collector-backup` produces into `backup.binance.<stream>`. They share neither code path nor egress network — restarting one does not pause the other.
2. **Redpanda buffers messages.** Primary topics retain 2 days, backup topics 30 minutes. As long as a producer is alive, messages accumulate and survive any consumer restart.
3. **Writer commits offsets only after fsync.** On restart, the writer resumes from the last durably-flushed message. Records that were in-flight but not yet on disk are re-read and re-written — never dropped, never duplicated past the dedupe window.

So an update is "safe" if at every point in time **at least one collector is producing AND Redpanda is up.** The writer can be down briefly without any data loss because Redpanda buffers it; postgres + redpanda + alertmanager + prometheus stay up the entire time.

## What the script can and cannot do automatically

| Change category | Example | Auto rolling? |
|---|---|---|
| **A. Image swap** | New `cryptolake/writer:<sha>` | ✅ yes — script handles it |
| **B. Env / command change** | New `JAVA_TOOL_OPTIONS`, new `--base-dir` | ✅ yes — same recreate semantics |
| **C. Bind-mounted config edit** | `config/config.yaml` modified | ✅ yes — recreate picks up the new file |
| **D. New port / new network** | `consolidation` gains a published port | ⚠️ rolling-safe but each affected service hits a brief gap |
| **E. Schema-incompatible change** | New envelope shape between writer/collector | ❌ requires a coordinated stop — out of scope |
| **F. Redpanda or Postgres image bump** | `redpandadata/redpanda:v25.x` | ❌ producers cannot publish during the swap; full restart with planned-maintenance marker |

Categories A–C cover essentially every code change. D is rare enough to flag manually. E and F are migrations, not updates.

## The sequence

```
writer  →  consolidation  →  backfill  →  collector-backup  →  collector
```

Why this order:

- **Writer first.** Writer holds all the state we care about (PG, on-disk archive, Kafka offsets). Validating it first means we shake out incompatibilities before touching the producers. While the writer recreates (~30 s on average), both collectors are still emitting into Redpanda; Redpanda's 2-day retention absorbs the lag. On restart the writer's primary consumer reads from its last committed offset and the backup consumer rejoins at "latest" — exactly what we want.
- **Consolidation and backfill next.** Both are scheduled (consolidation 02:30 UTC daily; backfill on its own cadence). They have no role in live capture, so updating them in parallel with collectors is fine in principle — but doing them here keeps the sequence boringly serial. The script refuses to run between 02:20 and 03:30 UTC by default to avoid restarting consolidation mid-seal.
- **collector-backup before collector.** Backup is the safety net; touching it while primary is healthy means the *primary* is the live source for those ~30 s. After backup is healthy on the new image, primary is recreated last — the writer's `BackupTailConsumer` reads `backup.binance.*` during the gap, so the writer never goes idle.

At every step, exactly one of (`collector`, `collector-backup`) is producing. The writer always has a producer to read from.

## Health gating

Each step waits up to 90 s for the service's `/ready` endpoint to return 200 before moving on:

| Service | Health URL |
|---|---|
| writer | `http://127.0.0.1:8001/ready` |
| collector | `http://127.0.0.1:8000/ready` |
| collector-backup | `http://127.0.0.1:8004/ready` |
| consolidation | `http://127.0.0.1:8003/ready` |
| backfill | (no endpoint — wait for container `running` + 30 s no-crash) |

If a service does not become ready within the timeout, the script rolls **that one service** back to the previous `CRYPTOLAKE_TAG` and aborts. The services already updated stay on the new tag. You investigate, fix, and re-run with the same tag — services already on the new tag are skipped (compose recreates only when the image actually differs).

## Rollback semantics

The script persists the current `CRYPTOLAKE_TAG` from `.env` as `ROLLBACK_TAG` at the top of the run. On any healthcheck failure:

1. Restore `CRYPTOLAKE_TAG=<old>` in `.env`.
2. `docker compose up -d --no-deps <service>` — back to previous image.
3. Wait for `/ready`.
4. Exit non-zero with the failed service named.

If the rollback itself fails, the script prints the manual rescue command and exits 2. At that point `docker compose ps` shows mixed state and you decide.

## Maintenance window

The script writes a `maintenance_intent` line directly to the lifecycle ledger (`/data/.cryptolake/lifecycle/events.jsonl`) *before* the first recreate. Without that, the writer's restart-gap classifier marks the resulting offset gap as `host_reboot` (unplanned) and the gap envelope shows up as a verify warning. The inline write produces the same effect as `cryptolake-maintenance.sh` would, but without `docker compose down` — which would defeat the rolling sequence.

Default refusal window: **02:20–03:30 UTC** (consolidation is sealing). Use `--force` to override; you'll just get a benign re-run of consolidation the next day if you hit the seal mid-restart.

## Pre-flight checks the script enforces

1. `.env` exists, `CRYPTOLAKE_TAG` is set.
2. All four `cryptolake/<svc>:<tag>` images are loaded locally (`scripts/publish-images.sh` was run successfully).
3. All currently-running services report `/ready` 200 — refuses to start a roll on an already-degraded stack.
4. Not inside the maintenance window (override with `--force`).

If any check fails, the script exits before touching anything.

## What's deliberately out of scope

- **Compose-network or volume topology changes.** If `docker compose config` shows a network or volume diff, you need a planned stop with `cryptolake-maintenance.sh restart`. The expected gap is ~30 s and is classified `planned`.
- **Database migrations.** Postgres schema changes need a stop, migrate, start dance. Use `cryptolake-maintenance.sh stop` first.
- **Redpanda/Postgres major version bumps.** Same — full planned restart, single-host limitation.
- **Schema-incompatible envelope changes.** If we ever ship them, both producers must be updated together; the rolling sequence above does not give a safe window.

## Usage

```bash
# Operator's laptop:
CRYPTOLAKE_TAG=$(git rev-parse --short HEAD) scripts/publish-images.sh cryptolake@<vps>

# On the VPS:
sudo ./scripts/rolling-update.sh <new-tag>           # full sequence
sudo ./scripts/rolling-update.sh <new-tag> writer    # just writer
sudo ./scripts/rolling-update.sh <new-tag> --force   # ignore maintenance window
```

The script is idempotent: re-running with the same tag is a no-op for services already at that tag.
