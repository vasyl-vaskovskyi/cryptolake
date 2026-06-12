# Collector hot-swap and WebSocket rotation — design

**Date:** 2026-06-09
**Status:** Draft (approved by user; pending self-review)
**Affects:** `docs/00-master-spec.md` §§5, 8, 9, 10, 13, 16

## 1. Problem

The Collector captures live Binance USD-M Futures WebSocket data and writes it to per-minute segment files. Two operational events force the Collector to swap its WS connection — and today both cause permanent data loss on **non-ID streams** (`kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`), which cannot be backfilled via REST:

1. **JAR deploys.** The current spec (§5.e) deploys updates by `systemctl restart`. Restart takes seconds, during which no WS frames are captured. ID-bearing streams recover via REST backfill at hourly merge; non-ID streams lose those seconds forever.
2. **Binance's 24-hour connection limit.** Binance forcibly closes any WS connection at the 24-hour mark (*"A single connection to stream.binancefuture.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark"*). A reactive reconnect after the close has the same gap problem as a restart.

Both events are *planned* — the old connection is still healthy at the moment we choose to swap. That makes a **make-before-break** pattern available: open the new connection (or new process) first, run both in parallel briefly, verify they're capturing the same data, then cut over and merge the overlap.

## 2. Goals and non-goals

**Goals.**

1. Zero permanent data loss on non-ID streams during planned WS connection swaps.
2. Single mechanism shared between JAR deploys and daily 24h rotations.
3. Auditable from the archive alone — a downstream consumer can see exactly which minutes came from a dual-source merge.
4. Operator-driven for JAR deploys; fully automatic for daily rotations.
5. Idempotent and restart-safe — a crash mid-rotation does not corrupt the archive.

**Non-goals.**

1. Zero data loss for *unplanned* WS failures (half-open detection, ping timeout). Those use immediate-reconnect; the gap is recorded normally.
2. Hot-swap for Sealer, Uploader, Node Agent. Those services operate on data already on disk and can restart freely.
3. Cross-region coordination. Each node performs its rotation independently. The cross-region merge tool (out of scope of CryptoPanner) handles any inter-node alignment.
4. Continuous dual-source redundancy (the CryptoLake pattern). We open a second connection only during planned swaps.

## 3. Architecture

### 3.1 One mechanism, two lifecycles

```
                        ┌─────────────────────────────────────┐
                        │  Shared core (library code)         │
                        │  - EquivalenceChecker               │
                        │  - OverlapMerger                    │
                        │  - WsConnectionManager (1-2 conns)  │
                        │  - MinuteSegmentWriter (E-bucketed) │
                        └─────────────────────────────────────┘
                              ▲                      ▲
                              │                      │
            ┌─────────────────┴─────┐   ┌────────────┴─────────────┐
            │  Variant A: JAR swap  │   │  Variant B: WS rotation  │
            │  - 2 systemd units    │   │  - 1 systemd unit        │
            │  - 2 JVMs             │   │  - 1 JVM, 2 sockets      │
            │  - operator-triggered │   │  - internal scheduler    │
            │  - sibling staging/   │   │  - .shadow file suffix   │
            │  - rare               │   │  - ~daily                │
            └───────────────────────┘   └──────────────────────────┘
```

### 3.2 Shared core components

| Component | Responsibility |
|---|---|
| `EquivalenceChecker` | Compares two minute-segment files for a given (symbol, stream, minute) and returns a per-minute PASS/FAIL with a structured diff report. |
| `OverlapMerger` | Given two minute-segment files (primary + shadow/candidate), produces a single merged file. Sequence-ID union for ID streams; `(server_event_time, payload_hash)` union for non-ID streams. **Output durability contract** below. |
| `WsConnectionManager` | Owns 1–2 WS connections per stream-set. Exposes `currentConnectionAge()`, `openShadow()`, `promoteShadow()`, `closePrimary()`. Tags every emitted frame with the connection it came from. |
| `MinuteSegmentWriter` | Writes raw frames to `minute-MM[.shadow].jsonl.zst` files. Buckets frames by **server-side event timestamp** (see §3.4). |

**`OverlapMerger` semantics and ordering.**

*Output content rules.*
- **ID-bearing streams** (`trade`, `aggTrade`): union by the stream's primary ID field — `t` for `trade`, `a` for `aggTrade`. Output records sorted by primary ID ascending. When the same ID appears on both sides with byte-identical payloads, keep one copy. When the same ID appears with diverging bytes (should not happen — Binance is deterministic per ID), keep the **primary** side's bytes and emit a `merger_divergent_id` log event with both payload hashes.
- **`depth@100ms`**: union by `u` (final updateId). The `pu`-chain may straddle the merged file; the verifier must allow `pu(N) == u(N-1)` across the union just as it does on a single source.
- **Non-ID streams** (`kline_1m`, `ticker`, `bookTicker`, `markPrice`, `forceOrder`): union by `(server_event_time, sha256(payload))` multiset. Output sorted by `(server_event_time ascending, sha256(payload) lexicographic)` as a stable tie-break.
- For all streams: a record present on **only one side** is included unconditionally; the §4.3 equivalence check is the gate that decides whether the merger runs at all.

*Output durability contract.* Per (symbol, stream, minute) the merger produces three on-disk artifacts: the merged `.jsonl.zst`, the `.sha256` sidecar, and a rotation/deploy event record. They MUST be written in this exact order to give crash recovery a single recovery rule per partial state:

1. Write merged content to `minute-MM.jsonl.zst.tmp` (or equivalent for the merge case). `fsync` the tmp file.
2. Compute SHA-256 of the tmp file. Write `minute-MM.jsonl.zst.sha256.tmp`. `fsync`.
3. `fsync` the containing directory.
4. `rename minute-MM.jsonl.zst.tmp → minute-MM.jsonl.zst` and `rename minute-MM.jsonl.zst.sha256.tmp → minute-MM.jsonl.zst.sha256`.
5. `fsync` the containing directory again.
6. `rename` pre-merge file into `deploy/superseded/<id>/` (or for Variant B: delete the `.shadow.jsonl.zst` file).
7. `fsync` the parent directory of step 6.
8. Append the rotation or deploy event to its record file (`deploy/rotations.jsonl` or `deploy/history.jsonl`). `fsync`.

Recovery rules (applied by startup recovery and by Sealer at hour boundary):

- Any `*.tmp` file in `segments/` or `staging/`: delete (incomplete write — atomic rename never happened).
- `*.jsonl.zst` without matching `.sha256` (or with mismatched SHA): recompute sidecar from the file contents and write atomically.
- `*.shadow.jsonl.zst` present alongside `*.jsonl.zst`: run `OverlapMerger` (per §5.6).
- Merged file exists but no event record in `rotations.jsonl`/`history.jsonl`: log `recovery_anomaly`; the operator inspects (this implies a crash in step 8 only, after the data was durable).

### 3.3 systemd unit layout

Two templated instances of the same unit, used as **slots** (not lifecycle states):

- `cryptopanner-collector@a.service`
- `cryptopanner-collector@b.service`

At any moment, exactly one slot is the **active** production slot; the other is empty (or running a candidate during a deploy). The currently-active slot is recorded at `/data/cryptopanner/deploy/active-slot` (contents: literal `a` or `b`). On node bootstrap, slot `a` is the initial active slot.

The deploy tooling reads `active-slot` to determine which slot to use for the candidate (always the *other* one). After a successful promote, `active-slot` is rewritten to point at the new slot. Slot labels never carry meaning beyond "which file/PID belongs to which side of the swap"; they alternate freely across deploys.

The Node Agent's `/status` exposes both slot states and which is currently active. The Monitor dashboard renders this as a single "Production PID: N (slot a|b)" line.

Both slot units are configured `Restart=always` while active and stopped when empty. The instance name (`a` or `b`) and the role (`primary` or `candidate`) are passed to the JAR via `--slot=%i --role=<primary|candidate>`. The role is **read from the role-config file** `/data/cryptopanner/deploy/role-<slot>.conf` (written by the deploy tool before starting the candidate unit, and re-read on `SIGUSR1` during promote — see §4.4). The role determines write paths; the slot determines unit name only.

The templated unit file `cryptopanner-collector@.service` looks roughly like:

```ini
[Unit]
Description=CryptoPanner Collector (slot %i)
After=network-online.target

[Service]
Type=simple
EnvironmentFile=-/data/cryptopanner/deploy/role-%i.conf
ExecStart=/opt/cryptopanner/current/bin/collector --slot=%i --role=${ROLE}
Restart=always
RestartSec=2
WatchdogSec=30

[Install]
WantedBy=multi-user.target
```

`Restart=always` is the static policy; "empty" simply means `systemctl stop @<slot>` was invoked and the unit isn't manually restarted.

**Initial bootstrap.** The package install script writes `active-slot = a` and starts only `cryptopanner-collector@a.service`. The opposite slot is enabled but stopped. The first deploy will stage into `@b` and flip `active-slot` to `b`; subsequent deploys alternate.

**Filesystem mount check at startup.** The Collector verifies via `statvfs` that `paths.segments` and `paths.staging` resolve to the same filesystem device. If they differ, the promote step 10 cross-device rename (`EXDEV`) would fail — the Collector refuses to start with a clear error so the operator can fix the mount layout before risking a deploy.

### 3.4 Frame bucketing by server-side event timestamp

**Change to §8.c of the master spec.** Frames are bucketed into minute segments by their server-side event timestamp, not by local receive time. The exact field per stream:

| Stream | Bucketing field (inside `data`) | Equivalence ID field (§4.3) |
|---|---|---|
| `trade` | `T` (trade time) | `t` (trade ID) |
| `depth@100ms` | `E` (event time) | `u` (final updateId) |
| `aggTrade` | `T` (trade time) | `a` (agg trade ID) |
| `kline_1m` | `E` | — |
| `ticker` | `E` | — |
| `bookTicker` | `E` | — |
| `markPrice` | `E` | — |
| `forceOrder` (`!forceOrder@arr` per master §7.a) | `E` of the outer envelope | — |

**Fallback rule.** If the bucketing field is missing or zero in a received frame (should not happen, but Binance has had rare cases), the Collector falls back to **local receive time** for that single frame and emits a `bucketing_fallback` log event with the frame's payload-hash so it can be diagnosed. This keeps the pipeline progressing rather than failing on malformed input.

REST poll response envelopes use the poll-issue wall-clock time. REST is single-source by design (no overlap concern).

**Why.** During an overlap, two parallel WS connections may receive the same Binance event at local times that straddle a minute boundary. Bucketing by `E` keeps the same logical event in the same minute file regardless of which connection received it.

**Late-frame grace window.** The `MinuteSegmentWriter` buffers frames for `frame_buffer_window` (default 5s) before flushing to the current minute file, so late-arriving frames near the boundary route to the correct minute. At minute close + `seal_grace_window` (default 10s) the minute file is sealed (fsync + sidecar) and any later-arriving frame for that minute is dropped and logged as a structured `late_frame_after_seal` event. This should be exceptionally rare under normal network conditions.

**Clock-step safety.** All grace-window and seal timers use **`CLOCK_MONOTONIC`** (Java: `System.nanoTime()`), never the wall clock. The wall clock is consulted only to determine which minute file a frame's server-event-time falls into; sealing decisions ("has the grace window elapsed?") use monotonic time. The writer additionally compares wall-clock progression against monotonic progression once per second; if it detects a wall-clock step of more than 5 seconds (forward or backward), it **pauses sealing** for the duration of `seal_grace_window` extra, emits a `clock_step_detected` log event, and continues. This prevents an NTP step forward from prematurely sealing the current minute and dropping in-flight frames, and prevents a backward step from sealing the same minute twice.

## 4. Variant A: JAR deploy

### 4.1 Directory layout

```
/opt/cryptopanner/
├── current/                    # symlink → versions/<active>/
├── candidate/                  # symlink → versions/<staged>/ (deploy only)
└── versions/
    ├── 1.0.0/cryptopanner-collector.jar
    └── 1.0.1/cryptopanner-collector.jar

/data/cryptopanner/
├── segments/                   # primary writes here
├── staging/<deploy-id>/        # candidate writes here (deploy only)
├── sealed/                     # untouched by deploys
├── deploy/
│   ├── .lock                   # flock guard
│   ├── history.jsonl           # state-transition log
│   ├── superseded/<deploy-id>/ # pre-merge segment files (kept until cleanup)
│   └── verify-<deploy-id>.report.json
```

### 4.2 State machine

```
IDLE
  → operator: cryptopanner-deploy stage <new.jar>
STAGED                    # JAR copied; cryptopanner-collector@<inactive-slot>.service started in candidate role
  → candidate completes first full minute in staging/
OVERLAP_READY             # ≥1 full minute exists in both trees, every (symbol, stream)
  → operator: cryptopanner-deploy verify
VERIFIED                  # EquivalenceChecker PASSED on all minutes
  → operator: cryptopanner-deploy promote
PROMOTING                 # atomic switchover (§4.4)
PROMOTED                  # active-slot flipped; old slot stopped; candidate JVM now serves production
  → operator: cryptopanner-deploy cleanup
IDLE
```

Each transition is written to `/data/cryptopanner/deploy/history.jsonl` as one JSONL record, and the current state is exposed via the Node Agent's `/status` so the Monitor dashboard can show a deploy banner.

**Stage holds the deploy lock too.** `cryptopanner-deploy stage` acquires `flock /data/cryptopanner/deploy/.lock` *before* writing into `staging/<deploy-id>/` or starting the candidate unit, and releases it once `STAGED` is recorded. Without this, two operators running `stage` concurrently could both target the same inactive slot, overwrite each other's staging trees, and produce an undefined deploy state. `verify` and `promote` reuse the same lock.

### 4.3 Equivalence check rules (Variant A and B share these)

Overlap minutes are the **intersection of full minute files in both trees** for every (symbol, stream).

For each overlap minute:

- **ID-bearing streams** (`trade`, `depth@100ms`, `aggTrade`): extract sequence IDs from each file, compute `set(primary) ⊕ set(candidate)`. Allowed symmetric difference: `≤ 1` ID at each minute edge (one frame straddling the boundary). Any larger diff → FAIL.
- **Non-ID streams**: extract `(server_event_time, sha256(payload))` tuples. Both files must have the same multiset. Allowed difference: `0`. (With `E`-field bucketing in §3.4, this is achievable.) Any non-zero diff → FAIL.
- **REST responses**: not checked. REST polls are not synchronized between instances; the candidate's REST data is discarded at promotion (a fresh poll happens on the next interval after promotion).

The overall verify result is PASS iff every checked minute is PASS. The structured diff report is written to `verify-<deploy-id>.report.json` with per-minute, per-(symbol, stream) results.

### 4.4 Promote: atomic switchover

`promote` first checks the **deploy forbidden window** (`HH:50 → HH:15` per master spec §5.f) — if the current minute falls inside it, the command returns an error to the operator and exits without changing state. Once past the window check, `promote` acquires **two locks**:

- `flock /data/cryptopanner/deploy/.lock` — guards the deploy state machine; held for the duration of the `promote` invocation.
- `flock /data/cryptopanner/.fs-heavy.lock` — node-level mutex shared with the Sealer and the WS rotation cutover (master spec §6.a.7). Blocks until the lock is free; the Sealer can be holding it for an in-progress hourly merge. If acquisition would block for longer than 30s, `promote` releases the deploy lock and exits with an error so the operator can retry.

With both locks held, let `OLD` = the slot in `active-slot` at start of promote (the production process). Let `NEW` = the other slot (the candidate JVM that has been verified).

Each numbered step below appends a record to `deploy/history.jsonl` with `fsync` **before** proceeding. On crash mid-promote, `cryptopanner-deploy resume` reads `history.jsonl` plus filesystem state and picks up at the last completed step (§4.5).

1. **PROMOTING_STARTED** — record `{deploy_id, OLD, NEW, started_at}`.
2. **Stop OLD.** `systemctl stop cryptopanner-collector@<OLD>.service` — sends SIGTERM. Poll `systemctl is-active` until inactive (up to 30s). The old JVM finishes its current minute, fsyncs, writes the sidecar, and exits.
3. **OLD_STOPPED** — record marker.
4. **Flip `active-slot` (durable cutover marker).** Atomically rewrite `/data/cryptopanner/deploy/active-slot` from `<OLD>` to `<NEW>`: write `active-slot.tmp` with the new value, fsync the tmp file, rename to `active-slot`, fsync the parent directory. The rewrite is **compare-and-swap** — if the current contents differ from `<OLD>`, abort with an error (catches operator interference). **A crash after this point cannot revert to OLD**; resume always continues forward.
5. **ACTIVE_SLOT_FLIPPED** — record marker.
6. **Flip NEW role.** `systemctl kill -s SIGUSR1 cryptopanner-collector@<NEW>.service`. NEW's signal handler re-reads its role-config file (path: `/data/cryptopanner/deploy/role-<NEW>.conf`, written by `stage` before the candidate started) and enqueues a "switch write-root" marker into its write loop. The actual switch from `staging/<deploy-id>/` to `segments/` happens at the **next minute boundary** so frames mid-flight are not split across roots. The WS connection is preserved across the role flip. NEW also immediately fires fresh polls for every REST endpoint (replacing the discarded candidate-side polls) so there is no observable gap on, e.g., `openInterest`.
7. **NEW_ROLE_FLIPPED** — record marker.
8. **Walk overlap minutes.** For each `(symbol, stream, minute)` that exists in both trees, run `OverlapMerger` (output-ordering contract per §3.2) to produce a single merged file in `segments/`. The pre-merge `segments/` file is moved to `deploy/superseded/<deploy-id>/`. Per-minute merging is idempotent: skip when `superseded/<deploy-id>/.../<file>` already exists (the pre-move record).
9. **OVERLAP_MERGED** — record marker.
10. **Drain staging-only minutes.** Move post-overlap minutes that exist only in `staging/` into `segments/`. Idempotent: skip files already present in `segments/`.
11. **STAGING_DRAINED** — record marker.
12. **Release both locks.**
13. **PROMOTED** — record marker.

No systemd unit is renamed or modified during promote. The JVM that handled the candidate role keeps running unchanged; only its `--role` argument (re-read from the role-config file on SIGUSR1) flipped from `candidate` to `primary`. On the next deploy, the deploy tool reads `active-slot` and stages into the *other* slot — the two slots alternate freely with each deploy.

### 4.5 Failure handling

| Failure | Response |
|---|---|
| Candidate fails to start | Operator runs `cryptopanner-deploy abort`. Candidate unit stopped, staging tree deleted. No data lost. |
| Equivalence check FAILS | `verify` returns non-zero. Operator inspects `verify-*.report.json`. Options: abort, extend overlap and re-verify, or `--accept-divergence` (records divergence in manifest; never silently accepted). |
| Promote interrupted mid-step | flock holds for the running invocation. If the host itself crashes, `cryptopanner-deploy resume` reads `history.jsonl` and picks up at the last recorded marker (see §4.4). Resume behavior is split by which side of the **`ACTIVE_SLOT_FLIPPED`** marker the crash occurred on: before, the deploy may be aborted (OLD's systemd unit re-starts via `Restart=always` since it was only `stop`-ped) or resumed forward (operator's choice); after, resume is **always forward** — there is no path back to OLD. Each merger write is atomic (rename), and each merge-target is idempotent (skip if `superseded/.../<file>` exists), so re-running any step from PROMOTING_STARTED onward is safe. |
| New JAR breaks after promote | Standard component-failure path (Monitor alerts). Old JAR is still in `versions/`; rollback is `cryptopanner-deploy rollback <old-version>` until `cleanup` runs (which keeps the last 2 versions). |

### 4.6 Cleanup

`cryptopanner-deploy cleanup` deletes `staging/<deploy-id>/`, deletes `deploy/superseded/<deploy-id>/`, prunes `versions/` to the last 2 versions, removes the `candidate` symlink.

## 5. Variant B: WS rotation (in-process)

### 5.1 Internal scheduler

The Collector runs a `RotationScheduler` background task that wakes once per minute and evaluates three conditions:

```
age_ready    = WsConnectionManager.currentConnectionAge() > connection_max_age
window_ok    = current_minute ∈ [HH:10, HH:50)    # rotation_window (master spec §8.b.2)
slot_unique  = current_minute_of_hour == per_node_minute(node_id)

if age_ready and window_ok and slot_unique:
    WsConnectionManager.rotate(reason=SCHEDULED)
```

The hash function for `per_node_minute` is pinned to **SHA-256**: take the first 4 bytes of `sha256(node_id_utf8)`, interpret as unsigned big-endian uint32, modulo 40, plus 10. This is platform-independent (no Java `String.hashCode()` portability concerns), deterministic, and produces a uniform distribution across the 40 minutes. Collision probability for an N-node fleet within a region is `1 - (40! / (40-N)! / 40^N)` — ~2.5% for 2 nodes, ~7.3% for 3, ~14% for 4. Collisions are reported as `peer_minute_collision` Warning at startup when peer nodes are reachable via Tailscale; the operator can fix by renaming a node (since `node_id` is operator-set).

Configuration:

- `connection_max_age` — default `23h`. Hard upper bound below Binance's 24h limit.
- `rotation_window` — default `HH:10-HH:50`. Per-hour window in which rotation start is allowed. Avoids the hourly Sealer merge (master spec §9.d) and prevents the overlap minute from spanning the hour boundary.
- Per-node rotation minute is deterministic: `(hash(node_id) % 40) + 10` past the hour. Different nodes in the same region land at different minutes within the window, eliminating correlated Binance-side reconnect load without explicit coordination.

A freshly-started Collector has a young connection, so the rotation timer naturally resets after startup or after the previous rotation. If `connection_max_age` is reached but the current minute is outside `rotation_window` or not the node's deterministic minute, the scheduler defers. Escalation as the 24h cliff approaches:

- `currentConnectionAge() > connection_max_age` (default 23h) — scheduler attempts rotation at each eligible minute; deferrals on lock contention are normal.
- `currentConnectionAge() > connection_max_age + 30 min` (default 23h30m) — **Critical** alert. Operator must intervene; the connection is within 30 min of Binance's 24h forced-close. The Monitor's restart logic gets an exception (master spec §13.d): if the `.fs-heavy.lock` holder is `degraded` (heartbeat 15–60s) and blocking, the Monitor restarts the holder to release the lock.
- `currentConnectionAge() > connection_max_age + 45 min` (default 23h45m) — **emergency rotation**: scheduler bypasses both the `rotation_window` and the deterministic per-node minute constraint, attempts to force-acquire `.fs-heavy.lock` (kill the holder if necessary), and starts a shadow connection immediately. The resulting rotation event is annotated `reason: EMERGENCY` in the manifest. Better an out-of-window rotation than a hard disconnect.

### 5.2 Rotation lifecycle

`WsConnectionManager.rotate(reason)`:

1. Open a `shadow` WS connection with a distinct `User-Agent: cryptopanner/<ver>+shadow`. Subscribe to the same combined streams as `primary`. Subscription state is captured at this moment — if master spec §15.f symbol-set changes added or removed streams since `primary` last subscribed, shadow uses the new set. The equivalence check (§4.3) handles this by intersecting on (symbol, stream): pairs present on only one side are treated as **PASS** for that pair (no comparison possible — the divergence is intentional). The forced single-side pairs are flagged in the rotation event for audit.
2. **Liveness probe.** Within 30 seconds of shadow open, verify the shadow has received at least one frame on a high-frequency stream (any one of `trade`, `depth@100ms`, `aggTrade`, `bookTicker`). If not, treat the shadow as **half-open**, close it, alert Warning, and retry at the next eligible minute. Without this probe, an undetected half-open shadow would write empty `.shadow.jsonl.zst` files and burn the equivalence check on what is effectively no data.
3. Wait for the shadow's first **full** minute of frames to complete on disk. A "full minute" means the minute file has passed its `seal_grace_window` and been sealed (sidecar written). The shadow uses `MinuteSegmentWriter` with file suffix `.shadow` — see §5.3.
4. Run `EquivalenceChecker` on the overlap minute(s).
5. If PASS: at the **next minute boundary**, acquire `flock /data/cryptopanner/.fs-heavy.lock` (master spec §6.a.7). If acquisition would block for longer than 60s, defer the cutover to the following minute and try again. The 60s wait (vs `promote`'s 30s — see §4.4) reflects different urgencies: rotation has a 24h cliff but no operator waiting for an answer, so a longer wait is preferable to a deferred retry; promote has an operator on the keyboard who needs a fast yes/no. Once the lock is held, call `OverlapMerger` (output contract §3.2) to merge `minute-MM.jsonl.zst` + `minute-MM.shadow.jsonl.zst` → `minute-MM.jsonl.zst` (atomic). Mark `shadow` as the new `primary`. Close the old `primary` WS connection cleanly. Append an entry to `deploy/rotations.jsonl` (which the Sealer reads at hour boundary per master spec §9.b.6). Release the lock.
6. If FAIL: log the diff, alert Warning, leave both connections running. Re-verify after the next minute. If 3 consecutive minutes fail equivalence, escalate Critical and **force cutover anyway** — rotations face the 24h cliff and cannot defer indefinitely, unlike deploys which can wait for operator decision (this is why Variant A has an `--accept-divergence` operator path but Variant B does not). Forced cutover writes `verify_result: "FORCED"` and the diff into the rotation record.
7. **Non-ID divergence tolerance.** For non-ID streams, `EquivalenceChecker` accepts up to `K` differing `(E, payload_hash)` tuples per minute (default `K = 0` — strict). A future tuning may raise `K` if Binance fan-out produces small per-connection divergences in `bookTicker` near-real-time updates; the default stays at `0` until measured.

### 5.3 File naming during overlap

```
segments/btcusdt/trade/2026-06-09/
├── minute-14-22.jsonl.zst              # pre-overlap, single connection
├── minute-14-22.jsonl.zst.sha256
├── minute-14-23.jsonl.zst              # overlap minute (primary connection)
├── minute-14-23.jsonl.zst.sha256
├── minute-14-23.shadow.jsonl.zst       # overlap minute (shadow connection)
├── minute-14-23.shadow.jsonl.zst.sha256
└── minute-14-24.jsonl.zst              # post-cutover, single connection
```

Both `.jsonl.zst` and `.shadow.jsonl.zst` are real on-disk segments with their own sidecars. Both are fsynced at minute close. This is what makes Variant B crash-safe (§5.6).

### 5.5 Failure handling

| Failure | Response |
|---|---|
| Shadow fails to connect | Log + alert Warning. Keep old connection. Retry rotation in 10 min. After 3 failures, escalate Critical (approaching 24h limit). |
| Equivalence FAIL | See §5.2 step 5. |
| Old connection drops during overlap | `WsConnectionManager` detects the drop, immediately promotes shadow without waiting for next minute boundary. Merge whatever minute data exists. This is early cutover, treated as `reason: SCHEDULED, early=true`. |
| Both connections drop during overlap | Both reconnect independently (treated as unplanned faults). Gap recorded normally. The rotation attempt is recorded as `verify_result: "ABORTED"`. |
| Collector crashes during overlap | On restart, the Collector finds `.shadow.jsonl.zst` files on disk. The startup recovery path runs `OverlapMerger` for any such pairs (idempotent — repeated runs produce the same merged file), then continues with a fresh single connection. The next age-based rotation kicks in normally. |

### 5.4 Operator-triggered rotation (`POST /rotation/trigger`)

Master spec §15.f promises an operator-facing endpoint to force an immediate rotation (typically used after a symbol-set config edit so the new set takes effect within minutes rather than waiting up to 24h for the next scheduled rotation). The endpoint lives on the Node Agent (master spec §11.c).

- **Path:** `POST /rotation/trigger`
- **Auth:** same bearer-token + timestamp validation as `/restart/<component>` (master spec §4.f).
- **Body:** `{ "reason": "OPERATOR_TRIGGERED" }` (the `reason` field is recorded in the rotation event for audit).
- **Response (sync):** `202 Accepted` immediately if the rotation can begin; `409 Conflict` if a rotation or deploy is already in progress, or if `currentConnectionAge() < 5 min` (refuse trivially-young rotations). Response body includes the assigned `rotation_id`.
- **Behavior:** the scheduler runs `rotate(reason=OPERATOR_TRIGGERED)`. The `rotation_window` and `slot_unique` gates are **bypassed** (this is an operator override). The `.fs-heavy.lock` is still acquired normally; if blocked, the trigger returns `503 Service Unavailable` rather than queueing — the operator can retry. The rotation event in the manifest carries `reason: OPERATOR_TRIGGERED`.

### 5.6 Crash recovery (startup procedure)

On every Collector startup, before opening any WS connection, the Collector acquires `.fs-heavy.lock` (so the Sealer cannot run a merge against in-flux files) and runs:

1. **Sweep `*.tmp` files** under `segments/` and `staging/`. Any `*.tmp` is an incomplete write that never reached the atomic rename — delete it. Log `recovery_tmp_swept` per file.
2. **Validate sidecars.** For every `*.jsonl.zst` in `segments/`, verify that a matching `.sha256` exists and that its recorded SHA matches the file. On mismatch or missing, recompute and write the sidecar atomically (per §3.2 durability contract). Log `recovery_sidecar_rewritten` per file.
3. **Merge leftover shadows.** Scan `segments/` for `*.shadow.jsonl.zst` files. For each:
   - **Both `*.jsonl.zst` and `*.shadow.jsonl.zst` exist** → run `OverlapMerger`; append a record to `deploy/rotations.jsonl` with `verify_result: "RECOVERED_AT_STARTUP"` so the next Sealer cycle includes it in the manifest event array.
   - **Only `*.shadow.jsonl.zst` exists** → rename to `*.jsonl.zst` (shadow data is all we have); also rename `*.shadow.jsonl.zst.sha256` → `*.jsonl.zst.sha256`. Log `recovery_shadow_promoted_orphan`. Record this minute in the next manifest's `partial_minutes[]` (the primary's data for the minute was lost in the crash).
4. **Verify event records against merged files.** For each merged minute referenced in `rotations.jsonl` / `history.jsonl`, confirm the on-disk file exists. Conversely, for each merged file that lacks an event record, log `recovery_orphan_merge` for operator investigation (this means the crash happened in step 8 of the durability contract — data is durable but auditability is incomplete).
5. **Release `.fs-heavy.lock`.**
6. Proceed with normal startup: read `active-slot`, start the WS connection in the appropriate role.

After recovery, the steady-state invariant holds: **at most one `*.jsonl.zst` file per (symbol, stream, minute); no `*.shadow.jsonl.zst` files outside an active rotation; every `*.jsonl.zst` has a matching valid `.sha256`; every merged minute has a matching event record (or is logged as an anomaly).**

## 6. Manifest schema additions (§10.d)

Two optional arrays, populated only when an hour contained the corresponding event:

```json
{
  "...": "...",
  "deploy_events": [
    {
      "deploy_id": "2026-06-09T14:23-v1.0.1",
      "old_version": "1.0.0",
      "new_version": "1.0.1",
      "promoted_at": "2026-06-09T14:25:12Z",
      "minutes_merged": [23, 24, 25],
      "verify_result": "PASS",
      "verify_report_sha256": "abcdef..."
    }
  ],
  "connection_rotation_events": [
    {
      "rotation_id": "rot-2026-06-09T02:31:00Z",
      "reason": "SCHEDULED",
      "old_connection_age_hours": 22.93,
      "promoted_at": "2026-06-09T02:33:00Z",
      "minutes_merged": [32],
      "verify_result": "PASS",
      "diff_summary": { "id_streams": "0 symmetric diff", "non_id_streams": "0 diff" }
    }
  ]
}
```

Both arrays are absent (not empty) on hours with no events of that type.

## 7. Configuration additions (§15.b)

New keys under the Collector, Sealer, and deploy configs:

```yaml
collector:
  connection_max_age: 23h         # Variant B trigger
  frame_buffer_window: 5s         # late-frame buffering before minute flush
  seal_grace_window: 10s          # window past minute boundary before sealing
  rotation_window: "HH:10-HH:50"  # per-hour allowed window for rotation start (§5.1)

sealer:
  hour_grace_window: 120s         # delay past hour boundary before merge begins (master spec §9.d)

deploy:
  # staging tree and history log paths are derived from master spec §15.b paths.staging
  # and paths.deploy — not separately configurable here.
  superseded_retention_days: 7    # rollback window for rotation merges
  versions_kept: 2                # rollback window for JAR deploys
  forbidden_window: "HH:50-HH:15" # per-hour window where promote is refused (§4.4)
  recommended_window: "HH:15-HH:45" # operator runbook target window

paths:
  fs_heavy_lock: /data/cryptopanner/.fs-heavy.lock  # node-level mutex (master spec §6.a.7)
```

## 8. Alerting additions (§13)

- **Warning** — equivalence FAIL on a rotation attempt; shadow connection fails to open on first attempt.
- **Critical** — 3 consecutive rotation failures; forced cutover after 3 consecutive equivalence FAILs.
- Deploy state machine stuck (not progressing for >1h) — Warning. (Avoids ghost deploys left in `STAGED`.)

## 9. Testing strategy additions (§14)

- **Unit tests** for `EquivalenceChecker` and `OverlapMerger` with synthetic primary/shadow pairs covering: identical, boundary-frame diffs, missing-frame diffs, ID gaps, payload-hash collisions.
- **Integration test** for full Variant A flow: stage → verify → promote → cleanup against a mock Binance WS that serves identical data to both connections.
- **Integration test** for Variant B: simulate connection-age trigger, run full rotation, verify manifest event and merged file.
- **Chaos tests**:
  - Kill the candidate JVM mid-overlap → resume / abort works.
  - Kill the Collector mid-rotation → startup recovery merges correctly (§5.6).
  - Inject equivalence FAIL → operator-accept-divergence path works.
  - Make shadow connection slow → first-minute completion delayed → rotation eventually proceeds or fails cleanly.
- **Equivalence under load**: parallel WS connections receiving high-frequency data; verify the equivalence check passes deterministically.

## 10. Changes to the master spec (`docs/00-master-spec.md`)

**Status (2026-06-12): all changes from this design doc have been applied to the master spec end-to-end.** Master spec sections 5.f, 5.g (later removed in favor of 8.b.2), 6.a, 6.b, 6.c, 6.d, 8.a, 8.b, 8.c, 8.e, 9.b, 9.d, 10.c, 10.d, 10.e, 10.f, 11.a, 11.b, 11.c, 11.d, 11.e, 12.a, 12.b, 12.c, 12.d, 12.e, 12.f, 12.g, 12.h, 12.i, 12.j, 12.k, 12.l, 12.m, 12.n, 13.a, 13.b, 13.c, 13.d, 13.e, 14, 15.b, 15.c, 15.d, 15.e, 15.f, and 16 (closed/restructured) all reflect the design herein. Cross-references in both directions point each doc at the other for details.

This section is retained as a historical record of which master-spec areas were touched, not as a live change-list.

## 11. Open questions

1. **Default for `frame_buffer_window` (5s) and `seal_grace_window` (10s).** These are guesses. The right values depend on observed worst-case `E`-to-local-receive delay across Binance regions. Will be tuned during initial deployment based on `late_frame_after_seal` event counts.
2. ~~Rotation jitter.~~ **Resolved 2026-06-09.** Rotation start is constrained to `HH:10 → HH:50` and within that window each node uses a deterministic offset `(hash(node_id) % 40) + 10` minutes past the hour (§5.1). No two nodes in a region rotate at the same minute; no random jitter needed.
3. **Variant A on the candidate's REST timers.** The candidate JVM also runs REST polls during the overlap window. These duplicate the primary's polls but the candidate's poll responses are discarded at promotion. Mildly wasteful; not enough to optimize in v1.
4. **Upper-bound timeout on a stuck Sealer merge.** With hourly merges now serialized via `.fs-heavy.lock` (master spec §9.d), a slow hour queues the next behind it indefinitely. Whether to add a hard timeout that abandons a stuck merge (and records the hour as incomplete) versus blocking indefinitely is open. See master spec §16.b.
