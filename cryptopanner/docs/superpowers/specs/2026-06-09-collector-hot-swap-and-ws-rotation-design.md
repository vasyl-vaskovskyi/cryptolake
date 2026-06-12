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
| `OverlapMerger` | Given two minute-segment files (primary + shadow/candidate), produces a single merged file. Sequence-ID union for ID streams; `(server_event_time, payload_hash)` union for non-ID streams. Atomic write (`.tmp` + rename), sidecar regeneration. |
| `WsConnectionManager` | Owns 1–2 WS connections per stream-set. Exposes `currentConnectionAge()`, `openShadow()`, `promoteShadow()`, `closePrimary()`. Tags every emitted frame with the connection it came from. |
| `MinuteSegmentWriter` | Writes raw frames to `minute-MM[.shadow].jsonl.zst` files. Buckets frames by **server-side event timestamp** (see §3.4). |

### 3.3 systemd unit layout

Two templated instances of the same unit, used as **slots** (not lifecycle states):

- `cryptopanner-collector@a.service`
- `cryptopanner-collector@b.service`

At any moment, exactly one slot is the **active** production slot; the other is empty (or running a candidate during a deploy). The currently-active slot is recorded at `/data/cryptopanner/deploy/active-slot` (contents: literal `a` or `b`). On node bootstrap, slot `a` is the initial active slot.

The deploy tooling reads `active-slot` to determine which slot to use for the candidate (always the *other* one). After a successful promote, `active-slot` is rewritten to point at the new slot. Slot labels never carry meaning beyond "which file/PID belongs to which side of the swap"; they alternate freely across deploys.

The Node Agent's `/status` exposes both slot states and which is currently active. The Monitor dashboard renders this as a single "Production PID: N (slot a|b)" line.

Both slot units are configured `Restart=always` while active and stopped when empty. The instance name (`a` or `b`) and the role (`primary` or `candidate` — read from a per-deploy config file) are passed to the JAR via `--slot=%i --role=<primary|candidate>`. The role determines write paths; the slot determines unit name only.

### 3.4 Frame bucketing by server-side event timestamp

**Change to §8.c of the master spec.** Frames are bucketed into minute segments by their server-side event timestamp, not by local receive time:

- `trade` → `T` (trade time)
- `depth@100ms` → `E` (event time)
- `aggTrade` → `T`
- `kline_1m` → `E`
- `ticker` → `E`
- `bookTicker` → `E`
- `markPrice` → `E`
- `forceOrder` → `E`

REST poll responses use the poll-issue wall-clock time. REST is single-source by design (no overlap concern).

**Why.** During an overlap, two parallel WS connections may receive the same Binance event at local times that straddle a minute boundary. Bucketing by `E` keeps the same logical event in the same minute file regardless of which connection received it.

**Late-frame grace window.** The `MinuteSegmentWriter` buffers frames for `frame_buffer_window` (default 5s) before flushing to the current minute file, so late-arriving frames near the boundary route to the correct minute. At minute close + `seal_grace_window` (default 10s) the minute file is sealed (fsync + sidecar) and any later-arriving frame for that minute is dropped and logged as a structured `late_frame_after_seal` event. This should be exceptionally rare under normal network conditions.

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

1. `systemctl stop cryptopanner-collector@<OLD>.service` — sends SIGTERM. The old JVM finishes its current minute, fsyncs, writes the sidecar, and exits.
2. `systemctl kill -s SIGUSR1 cryptopanner-collector@<NEW>.service` — the candidate JVM switches its role from `candidate` to `primary` and switches its write-root from `staging/<deploy-id>/` to `segments/`. No process restart; the WS connection is preserved.
3. Walk overlap minutes. For each `(symbol, stream, minute)` that exists in both trees, run `OverlapMerger` to produce a single merged file in `segments/`. The pre-merge `segments/` file is moved to `deploy/superseded/<deploy-id>/`.
4. Move staging-only minutes (post-overlap minutes that exist only in `staging/`) into `segments/`. These need no merge.
5. Atomically rewrite `/data/cryptopanner/deploy/active-slot` to `<NEW>` (write to `.tmp`, rename). This is the durable cutover marker: a crash after this point leaves the system in a consistent post-promote state.

Both locks are released after step 5. No systemd unit is renamed or modified during promote. The JVM that handled the candidate role keeps running unchanged; only its `--role` argument (passed via a config file it re-reads on SIGUSR1) flipped from `candidate` to `primary`. On the next deploy, the deploy tool reads `active-slot` and stages into the *other* slot — the two slots alternate freely with each deploy.

### 4.5 Failure handling

| Failure | Response |
|---|---|
| Candidate fails to start | Operator runs `cryptopanner-deploy abort`. Candidate unit stopped, staging tree deleted. No data lost. |
| Equivalence check FAILS | `verify` returns non-zero. Operator inspects `verify-*.report.json`. Options: abort, extend overlap and re-verify, or `--accept-divergence` (records divergence in manifest; never silently accepted). |
| Promote interrupted mid-step | flock holds for the running invocation. If the host itself crashes, `cryptopanner-deploy resume` reads `history.jsonl` and continues from the last completed step. Each merger write is atomic (rename), so partial state is recoverable. |
| New JAR breaks after promote | Standard component-failure path (Monitor alerts). Old JAR is still in `versions/`; rollback is `cryptopanner-deploy rollback <old-version>` until `cleanup` runs (which keeps the last 2 versions). |

### 4.6 Cleanup

`cryptopanner-deploy cleanup` deletes `staging/<deploy-id>/`, deletes `deploy/superseded/<deploy-id>/`, prunes `versions/` to the last 2 versions, removes the `candidate` symlink.

## 5. Variant B: WS rotation (in-process)

### 5.1 Internal scheduler

The Collector runs a `RotationScheduler` background task that wakes once per minute and evaluates three conditions:

```
age_ready    = WsConnectionManager.currentConnectionAge() > connection_max_age
window_ok    = current_minute ∈ [HH:10, HH:50)    # rotation_window (master spec §8.b.2)
slot_unique  = current_minute_of_hour == (hash(node_id) % 40) + 10

if age_ready and window_ok and slot_unique:
    WsConnectionManager.rotate(reason=SCHEDULED)
```

Configuration:

- `connection_max_age` — default `23h`. Hard upper bound below Binance's 24h limit.
- `rotation_window` — default `HH:10-HH:50`. Per-hour window in which rotation start is allowed. Avoids the hourly Sealer merge (master spec §9.d) and prevents the overlap minute from spanning the hour boundary.
- Per-node rotation minute is deterministic: `(hash(node_id) % 40) + 10` past the hour. Different nodes in the same region land at different minutes within the window, eliminating correlated Binance-side reconnect load without explicit coordination.

A freshly-started Collector has a young connection, so the rotation timer naturally resets after startup or after the previous rotation. If `connection_max_age` is reached but the current minute is outside `rotation_window` or not the node's deterministic minute, the scheduler defers; once `currentConnectionAge()` exceeds `connection_max_age + 90 min` (allowing for the worst case of being just past the node's minute when the threshold trips), the scheduler escalates to **Critical** alert — the connection is approaching Binance's 24h hard limit without a rotation opportunity.

### 5.2 Rotation lifecycle

`WsConnectionManager.rotate(reason)`:

1. Open a `shadow` WS connection with a distinct `User-Agent: cryptopanner/<ver>+shadow`. Subscribe to the same combined streams as `primary`.
2. Wait for the shadow's first **full** minute of frames to complete on disk. (The shadow uses `MinuteSegmentWriter` with file suffix `.shadow` — see §5.3.)
3. Run `EquivalenceChecker` on the overlap minute(s).
4. If PASS: at the **next minute boundary**, acquire `flock /data/cryptopanner/.fs-heavy.lock` (master spec §6.a.7). If acquisition would block for longer than 60s, defer the cutover to the following minute and try again (the Sealer or a deploy promote may be holding the lock; either way the rotation can wait). Once the lock is held, call `OverlapMerger` to merge `minute-MM.jsonl.zst` + `minute-MM.shadow.jsonl.zst` → `minute-MM.jsonl.zst` (atomic). Mark `shadow` as the new `primary`. Close the old `primary` WS connection cleanly. Append a `connection_rotation_events[]` entry to the in-progress hourly manifest. Release the lock.
5. If FAIL: log the diff, alert Warning, leave both connections running. Re-verify after the next minute. If 3 consecutive minutes fail equivalence, escalate Critical and **force cutover anyway** (better one connection than approaching the 24h cliff). Forced cutover writes `verify_result: "FORCED"` and the diff into the manifest event.

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

Both `.jsonl.zst` and `.shadow.jsonl.zst` are real on-disk segments with their own sidecars. Both are fsynced at minute close. This is what makes Variant B crash-safe (§5.5).

### 5.4 Failure handling

| Failure | Response |
|---|---|
| Shadow fails to connect | Log + alert Warning. Keep old connection. Retry rotation in 10 min. After 3 failures, escalate Critical (approaching 24h limit). |
| Equivalence FAIL | See §5.2 step 5. |
| Old connection drops during overlap | `WsConnectionManager` detects the drop, immediately promotes shadow without waiting for next minute boundary. Merge whatever minute data exists. This is early cutover, treated as `reason: SCHEDULED, early=true`. |
| Both connections drop during overlap | Both reconnect independently (treated as unplanned faults). Gap recorded normally. The rotation attempt is recorded as `verify_result: "ABORTED"`. |
| Collector crashes during overlap | On restart, the Collector finds `.shadow.jsonl.zst` files on disk. The startup recovery path runs `OverlapMerger` for any such pairs (idempotent — repeated runs produce the same merged file), then continues with a fresh single connection. The next age-based rotation kicks in normally. |

### 5.5 Crash recovery (startup procedure)

On every Collector startup, before opening any WS connection:

1. Scan `segments/` for any `*.shadow.jsonl.zst` files.
2. For each, check whether the corresponding `*.jsonl.zst` exists.
   - Both exist → run `OverlapMerger`, remove the shadow file. Append a `connection_rotation_events[]` entry to the manifest with `verify_result: "RECOVERED_AT_STARTUP"`.
   - Only shadow exists → rename shadow to non-shadow (shadow won the race; primary's minute was lost in the crash).
3. Proceed with normal startup.

This guarantees the steady-state invariant: **at most one `*.jsonl.zst` file per (symbol, stream, minute), no `*.shadow.jsonl.zst` files outside an active rotation.**

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
      "rotation_id": "rot-2026-06-09T02:31:14Z",
      "reason": "SCHEDULED",
      "old_connection_age_hours": 22.93,
      "promoted_at": "2026-06-09T02:32:00Z",
      "minutes_merged": [31, 32],
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
  staging_root: /data/cryptopanner/staging
  history_log: /data/cryptopanner/deploy/history.jsonl
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
  - Kill the Collector mid-rotation → startup recovery merges correctly (§5.5).
  - Inject equivalence FAIL → operator-accept-divergence path works.
  - Make shadow connection slow → first-minute completion delayed → rotation eventually proceeds or fails cleanly.
- **Equivalence under load**: parallel WS connections receiving high-frequency data; verify the equivalence check passes deterministically.

## 10. Changes to the master spec (`docs/00-master-spec.md`)

| Section | Change |
|---|---|
| §5.e | Replace "copy + restart" with reference to Variant A. |
| §8.b | Cross-reference Variant B; keep immediate-reconnect for unplanned faults. |
| §8.c | Change bucketing from local-receive-time to server-event-time (§3.4). |
| §8.e | Add late-frame grace window. |
| §9.b | Note that `.shadow` files are merged before the Sealer ever sees them. |
| §10.d | Add `deploy_events[]` and `connection_rotation_events[]`. |
| §13.a | Add rotation-failure alert rules (§8 above). |
| §15.b | Add config keys (§7 above). |
| §16 | Remove "deploy method" open question (now resolved); add "default for `frame_buffer_window` and `seal_grace_window`" as remaining tunables. |

Cross-references in the master spec point back to this doc for the full mechanism.

## 11. Open questions

1. **Default for `frame_buffer_window` (5s) and `seal_grace_window` (10s).** These are guesses. The right values depend on observed worst-case `E`-to-local-receive delay across Binance regions. Will be tuned during initial deployment based on `late_frame_after_seal` event counts.
2. ~~Rotation jitter.~~ **Resolved 2026-06-09.** Rotation start is constrained to `HH:10 → HH:50` and within that window each node uses a deterministic offset `(hash(node_id) % 40) + 10` minutes past the hour (§5.1). No two nodes in a region rotate at the same minute; no random jitter needed.
3. **Variant A on the candidate's REST timers.** The candidate JVM also runs REST polls during the overlap window. These duplicate the primary's polls but the candidate's poll responses are discarded at promotion. Mildly wasteful; not enough to optimize in v1.
4. **Upper-bound timeout on a stuck Sealer merge.** With hourly merges now serialized via `.fs-heavy.lock` (master spec §9.d), a slow hour queues the next behind it indefinitely. Whether to add a hard timeout that abandons a stuck merge (and records the hour as incomplete) versus blocking indefinitely is open. See master spec §16.b.
