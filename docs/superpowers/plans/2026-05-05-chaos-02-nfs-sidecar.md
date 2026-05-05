# Chaos Scenario 02 — NFS-Sidecar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `tests/chaos/02_fill_disk.sh` runnable by default on a dev Mac (and on Linux CI) without writing hundreds of GB to the host. Replace the host-side `dd`-into-`HOST_DATA_DIR` strategy with a sidecar NFSv4 server whose backing store is a 300 MiB tmpfs inside the chaos compose project. Filling that capped tmpfs to ~96 % triggers the writer's `appendAndFsync` `IOException` path exactly as a real disk-full would.

**Architecture:** A new compose override `docker-compose.chaos-02-nfs.yml` adds a `chaosfs` service (image `itsthenetwork/nfs-server-alpine:latest`, `tmpfs:/exports:size=300m`) and rebinds the writer's data mount from a host bind-mount to a Docker NFS volume served by `chaosfs`. `tests/chaos/common.sh` gains a generic `CHAOS_EXTRA_COMPOSE_FILES` / `CHAOS_EXTRA_SERVICES` mechanism so scenario 02 can opt in without changing other scenarios, plus two new helpers `fill_via_chaosfs` / `free_via_chaosfs` that run `dd` / `rm` inside the chaosfs container against `/exports`. After the scenario body, a new `materialize_archive_to_host` helper copies the archive out of chaosfs into `$HOST_DATA_DIR` so the existing host-side `verify` invocation works unchanged. The SKIP guard call is removed from scenario 02 only.

**Tech Stack:** Bash 3.2 (macOS), Docker Compose v2, Docker `local` volume driver with NFSv4 driver_opts, `itsthenetwork/nfs-server-alpine` image, existing Java 21 writer + verify CLI (no Java code changes).

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `docker-compose.chaos-02-nfs.yml` | new | Adds `chaosfs` service, the NFS-backed `chaos02_data` named volume on a static-IP network, and `!override`s the writer's `volumes` + `depends_on`. Loaded only for scenario 02. |
| `tests/chaos/common.sh` | modify | (a) `init_scenario` consumes `CHAOS_EXTRA_COMPOSE_FILES` and appends each as `--file` to `COMPOSE_OPTS`. (b) `start_stack` consumes `CHAOS_EXTRA_SERVICES` and appends to its service list. (c) New helpers `fill_via_chaosfs`, `free_via_chaosfs`, `materialize_archive_to_host`. (d) `teardown_stack` calls `free_via_chaosfs` in addition to the existing `free_disk`. |
| `tests/chaos/02_fill_disk.sh` | modify | Set `CHAOS_EXTRA_COMPOSE_FILES` and `CHAOS_EXTRA_SERVICES` before `init_scenario`. Drop the `safe_disk_fill_or_skip` call. Replace `fill_disk` / `free_disk` with `fill_via_chaosfs 290` / `free_via_chaosfs`. Call `materialize_archive_to_host` before `run_verify`. |
| `tests/chaos/README.md` | modify | Update scenario 02's row: no longer SKIPs by default. |

No production code changes. No Java changes. No changes to other scenarios.

---

## Task 1: Add the chaos-02 NFS compose override

**Files:**
- Create: `docker-compose.chaos-02-nfs.yml`

- [ ] **Step 1.1: Write the override file**

Create `/Users/vasyl.vaskovskyi/data/cryptolake/docker-compose.chaos-02-nfs.yml` with this exact content:

```yaml
# Chaos scenario 02 (writer_disk_full_brief) override.
#
# Adds a sidecar NFSv4 server (chaosfs) whose backing store is a 300 MiB
# tmpfs at /exports. The writer's archive directory (HOST_DATA_DIR, mounted
# at the same path inside the container) is rebound from a host bind-mount
# to a Docker NFS volume served by chaosfs.
#
# This is loaded only for tests/chaos/02_fill_disk.sh via CHAOS_EXTRA_COMPOSE_FILES.
# Other scenarios are unaffected.
#
# Static-IP network is used because Docker's local NFS volume driver runs
# the mount(2) syscall on the Docker host (the Linux VM on macOS) at
# container-start time. The host's resolver does not know Docker's internal
# DNS, so we cannot use `addr=chaosfs`; we pin chaosfs to a static IP on a
# dedicated bridge network and reference that IP in the volume's NFS opts.

services:
  chaosfs:
    image: itsthenetwork/nfs-server-alpine:latest
    privileged: true
    environment:
      SHARED_DIRECTORY: /exports
    tmpfs:
      - /exports:size=300m,mode=0777
    networks:
      chaos_nfs:
        ipv4_address: 172.30.42.10
    healthcheck:
      # The image listens on 2049/tcp once nfsd is up. nc -z is the simplest
      # portable readiness probe; rpcinfo isn't installed in this image.
      test: ["CMD-SHELL", "nc -z 127.0.0.1 2049 || exit 1"]
      interval: 3s
      timeout: 2s
      retries: 30
      start_period: 5s
    restart: "no"
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "2"

  writer:
    # !override replaces the entire list rather than appending. Without it,
    # compose merges the override list into the base list and the writer
    # ends up with BOTH the host bind-mount and the NFS volume, which is
    # not what we want.
    volumes: !override
      - ./config/config.yaml:/etc/cryptolake/config.yml:ro
      - chaos02_data:${HOST_DATA_DIR:-/data}
    depends_on: !override
      redpanda:
        condition: service_healthy
      postgres:
        condition: service_healthy
      chaosfs:
        condition: service_healthy
    networks:
      - cryptolake_internal
      - host_access
      - chaos_nfs

networks:
  chaos_nfs:
    driver: bridge
    ipam:
      config:
        - subnet: 172.30.42.0/24

volumes:
  chaos02_data:
    driver: local
    driver_opts:
      type: nfs
      # nfsvers=4 avoids portmapper/rpcbind. hard,timeo=600,retrans=2 keeps
      # the writer blocked on I/O during a server hiccup rather than failing
      # — we want ENOSPC, not ETIMEDOUT, when the tmpfs fills.
      o: "addr=172.30.42.10,rw,nfsvers=4,hard,timeo=600,retrans=2"
      device: ":/"
```

- [ ] **Step 1.2: Validate the override parses cleanly**

Run from repo root:

```bash
HOST_DATA_DIR=/tmp/cryptolake-chaos-02-data \
  docker compose \
    --project-name cryptolake-chaos-02-validate \
    --file docker-compose.yml \
    --file docker-compose.chaos-02-nfs.yml \
    config > /tmp/chaos02-merged.yml
```

Expected: exit 0, no error output. Then verify the merge took effect:

```bash
grep -A2 "chaosfs:" /tmp/chaos02-merged.yml | head -20
grep -A4 "chaos02_data:" /tmp/chaos02-merged.yml
grep -B1 -A4 "writer:" /tmp/chaos02-merged.yml | head -40
```

Expected: `chaosfs` service present with `image: itsthenetwork/nfs-server-alpine:latest`; `chaos02_data` volume present with `type: nfs`; writer's `volumes` list contains exactly two entries (`config.yaml` and `chaos02_data:/tmp/cryptolake-chaos-02-data`), not the host bind-mount.

If the writer still has the host bind-mount, the `!override` tag isn't being honored — Docker Compose ≥ v2.20 is required. Check `docker compose version` and upgrade if needed.

- [ ] **Step 1.3: Commit**

```bash
git add docker-compose.chaos-02-nfs.yml
git commit -m "chore(chaos): add NFS-sidecar compose override for scenario 02"
```

---

## Task 2: Plumb `CHAOS_EXTRA_COMPOSE_FILES` and `CHAOS_EXTRA_SERVICES` through `common.sh`

**Files:**
- Modify: `tests/chaos/common.sh:45-79` (`init_scenario`), `tests/chaos/common.sh:141-178` (`start_stack`)

- [ ] **Step 2.1: Read the existing `init_scenario` to see current `COMPOSE_OPTS` assembly**

Run:

```bash
sed -n '45,80p' tests/chaos/common.sh
```

Expected: shows `COMPOSE_OPTS=( --project-name "$COMPOSE_PROJECT" --file "$COMPOSE_FILE" )` and the conditional `WRITER_MODE=external` append. Note that line numbers may shift after edits — re-grep before each edit.

- [ ] **Step 2.2: Add `CHAOS_EXTRA_COMPOSE_FILES` consumption to `init_scenario`**

Use the `Edit` tool. Find this exact block in `tests/chaos/common.sh`:

```bash
    if [[ "$WRITER_MODE" == "external" ]]; then
        if [[ ! -f "$COMPOSE_DEBUG_WRITER_FILE" ]]; then
            die "WRITER_MODE=external but ${COMPOSE_DEBUG_WRITER_FILE} not found."
        fi
        COMPOSE_OPTS+=(--file "$COMPOSE_DEBUG_WRITER_FILE")
        msg "WRITER_MODE=external — writer service will not be started; run it from your IDE."
    fi
    SCENARIO_MODE="$mode"
```

Replace with:

```bash
    if [[ "$WRITER_MODE" == "external" ]]; then
        if [[ ! -f "$COMPOSE_DEBUG_WRITER_FILE" ]]; then
            die "WRITER_MODE=external but ${COMPOSE_DEBUG_WRITER_FILE} not found."
        fi
        COMPOSE_OPTS+=(--file "$COMPOSE_DEBUG_WRITER_FILE")
        msg "WRITER_MODE=external — writer service will not be started; run it from your IDE."
    fi

    # Per-scenario opt-in compose overrides. Set CHAOS_EXTRA_COMPOSE_FILES
    # to a colon-separated list of file paths (relative to REPO_ROOT or
    # absolute) before calling init_scenario. Each is appended as --file.
    if [[ -n "${CHAOS_EXTRA_COMPOSE_FILES:-}" ]]; then
        local IFS_SAVE="$IFS"
        IFS=':'
        for extra in $CHAOS_EXTRA_COMPOSE_FILES; do
            local extra_path="$extra"
            [[ "$extra_path" != /* ]] && extra_path="${REPO_ROOT}/${extra_path}"
            if [[ ! -f "$extra_path" ]]; then
                IFS="$IFS_SAVE"
                die "CHAOS_EXTRA_COMPOSE_FILES references missing file: ${extra_path}"
            fi
            COMPOSE_OPTS+=(--file "$extra_path")
            msg "Loaded extra compose file: ${extra_path}"
        done
        IFS="$IFS_SAVE"
    fi

    SCENARIO_MODE="$mode"
```

- [ ] **Step 2.3: Add `CHAOS_EXTRA_SERVICES` consumption to `start_stack`**

Find this exact block in `tests/chaos/common.sh`:

```bash
    # External writer: drop the writer service from the start list.
    if [[ "$WRITER_MODE" == "external" ]]; then
        local pruned=()
        for s in "${services[@]}"; do
            [[ "$s" == "writer" ]] && continue
            pruned+=("$s")
        done
        services=("${pruned[@]}")
    fi

    # Pull only core services, exclude monitoring/alerting for speed.
    # First scenario builds images; subsequent scenarios reuse buildkit cache.
    dc up -d "${services[@]}"
```

Replace with:

```bash
    # External writer: drop the writer service from the start list.
    if [[ "$WRITER_MODE" == "external" ]]; then
        local pruned=()
        for s in "${services[@]}"; do
            [[ "$s" == "writer" ]] && continue
            pruned+=("$s")
        done
        services=("${pruned[@]}")
    fi

    # Per-scenario opt-in extra services (e.g. chaosfs for scenario 02).
    # Set CHAOS_EXTRA_SERVICES to a space-separated list before calling
    # start_stack. Order matters: extras are prepended so they come up
    # before the writer (which depends_on them via the override file).
    if [[ -n "${CHAOS_EXTRA_SERVICES:-}" ]]; then
        local extras=()
        # shellcheck disable=SC2206 # we want word-splitting here
        extras=( ${CHAOS_EXTRA_SERVICES} )
        services=( "${extras[@]}" "${services[@]}" )
        msg "Including extra services: ${CHAOS_EXTRA_SERVICES}"
    fi

    # Pull only core services, exclude monitoring/alerting for speed.
    # First scenario builds images; subsequent scenarios reuse buildkit cache.
    dc up -d "${services[@]}"
```

- [ ] **Step 2.4: Verify the plumbing doesn't break existing scenarios**

Run a quick syntax check:

```bash
bash -n tests/chaos/common.sh
```

Expected: exit 0, no output.

Then dry-run scenario 01 to confirm no behavioral change when the env vars are unset:

```bash
unset CHAOS_EXTRA_COMPOSE_FILES CHAOS_EXTRA_SERVICES
HOST_DATA_DIR=/tmp/cryptolake-chaos-01-data bash -c '
  source tests/chaos/common.sh
  init_scenario "01" "primary"
  echo "COMPOSE_OPTS: ${COMPOSE_OPTS[*]}"
'
```

Expected output ends with a single `--file ./docker-compose.yml` (or absolute equivalent) and **no** `--file docker-compose.chaos-02-nfs.yml`. The teardown trap firing after the subshell exits is fine — the goal is to confirm `COMPOSE_OPTS` is unchanged.

- [ ] **Step 2.5: Commit**

```bash
git add tests/chaos/common.sh
git commit -m "feat(chaos): add CHAOS_EXTRA_COMPOSE_FILES and CHAOS_EXTRA_SERVICES hooks"
```

---

## Task 3: Add `fill_via_chaosfs` / `free_via_chaosfs` / `materialize_archive_to_host` helpers

**Files:**
- Modify: `tests/chaos/common.sh` — append immediately after the existing `free_disk()` definition (around line 914 today; re-locate with grep before editing).

- [ ] **Step 3.1: Locate the insertion point**

Run:

```bash
grep -n "^free_disk()" tests/chaos/common.sh
grep -n "^teardown_stack()" tests/chaos/common.sh
```

Expected: `free_disk()` precedes `teardown_stack()`. New helpers go between them.

- [ ] **Step 3.2: Insert the three helpers**

Find this exact block in `tests/chaos/common.sh`:

```bash
free_disk() {
    if [[ -n "${DISK_FILLER_FILE:-}" && -f "$DISK_FILLER_FILE" ]]; then
        rm -f "$DISK_FILLER_FILE"
        msg "Disk filler removed. Current usage: $(df -h "$HOST_DATA_DIR" | awk 'NR==2{print $5}')"
    fi
}
```

Replace with:

```bash
free_disk() {
    if [[ -n "${DISK_FILLER_FILE:-}" && -f "$DISK_FILLER_FILE" ]]; then
        rm -f "$DISK_FILLER_FILE"
        msg "Disk filler removed. Current usage: $(df -h "$HOST_DATA_DIR" | awk 'NR==2{print $5}')"
    fi
}

# ---------------------------------------------------------------------------
# fill_via_chaosfs <megabytes>
# Fills the chaosfs sidecar's /exports tmpfs (capped to 300 MiB by the
# scenario-02 compose override) by writing a single filler file inside the
# chaosfs container. The writer mounts /exports over NFS as its /data, so
# this is what triggers ENOSPC on the writer's next fsync.
# ---------------------------------------------------------------------------
fill_via_chaosfs() {
    local megs="${1:?fill_via_chaosfs requires a megabyte count}"
    msg "Filling chaosfs:/exports with ${megs} MiB filler…"
    # conv=fsync forces metadata flush so the tmpfs cap is hit synchronously
    # rather than at some later writeback. status=none keeps the chaos log
    # readable; dd's progress lines mangle the [chaos] prefix layout.
    if dc exec -T chaosfs sh -c "dd if=/dev/zero of=/exports/_chaos_filler bs=1M count=${megs} conv=fsync status=none"; then
        msg "Fill complete. chaosfs:/exports usage:"
        dc exec -T chaosfs sh -c 'df -h /exports' || true
    else
        # ENOSPC mid-write is the success case (we asked dd to overshoot or
        # land exactly at the cap). Surface the usage either way.
        msg "dd returned non-zero (likely ENOSPC — that's expected). Current usage:"
        dc exec -T chaosfs sh -c 'df -h /exports' || true
    fi
    CHAOSFS_FILLED=1
}

# ---------------------------------------------------------------------------
# free_via_chaosfs
# Removes the chaosfs filler file. Idempotent; safe to call from teardown.
# ---------------------------------------------------------------------------
free_via_chaosfs() {
    [[ -z "${CHAOSFS_FILLED:-}" ]] && return 0
    msg "Removing chaosfs:/exports/_chaos_filler…"
    dc exec -T chaosfs sh -c 'rm -f /exports/_chaos_filler' || true
    dc exec -T chaosfs sh -c 'df -h /exports' || true
    unset CHAOSFS_FILLED
}

# ---------------------------------------------------------------------------
# materialize_archive_to_host
# When the writer's /data is backed by an NFS volume served by chaosfs, the
# archive lives inside the chaosfs container's tmpfs and is invisible to
# the host-side `verify` CLI. This helper copies the archive tree out of
# chaosfs into $HOST_DATA_DIR so the existing `run_verify` invocation can
# read it without changes.
#
# Using `docker compose cp` (not bind-mount tricks) keeps this hermetic:
# the host dir is empty before copy and gets populated only with what the
# writer actually persisted.
# ---------------------------------------------------------------------------
materialize_archive_to_host() {
    [[ -z "${HOST_DATA_DIR:-}" ]] && die "materialize_archive_to_host: HOST_DATA_DIR unset"
    msg "Copying archive from chaosfs:/exports → ${HOST_DATA_DIR}…"
    mkdir -p "$HOST_DATA_DIR"
    # `cp -a` preserves mtimes; `dc cp` doesn't have -a, so use tar over a pipe.
    dc exec -T chaosfs sh -c 'tar -C /exports -cf - . 2>/dev/null' \
        | tar -C "$HOST_DATA_DIR" -xf - 2>/dev/null \
        || die "materialize_archive_to_host: copy failed"
    msg "Archive materialized. Top-level entries:"
    ls -la "$HOST_DATA_DIR" | head -10 || true
}
```

- [ ] **Step 3.3: Wire `free_via_chaosfs` into `teardown_stack`**

Find this exact line in `tests/chaos/common.sh`:

```bash
    free_disk 2>/dev/null || true
```

Replace with:

```bash
    free_disk 2>/dev/null || true
    free_via_chaosfs 2>/dev/null || true
```

- [ ] **Step 3.4: Syntax check**

Run:

```bash
bash -n tests/chaos/common.sh
```

Expected: exit 0, no output.

- [ ] **Step 3.5: Commit**

```bash
git add tests/chaos/common.sh
git commit -m "feat(chaos): add fill_via_chaosfs / free_via_chaosfs / materialize_archive_to_host helpers"
```

---

## Task 4: Convert `02_fill_disk.sh` to use the chaosfs sidecar

**Files:**
- Modify: `tests/chaos/02_fill_disk.sh`

- [ ] **Step 4.1: Read the current scenario top-to-bottom**

Run:

```bash
cat tests/chaos/02_fill_disk.sh
```

Expected: matches the body shown in CLAUDE.md context — `init_scenario`, `safe_disk_fill_or_skip`, `start_stack`, `wait_healthy`, `warm_up`, `fill_disk`, `sleep 120`, `free_disk`, `sleep 90`, `run_verify`, assertions, `verdict`.

- [ ] **Step 4.2: Replace the file with the chaosfs version**

Use the `Write` tool to overwrite `tests/chaos/02_fill_disk.sh` with this exact content (keep the leading shebang + comment block updated):

```bash
#!/usr/bin/env bash
# 02_fill_disk.sh
#
# Scenario: writer_disk_full_brief
# Chaos:    Fill the writer's /data filesystem (a 300 MiB tmpfs served by
#           the chaosfs NFS sidecar) to ~96%; hold ~120s; free disk.
# Expected: NO gap (writer recovers from Kafka after disk freed)
# Flow:     MAIN+BACKUP both delivering normally → writer's appendAndFsync
#           hits IOException on disk-full → writeErrors metric increments,
#           Kafka offsets are NOT committed (PG-then-Kafka ordering in
#           OffsetCommitCoordinator) → MAIN+BACKUP keep producing to Kafka,
#           records remain durable for 48h → disk freed → writer's next
#           flushAndCommit succeeds → consumer re-reads uncommitted
#           offsets → archive completes with no missing data.
# Why:      Under TWO-COLLECTOR + Kafka 48h retention + commit-after-fsync
#           invariant, a brief disk-full episode is fully recoverable.
#           Both MAIN and BACKUP records during the held window are
#           durable in Kafka and replayed on recovery. No real data loss,
#           no gap envelope. (A SUSTAINED hold exceeding 48h would lose
#           data — that case is operationally surfaced via writer_write_errors
#           rate alerting, not via a gap envelope, since chaos-testing it
#           reliably is impractical.)
#
# Implementation: This scenario uses a sidecar NFSv4 server (`chaosfs`)
# whose backing store is a 300 MiB tmpfs. The writer mounts /data over NFS
# from chaosfs. Filling 290 MiB of the tmpfs leaves ~10 MiB headroom and
# triggers ENOSPC on the writer's next fsync without touching the host
# filesystem. After recovery, the archive is materialized from chaosfs
# back to HOST_DATA_DIR so the host-side verify CLI can read it.

set -euo pipefail
source "$(dirname "$0")/common.sh"

# Opt in to the chaosfs sidecar via the generic CHAOS_EXTRA_* hooks.
# init_scenario reads CHAOS_EXTRA_COMPOSE_FILES; start_stack reads CHAOS_EXTRA_SERVICES.
export CHAOS_EXTRA_COMPOSE_FILES="docker-compose.chaos-02-nfs.yml"
export CHAOS_EXTRA_SERVICES="chaosfs"

init_scenario "02" "primary+backup"

start_stack "primary+backup"
wait_healthy 180

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Filling chaosfs tmpfs (290 MiB of 300 MiB cap) ==="
fill_via_chaosfs 290

# Hold the disk-full state long enough for the writer to attempt several
# flush cycles and accumulate writeErrors. Default flush interval is 5s,
# so 120s gives ~24 failed-flush attempts.
msg "Holding disk-full state for 120s (writer's flushAndCommit will fail repeatedly)…"
sleep 120

msg "Freeing chaosfs tmpfs…"
free_via_chaosfs

# After the disk is freed, the writer's next flushAndCommit succeeds. The
# Kafka consumer position is unchanged (no commits happened during the
# hold), so the next poll re-reads the records published during the hold
# from BOTH topics. They get archived now, completing the gap.
msg "Waiting 90s for writer recovery (re-poll uncommitted offsets, archive backlog)…"
sleep 90

# The archive lives inside chaosfs:/exports (the writer's NFS-mounted /data).
# Copy it to HOST_DATA_DIR so the host-side verify CLI can see it.
materialize_archive_to_host

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — brief disk-full is recoverable; no real data loss.
expect_lifecycle_event        "writer enters disk-full hold"     "WRITER_DISK_FULL_HOLD_ENTERED"
expect_lifecycle_event        "writer exits disk-full hold"      "WRITER_DISK_FULL_HOLD_EXITED"
expect_lifecycle_event_absent "no uncovered gap accepted"        "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
```

- [ ] **Step 4.3: Syntax check**

Run:

```bash
bash -n tests/chaos/02_fill_disk.sh
```

Expected: exit 0, no output.

- [ ] **Step 4.4: Confirm the new wiring lands `chaosfs` in the compose plan**

Run:

```bash
HOST_DATA_DIR=/tmp/cryptolake-chaos-02-data \
CHAOS_EXTRA_COMPOSE_FILES=docker-compose.chaos-02-nfs.yml \
CHAOS_EXTRA_SERVICES=chaosfs \
  docker compose \
    --project-name cryptolake-chaos-02-validate \
    --file docker-compose.yml \
    --file docker-compose.chaos-02-nfs.yml \
    config --services | sort
```

Expected: list includes both `chaosfs` and `writer`.

- [ ] **Step 4.5: Commit**

```bash
git add tests/chaos/02_fill_disk.sh
git commit -m "test(chaos-02): use chaosfs NFS sidecar; drop host-disk SKIP guard"
```

---

## Task 5: Smoke-test the chaosfs container in isolation

This task verifies the sidecar before paying the cost of a full scenario run. If chaosfs cannot serve NFS to the writer, scenario 02 will hang at `wait_healthy` with no useful diagnostic — surface that early.

**Files:** none (verification-only)

- [ ] **Step 5.1: Bring up chaosfs alone**

Run:

```bash
HOST_DATA_DIR=/tmp/chaosfs-smoke \
  docker compose \
    --project-name chaosfs-smoke \
    --file docker-compose.yml \
    --file docker-compose.chaos-02-nfs.yml \
    up -d chaosfs
```

Expected: `chaosfs` starts within ~10 s. Check health:

```bash
docker compose --project-name chaosfs-smoke ps chaosfs
```

Expected: `STATUS` shows `(healthy)` after up to ~30 s.

- [ ] **Step 5.2: Verify the tmpfs cap is enforced**

Run:

```bash
docker compose --project-name chaosfs-smoke exec chaosfs sh -c 'df -h /exports'
docker compose --project-name chaosfs-smoke exec chaosfs sh -c \
  'dd if=/dev/zero of=/exports/probe bs=1M count=400 status=none; df -h /exports; rm -f /exports/probe'
```

Expected: first `df` shows `Size 300M`. The `dd` returns non-zero (ENOSPC after ~300 MiB written). Second `df` shows `Use% 99%` or `100%` before `rm`. After `rm`, the directory is back to empty.

- [ ] **Step 5.3: Tear down the smoke project**

Run:

```bash
docker compose --project-name chaosfs-smoke down -v --remove-orphans --rmi local
rm -rf /tmp/chaosfs-smoke
```

Expected: clean teardown, no leftover containers (`docker ps -a | grep chaosfs-smoke` is empty).

- [ ] **Step 5.4: No commit**

This task is verification-only. If it passed, proceed to Task 6.

---

## Task 6: End-to-end run of scenario 02 against a real stack

**Files:** none (verification-only; if it fails, fix in earlier tasks).

- [ ] **Step 6.1: Ensure the verify CLI is built**

Run:

```bash
./gradlew :verify:installDist -q
test -x verify/build/install/verify/bin/verify && echo OK
```

Expected: prints `OK`.

- [ ] **Step 6.2: Run scenario 02 in isolation**

Run from repo root:

```bash
bash tests/chaos/02_fill_disk.sh 2>&1 | tee /tmp/chaos-02-run.log
```

Expected: exit code 0. Final lines should show the green `SCENARIO 02: PASS` banner. The log should contain (search with grep):

- `[chaos] Filling chaosfs:/exports with 290 MiB filler`
- `[chaos] Holding disk-full state for 120s`
- `[chaos] Freeing chaosfs tmpfs…`
- `[chaos] Copying archive from chaosfs:/exports → /tmp/cryptolake-chaos-02-data`
- `[chaos] PASS: verify ERRORS=0` (or `[chaos] PASS: verify completed (no archive files or no ERRORS line)`)
- `[chaos] PASS: ... WRITER_DISK_FULL_HOLD_ENTERED`
- `[chaos] PASS: ... WRITER_DISK_FULL_HOLD_EXITED`
- `[chaos] PASS: no uncovered gap accepted` (event absent)
- `[chaos] PASS: no gap envelopes archived`

If any assertion fails, **stop and diagnose** before moving on. Common failure modes:

| Symptom | Likely cause | Where to fix |
|---|---|---|
| `wait_healthy` times out on writer | NFS mount on writer container failed | `docker compose --project-name cryptolake-chaos-02 logs writer` will show `mount failed`. Check chaosfs IP / `addr=` in the override. |
| Scenario passes but `materialize_archive_to_host` shows empty `HOST_DATA_DIR` | tar pipe failed silently | Re-run the inner command manually: `docker compose --project-name cryptolake-chaos-02 exec chaosfs ls -la /exports`. If empty, writer never wrote — investigate writer logs. |
| `expect_lifecycle_event WRITER_DISK_FULL_HOLD_ENTERED` fails | tmpfs filled but writer didn't observe ENOSPC | Inspect `build/chaos-logs/02/writer.log` post-teardown for `IOException`. If absent, the NFS mount may be silently buffering writes — set `o: "...,sync"` in the volume opts. |
| `chaosfs unhealthy` | Image lacks `nc` or 2049 not listening | Switch healthcheck to `["CMD-SHELL", "ls /proc/$(pgrep nfsd)/status >/dev/null 2>&1 || exit 1"]`. |

- [ ] **Step 6.3: Confirm clean teardown**

Run:

```bash
docker ps -a | grep cryptolake-chaos-02 || echo "no leftover containers"
docker volume ls | grep chaos02_data || echo "no leftover volumes"
docker images | grep cryptolake-chaos-02 || echo "no leftover images"
test -d /tmp/cryptolake-chaos-02-data && echo "LEFTOVER" || echo "no leftover data"
```

Expected: all four lines print "no leftover ..."—nothing else.

- [ ] **Step 6.4: Run the full chaos suite to confirm no regression in other scenarios**

Run:

```bash
bash scripts/run-chaos-tests.sh 2>&1 | tee /tmp/chaos-suite.log
```

Expected: final banner is green, all 16 scenarios PASS. In particular scenarios 01, 03, 04 (which don't use chaosfs) must still work — confirms the `CHAOS_EXTRA_*` plumbing is opt-in only.

- [ ] **Step 6.5: No commit**

If everything passes, proceed to Task 7.

---

## Task 7: Update README.md

**Files:**
- Modify: `tests/chaos/README.md`

- [ ] **Step 7.1: Locate the scenario-02 entry**

Run:

```bash
grep -n "02" tests/chaos/README.md | head -5
```

Note the line(s) where scenario 02's row appears. The README should describe each scenario.

- [ ] **Step 7.2: Update the scenario-02 description**

Open `tests/chaos/README.md` and find the section/row describing scenario 02 (search for `writer_disk_full_brief` or `02_fill_disk`). Replace any text that says the scenario "SKIPs unless …" or mentions `safe_disk_fill_or_skip` / `CRYPTOLAKE_CHAOS_DANGEROUS_DISK` with:

> Runs by default. Uses a sidecar NFS server (`chaosfs`, see `docker-compose.chaos-02-nfs.yml`) whose backing store is a 300 MiB tmpfs. No host-disk impact; safe on dev machines and CI.

Keep the rest of the row (gap-reason taxonomy, expected behaviour) unchanged.

If the README has a "Prerequisites" section that mentions the host-disk requirement for scenario 02, remove that bullet — it no longer applies.

- [ ] **Step 7.3: Commit**

```bash
git add tests/chaos/README.md
git commit -m "docs(chaos): scenario 02 now runs by default via NFS sidecar"
```

---

## Self-Review Checklist (run before handoff)

- **Spec coverage:**
  - [x] 300 MiB tmpfs cap — set in Task 1 step 1.1.
  - [x] Sidecar container provides the filesystem — `chaosfs` service in Task 1.
  - [x] No host-disk impact — Task 1 uses `tmpfs` inside chaosfs; Task 4 drops `safe_disk_fill_or_skip` and the host-side `dd`.
  - [x] Collectors NOT touched — they remain on default networks; only the writer (and only in scenario 02) gets its `/data` rebound.
  - [x] Verify CLI still works — Task 3's `materialize_archive_to_host` copies data back to `$HOST_DATA_DIR` for host-side verify.
  - [x] Other scenarios unaffected — Task 6.4 explicitly re-runs the full suite.
  - [x] Cleanup on failure — `free_via_chaosfs` is wired into `teardown_stack` (Task 3.3); `down -v` removes the NFS volume and tmpfs.

- **Placeholder scan:** No `TBD`, `TODO`, or `Add appropriate ...`. Each step has either complete YAML/bash or a concrete verification command.

- **Type / name consistency:**
  - `chaosfs` — service name used everywhere.
  - `chaos02_data` — volume name in override (Task 1) and inferred via Docker, not referenced from bash.
  - `CHAOS_EXTRA_COMPOSE_FILES`, `CHAOS_EXTRA_SERVICES` — defined in Task 2, used in Task 4.
  - `fill_via_chaosfs`, `free_via_chaosfs`, `materialize_archive_to_host` — defined in Task 3, called in Task 4.
  - `CHAOSFS_FILLED` — set in `fill_via_chaosfs`, read in `free_via_chaosfs`.

---

## Acceptance Criteria

- `bash tests/chaos/02_fill_disk.sh` exits 0 on a clean dev Mac with no special host setup, no `CRYPTOLAKE_CHAOS_DANGEROUS_DISK` flag.
- Scenario log shows `WRITER_DISK_FULL_HOLD_ENTERED` and `WRITER_DISK_FULL_HOLD_EXITED` lifecycle events.
- `cryptolake-verify` reports `ERRORS=0` and **no gap envelope** for the held window.
- Compose teardown leaves no `cryptolake-chaos-02-*` containers, volumes, or images, and no `/tmp/cryptolake-chaos-02-data` directory.
- `bash scripts/run-chaos-tests.sh` continues to pass all 16 scenarios.
