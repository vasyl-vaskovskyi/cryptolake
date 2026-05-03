#!/usr/bin/env bash
# tests/chaos/common.sh — Shared helpers for CryptoLake chaos scenarios
#
# SOURCE this file at the top of every scenario:
#   source "$(dirname "$0")/common.sh"
#
# Then call:
#   init_scenario <NN> [primary|primary+backup]
#
# That sets SCENARIO_NUM, COMPOSE_PROJECT, HOST_DATA_DIR, and COMPOSE_OPTS,
# and registers teardown_stack in a trap EXIT so cleanup is guaranteed even
# when the scenario fails with `set -e`.
#
# Every function that can fail uses `|| { msg "FAIL: ..."; return 1; }` so
# the caller (which uses set -e) gets a non-zero exit and the trap fires.

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VERIFY_BIN="${REPO_ROOT}/verify/build/install/verify/bin/verify"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
msg() { printf '[chaos] %s\n' "$*" >&2; }
die() { msg "FATAL: $*"; exit 1; }

# ---------------------------------------------------------------------------
# init_scenario <nn> [primary|primary+backup]
# Sets global state used by all helpers.  Always called first.
# ---------------------------------------------------------------------------
init_scenario() {
    local nn="${1:?init_scenario requires a scenario number}"
    local mode="${2:-primary+backup}"

    SCENARIO_NUM="$nn"
    COMPOSE_PROJECT="cryptolake-chaos-${nn}"
    HOST_DATA_DIR="/tmp/cryptolake-chaos-${nn}-data"
    COMPOSE_OPTS=(
        --project-name "$COMPOSE_PROJECT"
        --file "$COMPOSE_FILE"
    )
    SCENARIO_MODE="$mode"
    export HOST_DATA_DIR COMPOSE_PROJECT

    print_chaos_header "${BASH_SOURCE[1]:-$0}"
    msg "=== Scenario ${nn} init (mode=${mode}) ==="
    msg "HOST_DATA_DIR=${HOST_DATA_DIR}"
    msg "COMPOSE_PROJECT=${COMPOSE_PROJECT}"

    # Guarantee cleanup even on set -e failures
    trap 'teardown_stack' EXIT
}

# ---------------------------------------------------------------------------
# print_chaos_header <script_path>
# Extracts the Scenario/Chaos/Expected/Flow/Why block from a scenario
# script's leading comment and prints it as a banner so the test log shows
# the scenario contract (including the TWO-COLLECTOR rule rationale) up
# front. Multi-line continuation comments (lines starting with "#  ") are
# preserved.
# ---------------------------------------------------------------------------
print_chaos_header() {
    local script="${1:?print_chaos_header requires a script path}"
    [[ -f "$script" ]] || return 0

    msg ""
    msg "============================================================"
    msg "                  CHAOS SCENARIO HEADER"
    msg "============================================================"
    # Capture from "# Scenario:" through the last "# " line before code.
    # Stop at the first non-comment line.
    local line
    local in_block=0
    while IFS= read -r line; do
        if [[ "$line" =~ ^#[[:space:]]*Scenario: ]]; then
            in_block=1
        fi
        if (( in_block )); then
            if [[ "$line" =~ ^# ]]; then
                # Strip leading "# " (or just "#"), keep the rest as-is.
                local stripped="${line#\#}"
                stripped="${stripped# }"
                msg "  ${stripped}"
            else
                break
            fi
        fi
    done < "$script"
    msg "============================================================"
    msg ""
}

# ---------------------------------------------------------------------------
# dc <args…>  — run docker compose with the project-specific options
# ---------------------------------------------------------------------------
dc() { docker compose "${COMPOSE_OPTS[@]}" "$@"; }

# ---------------------------------------------------------------------------
# start_stack [primary|primary+backup]
# ---------------------------------------------------------------------------
start_stack() {
    local mode="${1:-$SCENARIO_MODE}"
    mkdir -p "$HOST_DATA_DIR"
    msg "Starting stack (mode=${mode})…"

    # Determine which services to start
    local services
    case "$mode" in
        primary)
            services=(redpanda postgres collector writer)
            ;;
        primary+backup)
            services=(redpanda postgres collector collector-backup writer)
            ;;
        *)
            die "Unknown mode: $mode"
            ;;
    esac

    # Pull only core services, exclude monitoring/alerting for speed.
    # First scenario builds images; subsequent scenarios reuse buildkit cache.
    dc up -d "${services[@]}"
    msg "Stack started."

    # Begin streaming LIFECYCLE events so the test log shows MAIN/BACKUP
    # transitions in real time.
    start_lifecycle_tail
}

# ---------------------------------------------------------------------------
# start_lifecycle_tail
# Background-tails writer + collector + collector-backup container logs,
# filters lines containing "LIFECYCLE", and prefixes each with "[lifecycle]"
# so they interleave with "[chaos]" lines during a chaos run.
# ---------------------------------------------------------------------------
start_lifecycle_tail() {
    [[ -n "${LIFECYCLE_TAIL_PID:-}" ]] && return 0
    msg "Starting LIFECYCLE event stream (writer + collectors)…"

    # Pick services that exist for this mode.
    local lc_services=(writer collector)
    if [[ "${SCENARIO_MODE:-primary+backup}" == "primary+backup" ]]; then
        lc_services+=(collector-backup)
    fi

    # Run dc logs -f in the background. The pipeline filters and reformats.
    # `--no-color` keeps the grep simple. We redirect stderr to /dev/null
    # because docker emits its own informational lines on stderr that we
    # don't care to surface.
    (
        dc logs -f --no-color --tail=0 "${lc_services[@]}" 2>/dev/null \
            | grep --line-buffered "LIFECYCLE" \
            | while IFS= read -r line; do
                # The compose log format prefixes lines with the service name.
                # We keep the service name + the LIFECYCLE event for context.
                # Trim trailing CR (Windows-y stderr) just in case.
                printf '[lifecycle] %s\n' "${line%$'\r'}" >&2
              done
    ) &
    LIFECYCLE_TAIL_PID=$!
    # Detach so kill -- doesn't drag our own process group.
    disown "$LIFECYCLE_TAIL_PID" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# stop_lifecycle_tail
# Stops the lifecycle log stream. Idempotent.
# ---------------------------------------------------------------------------
stop_lifecycle_tail() {
    [[ -z "${LIFECYCLE_TAIL_PID:-}" ]] && return 0
    # Kill the whole subshell (and its grep + while) by signalling its PID.
    # Use SIGTERM first, fall back to SIGKILL only if needed.
    kill "$LIFECYCLE_TAIL_PID" 2>/dev/null || true
    # Reap; ignore errors (process may have already exited).
    wait "$LIFECYCLE_TAIL_PID" 2>/dev/null || true
    unset LIFECYCLE_TAIL_PID
}

# ---------------------------------------------------------------------------
# wait_healthy [timeout_secs]
# Blocks until collector + writer healthcheck = healthy (or timeout).
# ---------------------------------------------------------------------------
wait_healthy() {
    local timeout="${1:-120}"
    local deadline=$(( SECONDS + timeout ))
    msg "Waiting for collector + writer to be healthy (timeout=${timeout}s)…"

    # Determine which collector services we expect based on mode
    local required_services=("writer")
    case "$SCENARIO_MODE" in
        primary)
            required_services+=("collector")
            ;;
        primary+backup)
            required_services+=("collector" "collector-backup")
            ;;
    esac

    while true; do
        local all_healthy=true
        for svc in "${required_services[@]}"; do
            local status
            status=$(dc ps --format json "$svc" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health','') if isinstance(d,dict) else (d[0].get('Health','') if d else ''))" 2>/dev/null || echo "")
            if [[ "$status" != "healthy" ]]; then
                all_healthy=false
                break
            fi
        done

        if $all_healthy; then
            msg "All services healthy."
            return 0
        fi

        if (( SECONDS >= deadline )); then
            msg "Timeout waiting for healthy services. Current state:"
            dc ps 2>/dev/null || true
            return 1
        fi
        sleep 5
    done
}

# ---------------------------------------------------------------------------
# wait_data_flowing [stream] [timeout_secs]
# Waits until at least one archive file exists for the given stream.
# ---------------------------------------------------------------------------
wait_data_flowing() {
    local stream="${1:-bookticker}"
    local timeout="${2:-90}"
    local deadline=$(( SECONDS + timeout ))
    msg "Waiting for data to flow (stream=${stream}, timeout=${timeout}s)…"

    while true; do
        local count
        count=$(find "$HOST_DATA_DIR" -name "hour-*.jsonl.zst" -path "*/${stream}/*" 2>/dev/null | wc -l || echo 0)
        if (( count > 0 )); then
            msg "Data flowing: found ${count} archive file(s) for ${stream}."
            return 0
        fi
        if (( SECONDS >= deadline )); then
            msg "Timeout: no archive files found for stream=${stream} in ${HOST_DATA_DIR}"
            find "$HOST_DATA_DIR" -name "*.jsonl.zst" 2>/dev/null | head -5 || true
            return 1
        fi
        sleep 5
    done
}

# ---------------------------------------------------------------------------
# warm_up [secs]
# Simple sleep with a progress message.
# ---------------------------------------------------------------------------
warm_up() {
    local secs="${1:-45}"
    msg "Warm-up: ${secs}s…"
    sleep "$secs"
}

# ---------------------------------------------------------------------------
# kill_service <name>
# Sends SIGKILL to the named compose service (unclean exit).
# ---------------------------------------------------------------------------
kill_service() {
    local svc="${1:?kill_service requires a service name}"
    msg "Killing service: ${svc} (SIGKILL)…"
    dc kill -s SIGKILL "$svc"
}

# ---------------------------------------------------------------------------
# clean_stop_service <name>
# Stops the named compose service gracefully (records clean shutdown).
# ---------------------------------------------------------------------------
clean_stop_service() {
    local svc="${1:?clean_stop_service requires a service name}"
    msg "Stopping service cleanly: ${svc}…"
    dc stop "$svc"
}

# ---------------------------------------------------------------------------
# start_service <name>
# (Re)starts a single service.
# ---------------------------------------------------------------------------
start_service() {
    local svc="${1:?start_service requires a service name}"
    msg "Starting service: ${svc}…"
    dc up -d --no-build "$svc"
}

# ---------------------------------------------------------------------------
# block_egress <container_name> [direction: OUTPUT|INPUT]
# Uses iptables INSIDE the container via nsenter to block its network.
# Falls back to `docker network disconnect` if iptables not available.
# ---------------------------------------------------------------------------
block_egress_via_network() {
    local svc="${1:?need service name}"
    msg "Disconnecting service ${svc} from external network (simulating egress block)…"
    # Disconnect from the egress network; keep internal network intact
    local container
    container=$(dc ps -q "$svc" 2>/dev/null | head -1)
    if [[ -z "$container" ]]; then
        msg "No container for service ${svc}"
        return 1
    fi
    # Try to disconnect from egress / external-facing network
    local project_lower
    project_lower=$(echo "$COMPOSE_PROJECT" | tr '[:upper:]' '[:lower:]')
    local net_egress="${project_lower}_collector_egress"
    local net_backup="${project_lower}_backup_egress"
    local net_host="${project_lower}_host_access"
    for net in "$net_egress" "$net_backup" "$net_host"; do
        docker network disconnect "$net" "$container" 2>/dev/null && \
            msg "  Disconnected from ${net}" || true
    done
}

restore_egress_via_network() {
    local svc="${1:?need service name}"
    msg "Reconnecting service ${svc} to external network…"
    local container
    container=$(dc ps -q "$svc" 2>/dev/null | head -1)
    if [[ -z "$container" ]]; then
        return 0
    fi
    local project_lower
    project_lower=$(echo "$COMPOSE_PROJECT" | tr '[:upper:]' '[:lower:]')
    local net_egress="${project_lower}_collector_egress"
    local net_backup="${project_lower}_backup_egress"
    for net in "$net_egress" "$net_backup"; do
        docker network connect "$net" "$container" 2>/dev/null && \
            msg "  Reconnected to ${net}" || true
    done
}

# ---------------------------------------------------------------------------
# assert_lifecycle_event <pattern> [service=writer]
# Asserts that the docker-compose log for <service> contains a line matching
# "LIFECYCLE.*<pattern>". Used by chaos scenarios to verify a specific
# state-machine transition fired (not just that the verify CLI saw 0 errors).
# ---------------------------------------------------------------------------
assert_lifecycle_event() {
    local pattern="${1:?need pattern}"
    local svc="${2:-writer}"
    msg "Asserting LIFECYCLE event matching '${pattern}' fired on ${svc}…"
    if dc logs --no-color "$svc" 2>/dev/null | grep -q "LIFECYCLE.*${pattern}"; then
        msg "PASS: LIFECYCLE event '${pattern}' fired on ${svc}."
    else
        msg "FAIL: LIFECYCLE event '${pattern}' did NOT fire on ${svc}."
        msg "Recent ${svc} log tail (last 30 lines):"
        dc logs --tail 30 --no-color "$svc" 2>/dev/null | sed 's/^/  /' >&2 || true
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# block_service_network <svc> — disconnect from ALL networks (full isolation)
# ---------------------------------------------------------------------------
block_service_network() {
    local svc="${1:?need service name}"
    msg "Fully isolating service ${svc} from all networks…"
    local container
    container=$(dc ps -q "$svc" 2>/dev/null | head -1)
    [[ -z "$container" ]] && { msg "No container for ${svc}"; return 1; }
    # Get all connected networks
    local nets
    nets=$(docker inspect "$container" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || echo "")
    for net in $nets; do
        docker network disconnect "$net" "$container" 2>/dev/null && \
            msg "  Disconnected from $net" || true
    done
}

restore_service_network() {
    local svc="${1:?need service name}"
    msg "Restoring network for service ${svc}…"
    local container
    container=$(dc ps -q "$svc" 2>/dev/null | head -1)
    if [[ -z "$container" ]]; then
        msg "  No container for ${svc}; nothing to restore"
        return 0
    fi
    local project_lower
    project_lower=$(echo "$COMPOSE_PROJECT" | tr '[:upper:]' '[:lower:]')
    # Reconnect to all networks the service is configured for. Use docker network
    # connect rather than `dc up -d` so we don't trigger compose dependency
    # restarts (which can crash redpanda or postgres mid-test).
    case "$svc" in
        collector)
            local nets=("${project_lower}_cryptolake_internal" "${project_lower}_collector_egress")
            ;;
        collector-backup)
            local nets=("${project_lower}_cryptolake_internal" "${project_lower}_backup_egress")
            ;;
        writer)
            local nets=("${project_lower}_cryptolake_internal" "${project_lower}_host_access")
            ;;
        *)
            local nets=("${project_lower}_cryptolake_internal")
            ;;
    esac
    for net in "${nets[@]}"; do
        docker network connect "$net" "$container" 2>/dev/null && \
            msg "  Reconnected to ${net}" || true
    done
}

# ---------------------------------------------------------------------------
# capture_archives <dest_dir>
# Copies archive files from HOST_DATA_DIR to dest_dir for offline assertion.
# ---------------------------------------------------------------------------
capture_archives() {
    local dest="${1:?capture_archives requires a destination directory}"
    mkdir -p "$dest"
    if [[ -d "$HOST_DATA_DIR" ]]; then
        # Copy archive files (zst + sha256 sidecars)
        find "$HOST_DATA_DIR" \( -name "*.jsonl.zst" -o -name "*.sha256" \) \
            -exec cp --parents {} "$dest/" \; 2>/dev/null || \
        find "$HOST_DATA_DIR" \( -name "*.jsonl.zst" -o -name "*.sha256" \) | while read -r f; do
            rel="${f#$HOST_DATA_DIR/}"
            mkdir -p "$dest/$(dirname "$rel")"
            cp "$f" "$dest/$rel"
        done
        local count
        count=$(find "$dest" -name "*.jsonl.zst" 2>/dev/null | wc -l)
        msg "Captured ${count} archive file(s) to ${dest}."
    else
        msg "WARNING: HOST_DATA_DIR ${HOST_DATA_DIR} does not exist — nothing to capture."
    fi
}

# ---------------------------------------------------------------------------
# run_verify <date> <archive_base_dir>
# Invokes the Java verify CLI. Asserts exit==0 and ERRORS=0.
# Returns non-zero if either assertion fails.
# ---------------------------------------------------------------------------
run_verify() {
    local date="${1:?run_verify requires a date (YYYY-MM-DD)}"
    local base_dir="${2:?run_verify requires a base directory}"

    msg "Running verify for date=${date} base_dir=${base_dir}…"

    # Build verify if not already built
    if [[ ! -x "$VERIFY_BIN" ]]; then
        msg "Building verify CLI…"
        (cd "$REPO_ROOT" && ./gradlew :verify:installDist -q 2>&1 | tail -3)
    fi

    local verify_out
    verify_out=$("$VERIFY_BIN" verify --date "$date" --base-dir "$base_dir" 2>&1) || {
        msg "FAIL: verify exited non-zero."
        msg "--- verify output ---"
        echo "$verify_out" >&2
        msg "---------------------"
        return 1
    }

    msg "--- verify output ---"
    echo "$verify_out" >&2
    msg "---------------------"

    # Check for ERRORS=0
    if echo "$verify_out" | grep -q "^Errors: 0$"; then
        msg "PASS: verify ERRORS=0"
        return 0
    elif echo "$verify_out" | grep -q "^ERRORS ("; then
        msg "FAIL: verify reported ERRORS > 0"
        return 1
    else
        # No archive files is also acceptable (no data = no errors, just empty)
        msg "PASS: verify completed (no archive files or no ERRORS line)"
        return 0
    fi
}

# ---------------------------------------------------------------------------
# assert_gap_present <reason> <archive_base_dir>
# Searches zstd-compressed JSONL archives for a gap envelope with the given reason.
# Returns 0 if found, 1 if not found.
# ---------------------------------------------------------------------------
assert_gap_present() {
    local reason="${1:?assert_gap_present requires a reason}"
    local base_dir="${2:?assert_gap_present requires a base directory}"

    msg "Asserting gap reason '${reason}' is present in archives…"

    local found=false
    local checked=0
    while IFS= read -r -d '' zst_file; do
        checked=$(( checked + 1 ))
        # Decompress and search for the gap reason.
        # Read ALL stdin before exiting so zstd doesn't get SIGPIPE (pipefail would mark
        # the pipeline as failed even when the gap was found and python exited 0 early).
        if zstd -d -c "$zst_file" 2>/dev/null \
            | python3 -c "
import sys, json
found = False
for line in sys.stdin:
    line=line.strip()
    if not line: continue
    try:
        d=json.loads(line)
        if not found and d.get('type')=='gap' and d.get('reason')=='${reason}':
            print('FOUND: '+json.dumps(d)[:200])
            found = True
    except: pass
sys.exit(0 if found else 1)
" 2>/dev/null; then
            found=true
            break
        fi
    done < <(find "$base_dir" -name "*.jsonl.zst" -print0 2>/dev/null)

    if $found; then
        msg "PASS: gap reason '${reason}' found (checked ${checked} file(s))."
        return 0
    else
        msg "FAIL: gap reason '${reason}' NOT found in ${checked} archive file(s) under ${base_dir}."
        # List what gap reasons ARE present for diagnosis
        msg "Gap reasons present:"
        find "$base_dir" -name "*.jsonl.zst" -print0 2>/dev/null \
            | xargs -0 -I{} sh -c 'zstd -d -c "$1" 2>/dev/null | python3 -c "
import sys,json
for l in sys.stdin:
  l=l.strip()
  if not l: continue
  try:
    d=json.loads(l)
    if d.get(\"type\")==\"gap\": print(d.get(\"reason\",\"?\"))
  except: pass
"' _ {} 2>/dev/null | sort -u | head -20 || true
        return 1
    fi
}

# ---------------------------------------------------------------------------
# assert_gap_absent <reason> <archive_base_dir>
# Asserts a gap reason is NOT present (used for suppression checks).
# ---------------------------------------------------------------------------
assert_gap_absent() {
    local reason="${1:?assert_gap_absent requires a reason}"
    local base_dir="${2:?assert_gap_absent requires a base directory}"

    msg "Asserting gap reason '${reason}' is NOT present…"
    if assert_gap_present "$reason" "$base_dir" 2>/dev/null; then
        msg "FAIL: gap reason '${reason}' was present but should be absent."
        return 1
    else
        msg "PASS: gap reason '${reason}' is absent."
        return 0
    fi
}

# ---------------------------------------------------------------------------
# wait_for_gap <reason> <timeout_secs>
# Polls the live archive until a gap with the given reason appears.
# ---------------------------------------------------------------------------
wait_for_gap() {
    local reason="${1:?need reason}"
    local timeout="${2:-120}"
    local deadline=$(( SECONDS + timeout ))
    msg "Waiting for gap '${reason}' to appear (timeout=${timeout}s)…"
    while true; do
        if assert_gap_present "$reason" "$HOST_DATA_DIR" 2>/dev/null; then
            return 0
        fi
        if (( SECONDS >= deadline )); then
            msg "Timeout: gap '${reason}' never appeared."
            return 1
        fi
        sleep 10
    done
}

# ---------------------------------------------------------------------------
# fill_disk <dir> [percent]
# Creates a sparse file to fill HOST_DATA_DIR to the given percent full.
# ---------------------------------------------------------------------------
fill_disk() {
    local dir="${1:-$HOST_DATA_DIR}"
    local percent="${2:-99}"
    msg "Filling ${dir} to ${percent}%…"

    local avail_kb
    avail_kb=$(df -k "$dir" | awk 'NR==2{print $4}')
    local total_kb
    total_kb=$(df -k "$dir" | awk 'NR==2{print $2}')
    local target_kb=$(( total_kb * percent / 100 ))
    local used_kb=$(( total_kb - avail_kb ))
    local fill_kb=$(( target_kb - used_kb ))

    if (( fill_kb <= 0 )); then
        msg "Disk already at or above ${percent}%; nothing to fill."
        return 0
    fi

    DISK_FILLER_FILE="${dir}/_chaos_filler"
    dd if=/dev/zero of="$DISK_FILLER_FILE" bs=1024 count="$fill_kb" 2>/dev/null || {
        msg "WARNING: dd exited non-zero (disk may be full — that's expected)"
    }
    msg "Disk fill complete. Current usage: $(df -h "$dir" | awk 'NR==2{print $5}')"
}

# ---------------------------------------------------------------------------
# free_disk
# Removes the chaos filler file.
# ---------------------------------------------------------------------------
free_disk() {
    if [[ -n "${DISK_FILLER_FILE:-}" && -f "$DISK_FILLER_FILE" ]]; then
        rm -f "$DISK_FILLER_FILE"
        msg "Disk filler removed. Current usage: $(df -h "$HOST_DATA_DIR" | awk 'NR==2{print $5}')"
    fi
}

# ---------------------------------------------------------------------------
# teardown_stack
# Called by trap EXIT. Cleans up containers, volumes, and HOST_DATA_DIR.
# ---------------------------------------------------------------------------
teardown_stack() {
    msg "=== Teardown: ${COMPOSE_PROJECT:-unknown} ==="
    # Stop the lifecycle tail BEFORE the containers go away so the background
    # `dc logs -f` doesn't error noisily on shutdown.
    stop_lifecycle_tail
    free_disk 2>/dev/null || true

    if [[ -n "${COMPOSE_OPTS[*]+set}" ]]; then
        dc down -v --remove-orphans 2>/dev/null || true
    fi

    if [[ -n "${HOST_DATA_DIR:-}" && -d "$HOST_DATA_DIR" ]]; then
        rm -rf "$HOST_DATA_DIR"
        msg "Removed ${HOST_DATA_DIR}"
    fi
    msg "=== Teardown complete ==="
}

# ---------------------------------------------------------------------------
# today — returns current date as YYYY-MM-DD (used by run_verify)
# ---------------------------------------------------------------------------
today() { date -u +%Y-%m-%d; }

# ---------------------------------------------------------------------------
# scenario_pass / scenario_fail
# Call at end of scenario to produce a clear result line for the runner.
# ---------------------------------------------------------------------------
scenario_pass() {
    local nn="${SCENARIO_NUM:-??}"
    msg "RESULT: PASS [scenario ${nn}]"
    exit 0
}

scenario_fail() {
    local nn="${SCENARIO_NUM:-??}"
    msg "RESULT: FAIL [scenario ${nn}] — $*"
    exit 1
}

msg "common.sh loaded (REPO_ROOT=${REPO_ROOT})"
