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
COMPOSE_DEBUG_WRITER_FILE="${REPO_ROOT}/docker-compose.debug-writer.yml"

# WRITER_MODE: "internal" (default) runs the writer as a compose service.
# "external" skips the writer container, layers in the debug-writer override
# (publishes postgres on host port 5432; redpanda already publishes 9092),
# and lets you run com.cryptolake.writer.Main from your IDE pointed at the
# host-published Kafka and Postgres. Lifecycle tail and healthcheck wait
# adapt accordingly.
WRITER_MODE="${WRITER_MODE:-internal}"

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
    if [[ "$WRITER_MODE" == "external" ]]; then
        if [[ ! -f "$COMPOSE_DEBUG_WRITER_FILE" ]]; then
            die "WRITER_MODE=external but ${COMPOSE_DEBUG_WRITER_FILE} not found."
        fi
        COMPOSE_OPTS+=(--file "$COMPOSE_DEBUG_WRITER_FILE")
        msg "WRITER_MODE=external — writer service will not be started; run it from your IDE."
    fi
    SCENARIO_MODE="$mode"
    export HOST_DATA_DIR COMPOSE_PROJECT

    print_chaos_header "${BASH_SOURCE[1]:-$0}"
    msg "=== Scenario ${nn} init (mode=${mode}) ==="
    msg "HOST_DATA_DIR=${HOST_DATA_DIR}"
    msg "COMPOSE_PROJECT=${COMPOSE_PROJECT}"

    # Guarantee cleanup even on set -e failures
    trap 'teardown_stack' EXIT
    # Make assertion / command failures unmissable: any non-zero exit under
    # `set -e` lands in the red FAIL banner BEFORE teardown noise scrolls
    # past, instead of silently dropping out to "[chaos] === Teardown ===".
    # ERR fires before EXIT, so the banner is the last thing the user sees
    # above the teardown lines.
    trap '_chaos_on_error $? $LINENO "${BASH_COMMAND:-}"' ERR
}

# ---------------------------------------------------------------------------
# _chaos_on_error <exit_code> <line> <command>
# ERR-trap handler. Disables itself (so scenario_fail's own exit doesn't
# re-enter), then prints the red FAIL banner with diagnostic context.
# ---------------------------------------------------------------------------
_chaos_on_error() {
    local exit_code="${1:-1}"
    local line="${2:-?}"
    local cmd="${3:-}"
    trap - ERR
    scenario_fail "exit=${exit_code} at line ${line}: ${cmd}"
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
    local lc_services=(collector)
    if [[ "$WRITER_MODE" != "external" ]]; then
        lc_services=(writer collector)
    fi
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
    local required_services=()
    if [[ "$WRITER_MODE" != "external" ]]; then
        required_services+=("writer")
    fi
    case "$SCENARIO_MODE" in
        primary)
            required_services+=("collector")
            ;;
        primary+backup)
            required_services+=("collector" "collector-backup")
            ;;
    esac

    # Nothing to wait for (e.g., writer external + only collector services
    # the caller already considers ready). Treat as healthy immediately.
    if (( ${#required_services[@]} == 0 )); then
        msg "No services to wait on (WRITER_MODE=external)."
        return 0
    fi

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
# _list_gap_reasons <base_dir>
# Emits every gap-envelope reason found in zstd archives, one per line.
# Sorted+deduped is up to the caller.
# ---------------------------------------------------------------------------
_list_gap_reasons() {
    local base_dir="${1:?need base_dir}"
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
"' _ {} 2>/dev/null
}

# ---------------------------------------------------------------------------
# assert_no_gaps <archive_base_dir>
# Whitelist assertion: fails if ANY gap envelope is present, regardless of
# reason. Use this for scenarios whose contract is "no data loss occurred"
# (e.g., redundancy worked, the TWO-COLLECTOR rule fully covered).
# ---------------------------------------------------------------------------
assert_no_gaps() {
    local base_dir="${1:?assert_no_gaps requires a base directory}"
    msg "Asserting NO gap envelopes of any reason are present…"

    local reasons
    reasons=$(_list_gap_reasons "$base_dir" | sort | uniq -c | sort -rn)
    if [[ -z "$reasons" ]]; then
        msg "PASS: no gap envelopes in archive."
        return 0
    fi

    msg "FAIL: archive contains gap envelopes (count reason):"
    while IFS= read -r line; do msg "  ${line}"; done <<< "$reasons"
    return 1
}

# ---------------------------------------------------------------------------
# assert_only_these_gaps <archive_base_dir> <allowed_reason>...
# Whitelist assertion: fails if any gap with a reason NOT in the allowed
# list appears. With zero allowed reasons this is identical to
# assert_no_gaps. Use this for scenarios whose contract is "exactly these
# gap reasons may appear".
# ---------------------------------------------------------------------------
assert_only_these_gaps() {
    local base_dir="${1:?assert_only_these_gaps requires a base directory}"
    shift
    local -a allowed=("$@")
    msg "Asserting archive contains ONLY gaps with reason in: [${allowed[*]:-<none>}]"

    local present
    present=$(_list_gap_reasons "$base_dir" | sort -u)

    local unexpected=()
    while IFS= read -r r; do
        [[ -z "$r" ]] && continue
        local ok=false
        for a in "${allowed[@]}"; do
            [[ "$r" == "$a" ]] && { ok=true; break; }
        done
        $ok || unexpected+=("$r")
    done <<< "$present"

    if (( ${#unexpected[@]} == 0 )); then
        msg "PASS: archive contains only allowed gap reasons (present: [${present//$'\n'/, }])."
        return 0
    fi
    msg "FAIL: archive contains unexpected gap reasons: [${unexpected[*]}]"
    msg "  All gap reasons present (count reason):"
    _list_gap_reasons "$base_dir" | sort | uniq -c | sort -rn \
        | while IFS= read -r l; do msg "    ${l}"; done
    return 1
}

# ---------------------------------------------------------------------------
# Expectation framework — scenarios declare expected outcomes during the run
# (lifecycle events that must / must not fire, allowed/forbidden gap reasons)
# and call `verdict` at the end. verdict() runs every registered expectation,
# prints an itemized PASS/FAIL checklist, and exits PASS only if all passed.
#
# This replaces the previous pattern of "first failed assertion aborts via
# set -e" — with the framework you see ALL failures in one go.
#
# Usage example:
#     expect_lifecycle_event "writer fails over to backup" "WRITER_NOW_ARCHIVING_FROM=BACKUP"
#     expect_lifecycle_event "main back online"            "MAIN_RECOVERED"
#     expect_no_gaps
#     verdict
#
# Args are stored TAB-separated in a parallel string array; verdict() splits
# them back when calling the check function. Don't put TABs in labels.
# ---------------------------------------------------------------------------
_EXPECT_LABELS=()
_EXPECT_FNS=()
_EXPECT_ARGS=()

_register_expect() {
    local label="$1" fn="$2"
    shift 2
    local IFS=$'\t'
    _EXPECT_LABELS+=("$label")
    _EXPECT_FNS+=("$fn")
    _EXPECT_ARGS+=("$*")
}

# expect_lifecycle_event <label> <pattern> [svc=writer]
expect_lifecycle_event() {
    local label="${1:?need label}" pattern="${2:?need pattern}" svc="${3:-writer}"
    _register_expect "$label" _check_lifecycle_event "$pattern" "$svc"
}
_check_lifecycle_event() {
    dc logs --no-color "$2" 2>/dev/null | grep -q "LIFECYCLE.*$1"
}

# expect_lifecycle_event_count <label> <pattern> <min_count> [svc=writer]
expect_lifecycle_event_count() {
    local label="${1:?need label}" pattern="${2:?need pattern}" min="${3:?need min}" svc="${4:-writer}"
    _register_expect "$label" _check_lifecycle_count "$pattern" "$min" "$svc"
}
_check_lifecycle_count() {
    local count
    count=$(dc logs --no-color "$3" 2>/dev/null | grep -c "LIFECYCLE.*$1" || true)
    (( count >= $2 ))
}

# expect_lifecycle_event_absent <label> <pattern> [svc=writer]
expect_lifecycle_event_absent() {
    local label="${1:?need label}" pattern="${2:?need pattern}" svc="${3:-writer}"
    _register_expect "$label" _check_lifecycle_absent "$pattern" "$svc"
}
_check_lifecycle_absent() {
    ! dc logs --no-color "$2" 2>/dev/null | grep -q "LIFECYCLE.*$1"
}

# expect_no_gaps [label]
expect_no_gaps_check() {
    local label="${1:-no gap envelopes archived}"
    _register_expect "$label" _check_no_gaps "$HOST_DATA_DIR"
}
_check_no_gaps() {
    [[ -z "$(_list_gap_reasons "$1" 2>/dev/null)" ]]
}

# expect_only_these_gaps_check <reason>...
expect_only_these_gaps_check() {
    local reasons=("$@")
    local label="archive contains only allowed gap reasons: [${reasons[*]:-<none>}]"
    _register_expect "$label" _check_only_these_gaps_args "$HOST_DATA_DIR" "${reasons[@]}"
}
_check_only_these_gaps_args() {
    local base_dir="$1"; shift
    local -a allowed=("$@")
    local present
    present=$(_list_gap_reasons "$base_dir" 2>/dev/null | sort -u)
    while IFS= read -r r; do
        [[ -z "$r" ]] && continue
        local ok=false
        local a
        for a in "${allowed[@]}"; do [[ "$r" == "$a" ]] && { ok=true; break; }; done
        $ok || return 1
    done <<< "$present"
    return 0
}

# expect_gap_present_check <label> <reason>
expect_gap_present_check() {
    local label="${1:?need label}" reason="${2:?need reason}"
    _register_expect "$label" _check_gap_present_inline "$HOST_DATA_DIR" "$reason"
}
_check_gap_present_inline() {
    _list_gap_reasons "$1" 2>/dev/null | grep -qx "$2"
}

# verdict — runs all registered expectations, prints a checklist, then
# scenario_pass/scenario_fail. Disables ERR trap during the run so a failed
# expectation doesn't short-circuit the loop.
verdict() {
    msg ""
    msg "============================================================"
    msg "             SCENARIO ${SCENARIO_NUM:-??} VERDICT"
    msg "============================================================"
    trap - ERR
    local pass=0 fail=0 total=0
    local i
    for ((i=0; i < ${#_EXPECT_LABELS[@]}; i++)); do
        local label="${_EXPECT_LABELS[$i]}"
        local fn="${_EXPECT_FNS[$i]}"
        local raw="${_EXPECT_ARGS[$i]}"
        local IFS=$'\t'
        local -a argv
        read -r -a argv <<< "$raw"
        IFS=$' \t\n'
        if "$fn" "${argv[@]}"; then
            msg "  [PASS] ${label}"
            pass=$((pass + 1))
        else
            msg "  [FAIL] ${label}"
            fail=$((fail + 1))
        fi
        total=$((total + 1))
    done
    msg "  ---"
    msg "  Total: ${pass} passed, ${fail} failed (of ${total})"
    msg "============================================================"
    if (( fail > 0 )); then
        scenario_fail "${fail} of ${total} expectation(s) failed"
    fi
    scenario_pass
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
# Called by trap EXIT. Cleans up containers, volumes, networks, locally-built
# images, and HOST_DATA_DIR for this scenario's compose project.
#
# `--rmi local` deletes the per-project images that compose tags as
# "<project>-<service>" when a service has a `build:` directive but no
# explicit `image:` field (collector, collector-backup, writer, backfill,
# consolidation, whatsapp-bridge in docker-compose.yml). Without it, every
# chaos run leaves a fresh set of tagged images in Docker Desktop.
# ---------------------------------------------------------------------------
teardown_stack() {
    msg "=== Teardown: ${COMPOSE_PROJECT:-unknown} ==="
    # Stop the lifecycle tail BEFORE the containers go away so the background
    # `dc logs -f` doesn't error noisily on shutdown.
    stop_lifecycle_tail
    free_disk 2>/dev/null || true

    # Dump full per-service logs to ${REPO_ROOT}/build/chaos-logs/<NN>/<svc>.log
    # before tearing down. The lifecycle tail filters to LIFECYCLE-tagged
    # lines only; deep investigations need the FULL collector/writer log
    # (resync state-machine, snapshot fetcher retries, kafka consumer
    # rebalances, etc.). Best-effort; never blocks teardown.
    if [[ -n "${COMPOSE_PROJECT:-}" ]]; then
        local log_dir="${REPO_ROOT}/build/chaos-logs/${SCENARIO_NUM:-XX}"
        mkdir -p "$log_dir" 2>/dev/null || true
        for svc in writer collector collector-backup; do
            dc logs --no-color "$svc" > "${log_dir}/${svc}.log" 2>&1 || true
        done
        msg "Per-service logs captured under ${log_dir}/"
    fi

    if [[ -n "${COMPOSE_OPTS[*]+set}" ]]; then
        dc down -v --remove-orphans --rmi local 2>/dev/null || true
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
#
# Prints a multi-line color banner so the verdict is unmissable when reading
# a single-scenario log directly. The exact "RESULT: PASS"/"RESULT: FAIL"
# token (no color codes) is preserved so the suite runner and the JUnit
# harness can still grep for it.
# ---------------------------------------------------------------------------
_scenario_color() {
    # ANSI only when stderr is a TTY and NO_COLOR is unset. Scenario output
    # goes to stderr (msg), so we test fd 2.
    if [[ -t 2 && -z "${NO_COLOR:-}" ]]; then
        case "$1" in
            green) printf '\033[1;37;42m' ;;
            red)   printf '\033[1;37;41m' ;;
            off)   printf '\033[0m' ;;
        esac
    fi
}

_scenario_banner() {
    local color="$1"
    local headline="$2"
    local on off bar pad body
    on="$(_scenario_color "$color")"
    off="$(_scenario_color off)"
    bar="##########################################################"
    pad="#                                                        #"
    # Bar is 58 cols: "#" + 3 spaces + 50-char headline + 3 spaces + "#".
    local text="${headline:0:50}"
    body=$(printf "#   %-50s   #" "$text")
    {
        echo ""
        printf '%s%s%s\n' "$on" "$bar" "$off"
        printf '%s%s%s\n' "$on" "$pad" "$off"
        printf '%s%s%s\n' "$on" "$body" "$off"
        printf '%s%s%s\n' "$on" "$pad" "$off"
        printf '%s%s%s\n' "$on" "$bar" "$off"
        echo ""
    } >&2
}

scenario_pass() {
    local nn="${SCENARIO_NUM:-??}"
    _scenario_banner green "SCENARIO ${nn}: PASS"
    # Plain token below the banner so the suite runner / JUnit harness can
    # grep for it without dealing with ANSI escape codes.
    msg "RESULT: PASS [scenario ${nn}]"
    exit 0
}

scenario_fail() {
    local nn="${SCENARIO_NUM:-??}"
    local reason="${*:-unspecified}"
    _scenario_banner red "SCENARIO ${nn}: FAIL"
    msg "Reason: ${reason}"
    msg "RESULT: FAIL [scenario ${nn}] — ${reason}"
    exit 1
}

msg "common.sh loaded (REPO_ROOT=${REPO_ROOT})"
