#!/usr/bin/env bash
# Live-tail the chaos scenario currently running under
# scripts/run-all-tests.sh / scripts/run-chaos-tests.sh.
#
# Each scenario writes to build/chaos-logs/<NN_name>.log while it runs (the
# runner uses `bash <scenario> > log 2>&1`, so the log grows in real time).
# This script polls that directory and follows whichever log was most
# recently modified, automatically switching when the next scenario starts.
#
# Usage:
#   bash scripts/watch-chaos.sh              # follow the current scenario, switch as the suite advances
#   bash scripts/watch-chaos.sh 09           # follow scenario 09 only (waits for it if not started yet)
#   bash scripts/watch-chaos.sh 09_writer_kill_during_flush  # exact basename also works
#
# Run this in a second terminal alongside `bash scripts/run-all-tests.sh`.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHAOS_DIR="${REPO_ROOT}/build/chaos-logs"
FILTER="${1:-}"

mkdir -p "$CHAOS_DIR"

# Resolve a filter argument ("09" or "09_writer_kill_during_flush") to a glob
# under build/chaos-logs. Returns the matched .log path on stdout, or empty.
resolve_filter_log() {
    local f="$1"
    [[ -z "$f" ]] && return 0
    local matches=( "${CHAOS_DIR}/${f}"*.log )
    [[ -e "${matches[0]}" ]] && { echo "${matches[0]}"; return 0; }
    # Zero-pad single-digit ("9" -> "09")
    local padded
    padded=$(printf '%02d' "$f" 2>/dev/null || true)
    if [[ -n "$padded" && "$padded" != "$f" ]]; then
        matches=( "${CHAOS_DIR}/${padded}"*.log )
        [[ -e "${matches[0]}" ]] && { echo "${matches[0]}"; return 0; }
    fi
    return 0
}

# Find the most-recently-modified .log in build/chaos-logs. Empty if none.
latest_log() {
    # Sort by mtime (newest first), pick first. BSD `stat` on macOS uses -f.
    local newest="" newest_mtime=0
    shopt -s nullglob
    for f in "$CHAOS_DIR"/*.log; do
        local m
        if m=$(stat -f %m "$f" 2>/dev/null); then :; else m=$(stat -c %Y "$f" 2>/dev/null || echo 0); fi
        if (( m > newest_mtime )); then
            newest_mtime=$m
            newest=$f
        fi
    done
    shopt -u nullglob
    echo "$newest"
}

# ---------------------------------------------------------------------------
# Single-scenario mode: tail one log and exit when it stops growing.
# ---------------------------------------------------------------------------
if [[ -n "$FILTER" ]]; then
    target=""
    echo "[watch-chaos] Waiting for scenario matching '${FILTER}' to start…"
    while :; do
        target=$(resolve_filter_log "$FILTER")
        [[ -n "$target" ]] && break
        sleep 1
    done
    echo "[watch-chaos] Following ${target}  (Ctrl-C to stop)"
    echo "------------------------------------------------------------"
    exec tail -n +1 -F "$target"
fi

# ---------------------------------------------------------------------------
# Suite mode: follow whichever scenario is currently running. When a newer
# log appears (next scenario started), kill the current tail and switch to
# it. Exits when the user hits Ctrl-C.
# ---------------------------------------------------------------------------
echo "[watch-chaos] Auto-following the active chaos scenario."
echo "[watch-chaos] Switches to the next log as the suite advances. Ctrl-C to stop."
echo

current=""
tail_pid=""

cleanup() {
    [[ -n "$tail_pid" ]] && kill "$tail_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

while :; do
    newest=$(latest_log)
    if [[ -n "$newest" && "$newest" != "$current" ]]; then
        # Switch.
        if [[ -n "$tail_pid" ]]; then
            kill "$tail_pid" 2>/dev/null || true
            wait "$tail_pid" 2>/dev/null || true
        fi
        current=$newest
        echo
        echo "============================================================"
        echo "[watch-chaos] >>> $(basename "$current")"
        echo "============================================================"
        tail -n +1 -F "$current" &
        tail_pid=$!
    fi
    sleep 1
done
