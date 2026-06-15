#!/usr/bin/env bash
# tests/chaos/01_collector_active_crash.sh
# Scenario 01 — master spec §14.e: Active collector SIGKILL mid-stream. systemd would
# restart it in production; here we restart it manually. The pipeline must still produce a
# sealed archive that passes cryptopanner-verify with ERRORS=0 and reports the §10.d
# manifest annotations expected for ID-bearing streams.
#
# Asserts:
#   1) verify exits 0 (ERRORS=0)
#   2) trade manifest contains at least one backfill_outcome=FILLED entry, proving the
#      Sealer's gap-detect + REST-backfill path ran end-to-end on the post-restart archive.
set -euo pipefail

SCENARIO_NUM="01"
SCENARIO_NAME="collector_active_crash"
# shellcheck source=_common.sh
source "$(dirname "$0")/_common.sh"

chaos::bootstrap_stack

# Stage 1: run Collector for ~10s, then SIGKILL it. The mock keeps replaying frames during
# the kill window — those go to /dev/null, simulating data loss during the outage.
chaos::log "stage 1: collector run for 10s, then SIGKILL"
chaos::collector_start_bg "${CHAOS_DATA_DIR}.collector-1.log"
sleep 10
chaos::log "SIGKILL collector pid=$CHAOS_COLLECTOR_PID"
kill -9 "$CHAOS_COLLECTOR_PID" 2>/dev/null || true
wait "$CHAOS_COLLECTOR_PID" 2>/dev/null || true

# Brief outage window so the mock keeps emitting into the void.
sleep 5

# Stage 2: restart the collector. It self-exits after collector_max_runtime_s (30s).
chaos::log "stage 2: restarting collector (self-exits in ~30s)"
chaos::collector_start_bg "${CHAOS_DATA_DIR}.collector-2.log"
wait "$CHAOS_COLLECTOR_PID" 2>/dev/null || true

# Stage 3: seal + assert + upload + verify.
chaos::discover_date_hour
chaos::run_sealer

TRADE_MANIFEST="${CHAOS_DATA_DIR}/sealed/btcusdt/trade/${CHAOS_DATE}/hour-${CHAOS_HOUR}.manifest.json"
chaos::log "asserting backfill ran in $TRADE_MANIFEST"
chaos::assert_manifest_kv "$TRADE_MANIFEST" backfill_outcome FILLED
chaos::assert_manifest_kv "$TRADE_MANIFEST" endpoint "/fapi/v1/historicalTrades"

chaos::run_uploader
chaos::run_verify

chaos::pass
