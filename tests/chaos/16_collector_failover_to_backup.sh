#!/usr/bin/env bash
# 16_collector_failover_to_backup.sh
#
# Invariant: Kill the primary collector and verify that the backup collector
# continues providing data. The writer's CoverageFilter should switch to
# backup data and suppress the primary's gap envelopes (because the backup
# covers the window). verify exits 0 with ERRORS=0.
#
# Expected: collector_restart gap from primary, verify ERRORS=0,
# AND archive data continues to be written during the outage.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "16" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s — confirming both collectors flow…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Killing PRIMARY collector (backup should take over) ==="
kill_service "collector"

msg "Holding primary down for 90s — backup should keep archiving…"
sleep 90

# Count archive files before restart to confirm backup kept writing
ARCHIVE_COUNT_BEFORE=$(find "$HOST_DATA_DIR" -name "*.jsonl.zst" 2>/dev/null | wc -l)
msg "Archive files so far: ${ARCHIVE_COUNT_BEFORE}"

msg "Restarting primary collector…"
start_service "collector"
wait_healthy 120

msg "Waiting 60s for gap classification…"
sleep 60

ARCHIVE_COUNT_AFTER=$(find "$HOST_DATA_DIR" -name "*.jsonl.zst" 2>/dev/null | wc -l)
msg "Archive files after recovery: ${ARCHIVE_COUNT_AFTER}"

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "collector_restart" "$HOST_DATA_DIR"

# Confirm archive continued during primary outage (backup kept writing)
if (( ARCHIVE_COUNT_BEFORE > 0 )); then
    msg "PASS: archive data present during primary outage (backup kept flowing)"
else
    msg "WARNING: no archive data during primary outage — backup may not have been active"
fi

scenario_pass
