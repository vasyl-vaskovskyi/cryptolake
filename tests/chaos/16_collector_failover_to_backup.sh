#!/usr/bin/env bash
# 16_collector_failover_to_backup.sh
#
# Scenario: main_failover_to_backup
# Chaos:    SIGKILL MAIN; observe writer's failover to BACKUP; restart MAIN
# Expected: NO gap (redundancy worked)
# Flow:     MAIN healthy + BACKUP healthy (writer prefers MAIN) →
#           MAIN killed → writer's failover controller detects MAIN's
#           silence → writer switches active source to BACKUP and starts
#           archiving BACKUP's records → MAIN restarts and resumes →
#           writer's controller observes MAIN's data flowing again →
#           writer switches back to MAIN. Test asserts both directions
#           of the handoff occurred.
# Why:      This scenario exists specifically to validate the failover
#           mechanism end-to-end. By definition the redundancy worked,
#           so under the TWO-COLLECTOR rule no gap is emitted.

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

# Assertions — failover scenario; BACKUP must cover.
expect_lifecycle_event        "writer detects MAIN failure"      "MAIN_FAILURE_DETECTED"
expect_lifecycle_event        "writer fails over to BACKUP"      "WRITER_NOW_ARCHIVING_FROM=BACKUP"
expect_lifecycle_event        "MAIN comes back online"           "MAIN_RECOVERED"
expect_lifecycle_event        "writer switches back to MAIN"     "WRITER_NOW_ARCHIVING_FROM=MAIN"
expect_lifecycle_event_absent "no uncovered gap accepted"        "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

# Plus: backup actually wrote archive files during the outage.
if (( ARCHIVE_COUNT_BEFORE > 0 )); then
    msg "PASS: archive data present during primary outage (backup kept flowing)"
else
    msg "WARNING: no archive data during primary outage — backup may not have been active"
fi

verdict
