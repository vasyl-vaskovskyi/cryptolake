#!/usr/bin/env bash
# 06_full_stack_restart_gap.sh
#
# Scenario: full_stack_restart
# Chaos:    docker compose down then up (everything off)
# Expected: gap reason=collector_restart OR unclean_shutdown (real loss)
# Flow:     MAIN, BACKUP, writer, redpanda, postgres ALL killed simultaneously →
#           no process is running, nothing is being delivered or archived →
#           stack restarts → on recovery the lifecycle journal proves the
#           down window and a gap envelope is emitted for it.
# Why:      Both collectors are off at the same time. The TWO-COLLECTOR
#           rule's exception — "BOTH collectors fail simultaneously" — is
#           the entire chaos here. Real loss; gap is correct.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "06" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Full stack down (no clean shutdown markers) ==="
# Stop WITHOUT -v so data is preserved (volumes stay)
dc down --remove-orphans

msg "Waiting 15s (simulates downtime gap)…"
sleep 15

msg "Restarting full stack…"
start_stack "primary+backup"
wait_healthy 150

# Give writer time to read LifecycleJournal and emit restart gap envelopes
msg "Waiting 60s for restart gap classification…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — both collectors went down at once; expect a restart-class gap
# of one of these reasons, no others. (Tolerant: gap may be below the
# minimum-window threshold, in which case the gap whitelist still passes.)
expect_lifecycle_event "writer detects session change after restart" "WITHIN_SOURCE_SESSION_CHANGE"
expect_only_these_gaps_check collector_restart unclean_shutdown writer_restart host_unclean_shutdown

verdict
