#!/usr/bin/env bash
# 05_depth_reconnect_inflight.sh
#
# Scenario: main_depth_resync_inflight
# Chaos:    Drop MAIN's depth WS mid-flow; MAIN recovers via REST snapshot
# Expected: NO gap (redundancy worked)
# Flow:     MAIN's depth WS broken → MAIN buffers diffs internally and pulls
#           a fresh snapshot to resync → BACKUP's depth WS unaffected,
#           BACKUP delivers diffs continuously → writer archives BACKUP's
#           depth diffs through the gap → MAIN finishes resync and resumes
#           delivery → CrossSourcePuChainValidator confirms u-chain continuity.
# Why:      Only MAIN's depth stream broke. BACKUP's depth pu-chain bridged
#           the missing diffs, so the merged stream had no joint hole.
#           No gap under the TWO-COLLECTOR rule.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "05" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "depth" 60

msg "=== CHAOS: Disconnecting primary collector egress (WS drop) ==="
block_egress_via_network "collector"

# Hold for 45s — enough for ping timeout (30s) to fire
msg "Holding egress block for 45s…"
sleep 45

msg "Restoring primary egress…"
restore_egress_via_network "collector"

# Give collector time to reconnect, re-subscribe, fetch snapshot, and resync
msg "Waiting 90s for reconnect and resync…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — only MAIN's depth was disturbed; BACKUP covered.
expect_lifecycle_event        "redundancy active before chaos"   "COVERAGE_FILTER_ACTIVATED"
expect_lifecycle_event        "depth gap parked under coverage"  "GAP_PARKED"
expect_lifecycle_event        "parked gap suppressed by backup"  "GAP_SUPPRESSED_BY_COVERAGE"
expect_lifecycle_event_absent "no uncovered gap accepted"        "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
