#!/usr/bin/env bash
# 08_ws_disconnect.sh
#
# Scenario: main_ws_disconnect
# Chaos:    iptables-block MAIN's egress to fstream.binance.com
# Expected: NO gap (redundancy worked)
# Flow:     MAIN's WS to Binance severed → MAIN's stream goes silent →
#           BACKUP's WS still up, BACKUP keeps delivering → writer archives
#           BACKUP throughout → MAIN's egress restored, MAIN reconnects
#           and resumes → writer switches back to MAIN.
# Why:      Only MAIN's upstream link broke. BACKUP fed the writer the
#           whole time; no window had zero sources. No gap under the
#           TWO-COLLECTOR rule.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "08" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Blocking primary collector egress (WS disconnect simulation) ==="
block_egress_via_network "collector"

# Hold for 45s (> 30s ping timeout)
msg "Holding egress block for 45s…"
sleep 45

msg "Restoring egress…"
restore_egress_via_network "collector"

# Wait for reconnect + gap emission
msg "Waiting 90s for ws_disconnect gap to appear…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — only MAIN's upstream broke; BACKUP covered.
expect_lifecycle_event "MAIN ws disconnect observed"        "COLLECTOR_UPSTREAM_WS_DISCONNECTED" collector
expect_lifecycle_event "MAIN ws reconnected"               "COLLECTOR_UPSTREAM_WS_CONNECTED"    collector
expect_lifecycle_event "writer detects MAIN failure"       "MAIN_FAILURE_DETECTED"
expect_lifecycle_event "writer fails over to BACKUP"       "WRITER_NOW_ARCHIVING_FROM=BACKUP"
expect_lifecycle_event "MAIN comes back online"            "MAIN_RECOVERED"
expect_lifecycle_event "writer switches back to MAIN"      "WRITER_NOW_ARCHIVING_FROM=MAIN"
expect_lifecycle_event_absent "no uncovered gap accepted"  "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
