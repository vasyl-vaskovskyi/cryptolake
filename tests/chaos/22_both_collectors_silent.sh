#!/usr/bin/env bash
# 22_both_collectors_silent.sh
#
# Scenario: both_collectors_silent_inferred
# Chaos:    iptables-block fstream.binance.com for BOTH MAIN and BACKUP,
#           leaving heartbeats firing (both processes look "alive" but
#           are receiving no upstream data)
# Expected: gap reason=both_collectors_silent (real loss)
# Flow:     MAIN+BACKUP both alive (heartbeats OK) but both are blocked
#           from Binance → neither produces any market-data records →
#           SilenceInferredGapEmitter sees both sources stale (no records
#           for >threshold), confirms via heartbeat+lifecycle that they
#           are alive-but-silent → emits a both_collectors_silent gap.
# Why:      Both collectors fail to deliver simultaneously (silently —
#           they didn't crash, they just have no upstream). This is the
#           inferred-from-silence variant of the TWO-COLLECTOR rule's
#           "BOTH fail" case. Real loss; gap is correct.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "22" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s — both collectors flowing…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Blocking BOTH collector egress networks ==="
block_egress_via_network "collector"
block_egress_via_network "collector-backup"

# SilenceInferredGapEmitter fires when both sources have data >30s AND hb >15s.
# Hold for 60s to be safely beyond both thresholds.
msg "Holding both egress blocks for 60s…"
sleep 60

msg "Restoring both egress networks…"
restore_egress_via_network "collector"
restore_egress_via_network "collector-backup"

msg "Waiting 90s for both_collectors_silent gap to be emitted…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — both collectors went silent at once; expect inferred gap, no failover.
expect_lifecycle_event        "BOTH collectors silent observed"  "BOTH_COLLECTORS_SILENT"
expect_lifecycle_event        "BOTH collectors recovered"        "BOTH_COLLECTORS_RECOVERED"
expect_lifecycle_event        "gap was archived"                 "GAP_ARCHIVED"
expect_lifecycle_event_absent "no failover to BACKUP"            "WRITER_NOW_ARCHIVING_FROM=BACKUP"
expect_gap_present_check      "both_collectors_silent recorded"  "both_collectors_silent"
expect_only_these_gaps_check  both_collectors_silent

verdict
