#!/usr/bin/env bash
# 05_depth_reconnect_inflight.sh
#
# Scenario: main_depth_resync_inflight
# Chaos:    Drop MAIN's egress (depth WS dies, REST snapshot blocked) for 45s
# Expected: only `snapshot_poll_miss` on depth_snapshot may slip through;
#           the live depth diff stream (continuous WS) is fully covered
#           by BACKUP, so no other gap reason is permitted.
# Flow:     MAIN's depth WS broken → MAIN stops publishing → BACKUP keeps
#           publishing diffs → egress restored → MAIN reconnects, fetches
#           a fresh depth snapshot, resumes. The snapshot resync RESETS
#           the pu-chain so no pu_chain_break is reported, and MAIN's
#           session_id is unchanged so no WITHIN_SOURCE_SESSION_CHANGE
#           fires either.
# Why:      depth_snapshot is polled every 30s (btcusdt override) but the
#           gap-filter grace window is 10s, so when MAIN misses a
#           snapshot poll during the egress block, BACKUP may not have
#           produced a snapshot within the 10s grace and the gap is
#           legitimately recorded with reason `snapshot_poll_miss`. This
#           is the same poll-cadence-vs-grace-window fact that test 01
#           handles for open_interest. Live depth/bookticker diffs ARE
#           fully covered by backup and must show no gap envelopes.

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

# Assertions — primary's WS reconnect + snapshot resync is transparent for
# live diffs; the only legitimate gap reason is `snapshot_poll_miss` on the
# 30s-polled depth_snapshot stream.
expect_lifecycle_event       "redundancy active before chaos"   "COVERAGE_FILTER_ACTIVATED"
expect_only_these_gaps_check "snapshot_poll_miss"

verdict
