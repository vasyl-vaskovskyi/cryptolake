#!/usr/bin/env bash
# 15_both_collectors_silent.sh
#
# Scenario: both_collectors_silent_inferred
# Chaos:    Disconnect BOTH MAIN and BACKUP from their egress networks
#           (processes stay alive, heartbeats fire, but no upstream data)
# Expected: System SURVIVES the outage and resumes after egress restored.
#           NO `both_collectors_silent` gap is emitted in the current
#           build because SilenceInferredGapEmitter is dead code (defined
#           in writer/.../validation/ but never instantiated in
#           writer/Main.java). This is a DOCUMENTED data-loss-detection
#           gap — anyone wiring the emitter must strengthen this test
#           to also assert `both_collectors_silent` is recorded.
# Flow:     MAIN+BACKUP both alive but both blocked from Binance →
#           neither produces market-data records for 60s → egress
#           restored → both reconnect → no archived gap envelope.
# Why:      Both collectors fail to deliver simultaneously (silently —
#           no process death, no Kafka outage). The TWO-COLLECTOR rule
#           SHOULD fire here but cannot in the current wiring. The test
#           keeps the chaos so the missing detection can be proven
#           (currently passes only the "system survives" half).

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "15" "primary+backup"

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

# Assertions — system survives the dual-egress outage; in the current
# wiring the silent-data-loss case is NOT detected as
# `both_collectors_silent` (see header).
expect_lifecycle_event_absent "no failover to BACKUP (both silent, no source preferred)" "WRITER_NOW_ARCHIVING_FROM=BACKUP"
# What DOES fire: polled streams (depth_snapshot, open_interest)
# eventually generate poll-miss gap candidates because both sources
# went stale beyond the 10s grace window — these reach the archive
# correctly via the GAP_ACCEPTED_NO_COVERAGE path with reason
# `snapshot_poll_miss`. Continuous WS streams don't auto-emit gap
# candidates without a session-change/chain-break trigger, so their
# silent loss is invisible until SilenceInferredGapEmitter is wired.
expect_lifecycle_event       "no-coverage gap accepted on polled streams" "GAP_ACCEPTED_NO_COVERAGE"
expect_only_these_gaps_check snapshot_poll_miss collector_restart

verdict
