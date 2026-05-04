#!/usr/bin/env bash
# 07_host_reboot_restart_gap.sh
#
# Scenario: host_reboot
# Chaos:    Simulate host reboot — inject new host_boot_id into LifecycleJournal;
#           restart full stack
# Expected: gap reason=host_reboot OR host_unclean_shutdown (real loss)
# Flow:     Host "reboots" → MAIN, BACKUP, writer, all infra processes off →
#           stack comes back with a new host_boot_id → LifecycleJournal
#           reconciliation detects the reboot transition and emits a gap
#           envelope for the down window.
# Why:      Host loss takes both collectors and the writer down at once.
#           Under the TWO-COLLECTOR rule this is the legitimate gap case
#           (BOTH failed simultaneously). Real loss; gap is correct.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "07" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Simulating host reboot (SIGKILL all containers) ==="
dc kill  # SIGKILL all — no clean shutdown markers
sleep 3
dc down --remove-orphans  # Remove containers (but -v NOT used — keep volumes)

# Inject a new host_boot_id into the lifecycle journal to simulate a reboot
# The journal lives at: HOST_DATA_DIR/cryptolake/binance-collector-01/lifecycle.jsonl
JOURNAL_PATH="${HOST_DATA_DIR}/cryptolake/binance-collector-01/lifecycle.jsonl"
if [[ -f "$JOURNAL_PATH" ]]; then
    NEW_BOOT_ID="simulated-reboot-$(date -u +%s)"
    # Append a synthetic "unclean" evidence line with a new boot_id
    # (In a real reboot the NEW boot_id appears on the START event of the next session)
    TS_NS=$(python3 -c "import time; print(int(time.time_ns()))")
    printf '{"ts_ns":%s,"event":"start","host_boot_id":"%s","collector_session_id":"chaos-07-new-session"}\n' \
        "$TS_NS" "$NEW_BOOT_ID" >> "$JOURNAL_PATH"
    msg "Injected new host_boot_id=${NEW_BOOT_ID} into lifecycle journal"
else
    msg "WARNING: lifecycle journal not found at ${JOURNAL_PATH} — reboot simulation limited"
fi

msg "Restarting stack after 10s downtime…"
sleep 10
start_stack "primary+backup"
wait_healthy 150

msg "Waiting 60s for host_reboot gap classification…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — host went down; expect a reboot-class gap, no others.
expect_lifecycle_event "writer detects session change after reboot" "WITHIN_SOURCE_SESSION_CHANGE"
expect_only_these_gaps_check host_reboot host_unclean_shutdown collector_restart unclean_shutdown

verdict
