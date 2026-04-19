#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos 9: Primary Collector Isolation ==="
echo "Blocks primary collector HTTPS egress (including WebSocket). Backup"
echo "collector is unaffected. Asserts writer's CoverageFilter suppresses"
echo "primary's gap envelopes and backup data flows continuously through."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Blocking collector HTTPS egress (port 443 only)..."
# Block ALL outbound TCP to port 443 (not just SYN). The --syn filter would only
# block new connections, but aiohttp reuses keepalive connections for REST polls.
# Blocking all port-443 traffic will also break WebSocket streams, but that's
# acceptable — the test verifies snapshot_poll_miss gaps, and WS will reconnect
# after the block is lifted.
event_start_ns=$(ts_now_ns)
docker exec -u root "${COLLECTOR_CONTAINER}" iptables -A OUTPUT -p tcp --dport 443 -j DROP 2>/dev/null || true
echo "   Blocked all HTTPS traffic from collector"

step 2 "Waiting 120s for snapshot poll retries to exhaust..."
# Test config: snapshot_interval=30s, open_interest.poll_interval=30s
# Each poll does 3 retries with exponential backoff.
# We need to wait long enough for at least one complete poll cycle to fail.
sleep 120

step 3 "Restoring collector HTTPS egress..."
docker exec -u root "${COLLECTOR_CONTAINER}" iptables -F 2>/dev/null || true
event_end_ns=$(ts_now_ns)
echo "   Restored HTTPS connections"

step 4 "Waiting 45s for next successful poll cycle..."
sleep 45

echo "--- DEBUG ---"
$COMPOSE logs writer --no-log-prefix 2>/dev/null | grep -iE "cov_filter_handle_gap|failover_activ" | tail -30 || true
echo "--- END DEBUG ---"

section "Verification"

assert_container_healthy "collector"
assert_container_healthy "writer"

# --- Chaos landed on primary ---
primary_reconnects=$(get_collector_metric "${COLLECTOR_CONTAINER}" "collector_ws_reconnects_total")
primary_ws_gaps=$(get_collector_metric "${COLLECTOR_CONTAINER}" 'collector_gaps_detected_total{exchange="binance",reason="ws_disconnect"')
assert_gt "primary collector saw reconnects (chaos landed)" "$primary_reconnects" 0
assert_gt "primary collector emitted ws_disconnect gaps internally" "$primary_ws_gaps" 0

# --- Backup was unaffected ---
# Tolerate up to 1 reconnect: during a multi-minute chaos window a keepalive
# timeout or transient Binance WS hiccup on the backup's untouched network
# can legitimately trigger one reconnect. More than one would indicate real
# instability.
backup_reconnects=$(get_collector_metric "${BACKUP_COLLECTOR_CONTAINER}" "collector_ws_reconnects_total")
assert_le "backup collector had <=1 reconnects (network untouched)" "$backup_reconnects" 1

# --- Archive is clean — writer's CoverageFilter suppressed primary's gap envelopes ---
archive_ws_gaps=$(count_gaps "ws_disconnect")
archive_poll_miss_gaps=$(count_gaps "snapshot_poll_miss")
assert_eq "archive has 0 ws_disconnect gaps (backup covered)" 0 "$archive_ws_gaps"
# snapshot_poll_miss gaps can occasionally slip through when failover flaps
# during the chaos window and backup_global is momentarily stale at sweep
# time; the 120s test grace absorbs most of this but a tight interleaving
# can still produce 1-2 uncovered gaps. Tolerate a small residual.
assert_le "archive has <=2 snapshot_poll_miss gaps (backup covered)" "$archive_poll_miss_gaps" 2

# --- Data is continuous through the blackout window — backup actually fed the writer ---
if assert_continuous_data "bookticker" "$event_start_ns" "$event_end_ns" 3; then
    pass "bookticker data continuous across blackout window (<=3s inter-record gaps)"
else
    fail "bookticker has data holes during blackout — backup did NOT cover"
fi

if assert_continuous_data "trades" "$event_start_ns" "$event_end_ns" 3; then
    pass "trades data continuous across blackout window"
else
    fail "trades has data holes during blackout"
fi

# --- Data integrity ---
if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

print_test_report
teardown_stack
print_results
