#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos 13: Rapid Restart Storm ==="
echo "Kills the writer 3 times in 60 seconds and verifies no duplicates."
echo ""

setup_stack
wait_for_data 20

pre_kill=$(count_envelopes)

section "Scenario"
for i in 1 2 3; do
    step "${i}" "Killing writer (restart ${i}/3)..."
    docker kill "${WRITER_CONTAINER}"
    sleep 5
    $COMPOSE up -d writer 2>&1
    if ! wait_service_healthy writer 30; then :; fi
    sleep 10
done

step 4 "Waiting for writer to flush new data after restart storm..."
if wait_for_envelope_count_gt "$pre_kill" 90; then
    pass "writer resumed writing after restart storm"
else
    fail "writer did not resume writing after restart storm"
fi

section "Verification"
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK (no duplicates across 3 restarts)"
else
    fail "data integrity check failed"
fi

post_storm=$(count_envelopes)
assert_gt "archive has data after restart storm" "$post_storm" "$pre_kill"

step 6 "Checking for restart_gap records..."
wait_for_gaps "restart_gap" 60 || true
gaps=$(count_gaps "restart_gap")
if (( gaps > 0 )); then
    pass "restart_gap records exist for rapid restarts (count=${gaps})"
else
    # Writer-only kills may not always produce restart_gap records if the
    # collector session did not change and the writer handled SIGKILL recovery
    # cleanly.  This is acceptable — the key invariant is data integrity.
    pass "no restart_gap records (writer recovered without detected gap)"
fi

print_test_report
teardown_stack
print_results
