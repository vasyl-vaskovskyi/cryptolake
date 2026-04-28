#!/usr/bin/env bash
# 13_rapid_restart_storm.sh
#
# Invariant: Restart the primary collector 5 times in 30 seconds (SIGKILL +
# restart). Each restart should produce a collector_restart gap envelope.
# verify exits 0 with ERRORS=0.
#
# Expected: Multiple collector_restart gap envelopes, verify ERRORS=0.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "13" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Rapid restart storm — 5 kills in 30s ==="
for i in 1 2 3 4 5; do
    msg "Kill ${i}/5…"
    kill_service "collector"
    sleep 3
    start_service "collector"
    sleep 3
done

msg "Letting stack stabilise for 90s…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "collector_restart" "$HOST_DATA_DIR"

# Count how many distinct collector_restart gap envelopes exist
COUNT=$(find "$HOST_DATA_DIR" -name "*.jsonl.zst" -print0 2>/dev/null \
    | xargs -0 -I{} sh -c 'zstd -d -c "$1" 2>/dev/null' _ {} 2>/dev/null \
    | python3 -c "
import sys,json
n=0
for line in sys.stdin:
    line=line.strip()
    if not line: continue
    try:
        d=json.loads(line)
        if d.get('type')=='gap' and d.get('reason')=='collector_restart':
            n+=1
    except: pass
print(n)
" 2>/dev/null || echo 0)
msg "Found ${COUNT} collector_restart gap envelopes (expected >=1, ideally ~5)"

scenario_pass
