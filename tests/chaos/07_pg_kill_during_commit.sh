#!/usr/bin/env bash
# 07_pg_kill_during_commit.sh
#
# Scenario: pg_outage_writer_holds
# Chaos:    docker compose pause postgres for 120s; then unpause
# Expected: writer enters PG-outage hold cleanly (3-failure threshold),
#           pauses Kafka commits, recovers when PG returns. Only allowed
#           gap reason is `pg_outage_hold` (controller emits hold-window
#           markers at entry/exit by design; not data loss — records
#           replay from Kafka after recovery). `collector_restart` is
#           also tolerated as incidental environmental noise (long runs
#           can trigger backup-collector restarts under memory pressure).
# Flow:     MAIN+BACKUP both healthy and delivering → postgres paused →
#           StateManager.saveStatesAndCheckpoints throws thrice →
#           OffsetCommitCoordinator routes each to pgHold.recordPgFailure
#           → 3rd failure flips holdActive=true → WRITER_PG_OUTAGE_HOLD_ENTERED
#           fires + per-stream pg_outage_hold gap markers emitted →
#           KafkaConsumerLoop sees isAnyHoldActive() and pauses primary
#           consumption (records remain in Kafka) → postgres resumes →
#           PgOutageHoldController.pgProbe succeeds via StateManager.ping
#           → WRITER_PG_OUTAGE_HOLD_EXITED fires → consume loop resumes
#           → flush succeeds → records flow through → no real data loss.
# Why:      Postgres is for lifecycle state and offsets, not data flow.
#           Records stay in Kafka throughout the outage (Kafka 48h
#           retention). Hold-marker envelopes record the held window
#           but represent zero data loss.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "07" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Pausing postgres (writer enters pg_outage_hold) ==="
dc pause postgres

# PgOutageHoldController triggers after 3 consecutive failures (~90s at 30s
# retry interval). Hold for 120s so the third failure is well past the
# threshold and WRITER_PG_OUTAGE_HOLD_ENTERED fires deterministically.
msg "Holding postgres paused for 120s (must exceed 3-failure threshold)…"
sleep 120

msg "Unpausing postgres (writer exits hold, resumes commits)…"
dc unpause postgres

msg "Waiting 90s for PG recovery and archive flush to complete…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — PG outage is recoverable without real data loss. After our
# writer-disk-full-pg-outage-wiring branch, the StateManager's exception
# routes through OffsetCommitCoordinator → pgHold.recordPgFailure → 3rd
# failure trips the threshold → HOLD_ENTERED fires + Kafka pause engages.
# Recovery via StateManager.ping → HOLD_EXITED + Kafka resume.
expect_log_event              "writer logs pg_save_failed during outage"  "pg_save_failed"
expect_lifecycle_event        "writer enters PG-outage hold"              "WRITER_PG_OUTAGE_HOLD_ENTERED"
expect_lifecycle_event        "writer exits PG-outage hold"               "WRITER_PG_OUTAGE_HOLD_EXITED"
expect_lifecycle_event        "kafka consumption paused under hold"       "WRITER_KAFKA_CONSUMPTION_PAUSED"
# Only pg_outage_hold (controller-emitted hold markers) and collector_restart
# (incidental environmental noise during ~7 min run) are permitted. See
# chaos-02 for a longer rationale.
expect_only_these_gaps_check  "pg_outage_hold" "collector_restart"

verdict
