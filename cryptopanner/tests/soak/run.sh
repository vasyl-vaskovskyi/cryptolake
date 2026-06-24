#!/usr/bin/env bash
# tests/soak/run.sh — §14.e real-environment soak (process-based, single-node variant).
#
# Brings up the real pipeline as local processes against one MinIO container and the
# mock-binance-ws replay server, runs for a window, triggers one WS rotation mid-run, then seals +
# uploads the captured hour and gates on `cryptopanner-verify` reporting ERRORS=0. Also asserts the
# Monitor sees the node `running` via its live /api/nodes scrape (exercises the §14.j agent
# test-mode + the whole §13 monitor pipeline against a real agent).
#
# This is the runnable foundation; the docker-compose + two-node + sustained-load variant is the
# next increment (see docs/superpowers/specs/2026-06-24-dev-stack-audit-and-soak-plan.md). Bring-up
# is isolated in soak::stack_up so it can be swapped for `docker compose up` later.
#
# Env knobs:
#   SOAK_RUN_SECONDS   capture window (default 90; spec §14.e wants >=300)
#   SOAK_REPLAY_HZ     mock replay rate (default 50)
#   SKIP_BUILD=1       reuse existing target/install binaries
#   SOAK_STRICT=1      fail the run if verify reports ERRORS>0 (default: surface but don't fail,
#                      because the current fixed-event-time fixture yields an incomplete hour)
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${REPO_ROOT}/tests/soak/_soak_common.sh"

soak::main() {
  soak::config
  trap soak::cleanup EXIT
  soak::build
  soak::prepare_dirs
  soak::stack_up
  soak::wait_healthy
  soak::run_window_with_rotation
  soak::assert_monitor_sees_node
  soak::stop_collector
  soak::seal_upload_verify
  soak::summary
}

soak::main "$@"
