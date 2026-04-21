---
description: One-time setup for the Python-to-Java port. Scaffolds Gradle project, optionally captures parity fixtures, writes state.json.
argument-hint: [--skip-fixtures]
---

# /port-init

This is the one-time bootstrap for the port. Run it once per project.

## Arguments

- `--skip-fixtures` (optional): perform state + Gradle setup, skip the 10-minute
  fixture capture step. Useful when the `docker-compose.yml` stack (Redpanda +
  Postgres + Binance-connected collector) is not running locally. Fixtures can
  be captured later by running `bash .claude/skills/python-to-java-port/scripts/capture_fixtures.sh`.

## Preconditions (Claude, verify before proceeding)

1. Working tree is clean: `git status --porcelain` returns empty.
2. `docs/superpowers/port/state.json` does NOT exist.
3. `cryptolake-java/` directory exists (already committed from this plan).
4. The Python collector module imports cleanly (side-effect-free probe):
   ```bash
   uv run python -c "import importlib; importlib.import_module('src.collector.main')"
   ```
   Do NOT run `python -m src.collector.main` — the module has no `--help` flag
   and starts the service immediately, which requires a full docker-compose
   stack (Redpanda + Postgres) and leaves a background process behind on
   failure.
5. `jq`, Java 21, and `cryptolake-java/gradlew` are available.
6. **If `--skip-fixtures` is NOT set**: docker-compose stack is running
   (`docker compose ps` shows at least `redpanda` and `postgres` healthy) —
   required for fixture capture. If the stack is down, tell the user and
   offer to run with `--skip-fixtures` instead.

If any precondition fails, STOP and report the failure to the user. Do not attempt to recover.

## Steps

1. **Initialize state.json.** Run:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/state.sh init
   ```
   Expected: creates `docs/superpowers/port/state.json` with 4 modules, `current_module: "common"`, all phases `pending`.

2. **Verify Gradle project builds.** Run:
   ```bash
   bash cryptolake-java/gradlew -p cryptolake-java projects --no-daemon
   ```
   Expected: lists 6 subprojects. If this fails, halt and escalate.

3. **Capture parity fixtures.** Skip this step if `--skip-fixtures` was passed.
   Otherwise warn the user this will take ~10 minutes plus a 2-minute metrics
   scrape, requires live Binance connection, and will produce ~50–200 MB of
   fixture data.
   Ask the user to confirm before proceeding.
   On confirmation, run:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/capture_fixtures.sh
   ```
   Expected summary output: frame count, metric line count, verify output line count.

4. **Commit the state + fixtures.**
   - If fixtures were captured:
     ```bash
     git add docs/superpowers/port/state.json cryptolake-java/parity-fixtures/metrics/
     git commit -m "chore(port): init state and capture parity fixtures"
     ```
   - If `--skip-fixtures` was used:
     ```bash
     git add docs/superpowers/port/state.json
     git commit -m "chore(port): init state (fixtures deferred)"
     ```
   Note: frames + verify expected output are gitignored; only the small metrics snapshots are committed.

5. **Report summary.** Print:
   - state.json location
   - fixture corpus summary (frames, metrics lines) — or "fixtures deferred" if
     `--skip-fixtures` was used
   - next user action:
     - with fixtures: "Run /port-module to begin porting `common`."
     - without fixtures: "Run capture_fixtures.sh when the docker-compose stack
       is up, THEN /port-module."

## Failure handling

If any step fails after preconditions passed, run:
```bash
bash .claude/skills/python-to-java-port/scripts/state.sh set_halt "port_init_failed: <reason>"
```
Then report the failure and halt. Do not retry automatically — the user decides next step.
