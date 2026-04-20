---
description: One-time setup for the Python-to-Java port. Scaffolds Gradle project, captures parity fixtures, writes state.json.
---

# /port-init

This is the one-time bootstrap for the port. Run it once per project.

## Preconditions (Claude, verify before proceeding)

1. Working tree is clean: `git status --porcelain` returns empty.
2. `docs/superpowers/port/state.json` does NOT exist.
3. `cryptolake-java/` directory exists (already committed from this plan).
4. The Python collector is runnable (`uv run python -m src.collector.main --help`).
5. `jq`, Java 21, and `gradle` wrapper are available.

If any precondition fails, STOP and report the failure to the user. Do not attempt to recover.

## Steps

1. **Initialize state.json.** Run:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/state.sh init
   ```
   Expected: creates `docs/superpowers/port/state.json` with 4 modules, `current_module: "common"`, all phases `pending`.

2. **Verify Gradle project builds.** Run:
   ```bash
   cd cryptolake-java && ./gradlew projects --no-daemon && cd ..
   ```
   Expected: lists 6 subprojects. If this fails, halt and escalate.

3. **Capture parity fixtures.** Warn the user this will take ~10 minutes plus a 2-minute metrics scrape, requires live Binance connection, and will produce ~50–200 MB of fixture data.
   Ask the user to confirm before proceeding.
   On confirmation, run:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/capture_fixtures.sh
   ```
   Expected summary output: frame count, metric line count, verify output line count.

4. **Commit the state + fixtures.** Run:
   ```bash
   git add docs/superpowers/port/state.json cryptolake-java/parity-fixtures/metrics/
   git commit -m "chore(port): init state and capture parity fixtures"
   ```
   Note: frames + verify expected output are gitignored; only the small metrics snapshots are committed.

5. **Report summary.** Print:
   - state.json location
   - fixture corpus summary (frames, metrics lines)
   - next user action: "Run /port-module to begin porting `common`."

## Failure handling

If any step fails after preconditions passed, run:
```bash
bash .claude/skills/python-to-java-port/scripts/state.sh set_halt "port_init_failed: <reason>"
```
Then report the failure and halt. Do not retry automatically — the user decides next step.
