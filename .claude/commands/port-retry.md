---
description: Clear halt_reason and re-dispatch the last failed agent for the current module's phase.
---

# /port-retry

Use after fixing an environmental issue (Docker down, Gradle cache, network, etc.).

## Steps

1. Read halt_reason:
   ```bash
   STATE=docs/superpowers/port/state.json
   REASON=$(jq -r '.halt_reason // empty' $STATE)
   [[ -n $REASON ]] || { echo "no halt in progress — nothing to retry"; exit 0; }
   echo "retrying from halt: $REASON"
   ```

2. Clear halt:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/state.sh clear_halt
   ```

3. Re-invoke `/port-module`. This is the same entry point; the state machine resumes from the saved phase.
