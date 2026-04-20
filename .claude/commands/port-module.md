---
description: Advance the current module by one phase (pending → analyst → architect → developer → gates → complete). Idempotent.
---

# /port-module

Drives the current module forward by exactly one phase.

## Preconditions (Claude, verify)

1. `docs/superpowers/port/state.json` exists.
2. `.halt_reason` is null (if non-null, tell the user and suggest `/port-retry` or `/port-rollback`).
3. Working tree is clean outside `docs/superpowers/port/` and `cryptolake-java/`.

## Read state

```bash
STATE_SH=.claude/skills/python-to-java-port/scripts/state.sh
MODULE=$(bash $STATE_SH get_current_module)
PHASE=$(bash $STATE_SH get_current_phase)
echo "module=$MODULE phase=$PHASE"
```

If MODULE is `DONE`, tell the user the port is complete and exit.

## Dispatch by phase

### If PHASE == "pending"

**Dispatch the Analyst.**

Assemble the prompt:
```bash
ATTEMPTS=$(bash $STATE_SH ... ) # current analyst attempts (read from state.json via jq if needed)
bash .claude/skills/python-to-java-port/scripts/assemble_prompt.sh analyst "$MODULE" 1 none > /tmp/analyst-prompt.txt
```

Invoke the `Agent` tool:
- `subagent_type`: `Explore`
- `description`: "Analyst: map <module>"
- `prompt`: the contents of `/tmp/analyst-prompt.txt`

After the agent returns:
1. Validate artifact:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/validate_artifact.sh mapping docs/superpowers/port/$MODULE/mapping.md
   ```
2. If invalid: bump attempts (`bash $STATE_SH increment_attempts analyst`). If attempts < 2, re-dispatch once with failure details. Else halt and escalate.
3. If valid: `bash $STATE_SH set_phase analyst`.
4. Commit the mapping: `git add docs/superpowers/port/$MODULE/mapping.md && git commit -m "chore(port): $MODULE analyst mapping"`.
5. If `$MODULE` == "common" AND `checkpoint0_done` is false: mark phase `analyst_review` via a jq update to state.json, set `halt_reason: "checkpoint 0: review common/mapping.md, run /port-advance"`. Stop and inform user.
6. Otherwise: set phase `architect` and tell user to run `/port-module` again to dispatch Architect.

### If PHASE == "analyst_review"

No-op. Tell the user: "In checkpoint 0. Review `docs/superpowers/port/common/mapping.md` and run `/port-advance` to continue."

### If PHASE == "analyst"

**Dispatch the Architect.**

Same pattern:
- Assemble prompt via `assemble_prompt.sh architect $MODULE`.
- Invoke `Agent` tool:
  - `subagent_type`: `general-purpose`
  - `model`: `opus`
  - `description`: "Architect: design Java for <module>"
  - `prompt`: assembled prompt.
- On return, check for the string `ESCALATE:` at the start of the agent's output → halt with that message.
- Validate `design.md`:
  ```bash
  bash .claude/skills/python-to-java-port/scripts/validate_artifact.sh design docs/superpowers/port/$MODULE/design.md
  ```
- If invalid: 1 retry, else halt.
- If valid: `bash $STATE_SH set_phase architect`, commit design, tell user to run `/port-module`.

### If PHASE == "architect"

**Dispatch the Developer.**

- Verify on `main` and clean:
  ```bash
  [[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || { echo "not on main — halting"; exit 1; }
  [[ -z "$(git status --porcelain)" ]] || { echo "working tree dirty — halting"; exit 1; }
  ```
- Record `developer_start_sha`:
  ```bash
  START_SHA=$(git rev-parse HEAD)
  STATE=docs/superpowers/port/state.json
  jq --arg m "$MODULE" --arg sha "$START_SHA" '
    .modules |= map(if .name == $m then .developer_start_sha = $sha else . end)
  ' $STATE > $STATE.tmp && mv $STATE.tmp $STATE
  git add $STATE && git commit -m "chore(port): record $MODULE developer_start_sha"
  ```
- Assemble prompt, dispatch `Agent` with `subagent_type: general-purpose`, `model: sonnet` (first attempt; opus on 3rd retry).
- On return, validate `completion.md`:
  ```bash
  bash .claude/skills/python-to-java-port/scripts/validate_artifact.sh completion docs/superpowers/port/$MODULE/completion.md
  ```
- If invalid: 1 retry, else halt.
- If valid: `bash $STATE_SH set_phase developer`, tell user to run `/port-module` to run gates.

### If PHASE == "developer"

**Run gates.**

```bash
bash .claude/skills/python-to-java-port/scripts/run_gates.sh $MODULE
```

- If exit 0: `bash $STATE_SH set_phase gates` then `set_phase complete`. Halt with "Module <m> complete. Review commits <developer_start_sha>..HEAD and run /port-advance."
- If exit 2 (gate 7 pending): dispatch Architect read-only to produce signoff file:
  - Read `developer_start_sha` from state.json: `START=$(jq -r --arg m "$MODULE" '.modules[] | select(.name==$m) | .developer_start_sha' $STATE)`
  - Prompt the Architect with the module diff (`git diff $START..HEAD -- cryptolake-java/`) and `design.md`, ask for `approved` or `rejected` on first line of output.
  - Write stdout to `docs/superpowers/port/$MODULE/architect-signoff.txt`.
  - Re-run `run_gates.sh`.
- If exit 1: increment developer attempts. If < 3: re-dispatch Developer with gate failure log attached. If == 3: halt and escalate.
- Special: if gate 3 failed, halt immediately regardless of attempts. Set halt_reason.

### If PHASE == "gates" or "complete"

Tell the user "Module is complete. Run `/port-advance` to merge and move to the next module."

## Post-dispatch commit

After any successful phase transition:
```bash
git add docs/superpowers/port/state.json
git commit -m "chore(port): $MODULE phase -> $(bash $STATE_SH get_current_phase)"
```
