---
description: Release the module-boundary checkpoint (or checkpoint 0). Tags the accepted commit on main; advances to next module.
---

# /port-advance

## Preconditions

1. `docs/superpowers/port/state.json` exists; `.halt_reason` is null.
2. Current phase is `complete` OR `analyst_review` (checkpoint 0).

## Steps

Read state:
```bash
STATE_SH=.claude/skills/python-to-java-port/scripts/state.sh
MODULE=$(bash $STATE_SH get_current_module)
PHASE=$(bash $STATE_SH get_current_phase)
```

### If PHASE == "analyst_review" (checkpoint 0, common only)

1. Confirm with user they have reviewed `docs/superpowers/port/common/mapping.md`.
2. Mark checkpoint done:
   ```bash
   jq '(.modules[] | select(.name=="common") | .checkpoint0_done) = true' $STATE > $STATE.tmp && mv $STATE.tmp $STATE
   ```
3. Set phase back to `analyst` so the next `/port-module` dispatches Architect:
   ```bash
   bash $STATE_SH set_phase analyst
   ```
4. Commit:
   ```bash
   git add docs/superpowers/port/state.json
   git commit -m "chore(port): release checkpoint 0 for common"
   ```
5. Tell user: run `/port-module` to dispatch Architect.

### If PHASE == "complete"

1. Verify on `main`:
   ```bash
   [[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || { echo "not on main — halting"; exit 1; }
   ```
2. Confirm with user they've reviewed the module's commit range:
   ```bash
   STATE=docs/superpowers/port/state.json
   START=$(jq -r --arg m "$MODULE" '.modules[] | select(.name==$m) | .developer_start_sha' $STATE)
   echo "Module $MODULE commits: $START..HEAD"
   git log --oneline $START..HEAD
   ```
3. Record `accepted_sha` and tag:
   ```bash
   ACCEPTED_SHA=$(git rev-parse HEAD)
   jq --arg m "$MODULE" --arg sha "$ACCEPTED_SHA" '
     .modules |= map(if .name == $m then .accepted_sha = $sha else . end)
   ' $STATE > $STATE.tmp && mv $STATE.tmp $STATE
   git add $STATE
   git commit -m "chore(port): $MODULE module accepted"
   git tag -a "port-$MODULE-accepted" -m "CryptoLake port: $MODULE module accepted"
   ```
4. Advance state (requires phase `accepted`):
   ```bash
   bash $STATE_SH set_phase accepted
   bash $STATE_SH advance_module
   git add $STATE
   git commit -m "chore(port): advance to $(bash $STATE_SH get_current_module)"
   ```
5. Tell user next module name and that `port-$MODULE-accepted` tag was created. Ask whether to run `/port-module` now.

### Else

Tell user the current phase doesn't allow /port-advance. Show current phase and suggest `/port-status`.
