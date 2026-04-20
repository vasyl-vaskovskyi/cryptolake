---
description: State-only rollback for a module. Leaves code in place for inspection.
argument-hint: [module-name]
---

# /port-rollback

Reset a module's state to `pending` without touching code. Use when a module has been completed but later found broken and you want to redo it without manually editing state.json.

Argument: module name (`common`, `writer`, `collector`, or `cli`).

## Steps

1. Read target module from `$1`. Abort if not one of the four.
2. Read the module's recorded commit range:
   ```bash
   STATE=docs/superpowers/port/state.json
   MODULE=$1
   START=$(jq -r --arg m "$MODULE" '.modules[] | select(.name==$m) | .developer_start_sha // empty' $STATE)
   ACCEPTED=$(jq -r --arg m "$MODULE" '.modules[] | select(.name==$m) | .accepted_sha // empty' $STATE)
   echo "Module $MODULE range to revert: ${START:-'(no commits yet)'}..${ACCEPTED:-HEAD}"
   ```
3. If a commit range exists, show the commits that would be reverted and ask the user to confirm:
   ```bash
   if [[ -n "$START" ]]; then
     git log --oneline "$START..${ACCEPTED:-HEAD}"
   fi
   ```
   Warning message to user: "This will REVERT the above $MODULE commits on main (creating new revert commits — nothing is rewritten) and clear $MODULE state. The original commits remain in git history. Proceed?"
4. On confirm, revert and reset state:
   ```bash
   if [[ -n "$START" ]]; then
     git revert --no-edit "$START..${ACCEPTED:-HEAD}"
   fi

   # Reset module state
   jq --arg m "$MODULE" '
     .modules |= map(
       if .name == $m then
         .status = "in_progress"
         | .phase = "pending"
         | .checkpoint0_done = false
         | .artifacts = {mapping:null, design:null, completion:null}
         | .gates = {ported_unit_tests:null, ported_chaos_tests:null, raw_text_byte_parity:null, metric_parity:null, verify_cli:null, static_checks:null, architect_signoff:null}
         | .attempts = {analyst:0, architect:0, developer:0}
         | .escalations = []
         | del(.developer_start_sha)
         | del(.accepted_sha)
       else . end
     )
     | .current_module = $m
     | .halt_reason = null
   ' $STATE > $STATE.tmp && mv $STATE.tmp $STATE

   # Remove artifacts (but keep them in git history — revert already covered that)
   git rm -rf "docs/superpowers/port/$MODULE/" 2>/dev/null || true

   git add $STATE
   git commit -m "chore(port): rollback $MODULE to pending"
   ```

5. Tell user: revert commits are on `main`; the `port-$MODULE-accepted` tag (if any) is still present but now points at a reverted state. Run `/port-module` to restart the module.
