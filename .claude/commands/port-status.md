---
description: Read-only. Print the current state of the port from state.json.
---

# /port-status

Read-only snapshot of port progress.

## Steps

1. Check state exists:
   ```bash
   STATE=docs/superpowers/port/state.json
   [[ -f $STATE ]] || { echo "no port in progress (state.json missing). Run /port-init"; exit 0; }
   ```

2. Print the state summary:
   ```bash
   jq -r '
     "=== Port status ===",
     "started:        " + .started_at,
     "current module: " + .current_module,
     "halt reason:    " + (.halt_reason // "none"),
     "",
     "Modules:",
     (.modules[] | "  \(.name): status=\(.status) phase=\(.phase) gates_passed=\(
       [.gates | to_entries[] | select(.value == "pass")] | length
     )/7 attempts=\(.attempts.analyst)/\(.attempts.architect)/\(.attempts.developer)")
   ' $STATE
   ```

3. Probe the filesystem for per-module artifacts (state.json doesn't currently track these):
   ```bash
   echo ""
   echo "Artifacts on disk:"
   for m in common writer collector cli; do
     for kind in mapping design completion; do
       p="docs/superpowers/port/$m/${kind}.md"
       [[ -f "$p" ]] && echo "  $p"
     done
   done
   ```

4. If `halt_reason` is non-null, suggest: `/port-retry` to resume, or `/port-rollback <module>` to restart the current module.
