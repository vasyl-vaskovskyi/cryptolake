#!/usr/bin/env bash
# gate7: architect sign-off. This script does NOT dispatch the Architect
# (that happens in the /port-module slash command's Claude flow).
# It checks whether a signoff file exists and is "approved".
set -euo pipefail
MODULE="${1:?module}"
SIGNOFF="docs/superpowers/port/${MODULE}/architect-signoff.txt"

if [[ ! -f "$SIGNOFF" ]]; then
  echo "gate7 PENDING: no signoff file at $SIGNOFF — orchestrator must dispatch Architect"
  exit 2  # 2 = pending, not a failure
fi

status=$(head -n1 "$SIGNOFF" | tr -d '\r\n ')
if [[ "$status" == "approved" ]]; then
  echo "gate7 OK: architect signoff approved"
  exit 0
fi
echo "gate7 FAIL: architect signoff status='$status' (not approved)"
exit 1
