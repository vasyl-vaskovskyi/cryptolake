#!/usr/bin/env bash
# gate4: diff Java Prometheus metric name+label set vs Python reference.
set -euo pipefail
MODULE="${1:?module}"

case "$MODULE" in
  common|cli)
    echo "gate4: $MODULE has no metric exposition (pass-by-definition)"
    exit 0
    ;;
esac

SERVICE="$MODULE"
# Absolute path so it survives the `cd "$REPO_ROOT"` below.
REPO_ROOT="$(cd "$(dirname "$0")/../../../../.." && pwd)"
REF="$REPO_ROOT/parity-fixtures/metrics/${SERVICE}.txt"
if [[ ! -f "$REF" ]]; then
  echo "gate4 FAIL: reference metrics missing: $REF" >&2
  exit 1
fi

cd "$REPO_ROOT"
# Gradle task :<module>:dumpMetricSkeleton starts the service in a test JVM,
# scrapes /metrics, writes to build/metrics-skeleton.txt (canonicalized:
# metric_name{sorted,label,keys} with VALUE stripped; only app-level
# <service>_* metrics).
./gradlew ":${SERVICE}:dumpMetricSkeleton" --info

ACTUAL="$REPO_ROOT/${SERVICE}/build/metrics-skeleton.txt"
if [[ ! -f "$ACTUAL" ]]; then
  echo "gate4 FAIL: harness did not produce $ACTUAL" >&2
  exit 1
fi
if ! diff -u "$REF" "$ACTUAL"; then
  echo "gate4 FAIL: metric skeleton differs from Python reference" >&2
  exit 1
fi
echo "gate4 OK: metric skeleton matches reference"
