#!/usr/bin/env bash
# Gate 3 spike runner. Starts the Python WS server, then runs the Java client.
# Reports PASS/FAIL and exits with the Java client's exit code.
set -uo pipefail

PORT=${PORT:-8765}
SPIKE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$(dirname "$SPIKE_DIR")"  # repo root

# Start server in background.
uv run python "$SPIKE_DIR/ws_server.py" "$PORT" "$SPIKE_DIR/payloads.jsonl" > /tmp/gate3-server.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

# Wait up to 5s for the server to be ready.
for i in 1 2 3 4 5; do
  if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
    break
  fi
  sleep 1
done

# Run the Java client in single-file mode (JEP 330).
java "$SPIKE_DIR/WebSocketCapture.java" "ws://127.0.0.1:$PORT" "$SPIKE_DIR/payloads.jsonl"
EXIT=$?

# Give server a moment to log shutdown.
sleep 1
echo
echo "--- server log ---"
cat /tmp/gate3-server.log || true

exit $EXIT
