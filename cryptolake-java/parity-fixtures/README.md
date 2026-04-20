# parity-fixtures

Ground-truth artifacts used by port gates 3, 4, 5.

- `websocket-frames/<stream>/*.raw` — verbatim inbound WebSocket frame bytes
- `websocket-frames/<stream>/*.json` — Python-produced envelope for each frame
- `metrics/<service>.txt` — canonicalized Prometheus metric name+label set
- `verify/expected.txt` — expected stdout of Python `cryptolake verify`

Populated by `/port-init` running `scripts/capture_fixtures.sh`. Do not edit by hand after initial capture — see spec §5.5 for immutability rules.

`websocket-frames/` and `verify/expected.txt` are gitignored (large, per-environment). `metrics/` snapshots are small and worth tracking.
