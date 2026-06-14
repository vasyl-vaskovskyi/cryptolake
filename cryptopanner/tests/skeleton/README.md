# Walking-skeleton smoke test

Runs the full pipeline end-to-end on a single host:

1. Builds all modules with Maven.
2. Starts MinIO in Docker.
3. Starts the Python mock-binance-ws.
4. Runs Collector against the mock for `collector_max_runtime_s` seconds.
5. Runs Sealer to merge captured minutes into one hour file.
6. Runs Uploader to push the sealed file to MinIO with manifest-last ordering.
7. Runs `verify verify` to recompute the SHA and assert ERRORS=0.

## Run

```bash
bash tests/skeleton/run-skeleton.sh
```

Expected final line: `[skeleton] OK` and `ERRORS=0` from the verify CLI.

Tunables (edit `config/dev/skeleton.yaml`):
- `collector_max_runtime_s` — default 30s in the skeleton. Raise to 60–180 for longer captures.

## Requirements

- Docker (for MinIO)
- Python 3.10+ (for the mock)
- JDK 21 (or compatible; project targets 21)
- Maven wrapper `./mvnw` (already in repo root)
