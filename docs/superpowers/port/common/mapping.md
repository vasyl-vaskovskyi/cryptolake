---
module: common
status: complete
produced_by: analyst
python_files:
  - src/common/__init__.py
  - src/common/async_utils.py
  - src/common/config.py
  - src/common/envelope.py
  - src/common/health_server.py
  - src/common/jsonl.py
  - src/common/logging.py
  - src/common/service_runner.py
  - src/common/system_identity.py
python_test_files:
  - tests/unit/test_async_utils.py
  - tests/unit/test_config.py
  - tests/unit/test_envelope.py
  - tests/unit/test_logging.py
  - tests/unit/test_system_identity.py
  - tests/unit/test_host_lifecycle_agent.py
  - tests/integration/test_redpanda_roundtrip.py
---

## 1. Module summary

The `common` module provides cross-cutting shared utilities for the collector and writer services. It defines the canonical envelope data contract (data and gap records with JSON serialization via orjson), configuration loading and validation (Pydantic models for Binance exchange, Redpanda, database, and writer settings), structured logging with JSON output (via structlog), crash-resilient JSONL file I/O for ledger storage, concurrency helpers (asyncio task cancellation), HTTP health/metrics endpoints, system identity helpers (Linux boot ID for restart-gap classification), and the async service bootstrap pattern (uvloop + signal handling). All services and CLI tools import from `common`; it has no upstream imports within the application.

## 2. File inventory

| File | Lines | Role | Public symbols |
|------|-------|------|-----------------|
| `__init__.py` | 2 | Glue (module marker) | (empty) |
| `async_utils.py` | 13 | Concurrency helper | `cancel_tasks(tasks: list[Task]) -> None` |
| `config.py` | 196 | Configuration & validation | `CryptoLakeConfig`, `BinanceExchangeConfig`, `ExchangesConfig`, `RedpandaConfig`, `WriterConfig`, `StreamsConfig`, `DepthConfig`, `MonitoringConfig`, `ProducerConfig`, `DatabaseConfig`, `GapFilterConfig`, `CollectorConfig`, `ConfigValidationError`, `load_config(path, env_overrides) -> CryptoLakeConfig`, `default_archive_dir() -> str` |
| `envelope.py` | 139 | Data contract | `create_data_envelope(...)`, `create_gap_envelope(...)`, `serialize_envelope(envelope) -> bytes`, `deserialize_envelope(data: bytes) -> dict`, `add_broker_coordinates(envelope, topic, partition, offset) -> dict`, `VALID_GAP_REASONS`, `DATA_ENVELOPE_FIELDS`, `GAP_ENVELOPE_FIELDS`, `BROKER_COORD_FIELDS` |
| `health_server.py` | 36 | IO (HTTP) | `start_health_server(port: int, ready_check: Callable[[], Awaitable[dict[str, bool]]]) -> None` (async) |
| `jsonl.py` | 48 | IO (file) | `read_jsonl(path: Path) -> list[dict]`, `append_jsonl(path: Path, record: dict) -> None` |
| `logging.py` | 36 | IO (stdout, structured) | `setup_logging(level: str = "INFO") -> None`, `get_logger(**initial_bindings) -> Any`, `_json_dumps(obj: dict, **_) -> str` |
| `service_runner.py` | 47 | Glue (bootstrap) | `run_service(service_factory, config_env, default_config) -> None` |
| `system_identity.py` | 31 | IO (file, env) | `get_host_boot_id() -> str` |

## 3. Public API surface

### `config.py`

**`load_config(path: Path, env_overrides: dict[str, str] | None = None) -> CryptoLakeConfig`**
- **Signature**: Takes a YAML config file path and optional environment variable overrides (keyed by `MODULE__KEY` pattern, e.g., `REDPANDA__BROKERS`).
- **Semantics**: Parses YAML via `yaml.safe_load()`, applies environment overrides via `_apply_env_overrides()` and `_normalize_env_overrides()` (latter handles `HOST_DATA_DIR` alias), validates via Pydantic `CryptoLakeConfig.model_validate()`, raises `ConfigValidationError` on validation failure.
- **Callers**: Collector `main()`, writer `main()`, CLI tools (backfill, consolidate, verify), test fixtures.
- **Critical behavior**: Environment overrides do NOT leak from `os.environ` when `env_overrides` dict is explicitly passed; this is tested. Symbols are lowercased at parse time. `HOST_DATA_DIR` env var is aliased to `writer.base_dir` if not explicitly overridden.

**`default_archive_dir() -> str`**
- Returns `os.environ.get("HOST_DATA_DIR", "/data")`. Used as default for `WriterConfig.base_dir`.

**`ConfigValidationError` exception class**
- Raised on validation failure; inherits from `Exception`.

**Pydantic models** (all inherit from `BaseModel`):
- `CryptoLakeConfig`: top-level config root. Required fields: `database`, `exchanges`. Defaults for `writer`, `monitoring`, `collector`.
- `BinanceExchangeConfig`: has field validators for `symbols` (lowercase) and `writer_streams_override` (auto-append `depth_snapshot` if `depth` present). Public method `get_enabled_streams() -> list[str]` returns dynamic stream list based on boolean flags.
- `StreamsConfig`, `DepthConfig`, `OpenInterestConfig`, `ProducerConfig`, `RedpandaConfig` (has validator for `retention_hours >= 12`), `DatabaseConfig`, `WriterConfig`, `MonitoringConfig`, `CollectorConfig`, `GapFilterConfig`.

### `envelope.py`

**`create_data_envelope(...) -> dict[str, Any]`**
- **Signature**: Keyword-only args: `exchange`, `symbol`, `stream`, `raw_text`, `exchange_ts`, `collector_session_id`, `session_seq`, optional `received_at` (defaults to `time.time_ns()`).
- **Semantics**: Returns dict with fields `v`, `type`, `exchange`, `symbol`, `stream`, `received_at`, `exchange_ts`, `collector_session_id`, `session_seq`, `raw_text`, `raw_sha256` (computed via `hashlib.sha256(raw_text.encode()).hexdigest()`). Field order matches Python dict insertion order and is fixed for JSON byte-identity on serialization.
- **Callers**: Collector when creating messages, writer when testing, CLI tools.

**`create_gap_envelope(...) -> dict[str, Any]`**
- **Signature**: Keyword-only args: `exchange`, `symbol`, `stream`, `collector_session_id`, `session_seq`, `gap_start_ts`, `gap_end_ts`, `reason`, `detail`, optional `received_at`. Optional restart metadata kwargs: `component`, `cause`, `planned`, `classifier`, `evidence`, `maintenance_id` (only included if provided, using sentinel `_SENTINEL` to distinguish "not provided" from `None`).
- **Semantics**: Returns dict with `v`, `type`, `exchange`, `symbol`, `stream`, `received_at`, `collector_session_id`, `session_seq`, `gap_start_ts`, `gap_end_ts`, `reason`, `detail`. Validates `reason` against `VALID_GAP_REASONS` frozenset; raises `ValueError` if invalid. Optionally includes restart metadata fields only when provided.
- **Callers**: Writer when emitting gaps, CLI for testing.

**`serialize_envelope(envelope: dict) -> bytes`**
- Returns `orjson.dumps(envelope)`. Preserves dict insertion order (Python 3.7+), no whitespace, no trailing newline.

**`deserialize_envelope(data: bytes) -> dict[str, Any]`**
- Returns `orjson.loads(data)`.

**`add_broker_coordinates(envelope: dict, *, topic: str, partition: int, offset: int) -> dict[str, Any]`**
- Mutates envelope in-place (adds `_topic`, `_partition`, `_offset` fields) and returns it.

**Constants**:
- `VALID_GAP_REASONS`: frozenset of 12 valid gap reason strings.
- `DATA_ENVELOPE_FIELDS`: frozenset of required data envelope field names.
- `GAP_ENVELOPE_FIELDS`: frozenset of required gap envelope field names.
- `BROKER_COORD_FIELDS`: frozenset of broker coordinate field names (`_topic`, `_partition`, `_offset`).

### `async_utils.py`

**`async def cancel_tasks(tasks: list[asyncio.Task]) -> None`**
- **Semantics**: Cancels all non-done tasks in the list via `t.cancel()`, awaits all with `asyncio.gather(..., return_exceptions=True)` to suppress `CancelledError`, clears the list in-place.
- **Callers**: Collector shutdown path (service_runner), direct task cleanup in tests.

### `logging.py`

**`setup_logging(level: str = "INFO") -> None`**
- Configures structlog: resets defaults, sets processor chain (contextvars merge → log level → ISO timestamp → stack info → exc info → JSON renderer), wraps with filtering bound logger at specified level, context class dict, logger factory to stdout.
- **JSON output**: Each log is a JSON object with `event` (the event name passed to `log.info(...)`, `timestamp`, `level`, and all KV args merged as top-level fields.
- **Callers**: `service_runner.run_service()`, test fixtures.

**`get_logger(**initial_bindings) -> Any`**
- Returns `structlog.get_logger(**initial_bindings)`. Bindings are passed as KV pairs and appear in all subsequent log events from this logger instance.
- **Callers**: Every module that logs (collector, writer, CLI).

### `health_server.py`

**`async def start_health_server(port: int, ready_check: Callable[[], Awaitable[dict[str, bool]]]) -> None`**
- **Signature**: Accepts port and an async callable that returns a dict of check names to bool statuses.
- **Endpoints**: `/health` (always returns `{"status": "ok"}` 200), `/ready` (calls `ready_check()`, returns 200 if all checks true else 503), `/metrics` (returns Prometheus metrics via `prometheus_client.generate_latest()`).
- **Callers**: Collector and writer `start()` methods.

### `jsonl.py`

**`read_jsonl(path: Path) -> list[dict]`**
- Reads all valid JSONL records from a file, silently skipping blank lines and malformed JSON. Returns empty list if file doesn't exist.
- **Resilience**: Used for crash-safe ledger files; partial last line is dropped.
- **Callers**: Host lifecycle agent, tests.

**`append_jsonl(path: Path, record: dict) -> None`**
- Appends a single JSON record (compacted via `json.dumps(..., separators=(",", ":")`) followed by `\n`) to the file. Creates parent directories if needed.
- **Callers**: Host lifecycle agent, tests.

### `system_identity.py`

**`get_host_boot_id() -> str`**
- **Resolution order**: 1) `CRYPTOLAKE_TEST_BOOT_ID` env var (stripped), 2) `/proc/sys/kernel/random/boot_id` (Linux, stripped), 3) fallback `"unknown"`.
- **Callers**: Writer restart-gap classification, host lifecycle agent.

### `service_runner.py`

**`run_service(service_factory: Callable[[str], object], config_env: str = "CONFIG_PATH", default_config: str = "config/config.yaml") -> None`**
- **Semantics**: Calls `setup_logging()`, installs `uvloop`, loads config from env var (default path), instantiates service via factory, creates asyncio event loop, registers signal handlers (SIGTERM, SIGINT) that create a shutdown task, runs `service.start()` to completion, finally runs shutdown task if needed.
- **Callers**: Collector `main()` and writer `main()` entry points. Does not return; either runs until shutdown or raises exception.

## 4. Internal structure

**Call graph (within-module only):**
- `config.py`: `load_config()` → `_normalize_env_overrides()` → `_apply_env_overrides()`. Pydantic validators (`lowercase_symbols`, `auto_include_depth_snapshot`, `validate_retention_hours`) called during `model_validate()`.
- `envelope.py`: `create_data_envelope()` → `time.time_ns()`, `hashlib.sha256()`. `create_gap_envelope()` validates `reason`, optionally includes restart metadata. `add_broker_coordinates()` mutates and returns.
- `logging.py`: `setup_logging()` configures structlog once; `get_logger()` delegates to structlog. `_json_dumps()` called by JSON renderer.
- `service_runner.py`: `run_service()` → `setup_logging()`. Registers signal handlers; no explicit task graph.
- `jsonl.py`, `health_server.py`, `async_utils.py`, `system_identity.py`: self-contained, no internal calls.

**No god-objects** — each file is focused.

**No functions > 80 lines** — longest functions: `load_config` (25 lines), `create_gap_envelope` (27 lines), `run_service` (35 lines).

**Cyclomatic complexity**: Modest. `_apply_env_overrides()` has a nested loop; `load_config()` has conditional branches for env overrides. No deep nesting.

## 5. Concurrency surface

**Async functions**:
- `cancel_tasks(tasks)` — cancels and awaits tasks.
- `start_health_server(port, ready_check)` — starts aiohttp server, awaits runner setup and site start.
- `run_service()` calls `loop.run_until_complete(service.start())` and `loop.run_until_complete(shutdown_task)` — drives the event loop.

**Asyncio primitives used**:
- `asyncio.Task` — list of tasks passed to `cancel_tasks()`.
- `asyncio.create_task()` — created in collector/writer, cancelled by `cancel_tasks()`.
- `asyncio.gather(..., return_exceptions=True)` — in `cancel_tasks()`, tolerates exceptions.
- `asyncio.new_event_loop()` — in `service_runner.run_service()`.
- `loop.add_signal_handler()` — registers signal callbacks in `run_service()`.
- `aiohttp.web` — used in `health_server.py` for HTTP endpoints.
- `uvloop.install()` — in `service_runner.run_service()`.

**Blocking calls within common**:
- `yaml.safe_load()` in `load_config()` — must run on event loop at startup (synchronous config parse).
- `open(file, "r")` in `read_jsonl()` — blocking I/O, used in ledger readers (non-hot path).
- `open(file, "a")` in `append_jsonl()` — blocking I/O, used in ledger writes.
- `open(_BOOT_ID_PATH)` in `get_host_boot_id()` — blocking read (startup).

**No virtual thread awareness in Python** — asyncio is the concurrency model. Java translation must map async functions to virtual threads and blocking calls to natural thread blocking (no executors).

**Backpressure**: `health_server.py` has no explicit backpressure; endpoints are stateless HTTP handlers.

## 6. External I/O

**Kafka**: None directly in `common` — envelope is the data contract (serialization/deserialization), not Kafka interaction.

**HTTP endpoints** (in `health_server.py`):
- `GET /health` → 200 always.
- `GET /ready` → 200 if all checks pass, 503 otherwise.
- `GET /metrics` → Prometheus metrics.

**WebSocket**: None in `common`.

**Filesystem**:
- `read_jsonl(path)` — reads JSONL ledger files from `path`.
- `append_jsonl(path, record)` — appends JSONL to `path` (creates parent dirs).
- `load_config(path)` — reads YAML config file.
- `get_host_boot_id()` — reads `/proc/sys/kernel/random/boot_id` (Linux-specific).

**Environment variables**:
- `HOST_DATA_DIR` — aliased to `writer.base_dir` in `load_config()`.
- `CRYPTOLAKE_TEST_BOOT_ID` — used in `get_host_boot_id()` for testing.
- `CONFIG_PATH` — default config file location in `run_service()`.
- All `MODULE__KEY` pattern env vars are parsed as config overrides.

**Database**: None in `common` — config only.

**Stdout**: Structured JSON logs via `structlog.PrintLoggerFactory(file=sys.stdout)`.

## 7. Data contracts

**Pydantic models** (all in `config.py`):

| Class | Fields | Nullable | Defaults | JSON output |
|-------|--------|----------|----------|-------------|
| `CryptoLakeConfig` | `database`, `exchanges`, `redpanda`, `writer`, `monitoring`, `collector` | No | — | — |
| `BinanceExchangeConfig` | `enabled`, `market`, `ws_base`, `rest_base`, `symbols`, `streams`, `writer_streams_override`, `depth`, `open_interest`, `collector_id` | `symbols` not null; `writer_streams_override` nullable | `enabled=True`, `market="usdm_futures"`, defaults for ws/rest URLs, `collector_id="binance-collector-01"` | Symbols lowercased; `depth_snapshot` auto-appended to `writer_streams_override` if `depth` present |
| `StreamsConfig` | 6 boolean flags (trades, depth, bookticker, funding_rate, liquidations, open_interest) | No | All True | — |
| `DepthConfig` | `update_speed`, `snapshot_interval`, `snapshot_overrides` | `snapshot_overrides` dict | Defaults: `"100ms"`, `"5m"`, empty dict | — |
| `MonitoringConfig` | `prometheus_port`, `webhook_url` | No | `8000`, `""` | — |
| `ProducerConfig` | `max_buffer`, `buffer_caps`, `default_stream_cap` | No | `100000`, dict with depth/trades caps, `10000` | — |
| `RedpandaConfig` | `brokers`, `retention_hours`, `producer` | No | `48`, factory default for producer | Validator requires `retention_hours >= 12` |
| `DatabaseConfig` | `url` | No | — | — |
| `WriterConfig` | `base_dir`, `rotation`, `compression`, `compression_level`, `checksum`, `flush_messages`, `flush_interval_seconds`, `gap_filter` | No | `"zstd"`, `3`, `"sha256"`, `10000`, `30`, factory for gap_filter | — |
| `GapFilterConfig` | `grace_period_seconds`, `snapshot_miss_grace_seconds` | No | `10.0`, `60.0` | — |
| `CollectorConfig` | `tap_output_dir` | Nullable | None | — |

**Envelopes** (in `envelope.py`):

**Data envelope** (dict with fields in insertion order):
```json
{
  "v": 1,
  "type": "data",
  "exchange": "binance",
  "symbol": "btcusdt",
  "stream": "trades",
  "received_at": <int nanoseconds>,
  "exchange_ts": <int milliseconds or nanoseconds from exchange>,
  "collector_session_id": "binance-collector-01_2026-04-18T12:34:56Z",
  "session_seq": <int>,
  "raw_text": <string, JSON text as received from exchange>,
  "raw_sha256": <hex string, 64 chars>
}
```

**Gap envelope** (dict):
```json
{
  "v": 1,
  "type": "gap",
  "exchange": "binance",
  "symbol": "btcusdt",
  "stream": "trades",
  "received_at": <int nanoseconds>,
  "collector_session_id": "...",
  "session_seq": <int>,
  "gap_start_ts": <int nanoseconds>,
  "gap_end_ts": <int nanoseconds>,
  "reason": <string from VALID_GAP_REASONS>,
  "detail": <string>,
  "component": <optional string or null>,
  "cause": <optional string or null>,
  "planned": <optional bool>,
  "classifier": <optional string>,
  "evidence": <optional dict>,
  "maintenance_id": <optional string>
}
```
(Optional restart metadata fields only appear if provided, not as nulls.)

**Broker coordinates** (mutated into envelope):
```json
{
  "_topic": "binance.trades",
  "_partition": <int>,
  "_offset": <int or -1 for synthetic>
}
```

**JSON serialization behavior**:
- Uses `orjson.dumps()` (compact, no spaces, preserves dict order).
- No trailing newline (caller must append `\n`).
- String field order fixed by Python dict insertion order.

**JSONL ledger records** (generic, in `host_lifecycle_agent` and tests):
```json
{
  "ts": <ISO 8601 UTC string>,
  "event_type": <string>,
  ... (event-specific fields)
}
```

## 8. Test catalog

### Unit tests

**`tests/unit/test_async_utils.py`** (2 tests):
- `test_cancel_tasks_cancels_and_clears` — verifies tasks are cancelled and list cleared.
- `test_cancel_tasks_handles_empty_list` — edge case: empty list.
- Scenario: Task cancellation and cleanup.

**`tests/unit/test_config.py`** (15 tests):
- `test_load_valid_config` — config loads and parses correctly.
- `test_config_symbols_are_lowercase` — symbol lowercasing validator.
- `test_config_all_streams_enabled` — all stream flags read from YAML.
- `test_config_depth_snapshot_override` — depth snapshot overrides parsed.
- `test_config_writer_defaults` — writer defaults applied.
- `test_config_retention_minimum_rejected` — validator rejects `retention_hours < 12`.
- `test_config_env_override` — environment variables override YAML.
- `test_host_data_dir_alias_overrides_writer_base_dir` — `HOST_DATA_DIR` env alias.
- `test_config_missing_file_raises` — `FileNotFoundError` for missing config.
- `test_disabled_stream_no_depth_snapshot` — when `depth` disabled, `depth_snapshot` not auto-added (renamed from original semantics).
- `test_enabled_stream_list` — `get_enabled_streams()` returns correct list.
- `test_writer_streams_override_auto_includes_depth_snapshot` — validator auto-appends.
- `test_writer_streams_override_no_duplicate_depth_snapshot` — idempotent.
- `test_writer_streams_override_none_by_default` — `None` by default, not empty list.
- `test_env_override_does_not_bleed_os_environ` — explicit env_overrides isolate from `os.environ`.
- Scenarios: Configuration loading, validation, environment override isolation.

**`tests/unit/test_envelope.py`** (15 tests):
- `test_create_data_envelope` — data envelope creation and field presence.
- `test_create_gap_envelope` — gap envelope creation.
- `test_data_envelope_raw_sha256_integrity` — SHA-256 over raw_text bytes.
- `test_envelope_received_at_is_nanoseconds` — `received_at` is `time.time_ns()` (nanosecond precision).
- `test_gap_reason_values` — `VALID_GAP_REASONS` contains expected strings.
- `test_create_restart_gap_envelope` — gap with optional restart metadata included.
- `test_restart_gap_envelope_optional_fields_omitted` — omitted optional fields don't appear as nulls.
- `test_restart_gap_envelope_partial_optional_fields` — only provided optional fields present.
- `test_non_restart_gap_ignores_extra_fields` — non-restart gaps don't have restart metadata.
- `test_checkpoint_lost_is_valid_gap_reason` — `"checkpoint_lost"` in valid reasons.
- `test_create_gap_envelope_with_checkpoint_lost` — creation with this reason.
- `test_gap_invalid_reason_raises` — ValueError on invalid reason.
- `test_serialize_data_envelope_to_bytes` — orjson serialization to bytes.
- `test_serialize_gap_envelope_to_bytes` — gap serialization.
- `test_deserialize_envelope_round_trip` — round-trip preserves data.
- `test_add_broker_coordinates` — coordinates added and returned.
- Scenarios: Envelope creation, optional field handling, serialization, gap reason validation.

**`tests/unit/test_logging.py`** (2 tests):
- `test_json_output` — logs are JSON objects with event name, bindings, timestamp, level.
- `test_warning_level_suppresses_info` — log level filtering works.
- Scenario: Structured JSON logging output.

**`tests/unit/test_system_identity.py`** (6 tests):
- `test_env_override_takes_precedence` — `CRYPTOLAKE_TEST_BOOT_ID` env var priority.
- `test_reads_proc_boot_id` — reads `/proc` on Linux.
- `test_strips_whitespace_from_boot_id` — whitespace handling.
- `test_fallback_when_proc_unavailable` — returns `"unknown"` on non-Linux.
- `test_fallback_on_permission_error` — handles permission errors gracefully.
- `test_return_type_is_str` — always returns string.
- `test_env_override_strips_whitespace` — env var also stripped.
- Scenarios: Boot ID resolution order, error handling, whitespace stripping.

**`tests/unit/test_host_lifecycle_agent.py`** (varies, multi-scenario):
- **Ledger I/O**: Read/write JSONL, partial line resilience, empty/nonexistent files.
- **Boot ID recording**: Calls `get_host_boot_id()`, writes record.
- **Pruning**: Removes events older than 7 days.
- **Docker event parsing**: Maps container start/stop/die events to ledger records, exit code handling, OOM detection.
- **Maintenance intent**: Records maintenance intent events.
- **Agent startup**: Boot ID + prune sequence.
- Note: This file tests the host lifecycle agent (scripts), not `common` directly, but uses `jsonl.py` and `system_identity.py` from `common`. Keep in test catalog as it exercises common APIs in integration context.
- Scenarios: Crash resilience (partial lines), event recording, lifecycle state management.

### Integration tests

**`tests/integration/test_redpanda_roundtrip.py`** (1 test):
- `test_produce_consume_envelope` — creates envelope, serializes, produces to Redpanda, consumes, deserializes, verifies round-trip.
- Uses: `create_data_envelope()`, `serialize_envelope()`, `deserialize_envelope()`.
- Requires: Testcontainers (Redpanda container).
- Scenario: Envelope serialization parity across Kafka round-trip.

## 9. Invariants touched (Tier 1 rules)

### Rule 1: `raw_text` captured before JSON parse
**Status**: Touched in `envelope.py:42-65` (data envelope creation).
**Reason**: `create_data_envelope()` accepts `raw_text` as a parameter (already extracted by caller); the module does not parse it. The caller (collector) is responsible for capturing raw bytes from WebSocket before parse. The envelope stores `raw_text` as-is and computes `raw_sha256` over `raw_text.encode()`. JSON serialization of the envelope preserves `raw_text` bytes via orjson's byte-identity contract.

### Rule 2: `raw_sha256` computed once at capture time
**Status**: Touched in `envelope.py:64`.
**Reason**: `create_data_envelope()` computes `raw_sha256 = hashlib.sha256(raw_text.encode()).hexdigest()` immediately at envelope creation time. Once set, it is never recomputed.

### Rule 3: Disabled streams emit zero artifacts
**Status**: Not applicable to `common`.
**Reason**: Stream enablement is a collector/writer concern; `common` only stores configuration (BinanceExchangeConfig.streams, get_enabled_streams()). The decision to emit or not is made at the application layer, not in the config module.

### Rule 4: Kafka consumer offsets committed only after file flush succeeds
**Status**: Not applicable to `common`.
**Reason**: This is a writer concern involving file I/O and Kafka commit orchestration. `common` provides envelope serialization and config; the writer owns commit timing.

### Rule 5: Every detected gap emits metric, log, AND archived gap record
**Status**: Touched in `envelope.py:68-118` (gap envelope creation).
**Reason**: `create_gap_envelope()` is the factory for gap records. The envelope format includes all required fields (reason, detail, gap_start_ts, gap_end_ts, optional restart metadata). Metrics and logging are caller concerns (writer), but the data contract is enforced here via reason validation.

### Rule 6: Recovery prefers replay from Kafka / exchange cursors over reconstruction
**Status**: Not applicable to `common`.
**Reason**: Recovery strategy is a writer algorithm concern, not a data contract or config matter.

### Rule 7: JSON codec must not re-order, re-quote, or re-format `raw_text`
**Status**: Touched in `envelope.py:121-122` (serialization).
**Reason**: Uses `orjson.dumps()` which preserves dict insertion order (Python 3.7+), emits no extra whitespace, and stores `raw_text` as a JSON string (no re-quoting or re-parsing). The `raw_text` field itself is a string and is serialized verbatim. Deserialization via `orjson.loads()` restores the dict without modification.

## 10. Port risks

### **B1 — Field order parity**
`envelope.py` relies on Python dict insertion order to match the canonical field order across serialization. **Java must use `@JsonPropertyOrder` annotation on the envelope record classes**, specifying the exact field sequence from `create_data_envelope()` and `create_gap_envelope()`. Deviation breaks byte-for-byte parity with Python archives.

### **B2 — orjson vs Jackson JSON formatting**
`envelope.py` uses `orjson.dumps()` which emits compact JSON with no spaces, ASCII-only strings, and no trailing newline. **Java's Jackson defaults are similar, but enable INDENT_OUTPUT or use a custom pretty-printer breaks parity**. Disable `SerializationFeature.INDENT_OUTPUT` explicitly. Lines are appended with `\n` by the caller, not the serializer.

### **B3 — `raw_text` string preservation**
Data envelopes store `raw_text` as a JSON string field. During round-trip serialization (envelope → JSON → envelope), the string must not be re-parsed or reformatted. **Jackson's `JsonNode` for raw_text is safe; use `String` type in record classes, never `JsonNode` for that field**.

### **B4 — `raw_sha256` computation scope**
`raw_sha256` is computed in `create_data_envelope()` over `raw_text.encode()`. **The Java translation must encode the `raw_text` field to bytes using `StandardCharsets.UTF_8` and compute SHA-256 over those bytes, matching Python's behavior**. This is load-bearing for gap detection algorithms that verify archive integrity.

### **E1 — Large numeric fields**
`session_seq` and numeric exchange fields can exceed 2^31. **Use `long` for `session_seq` and any Binance IDs in envelopes**. `_partition` in envelopes is `int` (Kafka limit), `_offset` is `long`.

### **E2 — Nanosecond timestamp precision**
`received_at` and `gap_start_ts` / `gap_end_ts` are nanosecond integers from `time.time_ns()`. **Java must use `Instant.now().getEpochSecond() * 1_000_000_000L + Instant.now().getNano()` to match Python's nanosecond precision**. Truncating to milliseconds loses Tier 1 invariant #2 (raw_sha256 computed once).

### **F1 — ISO 8601 UTC timestamps**
Config and logging code uses ISO 8601 strings for timestamps. **In Java, use `Instant.toString()` (emits `2026-04-18T12:34:56.123Z`) for config serialization and logging, not `LocalDateTime` which loses timezone info**. Python's `datetime.isoformat()` for UTC-aware datetimes matches this.

### **G2 — ConfigValidationError as RuntimeException**
`ConfigValidationError` is raised on config validation failure and is not caught at the module boundary. **Java must declare this as a custom `CryptoLakeConfigException extends RuntimeException`** (Tier 2 rule: no checked exceptions across module boundaries).

### **H1 — Structured logging field names**
`logging.py` uses structlog which outputs JSON with event name as `"event"` key and all KV pairs as top-level fields. **Java's SLF4J + Logstash encoder must use `StructuredArguments.kv()` to match field-per-key semantics**. The event name is the log message, not a field.

### **H2 — UTC-only logging timestamps**
`setup_logging()` configures `TimeStamper(fmt="iso", utc=True)`. **Java must configure SLF4J with `UTC` timezone for timestamp formatting, not local time**. Logstash encoder default is UTC if not overridden.

### **I3 — JSONL ledger crash resilience**
`jsonl.py` uses `open(path, "a")` and single `write()` per line to minimize crash window. `read_jsonl()` tolerates and drops partial JSON lines. **Java must use `Files.write(..., StandardOpenOption.APPEND)` or direct `FileOutputStream.write()` per record** and handle partial lines gracefully on read.

### **L2 — Testcontainers for Redpanda**
`test_redpanda_roundtrip.py` uses `testcontainers` with Redpanda container. **Java tests must use `@Testcontainers` + `@Container` with `KafkaContainer` compatible with Redpanda** (use `asCompatibleSubstituteFor("confluentinc/cp-kafka")`). Avoid hard-coded ports; use `getBootstrapServers()`.

### **M6 — Gap reason vocabulary**
Gap envelopes validate `reason` against a fixed frozenset. **Java must keep gap reasons as a `Set<String>` or sealed interface, NOT an enum**. Enum deserialization would crash on unknown future reasons added to the Python archives; enums are too rigid.

### **M7 — Collector session ID format**
`collector_session_id` is formatted as `{collector_id}_{ISO-8601-UTC}` without fractional seconds. **Java must use `DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)`, not `Instant.toString()`** which includes milliseconds. This is user-visible (archive directory names, PG primary keys).

### **Config environment override isolation**
`load_config()` with explicit `env_overrides` dict must NOT read from `os.environ`. Tests verify this isolation. **Java must accept environment overrides as a `Map<String, String>` parameter and not consult `System.getenv()` when the map is provided**. When no map is provided, read from `System.getenv()` (normal bootstrap case).

## 11. Rule compliance

### Rule 1 — `raw_text` captured before parse
**Surfaced in §9**: `raw_text` is parameter to `create_data_envelope()`, not parsed by the module. Caller responsibility (collector).

### Rule 2 — `raw_sha256` computed once
**Surfaced in §9**: Computed in `create_data_envelope()` at envelope creation time, never recomputed.

### Rule 3 — Disabled streams emit zero artifacts
**Not applicable to this module**: Stream enablement and artifact emission are application-layer decisions, not `common` concerns.

### Rule 4 — Offsets committed after file flush
**Not applicable to this module**: Commit timing is a writer concern; `common` does not orchestrate flushes or commits.

### Rule 5 — Every gap emits metric, log, and record
**Surfaced in §9**: `create_gap_envelope()` is the factory; reason validation ensures well-formed gap records. Metrics and logging are caller concerns.

### Rule 6 — Recovery prefers replay over reconstruction
**Not applicable to this module**: Recovery strategy is a writer algorithm, not a data contract or config concern.

### Rule 7 — JSON codec preserves `raw_text` bytes
**Surfaced in §9**: `orjson.dumps()` + field order preservation + no re-parsing ensures byte identity.

