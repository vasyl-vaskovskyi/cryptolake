---
module: writer
status: complete
produced_by: analyst
python_files:
  - src/writer/__init__.py
  - src/writer/buffer_manager.py
  - src/writer/compressor.py
  - src/writer/consumer.py
  - src/writer/failover.py
  - src/writer/file_rotator.py
  - src/writer/host_lifecycle_reader.py
  - src/writer/main.py
  - src/writer/metrics.py
  - src/writer/restart_gap_classifier.py
  - src/writer/state_manager.py
python_test_files:
  - tests/helpers.py (not writer-specific)
  - tests/integration/test_writer_rotation.py
  - tests/unit/test_buffer_manager.py
  - tests/unit/test_compressor.py
  - tests/unit/test_consumer_failover_integration.py
  - tests/unit/test_failover_metrics.py
  - tests/unit/test_file_rotator.py
  - tests/unit/test_gap_records_written_metric.py
  - tests/unit/test_host_lifecycle_reader.py
  - tests/unit/test_hours_sealed_metrics.py
  - tests/unit/test_restart_gap_classifier.py
  - tests/unit/test_state_manager.py
---

## 1. Module summary

The writer is the load-bearing durability module for CryptoLake. It consumes data envelopes from Redpanda Kafka, buffers them in memory, compresses with zstandard, and writes immutable JSONL files sealed with SHA-256 sidecars. Recovery from crashes is achieved via PostgreSQL-backed stream checkpoints, file-state truncation, and durable Kafka offset commits only after successful fsync. A real-time failover manager switches to backup topics when the primary goes silent (5s timeout), draining backup before switchback ensures archive continuity. Gap detection runs at two levels: one-time recovery gap check per stream and runtime session-change detection. The writer is the upstream consumer of collector data and sole provider of immutable hourly-sealed archive files to verify and reconstruct pipelines.

## 2. File inventory

| File | Lines | Role | Public Symbols |
|------|-------|------|-----------------|
| `__init__.py` | 2 | Package init | (none) |
| `buffer_manager.py` | 140 | IO / buffering | BufferManager, FlushResult, CheckpointMeta |
| `compressor.py` | 21 | IO / compression | ZstdFrameCompressor |
| `consumer.py` | 1489 | Main orchestrator | WriterConsumer |
| `failover.py` | 422 | Failover / coverage | FailoverManager, CoverageFilter, extract_natural_key() |
| `file_rotator.py` | 66 | File naming / IO | FileTarget, build_file_path(), sidecar_path(), write_sha256_sidecar(), compute_sha256() |
| `host_lifecycle_reader.py` | 119 | Recovery / config | HostLifecycleEvidence, load_host_evidence() |
| `main.py` | 139 | Entry point | Writer, main() |
| `metrics.py` | 139 | Instrumentation | 27 Prometheus metrics (Counter, Gauge, Histogram) |
| `restart_gap_classifier.py` | 291 | Recovery classification | classify_restart_gap() (pure function) |
| `state_manager.py` | 487 | Database / recovery | StateManager, FileState, StreamCheckpoint, ComponentRuntimeState, MaintenanceIntent |

## 3. Public API surface

### Writer (main.py:25-139)
- `__init__(config_path)`: Loads YAML config, instantiates all components.
- `async start()`: Connects state manager, starts consumer, registers runtime state, runs consume_loop and health server.
- `async shutdown()`: Marks clean shutdown, stops consumer, closes DB.
- `async _ready_checks()`: Returns liveness/readiness dict.

### WriterConsumer (consumer.py:43-1489)
- `__init__(...brokers, topics, group_id, buffer_manager, compressor, state_manager, base_dir, host_evidence, gap_filter_grace_period_seconds, gap_filter_snapshot_miss_grace_seconds)`: Initializes consumer state.
- `async start()`: Subscribes to Kafka, loads recovery state, applies seeks.
- `async consume_loop()`: Main event loop: poll, gap detect, buffer, rotate, flush.
- `async stop()`: Flushes pending gaps, rotates all hours, closes consumer.
- `_check_recovery_gap(envelope) -> dict | None`: One-time per-stream recovery check.
- `async _check_session_change(envelope) -> dict | None`: Runtime session change detection.
- `_maybe_close_depth_recovery_gap(envelope) -> dict | None`: Depth recovery state machine.
- `async _write_and_save(results)`: Normal flush: write → fsync → PG → commit.
- `async _rotate_file(file_key, date, hour)`: Hourly seal: flush → write → seal → PG → commit.
- `async _rotate_hour()`: Shutdown seal (skip current hour).
- `_write_to_disk(results) -> (FileState[], gaps)`: Compress and fsync (no PG commit).
- `async _commit_state(states, results, start)`: Atomic PG save, then async offset commit. **CRITICAL: Tier 1 #4 enforced here.**
- `is_connected() -> bool`: Returns true if consumer assigned.

### BufferManager (buffer_manager.py:46-141)
- `__init__(base_dir, flush_messages=10000, flush_interval_seconds=30)`: Init routing.
- `add(envelope) -> list[FlushResult] | None`: Routes to buffer, triggers flush on threshold.
- `flush_key(file_key) -> list[FlushResult]`: Flush matching (ex, sym, st).
- `flush_all() -> list[FlushResult]`: Flush all non-empty buffers.
- `route(envelope) -> FileTarget`: Extract (ex, sym, st, date, hour) from received_at.

### ZstdFrameCompressor (compressor.py:6-22)
- `__init__(level=3)`: Create compressor.
- `compress_frame(lines) -> bytes`: Concatenate and compress lines into one frame.

### FailoverManager (failover.py:45-205)
- `__init__(brokers, primary_topics, backup_prefix="backup.", silence_timeout=5.0, coverage_filter)`: Init.
- `activate()`: Create backup consumer, subscribe backup topics.
- `deactivate()`: Close backup, record metrics.
- `should_activate() -> bool`: True if primary silent > 5s.
- `track_record(envelope)`: Extract natural key, store.
- `should_filter(envelope) -> bool`: True if backup key <= last primary key.
- `begin_switchback()`: Activate switchback filter.
- `check_switchback_filter(envelope) -> bool`: True if primary key <= last backup key (drop).
- `cleanup()`: Close backup on shutdown.

### CoverageFilter (failover.py:207-422)
- `__init__(grace_period_seconds, snapshot_miss_grace_seconds)`: Init coverage tracking.
- `handle_data(source, envelope)`: Record data arrival, sweep covered gaps.
- `handle_gap(source, envelope) -> bool`: True if gap suppressed or parked.
- `sweep_expired() -> list[dict]`: Return gaps past grace period.
- `flush_all_pending() -> list[dict]`: Return all pending gaps (shutdown).

### StateManager (state_manager.py:71-487)
Data classes: FileState, StreamCheckpoint, ComponentRuntimeState, MaintenanceIntent.
- `async connect()`: psycopg connection, create tables.
- `async close()`: Close connection.
- `async load_all_states() -> dict`: Load file states for recovery.
- `async load_stream_checkpoints() -> dict`: Load per-stream checkpoints.
- `async save_states_and_checkpoints(states, checkpoints)`: Atomic upsert.
- `async upsert_component_runtime(state)`: Record component start/heartbeat.
- `async mark_component_clean_shutdown(...)`: Mark clean shutdown.
- `async load_latest_component_states()`: Load latest per component.
- `async load_component_state_by_instance(component, instance_id)`: Lookup for classification.
- `async load_active_maintenance_intent()`: Load active maintenance intent.

### restart_gap_classifier (restart_gap_classifier.py:37-291)
- `classify_restart_gap(previous_boot_id, current_boot_id, previous_session_id, current_session_id, collector_clean_shutdown, system_clean_shutdown, maintenance_intent, host_evidence) -> dict`: Pure function returns classification (component, cause, planned, evidence, maintenance_id).

### HostLifecycleEvidence (host_lifecycle_reader.py:18-119)
- `__init__(events)`: Index events by component.
- `has_component_die(component) -> bool`: Check for die event.
- `component_clean_exit(component) -> bool | None`: Return clean_exit flag.
- `load_host_evidence(ledger_path, window_start_iso, window_end_iso) -> HostLifecycleEvidence`: Read JSONL ledger.

### Metrics (metrics.py:1-140)
27 Prometheus metrics: messages_consumed_total, messages_skipped_total, files_rotated_total, bytes_written_total, compression_ratio, disk_usage_bytes, disk_usage_pct, session_gaps_detected_total, flush_duration_ms, write_errors_total, pg_commit_failures_total, kafka_commit_failures_total, gap_records_written_total, hours_sealed_today, hours_sealed_previous_day, failover_active, failover_total, failover_duration_seconds, failover_records_total, switchback_total, gap_envelopes_suppressed_total, gap_coalesced_total, gap_pending_size, consumer_lag.

## 4. Internal structure

**WriterConsumer is 1489 lines with ~28 methods and 15+ state dicts.** Cyclomatic hotspots:

- `consume_loop()` (lines 924-1115, ~200 lines, cyclomatic >15): Tight event loop with nested conditions for normal vs failover, backup drain, switchback. Multiple early returns and state checks. God-object risk: owns primary→backup decision, gap filtering, session tracking, rotation triggers.

- `_check_recovery_gap()` (lines 302-564, 262 lines, cyclomatic ~12): Recovery gap detection (no checkpoint vs checkpoint), archive file reading, classification, depth-recovery state setup. Mixes checkpoint loading, file I/O, gap emission.

- `_commit_state()` (lines 1354-1434, 80 lines): Atomic PG commit of file states and checkpoints, then Kafka offset commit. Critical ordering enforced here per Tier 1 #4.

**Natural split boundaries for Java port**:

1. **ConsumerLoopCoordinator**: Extract polling, failover decision, backup drain, switchback logic. Owns buffer manager feed, rotation triggers, metric updates.

2. **GapDetector**: Extract recovery gap check, session change detection, depth recovery state machine. Owns checkpoint cache, durable state queries.

3. **FileRotationManager**: Extract hourly seal logic, file path resolution, sidecar generation, seal-before-commit ordering. Owns sealed file tracking.

4. **OffsetCommitCoordinator**: Extract Kafka offset commit, PG state save ordering, retries. Enforce Tier 1 #4.

Each would be <400 lines; WriterConsumer becomes thin orchestrator.

**State dicts** (all updated inside event loop, safe due to GIL):

- `_last_session[(ex,sym,st)] = (session_id, received_at)`: Runtime session change.
- `_durable_checkpoints[(ex,sym,st)] = StreamCheckpoint`: Recovery cache.
- `_recovery_done = set[(ex,sym,st)]`: One-time marker.
- `_recovery_gap_emitted = set[(ex,sym,st)]`: Suppress duplicate gap.
- `_depth_recovery_pending[(ex,sym)] = {first_diff_ts, candidate_lid, candidate_ts}`: Depth state machine.
- `_sealed_files = set[Path]`: Sealed .jsonl.zst paths.
- `_late_seq[Path] = int`: Late-arrival sequence counters.
- `_recovery_high_water[(topic,part,path)] = offset`: Dedup marks.
- `_assigned_partitions = set[(topic, part)]`: Current assignment.
- `_pending_seeks[(topic, part)] = offset`: Computed seek targets.

**Concurrency**: asyncio + confluent_kafka (thread-based). Consumer poll via executor. State dicts updated only in event loop (GIL safe). Metrics are thread-safe.

## 5. Concurrency surface

**Async methods**: Writer.start(), Writer.shutdown(), WriterConsumer.start(), WriterConsumer.stop(), WriterConsumer.consume_loop(), WriterConsumer._handle_gap_detection(), WriterConsumer._handle_rotation_and_buffer(), WriterConsumer._rotate_file(), WriterConsumer._rotate_hour(), WriterConsumer._flush_and_commit(), WriterConsumer._write_and_save(), WriterConsumer._commit_state(), WriterConsumer._check_session_change(), StateManager.connect(), StateManager.load_*(), StateManager.save_states_and_checkpoints(), StateManager.upsert_component_runtime(), StateManager.mark_component_clean_shutdown().

**Asyncio primitives**: asyncio.get_running_loop(), loop.run_in_executor(None, fn, *args) [Kafka poll, backup poll], loop.create_task(coro) [fire-and-forget gap flush], asyncio.gather(*coros) [consume_loop + health server], asyncio.sleep(seconds) [backoff].

**Blocking calls**: consumer.poll(timeout) in executor (1.0s or 0.5s), consumer.subscribe() [rebalance], consumer.commit(asynchronous=True) [async callback], file I/O [open, write, fsync in _write_to_disk], PostgreSQL queries [async via psycopg].

**Backpressure**: Buffer flush at 10k messages or 30s. Failover drain explicit 10s timeout with 3 empty poll exit. Coverage filter pending queue unbounded but gated by grace period. No backpressure if disk is slow; consume loop stalls during fsync.

## 6. External I/O

**Kafka (Redpanda)**:
- Primary topics: `binance.<stream>` (e.g., binance.trades, binance.depth).
- Backup topics: `backup.<stream>` (failover only, activated after 5s silence).
- Mode: enable.auto.commit=false, manual offset commit after fsync.
- Order enforced: write → fsync → PG → commit. Callback logs errors to kafka_commit_failures_total metric.

**PostgreSQL**:
- `writer_file_state(topic, partition, file_path, high_water_offset, file_byte_size, updated_at)`: PK (topic, partition, file_path). Upserted per flush.
- `stream_checkpoint(exchange, symbol, stream, last_received_at, last_collector_session_id, last_gap_reason, updated_at)`: PK (ex,sym,st). Upserted per flush from FlushResult checkpoint metadata.
- `component_runtime_state(component, instance_id, host_boot_id, started_at, last_heartbeat_at, clean_shutdown_at, planned_shutdown, maintenance_id, updated_at)`: PK (component, instance_id). Upserted on start, updated on clean shutdown.
- `maintenance_intent(maintenance_id, scope, planned_by, reason, created_at, expires_at, consumed_at)`: PK maintenance_id. Queried for active intents during classification.
- Atomicity: File states and checkpoints in single transaction. Retry with backoff (exponential 2^attempt, max 3) on failure.

**Filesystem**:
- Base: `config.writer.base_dir` (typically `/data/cryptolake/archive`).
- Archive: `{base}/{exchange}/{symbol}/{stream}/{YYYY-MM-DD}/hour-{H}.jsonl.zst`.
- Late-arrival: `{base}/{exchange}/{symbol}/{stream}/{YYYY-MM-DD}/hour-{H}.late-{seq}.jsonl.zst`.
- Sidecar: `{base}/{exchange}/{symbol}/{stream}/{YYYY-MM-DD}/hour-{H}.jsonl.zst.sha256`.
- Write: open("ab"), write compressed frame, flush(), os.fsync(fileno()). On partial failure, truncate back and retry fsync.
- Seal: Compute SHA-256 (chunked read), write sidecar `{digest}  {filename}\n`.
- Read (recovery): Scan for uncommitted .zst files without .sha256 sidecar and delete. Read last data envelope from sealed files (reversed JSONL, skip gap envelopes).
- Truncate (recovery): On startup, if actual file size > PG recorded size, truncate with fsync.

**Host lifecycle ledger**:
- Path: `os.environ.get("LIFECYCLE_LEDGER_PATH", "/data/.cryptolake/lifecycle/events.jsonl")`.
- Format: JSONL, each line {event_type: container_die|container_stop|maintenance_intent, ts: ISO, component, clean_exit}.
- Read once at startup via load_host_evidence(ledger_path, window_start_iso).

**HTTP health server**:
- Port: config.monitoring.prometheus_port + 1 (typically 9091).
- Endpoint: /health.
- Response: {consumer_connected: bool, storage_writable: bool}.
- Runs in parallel via asyncio.gather().

## 7. Data contracts

**Envelope** (deserialized from Kafka):
```
v: int, type: "data"|"gap", exchange: str, symbol: str, stream: str,
received_at: int (ns), exchange_ts: int (ns),
collector_session_id: str, session_seq: int,
raw_text: bytes|str, raw_sha256: str,
[if gap] reason: str, gap_start_ts: int, gap_end_ts: int, detail: str,
[optional] component: str, cause: str, planned: bool, classifier: str, evidence: list[str], maintenance_id: str
```

**Writer-added fields**: `_topic`, `_partition`, `_offset`, `_source` (optional, "backup" if from failover).

**FileTarget** (buffer routing):
```
exchange: str, symbol: str, stream: str, date: str (YYYY-MM-DD), hour: int (0-23)
```

**FlushResult** (buffer → disk):
```
target: FileTarget, file_path: Path, lines: list[bytes], high_water_offset: int,
partition: int, count: int, checkpoint_meta: CheckpointMeta | None, has_backup_source: bool
```

**CheckpointMeta**:
```
last_received_at: int, last_collector_session_id: str, last_session_seq: int, stream_key: tuple[str,str,str]
```

**Serialization**: JSONL via orjson.dumps(envelope) + b"\n". Orjson output: compact, ASCII-only, preserves insertion order. **Tier 1 #7**: No re-ordering, re-quoting, re-formatting.

**Compression**: zstd with independent frames. Each flush = one frame. Concatenating frames = valid zstd file.

**State tables**:
- FileState: topic, partition, high_water_offset, file_path, file_byte_size, updated_at.
- StreamCheckpoint: exchange, symbol, stream, last_received_at, last_collector_session_id, last_gap_reason, updated_at.
- ComponentRuntimeState: component, instance_id, host_boot_id, started_at, last_heartbeat_at, clean_shutdown_at, planned_shutdown, maintenance_id, updated_at.
- MaintenanceIntent: maintenance_id, scope, planned_by, reason, created_at, expires_at, consumed_at.

## 8. Test catalog

**Unit tests** (~80 total):
- test_buffer_manager.py (5 tests): routing, flush threshold, key composition.
- test_compressor.py (3 tests): frame compression, concatenation.
- test_consumer_failover_integration.py (20 tests): failover activation, backup consumption, switchback filtering, natural key tracking.
- test_failover_metrics.py (2 tests): activation/deactivation metrics, duration.
- test_file_rotator.py (6 tests): path construction, sidecar, SHA-256.
- test_gap_records_written_metric.py (1 test): gap metric emission.
- test_host_lifecycle_reader.py (10 tests): ledger parsing, window filtering, component die/stop, clean_exit.
- test_hours_sealed_metrics.py (1 test): hourly seal metrics.
- test_restart_gap_classifier.py (20 tests): classification matrix, boot ID, session, intent, Phase 2 promotion.
- test_state_manager.py (12 tests): table creation, upsert, load, atomic transaction, retry.

**Integration tests** (~2 total):
- test_writer_rotation.py (2 tests): hourly rotation, sealing, sidecar on boundary.

**Chaos tests** (16 bash-based, integration-level, to port as JUnit 5 + Testcontainers):
- 1_collector_unclean_exit: Detects session_id change, emits restart_gap.
- 2_buffer_overflow_recovery: Kafka buffer exhaustion, re-consume on restart.
- 3_writer_crash_before_commit: File truncation, Kafka re-delivery.
- 4_fill_disk: ENOSPC, write_error gap, recovery.
- 5_depth_reconnect_inflight: Diffs during reconnect, recovery_depth_anchor gap.
- 6_full_stack_restart_gap: Both components restart, classification = system.
- 7_host_reboot_restart_gap: Boot ID change, classification = host, host_reboot.
- 8_ws_disconnect: Primary silent 5s, backup activated, switchback.
- 9_snapshot_poll_miss: REST polled stream gap, coverage filter grace.
- 10_planned_collector_restart: Maintenance intent, clean shutdown, planned.
- 11_corrupt_message: Deserialization error gap, consumer continues.
- 12_pg_kill_during_commit: PG down, commit fails, offsets NOT committed.
- 13_rapid_restart_storm: Multiple restarts, recovery gap dedup (one-time per stream).
- 14_pg_outage_then_crash: PG down, retry backoff, eventual recovery.
- 15_redpanda_leader_change: Broker metadata refresh, rebalance, no loss.
- 16_collector_failover_to_backup: Primary collector dies, backup activated, coverage filter suppresses redundant gaps.

## 9. Invariants touched (Tier 1 rules)

**Rule 1**: raw_text captured before parse. **Not applicable to writer.** Invariant enforced at collector. Writer preserves raw_text.

**Rule 2**: raw_sha256 computed at capture. **Not applicable to writer.** Enforced at collector. Writer preserves raw_sha256.

**Rule 3**: Disabled streams emit zero artifacts. **Touched in main.py:29-31, consumer.py:257.** Disabled streams are not subscribed; no envelopes arrive; no files created. Invariant preserved.

**Rule 4**: Offsets committed after fsync. **Touched in consumer.py:_commit_state() lines 1354-1425.** Order enforced: (1) _write_to_disk() performs write + fsync, (2) state_manager.save_states_and_checkpoints() atomically persists file states and checkpoints, (3) consumer.commit() asynchronously commits offsets ONLY AFTER PG state is durable. No offset commit if PG fails (early return, line 1410). **Tier 1 #4 satisfied.**

**Rule 5**: Every gap emits metric, log, archived record. **Touched in multiple places:** _check_recovery_gap() emits session_gaps_detected_total + log + gap_envelope. _check_session_change() emits session_gaps_detected_total + log + gap_envelope. _maybe_close_depth_recovery_gap() returns gap_envelope. _deserialize_and_stamp() emits messages_skipped_total + error log + gap_envelope. _write_to_disk() counts gap_records_written_total per reason. CoverageFilter emits gap_envelopes_suppressed_total or gap_coalesced_total. **Invariant satisfied.**

**Rule 6**: Recovery prefers Kafka replay over inferred reconstruction. **Touched in _check_recovery_gap() and failover.py.** Primary path: durable checkpoint (load_stream_checkpoints) → seek target (consumer.py:202-208) → subscribe with seek (consumer.py:257, _on_assign applies seeks) → re-consume from Kafka. Reconstruction fallback: only if no durable checkpoint (lines 328-406), read last data envelope from archive files to infer gap bounds. Failover: primary always preferred; backup only after 5s silence (failover.py:85-91); switchback aggressive (drain backup until caught up, then deactivate). **Invariant satisfied.**

**Rule 7**: JSON codec preserves raw_text. **Touched in buffer_manager.py:109.** Envelopes serialized via orjson.dumps(env) + b"\n". Orjson preserves insertion order; raw_text is opaque field (already string at writer, captured at collector as bytes). Round-trip serialization does not alter it. Writer does not re-parse raw_text. **Invariant satisfied.**

## 10. Port risks

**Risk 1: Kafka offset commit ordering (Tier 1 #4 implementation)** — Enforce write → fsync → PG save → offset commit. Must call commitSync() after final flush during shutdown. **Tier 5: C3**.

**Risk 2: File write atomicity on partial failure** — Truncate + fsync may not be atomic on all filesystems. Consider temp file + atomic rename. **Tier 5: I3**.

**Risk 3: Zstandard frame concatenation and crash recovery** — Partial frames must raise exception on Java decompressor (not silently truncate). Test on target filesystem. **Tier 5: I3**.

**Risk 4: asyncio → virtual threads + executor** — Entire consume loop must run ON a virtual thread, not platform thread. Use Executors.newVirtualThreadPerTaskExecutor() or Thread.ofVirtual(). **Tier 5: A2**.

**Risk 5: State dict concurrency** — Event loop isolation (GIL safe in Python). Java virtual threads require explicit synchronization (ReentrantLock) if multiple threads access state dicts. Simpler: keep state in single ConsumerLoopRunnable on one virtual thread. **Tier 5: A5**.

**Risk 6: PostgreSQL transaction atomicity and retry logic** — Exponential backoff (0s, 2s, 4s). If PG down >4s, writer stops consuming and backs up. Consider jitter and longer timeouts. **Tier 5: A3, transaction retries**.

**Risk 7: Failover switchback filtering and natural key extraction** — Requires parsing raw_text (JSON parse). If malformed, extract_natural_key returns None, message not filtered. Coverage filter catches duplicates at write time. Extract natural key from JsonNode without re-serialization. **Tier 5: B4**.

**Risk 8: Depth recovery anchor gap state machine** — In-memory state lost on crash. Consider persisting in PG or marking already-emitted in checkpoint. **Tier 5: A3 (timeouts)**.

**Risk 9: Circular dependency in deserialize_envelope** — Outside writer module (src.common.envelope). If common module changes, writer fails silently. Define strict envelope schema (dataclass/record) with validation. Test round-trip serialization. **Tier 5: B3**.

**Risk 10: Coverage filter grace period vs actual recovery** — If both collectors down, grace expires and gap is written; primary recovers and re-sends window → duplicates. Verify pipeline must detect/handle. Grace periods are tuning parameters; document in Java port as configurable. **Tier 5: C2**.

**Risk 11: Sealed file discovery scan on large archives** — rglob("*.sha256") scans entire tree. For millions of files, O(N) scan takes minutes. Consider indexing in PG or limiting scan to recent months. **Tier 5: I3 (filesystem ops)**.

**Risk 12: Compression ratio metric update per flush** — Division by zero if compressed == 0 (unlikely but fragile). Guard exists but test empty flush results. **Tier 5: E1 (numerics)**.

**Risk 13: Offset commit partial failure semantics** — kafka-clients commitAsync(callback) invokes callback with exception if partition failed. librdkafka silently retries. Callback must handle per-partition errors. **Tier 5: C3**.

## 11. Rule compliance

| Tier 1 Rule | Status |
|------------|--------|
| 1. raw_text captured before parse | Not applicable to writer |
| 2. raw_sha256 computed at capture | Not applicable to writer |
| 3. Disabled streams emit zero artifacts | Surfaced in §9 |
| 4. Offsets committed after fsync | Surfaced in §9 |
| 5. Every gap emits metric, log, archived record | Surfaced in §9 |
| 6. Recovery prefers Kafka replay over reconstruction | Surfaced in §9 |
| 7. JSON codec preserves raw_text | Surfaced in §9 |
