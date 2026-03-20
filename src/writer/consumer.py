from __future__ import annotations

import asyncio
import datetime
import os
import shutil
import time
from pathlib import Path

import structlog

from src.common.envelope import (
    add_broker_coordinates,
    create_gap_envelope,
    deserialize_envelope,
    serialize_envelope,
)
from src.common.system_identity import get_host_boot_id
from src.writer import metrics as writer_metrics
from src.writer.buffer_manager import BufferManager, CheckpointMeta, FlushResult
from src.writer.compressor import ZstdFrameCompressor
from src.writer.file_rotator import (
    build_file_path, sidecar_path, write_sha256_sidecar,
)
from src.writer.host_lifecycle_reader import HostLifecycleEvidence
from src.writer.restart_gap_classifier import classify_restart_gap
from src.writer.state_manager import (
    ComponentRuntimeState,
    FileState,
    StreamCheckpoint,
    StateManager,
)

logger = structlog.get_logger()

# Default REST poll interval threshold (3x the configured poll interval)
# to account for jitter. If time delta > this, it's a restart gap.
_DEFAULT_REST_POLL_INTERVAL_NS = 5 * 60 * 1_000_000_000  # 5 minutes in nanoseconds
_REST_POLL_GAP_MULTIPLIER = 3  # Must exceed 3x poll interval to be a gap


class WriterConsumer:
    """Consumes from Redpanda, buffers, compresses, and writes archive files.

    Key design decisions per spec:
    - Consumer.poll() runs in run_in_executor to avoid blocking the async event loop (spec 8.2)
    - Hourly rotation seals files with SHA-256 sidecar (spec 8.4)
    - Sealed files are never reopened; late arrivals go to spillover files (spec 8.2)
    - Flush sequence: write -> fsync -> PG state -> commit offsets (spec 8.3)
    - Recovery uses durable stream checkpoints from PG, not in-memory _last_session
    """

    def __init__(
        self,
        brokers: list[str],
        topics: list[str],
        group_id: str,
        buffer_manager: BufferManager,
        compressor: ZstdFrameCompressor,
        state_manager: StateManager,
        base_dir: str,
        host_evidence: HostLifecycleEvidence | None = None,
    ):
        self.brokers = brokers
        self.topics = topics
        self.group_id = group_id
        self.buffer_manager = buffer_manager
        self.compressor = compressor
        self.state_manager = state_manager
        self.base_dir = base_dir
        self._consumer = None
        self._running = False
        self._assigned = False  # True once consumer receives partition assignment
        self._assigned_partitions: set[tuple[str, int]] = set()
        self._sealed_files: set[Path] = set()  # tracks sealed .jsonl.zst paths
        self._late_seq: dict[Path, int] = {}  # late-arrival sequence counters
        # Track last seen session per (exchange, symbol, stream) for RUNTIME gap detection
        self._last_session: dict[tuple[str, str, str], tuple[str, int]] = {}  # -> (session_id, received_at)

        # --- Durable recovery state ---
        # Loaded from PG on start; updated after each durable commit
        self._durable_checkpoints: dict[tuple[str, str, str], StreamCheckpoint] = {}
        # Tracks which streams have completed one-time recovery check
        self._recovery_done: set[tuple[str, str, str]] = set()
        # Boot ID and component state for classification
        self._current_boot_id: str = get_host_boot_id()
        self._previous_writer_state: ComponentRuntimeState | None = None
        self._previous_collector_state: ComponentRuntimeState | None = None
        self._maintenance_intent = None
        # Host lifecycle evidence (Phase 2) — loaded once at startup
        self._host_evidence: HostLifecycleEvidence | None = host_evidence
        # REST-polled stream gap threshold (3x configured poll interval in ns)
        self._rest_poll_interval_ns: int = _DEFAULT_REST_POLL_INTERVAL_NS

    async def start(self) -> None:
        from confluent_kafka import Consumer as KafkaConsumer

        self._consumer = KafkaConsumer({
            "bootstrap.servers": ",".join(self.brokers),
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })

        # Load state and seek to last known offsets
        states = await self.state_manager.load_all_states()
        self._recover_files(states)

        # Load durable stream checkpoints for recovery gap detection
        self._durable_checkpoints = await self.state_manager.load_stream_checkpoints()
        self._recovery_done = set()

        # Load component runtime states to determine previous boot ID and shutdown evidence.
        # SQL returns DISTINCT ON (component) ORDER BY started_at DESC — one row per component.
        component_states = await self.state_manager.load_latest_component_states()
        for cs in component_states:
            if cs.component == "writer":
                self._previous_writer_state = cs
            elif cs.component == "collector":
                self._previous_collector_state = cs

        # Load active maintenance intent (if any) for gap classification.
        self._maintenance_intent = await self.state_manager.load_active_maintenance_intent()

        logger.info(
            "recovery_state_loaded",
            checkpoints=len(self._durable_checkpoints),
            previous_boot_id=(
                self._previous_writer_state.host_boot_id
                if self._previous_writer_state else None
            ),
            current_boot_id=self._current_boot_id,
            maintenance_intent=(
                self._maintenance_intent.maintenance_id
                if self._maintenance_intent else None
            ),
        )

        # Discover already-sealed files (those with .sha256 sidecar)
        self._discover_sealed_files()

        # Compute seek targets from PG state BEFORE subscribe so they're
        # available when on_assign fires during the first poll().
        # We take the MINIMUM offset across all files for a (topic, partition)
        # to ensure no data is lost; re-consumed messages will be truncated
        # by _recover_files based on their recorded byte sizes.
        seek_map: dict[tuple[str, int], int] = {}
        for s in states.values():
            key = (s.topic, s.partition)
            target = s.high_water_offset + 1
            if key not in seek_map or target < seek_map[key]:
                seek_map[key] = target
        self._pending_seeks = seek_map
        # Track recovery high-water marks to prevent duplicates when resuming
        # from a MIN offset across multiple files in a partition (spec 8.2).
        self._recovery_high_water = {
            s.state_key: s.high_water_offset for s in states.values()
        }

        def _on_assign(consumer, partitions):
            self._assigned = True
            self._assigned_partitions = set(
                (tp.topic, tp.partition) for tp in partitions
            )
            # Apply pending seeks by modifying the TopicPartition objects
            # in the partitions list. The consumer uses these offsets
            # when performing the assignment (spec 8.2).
            if self._pending_seeks:
                for tp in partitions:
                    key = (tp.topic, tp.partition)
                    if key in self._pending_seeks:
                        tp.offset = self._pending_seeks[key]
                        logger.debug("consumer_set_initial_offset",
                                     topic=tp.topic,
                                     partition=tp.partition,
                                     offset=tp.offset)
                self._pending_seeks = {}
            logger.info("consumer_partitions_assigned", count=len(partitions))

        def _on_revoke(consumer, partitions):
            revoked = set((tp.topic, tp.partition) for tp in partitions)
            logger.info("consumer_partitions_revoked",
                        revoked=list(revoked),
                        intentional=not self._running)

            # Flush any buffered data before losing ownership
            # (best-effort -- we may lose the race)
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._flush_and_commit())
            except RuntimeError:
                pass

            if self._running:
                # Spontaneous revocation while we should be running -- crash to prevent split-brain
                self._running = False
                raise RuntimeError(
                    f"Writer exclusivity violated: partitions {revoked} revoked. "
                    "Another writer may have joined consumer group. Shutting down."
                )

        self._consumer.subscribe(self.topics, on_assign=_on_assign,
                                 on_revoke=_on_revoke)
        self._running = True

    def _recover_files(self, states: dict) -> None:
        """Truncate files to PostgreSQL-recorded byte sizes on startup."""
        for state in states.values():
            path = Path(state.file_path)
            if path.exists():
                actual_size = path.stat().st_size
                if actual_size > state.file_byte_size:
                    logger.warning("truncating_file", path=str(path),
                                   actual=actual_size, expected=state.file_byte_size)
                    with open(path, "r+b") as f:
                        f.truncate(state.file_byte_size)

    def _discover_sealed_files(self) -> None:
        """Scan base_dir for files that already have .sha256 sidecars."""
        base = Path(self.base_dir)
        if base.exists():
            for sc in base.rglob("*.jsonl.zst.sha256"):
                data_path = sc.with_suffix("")  # remove .sha256
                self._sealed_files.add(data_path)

    def _check_recovery_gap(self, envelope: dict) -> dict | None:
        """One-time per-stream recovery check using durable stream checkpoints.

        On first post-recovery envelope for each stream:
        1. Compare durable checkpoint vs incoming envelope
        2. Classify via restart_gap_classifier
        3. Return a synthetic restart_gap envelope (or None if no gap)

        This replaces the in-memory _last_session dict for recovery detection.
        The _last_session dict is still used for runtime detection after recovery.
        """
        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )

        # Only run once per stream after startup
        if stream_key in self._recovery_done:
            return None
        self._recovery_done.add(stream_key)

        # No durable checkpoint = first-ever run, no gap to detect
        checkpoint = self._durable_checkpoints.get(stream_key)
        if checkpoint is None:
            return None

        # Extract current envelope data
        current_session_id = envelope.get("collector_session_id", "")
        current_received_at = envelope.get("received_at", 0)
        previous_session_id = checkpoint.last_collector_session_id

        # Determine if there is a session change
        session_changed = current_session_id != previous_session_id

        # For REST-polled streams (same session), check time delta
        if not session_changed:
            # Parse the checkpoint's last_received_at (ISO format from PG)
            try:
                cp_dt = datetime.datetime.fromisoformat(checkpoint.last_received_at)
                if cp_dt.tzinfo is None:
                    cp_dt = cp_dt.replace(tzinfo=datetime.timezone.utc)
                cp_received_at_ns = int(cp_dt.timestamp() * 1_000_000_000)
            except (ValueError, TypeError):
                cp_received_at_ns = 0

            time_delta_ns = current_received_at - cp_received_at_ns
            gap_threshold_ns = self._rest_poll_interval_ns * _REST_POLL_GAP_MULTIPLIER

            if time_delta_ns <= gap_threshold_ns:
                # Within expected interval -- no gap
                return None
            # Time delta exceeds threshold -- this is a gap even without session change
            logger.warning(
                "rest_poll_time_gap_detected",
                stream_key=stream_key,
                delta_seconds=time_delta_ns / 1_000_000_000,
                threshold_seconds=gap_threshold_ns / 1_000_000_000,
            )

        # Gather evidence for classifier
        previous_boot_id = (
            self._previous_writer_state.host_boot_id
            if self._previous_writer_state else None
        )
        collector_clean_shutdown = (
            self._previous_collector_state is not None
            and self._previous_collector_state.clean_shutdown_at is not None
        )
        system_clean_shutdown = (
            self._previous_writer_state is not None
            and self._previous_writer_state.clean_shutdown_at is not None
            and collector_clean_shutdown
        )

        classification = classify_restart_gap(
            previous_boot_id=previous_boot_id,
            current_boot_id=self._current_boot_id,
            previous_session_id=previous_session_id,
            current_session_id=current_session_id,
            collector_clean_shutdown=collector_clean_shutdown,
            system_clean_shutdown=system_clean_shutdown,
            maintenance_intent=self._maintenance_intent,
            host_evidence=self._host_evidence,
        )

        # Compute gap_start_ts from checkpoint
        try:
            cp_dt = datetime.datetime.fromisoformat(checkpoint.last_received_at)
            if cp_dt.tzinfo is None:
                cp_dt = cp_dt.replace(tzinfo=datetime.timezone.utc)
            gap_start_ts = int(cp_dt.timestamp() * 1_000_000_000)
        except (ValueError, TypeError):
            gap_start_ts = 0

        exchange, symbol, stream = stream_key
        logger.warning(
            "recovery_gap_detected",
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            classification=classification,
        )
        writer_metrics.session_gaps_detected_total.labels(
            exchange=exchange, symbol=symbol, stream=stream,
        ).inc()

        return create_gap_envelope(
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            collector_session_id=current_session_id,
            session_seq=-1,
            gap_start_ts=gap_start_ts,
            gap_end_ts=current_received_at,
            reason="restart_gap",
            detail=(
                f"Writer recovery: {classification.get('cause', 'unknown')} "
                f"(session {previous_session_id} -> {current_session_id})"
            ),
            received_at=current_received_at,
            component=classification.get("component"),
            cause=classification.get("cause"),
            planned=classification.get("planned"),
            classifier=classification.get("classifier"),
            evidence=classification.get("evidence"),
            maintenance_id=classification.get("maintenance_id"),
        )

    def _check_session_change(self, envelope: dict) -> dict | None:
        """Detect collector session changes and return a gap envelope if one occurred.

        When the collector crashes (SIGKILL), it cannot emit gap records itself.
        The writer detects the session_id change and injects a restart_gap
        record into the archive to record the data loss window.

        This is the RUNTIME detection path (after initial recovery is done).
        """
        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )
        session_id = envelope.get("collector_session_id", "")
        received_at = envelope.get("received_at", 0)

        prev = self._last_session.get(stream_key)
        self._last_session[stream_key] = (session_id, received_at)

        if prev is None:
            return None

        prev_session_id, prev_received_at = prev
        if session_id == prev_session_id:
            return None

        exchange, symbol, stream = stream_key
        logger.warning(
            "collector_session_change_detected",
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            old_session=prev_session_id,
            new_session=session_id,
        )
        writer_metrics.session_gaps_detected_total.labels(
            exchange=exchange, symbol=symbol, stream=stream,
        ).inc()
        return create_gap_envelope(
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            collector_session_id=session_id,
            session_seq=-1,
            gap_start_ts=prev_received_at,
            gap_end_ts=received_at,
            reason="restart_gap",
            detail=f"Collector session changed: {prev_session_id} -> {session_id}",
            received_at=received_at,
            component="collector",
            cause="unclean_exit",
            planned=False,
            classifier="writer_runtime_v1",
            evidence=["collector_session_changed_at_runtime"],
        )

    def _resolve_file_path(self, file_path: Path) -> Path:
        """If file_path is sealed (has sidecar), return a late-arrival spillover path.

        Loops through late-N sequence numbers to skip any that are also
        sealed (e.g. after a restart where late-1 was already written and
        sealed in a prior run).
        """
        if file_path not in self._sealed_files and not sidecar_path(file_path).exists():
            return file_path
        self._sealed_files.add(file_path)
        seq = self._late_seq.get(file_path, 0)
        stem = file_path.stem.split(".")[0]  # "hour-14"
        while True:
            seq += 1
            late_name = f"{stem}.late-{seq}.jsonl.zst"
            late_path = file_path.parent / late_name
            if late_path not in self._sealed_files and not sidecar_path(late_path).exists():
                self._late_seq[file_path] = seq
                return late_path
            # This late file is also sealed; mark it and try the next one
            self._sealed_files.add(late_path)

    async def consume_loop(self) -> None:
        """Main consume loop. Polls in executor, buffers, flushes, and commits."""
        loop = asyncio.get_running_loop()
        last_flush_time = time.monotonic()
        # Track active (hour, date) per file key (exchange, symbol, stream).
        # Stores the previous message's hour and date so that at day boundaries
        # (23->00) we seal the correct previous-day file, not the new day's.
        active_hours: dict[tuple[str, str, str], tuple[int, str]] = {}

        assert self._consumer is not None, "call start() first"
        while self._running:
            # Non-blocking poll via executor (spec 8.2: avoid blocking uvloop)
            msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)

            if msg is None:
                if time.monotonic() - last_flush_time >= self.buffer_manager.flush_interval_seconds:
                    await self._flush_and_commit()
                    last_flush_time = time.monotonic()
                continue

            if msg.error():
                logger.error("consumer_error", error=str(msg.error()))
                continue

            # Deserialize and stamp broker coordinates
            # After msg.error() check, these are guaranteed non-None
            raw_value = msg.value()
            msg_topic = msg.topic()
            msg_partition = msg.partition()
            msg_offset = msg.offset()
            assert raw_value is not None and msg_topic is not None
            assert msg_partition is not None and msg_offset is not None
            envelope = deserialize_envelope(raw_value)
            envelope = add_broker_coordinates(
                envelope,
                topic=msg_topic,
                partition=msg_partition,
                offset=msg_offset,
            )

            # De-duplication during recovery: skip messages already in the archive (spec 8.2)
            target = self.buffer_manager._route(envelope)
            from src.writer.file_rotator import build_file_path
            base_path = build_file_path(
                self.buffer_manager.base_dir, target.exchange, target.symbol,
                target.stream, target.date, target.hour,
            )
            file_path = self._resolve_file_path(base_path)
            state_key = (msg_topic, msg_partition, str(file_path))
            if state_key in self._recovery_high_water:
                if msg_offset <= self._recovery_high_water[state_key]:
                    writer_metrics.messages_skipped_total.labels(
                        exchange=envelope.get("exchange", ""),
                        symbol=envelope.get("symbol", ""),
                        stream=envelope.get("stream", ""),
                    ).inc()
                    continue
                else:
                    # Once we pass the recorded high water for this file, we can stop checking
                    del self._recovery_high_water[state_key]

            writer_metrics.messages_consumed_total.labels(
                exchange=envelope.get("exchange", ""),
                symbol=envelope.get("symbol", ""),
                stream=envelope.get("stream", ""),
            ).inc()

            # Detect gaps for data envelopes
            if envelope.get("type") == "data":
                # One-time recovery gap check (uses durable checkpoints)
                recovery_gap = self._check_recovery_gap(envelope)
                if recovery_gap is not None:
                    recovery_gap = add_broker_coordinates(
                        recovery_gap,
                        topic=msg_topic,
                        partition=msg_partition,
                        offset=-1,  # synthetic record, not from Kafka
                    )
                    gap_results = self.buffer_manager.add(recovery_gap)
                    if gap_results:
                        await self._write_and_save(gap_results)
                        last_flush_time = time.monotonic()

                # Runtime session change detection (covers collector crash/SIGKILL
                # after initial recovery is done)
                gap_envelope = self._check_session_change(envelope)
                if gap_envelope is not None:
                    gap_envelope = add_broker_coordinates(
                        gap_envelope,
                        topic=msg_topic,
                        partition=msg_partition,
                        offset=-1,  # synthetic record, not from Kafka
                    )
                    gap_results = self.buffer_manager.add(gap_envelope)
                    if gap_results:
                        await self._write_and_save(gap_results)
                        last_flush_time = time.monotonic()

            # Per-file hourly rotation (spec 8.2: file routing by message received_at)
            msg_dt = datetime.datetime.fromtimestamp(
                envelope["received_at"] / 1_000_000_000,
                tz=datetime.timezone.utc,
            )
            current_hour = msg_dt.hour
            current_date = msg_dt.strftime("%Y-%m-%d")
            file_key = (envelope.get("exchange", ""),
                        envelope.get("symbol", ""),
                        envelope.get("stream", ""))
            prev = active_hours.get(file_key)
            if prev is not None:
                prev_hour, prev_date = prev
                if current_hour != prev_hour or current_date != prev_date:
                    # Seal previous file using the PREVIOUS date/hour,
                    # not the current message's date (critical at 23->00 day boundary)
                    await self._rotate_file(file_key, prev_date, prev_hour)
            active_hours[file_key] = (current_hour, current_date)

            # Add to buffer -- may trigger flush
            flush_results = self.buffer_manager.add(envelope)
            if flush_results:
                await self._write_and_save(flush_results)
                last_flush_time = time.monotonic()

    async def _rotate_file(
        self,
        file_key: tuple[str, str, str],
        date_str: str,
        hour: int,
    ) -> None:
        """Seal files for a specific stream that has crossed an hour boundary.
        Order per spec 8.4: flush -> write to disk -> seal (.sha256 sidecar) ->
        save PG state -> commit Kafka offsets."""
        exchange, symbol, stream = file_key
        logger.info("hourly_rotation_triggered", exchange=exchange,
                     symbol=symbol, stream=stream, hour=hour)

        # 1. Flush buffer for this specific file key
        results = self.buffer_manager.flush_key(file_key)

        start = time.monotonic()
        states: list[FileState] = []

        # 2. Write to disk + fsync (no offset commit yet)
        if results:
            states = self._write_to_disk(results)

        # 3. Seal all flushed file paths BEFORE committing offsets.
        # Use actual written paths from states (may differ from FlushResult
        # if _resolve_file_path redirected to a late spillover file).
        files_to_seal = set()
        if states:
            for s in states:
                files_to_seal.add(Path(s.file_path))
        # Always include the explicitly targeted file
        files_to_seal.add(build_file_path(
            self.base_dir, exchange, symbol, stream, date_str, hour))
        for file_path in files_to_seal:
            sc = sidecar_path(file_path)
            if file_path.exists() and not sc.exists() and file_path.stat().st_size > 0:
                write_sha256_sidecar(file_path, sc)
                self._sealed_files.add(file_path)
                writer_metrics.files_rotated_total.labels(
                    exchange=exchange, symbol=symbol, stream=stream,
                ).inc()
                logger.info("file_sealed", path=str(file_path))

        # 4. Now safe to commit -- sidecar is durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _rotate_hour(self) -> None:
        """Seal all active files (used during shutdown). For normal operation,
        use _rotate_file() which seals per-stream files individually.
        Order: flush -> write to disk -> seal all -> commit offsets (spec 8.4)."""
        logger.info("rotation_seal_all")
        start = time.monotonic()

        # 1. Flush all buffers and write to disk (no offset commit yet)
        results = self.buffer_manager.flush_all()
        states: list[FileState] = []
        if results:
            states = self._write_to_disk(results)

        # 2. Seal all active files BEFORE committing offsets
        base = Path(self.base_dir)
        for zst_file in base.rglob("*.jsonl.zst"):
            sc = sidecar_path(zst_file)
            if not sc.exists() and zst_file.stat().st_size > 0:
                write_sha256_sidecar(zst_file, sc)
                self._sealed_files.add(zst_file)
                writer_metrics.files_rotated_total.labels(
                    exchange="binance",
                    symbol=zst_file.parent.parent.parent.name,
                    stream=zst_file.parent.parent.name,
                ).inc()
                logger.info("file_sealed", path=str(zst_file))

        # 3. Now safe to commit -- all sidecars are durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _flush_and_commit(self) -> None:
        results = self.buffer_manager.flush_all()
        if results:
            await self._write_and_save(results)
        self._update_disk_metrics()
        self._update_consumer_lag()

    def _write_to_disk(self, results: list[FlushResult]) -> list[FileState]:
        """Write compressed frames to disk and fsync. Returns FileState list
        for later commit. Does NOT save PG state or commit Kafka offsets."""
        states: list[FileState] = []
        for result in results:
            file_path = self._resolve_file_path(result.file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            compressed = self.compressor.compress_frame(result.lines)
            with open(file_path, "ab") as f:
                f.write(compressed)
                f.flush()
                os.fsync(f.fileno())

            file_size = file_path.stat().st_size
            writer_metrics.bytes_written_total.labels(
                exchange=result.target.exchange,
                symbol=result.target.symbol,
                stream=result.target.stream,
            ).inc(len(compressed))

            raw_size = sum(len(line) for line in result.lines)
            if len(compressed) > 0:
                writer_metrics.compression_ratio.labels(
                    exchange=result.target.exchange,
                    stream=result.target.stream,
                ).set(raw_size / len(compressed))

            states.append(FileState(
                topic=f"{result.target.exchange}.{result.target.stream}",
                partition=result.partition,
                high_water_offset=result.high_water_offset,
                file_path=str(file_path),
                file_byte_size=file_size,
            ))
        return states

    async def _commit_state(
        self,
        states: list[FileState],
        results: list[FlushResult],
        start: float,
    ) -> None:
        """Save file states and stream checkpoints to PG atomically, then commit Kafka offsets.

        Both file states and stream checkpoints are persisted in a single
        transaction at the durable flush boundary -- after write/fsync.
        """
        # Derive stream checkpoints from FlushResult checkpoint metadata
        checkpoints: list[StreamCheckpoint] = []
        for result in results:
            if result.checkpoint_meta is not None:
                meta = result.checkpoint_meta
                exchange, symbol, stream = meta.stream_key
                # Convert received_at (nanoseconds) to ISO timestamp for PG storage
                received_dt = datetime.datetime.fromtimestamp(
                    meta.last_received_at / 1_000_000_000,
                    tz=datetime.timezone.utc,
                )
                checkpoints.append(StreamCheckpoint(
                    exchange=exchange,
                    symbol=symbol,
                    stream=stream,
                    last_received_at=received_dt.isoformat(),
                    last_collector_session_id=meta.last_collector_session_id,
                ))

        # Save both file states and checkpoints in a single atomic transaction
        await self.state_manager.save_states_and_checkpoints(states, checkpoints)

        # Update in-memory checkpoint cache AFTER successful commit
        for cp in checkpoints:
            self._durable_checkpoints[cp.checkpoint_key] = cp

        # Commit Kafka offsets
        assert self._consumer is not None, "call start() first"
        self._consumer.commit(asynchronous=True)

        elapsed_ms = (time.monotonic() - start) * 1000
        for result in results:
            writer_metrics.flush_duration_ms.labels(
                exchange=result.target.exchange,
                stream=result.target.stream,
            ).observe(elapsed_ms)
        logger.debug("flush_complete", files=len(results), elapsed_ms=round(elapsed_ms, 1))

    async def _write_and_save(self, results: list[FlushResult]) -> None:
        """Normal flush: write files -> fsync -> save state to PG -> commit offsets.
        For rotation (seal-before-commit), use _write_to_disk + seal + _commit_state."""
        start = time.monotonic()
        states = self._write_to_disk(results)
        await self._commit_state(states, results, start)

    def _update_disk_metrics(self) -> None:
        try:
            usage = shutil.disk_usage(self.base_dir)
            writer_metrics.disk_usage_bytes.set(usage.used)
            writer_metrics.disk_usage_pct.set(usage.used / usage.total * 100)
        except OSError:
            pass

    def _update_consumer_lag(self) -> None:
        if not self._consumer or not self._assigned:
            return
        from confluent_kafka import TopicPartition
        for tp in self._consumer.assignment():
            try:
                _, high = self._consumer.get_watermark_offsets(tp)
                positions = self._consumer.position([TopicPartition(tp.topic, tp.partition)])
                if positions and positions[0].offset >= 0:
                    lag = max(0, high - positions[0].offset)
                    exchange, _, stream = tp.topic.partition(".")
                    if not stream:
                        exchange, stream = "", tp.topic
                    writer_metrics.consumer_lag.labels(
                        exchange=exchange, stream=stream,
                    ).set(lag)
            except Exception:
                pass

    def is_connected(self) -> bool:
        """True if consumer exists and has been assigned partitions."""
        return self._consumer is not None and self._assigned

    async def stop(self) -> None:
        self._running = False
        # _rotate_hour handles flush -> write -> seal -> commit in correct order
        await self._rotate_hour()
        if self._consumer:
            self._consumer.close()
