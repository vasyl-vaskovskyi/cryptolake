from __future__ import annotations

import asyncio
import datetime
import os
import shutil
import time
from pathlib import Path

import orjson
import structlog
import zstandard as zstd

from src.common.envelope import (
    add_broker_coordinates,
    create_gap_envelope,
    deserialize_envelope,
)
from src.writer.backup_recovery import recover_from_backup
from src.common.system_identity import get_host_boot_id
from src.writer import metrics as writer_metrics
from src.writer.buffer_manager import BufferManager, FlushResult
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
        backup_brokers: list[str] | None = None,
        backup_topic_prefix: str = "",
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
        # Streams where recovery emitted a gap — suppress the first runtime
        # session change detection (it would duplicate the recovery gap).
        self._recovery_gap_emitted: set[tuple[str, str, str]] = set()
        # Boot ID and component state for classification
        self._current_boot_id: str = get_host_boot_id()
        self._previous_writer_state: ComponentRuntimeState | None = None
        self._previous_collector_state: ComponentRuntimeState | None = None
        self._maintenance_intent = None
        # Host lifecycle evidence (Phase 2) — loaded once at startup
        self._host_evidence: HostLifecycleEvidence | None = host_evidence
        # REST-polled stream gap threshold (3x configured poll interval in ns)
        self._rest_poll_interval_ns: int = _DEFAULT_REST_POLL_INTERVAL_NS
        self._hours_sealed_count: dict[tuple[str, str, str], int] = {}
        self._backup_brokers: list[str] = backup_brokers or []
        self._backup_topic_prefix: str = backup_topic_prefix

    def _try_backup_recovery(self, gap_envelope: dict) -> tuple[list[dict], dict | None]:
        """Attempt to fill a gap from backup Redpanda topics.

        Returns (recovered_records, adjusted_gap_or_None):
        - On "full": (records, None) — gap fully covered, no gap envelope needed
        - On "partial": (records, narrowed_gap) — some records recovered, gap narrowed
        - On "none": ([], original_gap_envelope) — nothing recovered
        """
        if not self._backup_brokers or not self._backup_topic_prefix:
            return [], gap_envelope

        stream = gap_envelope.get("stream", "")
        symbol = gap_envelope.get("symbol", "")
        exchange = gap_envelope.get("exchange", "")
        gap_start = gap_envelope.get("gap_start_ts", 0)
        gap_end = gap_envelope.get("gap_end_ts", 0)

        if gap_start <= 0 or gap_end <= 0:
            return [], gap_envelope

        records, coverage = recover_from_backup(
            brokers=self._backup_brokers,
            backup_topic_prefix=self._backup_topic_prefix,
            stream=stream, symbol=symbol, exchange=exchange,
            gap_start_ns=gap_start, gap_end_ns=gap_end,
        )

        writer_metrics.backup_recovery_attempts.inc()

        if coverage == "full":
            writer_metrics.backup_recovery_success.inc()
            return records, None
        elif coverage == "partial":
            writer_metrics.backup_recovery_partial.inc()
            last_received = max(r.get("received_at", 0) for r in records)
            narrowed = dict(gap_envelope)
            narrowed["gap_start_ts"] = last_received + 1
            narrowed["detail"] = (
                f"{gap_envelope.get('detail', '')} "
                f"[partial backup recovery: {len(records)} records]"
            )
            return records, narrowed
        else:
            writer_metrics.backup_recovery_miss.inc()
            return [], gap_envelope

    async def start(self) -> None:
        from confluent_kafka import Consumer as KafkaConsumer

        def _on_commit(err, partitions):
            if err is not None:
                failed_parts = []
                if partitions:
                    for tp in partitions:
                        if tp.error is not None:
                            failed_parts.append(f"{tp.topic}[{tp.partition}]@{tp.offset}: {tp.error}")
                logger.error("kafka_commit_failed", error=str(err),
                             partitions=failed_parts or str(partitions))
                writer_metrics.kafka_commit_failures_total.inc()

        self._consumer = KafkaConsumer({
            "bootstrap.servers": ",".join(self.brokers),
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "on_commit": _on_commit,
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

        # Remove uncommitted .zst files that may have corrupt partial frames
        # from a crash during ENOSPC or similar disk failures. Data in these
        # files was never committed to PG, so Kafka will re-deliver it.
        self._cleanup_uncommitted_files(states)

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

    def _cleanup_uncommitted_files(self, states: dict) -> None:
        """Remove unsealed .zst files not tracked in PG state.

        Files created but never durably committed may contain corrupt partial
        zstd frames from crashes during disk-full or similar failures.
        Since PG has no record of them, Kafka offsets were never committed
        either — the data will be re-consumed from Kafka on restart.
        """
        known_paths = {Path(s.file_path) for s in states.values()}
        base = Path(self.base_dir)
        if not base.exists():
            return
        for zst_file in base.rglob("*.jsonl.zst"):
            if zst_file in known_paths or zst_file in self._sealed_files:
                continue
            logger.warning("removing_uncommitted_file", path=str(zst_file))
            try:
                zst_file.unlink()
            except OSError:
                pass

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

        checkpoint = self._durable_checkpoints.get(stream_key)
        if checkpoint is None:
            exchange, symbol, stream = stream_key
            stream_dir = Path(self.base_dir) / exchange / symbol / stream
            if not stream_dir.exists() or not any(stream_dir.rglob("hour-*.jsonl.zst")):
                logger.info("first_run_no_checkpoint",
                            exchange=exchange, symbol=symbol, stream=stream)
                return None

            # Read the last data envelope from the most recent archive file
            # to get gap_start_ts and last_session_seq for continuity check
            hour_files = sorted(stream_dir.rglob("hour-*.jsonl.zst"))
            gap_start_ts = 0
            last_archive_seq: int | None = None
            last_archive_session: str | None = None
            for hf in reversed(hour_files):
                try:
                    dctx = zstd.ZstdDecompressor()
                    with open(hf, "rb") as f:
                        data = dctx.stream_reader(f).read()
                    lines = [l for l in data.strip().split(b"\n") if l]
                    # Walk backwards to find last data envelope (skip gap envelopes)
                    for line in reversed(lines):
                        env = orjson.loads(line)
                        if env.get("type") == "data":
                            gap_start_ts = env.get("received_at", 0)
                            last_archive_seq = env.get("session_seq")
                            last_archive_session = env.get("collector_session_id")
                            break
                    if gap_start_ts > 0:
                        break
                except Exception:
                    continue

            current_received_at = envelope.get("received_at", 0)
            current_session_id = envelope.get("collector_session_id", "")
            current_seq = envelope.get("session_seq", 0)

            # Check if records were actually missed
            if (last_archive_session == current_session_id
                    and last_archive_seq is not None
                    and current_seq == last_archive_seq + 1):
                # No records missed — Redpanda buffered everything
                logger.info("recovery_no_data_loss",
                            exchange=exchange, symbol=symbol, stream=stream)
                return None

            logger.warning("recovery_gap_no_checkpoint",
                           exchange=exchange, symbol=symbol, stream=stream,
                           gap_start_ts=gap_start_ts, gap_end_ts=current_received_at,
                           last_archive_seq=last_archive_seq, current_seq=current_seq)

            writer_metrics.session_gaps_detected_total.labels(
                exchange=exchange, symbol=symbol, stream=stream,
            ).inc()
            self._recovery_gap_emitted.add(stream_key)

            records_missed = current_seq - last_archive_seq - 1 if last_archive_seq is not None else None
            detail = "No durable checkpoint; recovered gap bounds from archive"
            if records_missed is not None:
                detail += f" ({records_missed} records missed)"

            return create_gap_envelope(
                exchange=exchange,
                symbol=symbol,
                stream=stream,
                collector_session_id=current_session_id,
                session_seq=-1,
                gap_start_ts=gap_start_ts,
                gap_end_ts=current_received_at,
                reason="checkpoint_lost",
                detail=detail,
                received_at=current_received_at,
            )

        # Extract current envelope data
        current_session_id = envelope.get("collector_session_id", "")
        current_received_at = envelope.get("received_at", 0)
        previous_session_id = checkpoint.last_collector_session_id

        # Determine if there is a session change
        session_changed = current_session_id != previous_session_id

        # Check if boot ID changed — if so, always proceed to classification
        # even when the first message has the same session (re-read from Redpanda).
        previous_boot_id_early = (
            self._previous_writer_state.host_boot_id
            if self._previous_writer_state else None
        )
        boot_id_changed = (
            previous_boot_id_early is not None
            and previous_boot_id_early != self._current_boot_id
        )

        # If the writer previously ran (has durable state in PG), it restarted.
        # A restart always means a potential data gap — even when the first
        # message has the same session (re-read from Redpanda) and a small
        # time delta.  Always proceed to classification so the gap is recorded.
        #
        # The time-delta check is only valid on first-ever run (no previous
        # writer state), which never reaches here because there would be no
        # durable checkpoints either.
        if not session_changed and not boot_id_changed and self._previous_writer_state is None:
            # First-ever run with a stale checkpoint — no gap expected
            return None

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

        # Check if records were actually missed by reading the last data
        # envelope from the archive and comparing session_seq
        exchange, symbol, stream = stream_key
        current_seq = envelope.get("session_seq", -1)
        stream_dir = Path(self.base_dir) / exchange / symbol / stream
        last_archive_seq: int | None = None
        last_archive_session: str | None = None
        if stream_dir.exists():
            for hf in reversed(sorted(stream_dir.rglob("hour-*.jsonl.zst"))):
                try:
                    dctx = zstd.ZstdDecompressor()
                    with open(hf, "rb") as f:
                        raw = dctx.stream_reader(f).read()
                    for line in reversed(raw.strip().split(b"\n")):
                        if not line:
                            continue
                        env = orjson.loads(line)
                        if env.get("type") == "data":
                            last_archive_seq = env.get("session_seq")
                            last_archive_session = env.get("collector_session_id")
                            break
                    if last_archive_seq is not None:
                        break
                except Exception:
                    continue

        if (last_archive_session == current_session_id
                and last_archive_seq is not None
                and current_seq == last_archive_seq + 1):
            # No records missed — Redpanda buffered everything
            logger.info("recovery_no_data_loss",
                        exchange=exchange, symbol=symbol, stream=stream,
                        classification=classification)
            return None

        # Compute gap_start_ts from checkpoint
        try:
            cp_dt = datetime.datetime.fromisoformat(checkpoint.last_received_at)
            if cp_dt.tzinfo is None:
                cp_dt = cp_dt.replace(tzinfo=datetime.timezone.utc)
            gap_start_ts = int(cp_dt.timestamp() * 1_000_000_000)
        except (ValueError, TypeError):
            gap_start_ts = 0

        # Clamp: checkpoint may carry a wall-clock received_at from an error-gap
        # envelope that was newer than messages Kafka will re-deliver after restart.
        if gap_start_ts > current_received_at > 0:
            gap_start_ts = current_received_at

        logger.warning(
            "recovery_gap_detected",
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            classification=classification,
            last_archive_seq=last_archive_seq,
            current_seq=current_seq,
        )
        writer_metrics.session_gaps_detected_total.labels(
            exchange=exchange, symbol=symbol, stream=stream,
        ).inc()

        # Mark this stream so that the runtime _check_session_change path
        # suppresses its first session transition (already covered here).
        self._recovery_gap_emitted.add(stream_key)

        records_missed = current_seq - last_archive_seq - 1 if last_archive_seq is not None else None
        detail = (
            f"Writer recovery: {classification.get('cause', 'unknown')} "
            f"(session {previous_session_id} -> {current_session_id})"
        )
        if records_missed is not None:
            detail += f" ({records_missed} records missed)"

        return create_gap_envelope(
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            collector_session_id=current_session_id,
            session_seq=-1,
            gap_start_ts=gap_start_ts,
            gap_end_ts=current_received_at,
            reason="restart_gap",
            detail=detail,
            received_at=current_received_at,
            component=classification.get("component"),
            cause=classification.get("cause"),
            planned=classification.get("planned"),
            classifier=classification.get("classifier"),
            evidence=classification.get("evidence"),
            maintenance_id=classification.get("maintenance_id"),
        )

    async def _check_session_change(self, envelope: dict) -> dict | None:
        """Detect collector session changes and return a gap envelope if one occurred.

        When the collector restarts (gracefully or via crash/SIGKILL), it cannot
        emit gap records itself. The writer detects the session_id change and
        injects a restart_gap record into the archive to record the data loss
        window.

        This is the RUNTIME detection path (after initial recovery is done).
        It queries the DB for fresh collector shutdown state and maintenance
        intent so that planned collector restarts are correctly classified.
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

        # Suppress the first session change for streams that already had a
        # recovery gap emitted — the old→new transition is already recorded
        # by _check_recovery_gap.  This prevents duplicate restart_gap records
        # when the writer re-reads old-session messages from Redpanda after restart.
        if stream_key in self._recovery_gap_emitted:
            self._recovery_gap_emitted.discard(stream_key)
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

        # Load the PREVIOUS collector's state (by its instance_id/session_id)
        # and the active maintenance intent from DB so that planned
        # collector-only restarts are classified correctly.
        # We must look up the specific previous instance because the new
        # collector has already registered itself with clean_shutdown_at=NULL.
        collector_clean_shutdown = False
        maintenance_intent = None
        try:
            prev_state = await self.state_manager.load_component_state_by_instance(
                "collector", prev_session_id,
            )
            if prev_state is not None:
                collector_clean_shutdown = prev_state.clean_shutdown_at is not None
            maintenance_intent = await self.state_manager.load_active_maintenance_intent()
        except Exception:
            logger.warning("runtime_gap_classification_db_fallback",
                           detail="Could not load DB state; defaulting to unclean_exit")

        # system_clean_shutdown is true when BOTH previous writer AND previous
        # collector had clean shutdowns (full-stack planned restart).
        writer_clean_shutdown = (
            self._previous_writer_state is not None
            and self._previous_writer_state.clean_shutdown_at is not None
        )
        system_clean_shutdown = writer_clean_shutdown and collector_clean_shutdown

        previous_boot_id = (
            self._previous_writer_state.host_boot_id
            if self._previous_writer_state else self._current_boot_id
        )
        classification = classify_restart_gap(
            previous_boot_id=previous_boot_id,
            current_boot_id=self._current_boot_id,
            previous_session_id=prev_session_id,
            current_session_id=session_id,
            collector_clean_shutdown=collector_clean_shutdown,
            system_clean_shutdown=system_clean_shutdown,
            maintenance_intent=maintenance_intent,
            host_evidence=self._host_evidence,
        )

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
            component=classification.get("component"),
            cause=classification.get("cause"),
            planned=classification.get("planned"),
            classifier="writer_runtime_v1",
            evidence=classification.get("evidence"),
            maintenance_id=classification.get("maintenance_id"),
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
            try:
                envelope = deserialize_envelope(raw_value)
            except Exception:
                logger.error(
                    "corrupt_message_skipped",
                    topic=msg_topic,
                    partition=msg_partition,
                    offset=msg_offset,
                    raw_size=len(raw_value),
                )
                writer_metrics.messages_skipped_total.labels(
                    exchange="unknown", symbol="unknown", stream="unknown",
                ).inc()
                # Emit gap so archive documents the skipped message
                now_ns = time.time_ns()
                parts = msg_topic.split(".", 1) if msg_topic else ["unknown", "unknown"]
                gap_exchange = parts[0] if len(parts) > 0 else "unknown"
                gap_stream = parts[1] if len(parts) > 1 else "unknown"
                gap = create_gap_envelope(
                    exchange=gap_exchange,
                    symbol="unknown",
                    stream=gap_stream,
                    collector_session_id="",
                    session_seq=-1,
                    gap_start_ts=now_ns,
                    gap_end_ts=now_ns,
                    reason="deserialization_error",
                    detail=f"Corrupt message at offset {msg_offset} (size={len(raw_value)})",
                )
                gap = add_broker_coordinates(
                    gap, topic=msg_topic, partition=msg_partition, offset=-1,
                )
                gap_results = self.buffer_manager.add(gap)
                if gap_results:
                    await self._write_and_save(gap_results)
                continue
            envelope = add_broker_coordinates(
                envelope,
                topic=msg_topic,
                partition=msg_partition,
                offset=msg_offset,
            )

            # De-duplication during recovery: skip messages already in the archive (spec 8.2)
            target = self.buffer_manager.route(envelope)
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
                    recovered, adjusted_gap = self._try_backup_recovery(recovery_gap)
                    for rec in recovered:
                        rec_results = self.buffer_manager.add(rec)
                        if rec_results:
                            await self._write_and_save(rec_results)
                    if adjusted_gap is not None:
                        adjusted_gap = add_broker_coordinates(adjusted_gap, topic=msg_topic, partition=msg_partition, offset=-1)
                        gap_results = self.buffer_manager.add(adjusted_gap)
                        if gap_results:
                            await self._write_and_save(gap_results)

                # Runtime session change detection (covers collector crash/SIGKILL
                # after initial recovery is done)
                gap_envelope = await self._check_session_change(envelope)
                if gap_envelope is not None:
                    recovered, adjusted_gap = self._try_backup_recovery(gap_envelope)
                    for rec in recovered:
                        rec_results = self.buffer_manager.add(rec)
                        if rec_results:
                            await self._write_and_save(rec_results)
                    if adjusted_gap is not None:
                        adjusted_gap = add_broker_coordinates(adjusted_gap, topic=msg_topic, partition=msg_partition, offset=-1)
                        gap_results = self.buffer_manager.add(adjusted_gap)
                        if gap_results:
                            await self._write_and_save(gap_results)

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
                    if current_date != prev_date:
                        ex, sym, st = file_key
                        key = (ex, sym, st)
                        today_val = self._hours_sealed_count.get(key, 0)
                        writer_metrics.hours_sealed_previous_day.labels(
                            exchange=ex, symbol=sym, stream=st,
                        ).set(today_val)
                        writer_metrics.hours_sealed_today.labels(
                            exchange=ex, symbol=sym, stream=st,
                        ).set(0)
                        self._hours_sealed_count[key] = 0
                    # Seal previous file using the PREVIOUS date/hour,
                    # not the current message's date (critical at 23->00 day boundary)
                    await self._rotate_file(file_key, prev_date, prev_hour)
            active_hours[file_key] = (current_hour, current_date)

            # Add to buffer -- may trigger per-key flush (high-volume streams only)
            flush_results = self.buffer_manager.add(envelope)
            if flush_results:
                await self._write_and_save(flush_results)
                # Note: do NOT reset last_flush_time here — count-based flushes
                # only flush the single key that hit the threshold. The timer
                # must still fire to flush low-volume streams.

            # Timer-based flush: flush ALL buffers periodically to ensure
            # low-volume streams (depth_snapshot, open_interest) reach disk
            # even when high-volume streams keep poll() busy.
            if time.monotonic() - last_flush_time >= self.buffer_manager.flush_interval_seconds:
                await self._flush_and_commit()
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
            states, gap_envelopes = self._write_to_disk(results)
            for gap in gap_envelopes:
                self.buffer_manager.add(gap)

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
                try:
                    write_sha256_sidecar(file_path, sc)
                    self._sealed_files.add(file_path)
                    writer_metrics.files_rotated_total.labels(
                        exchange=exchange, symbol=symbol, stream=stream,
                    ).inc()
                    writer_metrics.hours_sealed_today.labels(
                        exchange=exchange, symbol=symbol, stream=stream,
                    ).inc()
                    key = (exchange, symbol, stream)
                    self._hours_sealed_count[key] = self._hours_sealed_count.get(key, 0) + 1
                    logger.info("file_sealed", path=str(file_path))
                except OSError as e:
                    logger.error("sidecar_write_failed", path=str(file_path), error=str(e))

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
            states, gap_envelopes = self._write_to_disk(results)
            for gap in gap_envelopes:
                self.buffer_manager.add(gap)

        # 2. Seal all active files BEFORE committing offsets
        base = Path(self.base_dir)
        for zst_file in base.rglob("*.jsonl.zst"):
            sc = sidecar_path(zst_file)
            if not sc.exists() and zst_file.stat().st_size > 0:
                try:
                    write_sha256_sidecar(zst_file, sc)
                    self._sealed_files.add(zst_file)
                    writer_metrics.files_rotated_total.labels(
                        exchange="binance",
                        symbol=zst_file.parent.parent.parent.name,
                        stream=zst_file.parent.parent.name,
                    ).inc()
                    logger.info("file_sealed", path=str(zst_file))
                except OSError as e:
                    logger.error("sidecar_write_failed", path=str(zst_file), error=str(e))

        # 3. Now safe to commit -- all sidecars are durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _flush_and_commit(self) -> None:
        results = self.buffer_manager.flush_all()
        if results:
            await self._write_and_save(results)
        self._update_disk_metrics()
        self._update_consumer_lag()

    @staticmethod
    def _extract_batch_time_range(lines: list[bytes]) -> tuple[int, int]:
        """Extract (first_ts, last_ts) from serialized envelope lines."""
        now = time.time_ns()
        if not lines:
            return (now, now)
        try:
            first_ts = orjson.loads(lines[0]).get("received_at", now)
            last_ts = orjson.loads(lines[-1]).get("received_at", now)
            return (first_ts, max(last_ts, first_ts))
        except Exception:
            return (now, now)

    @staticmethod
    def _make_error_gap(result: FlushResult, detail: str) -> dict:
        """Create a write_error gap envelope from a failed FlushResult."""
        first_ts, last_ts = WriterConsumer._extract_batch_time_range(result.lines)
        gap = create_gap_envelope(
            exchange=result.target.exchange,
            symbol=result.target.symbol,
            stream=result.target.stream,
            collector_session_id="",
            session_seq=-1,
            gap_start_ts=first_ts,
            gap_end_ts=last_ts,
            reason="write_error",
            detail=detail,
            received_at=last_ts,
        )
        return add_broker_coordinates(
            gap,
            topic=f"{result.target.exchange}.{result.target.stream}",
            partition=result.partition,
            offset=-1,
        )

    def _write_to_disk(self, results: list[FlushResult]) -> tuple[list[FileState], list[dict]]:
        """Write compressed frames to disk and fsync. Returns (FileState list, gap envelopes).
        Gap envelopes are emitted for any files that failed to write.
        Does NOT save PG state or commit Kafka offsets."""
        states: list[FileState] = []
        gap_envelopes: list[dict] = []
        for result in results:
            file_path = self._resolve_file_path(result.file_path)
            try:
                file_path.parent.mkdir(parents=True, exist_ok=True)

                compressed = self.compressor.compress_frame(result.lines)
                with open(file_path, "ab") as f:
                    pos_before = f.tell()
                    try:
                        f.write(compressed)
                        f.flush()
                        os.fsync(f.fileno())
                    except OSError:
                        # Truncate back to remove partial frame
                        try:
                            f.truncate(pos_before)
                            f.flush()
                            os.fsync(f.fileno())
                        except OSError:
                            pass  # best-effort truncation
                        raise
            except OSError as e:
                logger.error(
                    "write_to_disk_failed",
                    path=str(file_path),
                    error=str(e),
                    lines_lost=len(result.lines),
                )
                writer_metrics.write_errors_total.labels(
                    exchange=result.target.exchange,
                    symbol=result.target.symbol,
                    stream=result.target.stream,
                ).inc()
                # Emit gap envelope covering the batch's time range
                gap_envelopes.append(self._make_error_gap(result, f"Disk write failed: {e}"))
                continue  # skip this file, data in buffer is lost

            file_size = file_path.stat().st_size

            # Count gap envelopes written to disk
            for line in result.lines:
                try:
                    env = orjson.loads(line)
                    if env.get("type") == "gap":
                        writer_metrics.gap_records_written_total.labels(
                            exchange=result.target.exchange,
                            symbol=result.target.symbol,
                            stream=result.target.stream,
                            reason=env.get("reason", "unknown"),
                        ).inc()
                except Exception:
                    pass

            writer_metrics.bytes_written_total.labels(
                exchange=result.target.exchange,
                symbol=result.target.symbol,
                stream=result.target.stream,
            ).inc(len(compressed))

            raw_size = sum(len(line) for line in result.lines)
            if len(compressed) > 0:
                writer_metrics.compression_ratio.labels(
                    exchange=result.target.exchange,
                    symbol=result.target.symbol,
                    stream=result.target.stream,
                ).set(raw_size / len(compressed))

            states.append(FileState(
                topic=f"{result.target.exchange}.{result.target.stream}",
                partition=result.partition,
                high_water_offset=result.high_water_offset,
                file_path=str(file_path),
                file_byte_size=file_size,
            ))
        return (states, gap_envelopes)

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
        try:
            await self.state_manager.save_states_and_checkpoints(states, checkpoints)
        except Exception as e:
            logger.error(
                "pg_commit_failed_will_retry",
                error=str(e),
                states=len(states),
                checkpoints=len(checkpoints),
            )
            writer_metrics.pg_commit_failures_total.inc()
            # Emit gap for each affected stream covering the batch's time range
            for result in results:
                self.buffer_manager.add(self._make_error_gap(result, f"PostgreSQL commit failed: {e}"))
            # Do NOT commit Kafka offsets — messages will be re-consumed
            # and re-written on next flush (dedup handles duplicates).
            return

        # Update in-memory checkpoint cache AFTER successful commit
        for cp in checkpoints:
            self._durable_checkpoints[cp.checkpoint_key] = cp

        # Commit Kafka offsets only for partitions we've actually consumed from
        assert self._consumer is not None, "call start() first"
        from confluent_kafka import TopicPartition
        offsets_to_commit = []
        for tp in self._consumer.assignment():
            positions = self._consumer.position([TopicPartition(tp.topic, tp.partition)])
            if positions and positions[0].offset > 0:
                offsets_to_commit.append(positions[0])
        if offsets_to_commit:
            self._consumer.commit(offsets=offsets_to_commit, asynchronous=True)

        elapsed_ms = (time.monotonic() - start) * 1000
        for result in results:
            writer_metrics.flush_duration_ms.labels(
                exchange=result.target.exchange,
                symbol=result.target.symbol,
                stream=result.target.stream,
            ).observe(elapsed_ms)
        logger.debug("flush_complete", files=len(results), elapsed_ms=round(elapsed_ms, 1))

    async def _write_and_save(self, results: list[FlushResult]) -> None:
        """Normal flush: write files -> fsync -> save state to PG -> commit offsets.
        For rotation (seal-before-commit), use _write_to_disk + seal + _commit_state."""
        start = time.monotonic()
        states, gap_envelopes = self._write_to_disk(results)
        for gap in gap_envelopes:
            self.buffer_manager.add(gap)
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
