from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import structlog

from src.common.envelope import add_broker_coordinates, deserialize_envelope
from src.writer import metrics as writer_metrics
from src.writer.buffer_manager import BufferManager, FlushResult
from src.writer.compressor import ZstdFrameCompressor
from src.writer.file_rotator import (
    build_file_path, compute_sha256, sidecar_path, write_sha256_sidecar,
)
from src.writer.state_manager import FileState, StateManager

logger = structlog.get_logger()


class WriterConsumer:
    """Consumes from Redpanda, buffers, compresses, and writes archive files.

    Key design decisions per spec:
    - Consumer.poll() runs in run_in_executor to avoid blocking the async event loop (spec 8.2)
    - Hourly rotation seals files with SHA-256 sidecar (spec 8.4)
    - Sealed files are never reopened; late arrivals go to spillover files (spec 8.2)
    - Flush sequence: write → fsync → PG state → commit offsets (spec 8.3)
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
        self._sealed_files: set[Path] = set()  # tracks sealed .jsonl.zst paths
        self._late_seq: dict[Path, int] = {}  # late-arrival sequence counters

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

        # Discover already-sealed files (those with .sha256 sidecar)
        self._discover_sealed_files()

        def _on_assign(consumer, partitions):
            self._assigned = True
            self._assigned_partitions = set(
                (tp.topic, tp.partition) for tp in partitions
            )
            logger.info("consumer_partitions_assigned", count=len(partitions))

        def _on_revoke(consumer, partitions):
            revoked = set((tp.topic, tp.partition) for tp in partitions)
            logger.critical("consumer_partitions_revoked",
                            revoked=list(revoked),
                            detail="Writer exclusivity violated — another consumer "
                                   "joined the group or rebalance occurred")
            # Flush any buffered data before losing ownership
            # (best-effort — we may lose the race)
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._flush_and_commit())
            except RuntimeError:
                pass
            self._assigned = False
            # Crash the writer to prevent split-brain writes (spec 8.2)
            raise RuntimeError(
                f"Writer exclusivity violated: partitions {revoked} revoked. "
                "Another writer may have joined consumer group. Shutting down."
            )

        self._consumer.subscribe(self.topics, on_assign=_on_assign,
                                 on_revoke=_on_revoke)
        self._running = True

        # Collapse to max offset per (topic, partition) since state is now keyed per file
        seek_map: dict[tuple[str, int], int] = {}
        for s in states.values():
            key = (s.topic, s.partition)
            seek_map[key] = max(seek_map.get(key, 0), s.high_water_offset + 1)
        self._pending_seeks = seek_map

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

    def _resolve_file_path(self, file_path: Path) -> Path:
        """If file_path is sealed (has sidecar), return a late-arrival spillover path."""
        if file_path in self._sealed_files or sidecar_path(file_path).exists():
            self._sealed_files.add(file_path)
            seq = self._late_seq.get(file_path, 0) + 1
            self._late_seq[file_path] = seq
            # Build late path: hour-14.late-1.jsonl.zst
            stem = file_path.stem.split(".")[0]  # "hour-14"
            late_name = f"{stem}.late-{seq}.jsonl.zst"
            return file_path.parent / late_name
        return file_path

    async def consume_loop(self) -> None:
        """Main consume loop. Polls in executor, buffers, flushes, and commits."""
        loop = asyncio.get_running_loop()
        last_flush_time = time.monotonic()
        # Track active (hour, date) per file key (exchange, symbol, stream).
        # Stores the previous message's hour and date so that at day boundaries
        # (23→00) we seal the correct previous-day file, not the new day's.
        active_hours: dict[tuple[str, str, str], tuple[int, str]] = {}

        while self._running:
            # Non-blocking poll via executor (spec 8.2: avoid blocking uvloop)
            msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)

            # Handle pending seeks on assignment
            if self._pending_seeks:
                assignments = self._consumer.assignment()
                for tp in assignments:
                    key = (tp.topic, tp.partition)
                    if key in self._pending_seeks:
                        from confluent_kafka import TopicPartition
                        self._consumer.seek(
                            TopicPartition(tp.topic, tp.partition, self._pending_seeks[key])
                        )
                        logger.info("consumer_seek", topic=tp.topic,
                                    partition=tp.partition,
                                    offset=self._pending_seeks[key])
                self._pending_seeks = {}

            if msg is None:
                if time.monotonic() - last_flush_time >= self.buffer_manager.flush_interval_seconds:
                    await self._flush_and_commit()
                    last_flush_time = time.monotonic()
                continue

            if msg.error():
                logger.error("consumer_error", error=str(msg.error()))
                continue

            # Deserialize and stamp broker coordinates
            envelope = deserialize_envelope(msg.value())
            envelope = add_broker_coordinates(
                envelope,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

            writer_metrics.messages_consumed_total.labels(
                exchange=envelope.get("exchange", ""),
                symbol=envelope.get("symbol", ""),
                stream=envelope.get("stream", ""),
            ).inc()

            # Per-file hourly rotation (spec 8.2: file routing by message received_at)
            import datetime
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
                    # not the current message's date (critical at 23→00 day boundary)
                    await self._rotate_file(file_key, prev_date, prev_hour)
            active_hours[file_key] = (current_hour, current_date)

            # Add to buffer — may trigger flush
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
        Order per spec 8.4: flush → write to disk → seal (.sha256 sidecar) →
        save PG state → commit Kafka offsets."""
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

        # 3. Seal all flushed file paths BEFORE committing offsets
        files_to_seal = set()
        if results:
            for r in results:
                files_to_seal.add(r.file_path)
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

        # 4. Now safe to commit — sidecar is durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _rotate_hour(self) -> None:
        """Seal all active files (used during shutdown). For normal operation,
        use _rotate_file() which seals per-stream files individually.
        Order: flush → write to disk → seal all → commit offsets (spec 8.4)."""
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

        # 3. Now safe to commit — all sidecars are durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _flush_and_commit(self) -> None:
        results = self.buffer_manager.flush_all()
        if results:
            await self._write_and_save(results)

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
        """Save file states to PG and commit Kafka offsets.
        Called after writes (and optional sealing) are complete."""
        await self.state_manager.save_states(states)
        self._consumer.commit(asynchronous=True)

        elapsed_ms = (time.monotonic() - start) * 1000
        for result in results:
            writer_metrics.flush_duration_ms.labels(
                exchange=result.target.exchange,
                stream=result.target.stream,
            ).observe(elapsed_ms)
        logger.debug("flush_complete", files=len(results), elapsed_ms=round(elapsed_ms, 1))

    async def _write_and_save(self, results: list[FlushResult]) -> None:
        """Normal flush: write files → fsync → save state to PG → commit offsets.
        For rotation (seal-before-commit), use _write_to_disk + seal + _commit_state."""
        start = time.monotonic()
        states = self._write_to_disk(results)
        await self._commit_state(states, results, start)

    def is_connected(self) -> bool:
        """True if consumer exists and has been assigned partitions."""
        return self._consumer is not None and self._assigned

    async def stop(self) -> None:
        self._running = False
        # _rotate_hour handles flush → write → seal → commit in correct order
        await self._rotate_hour()
        if self._consumer:
            self._consumer.close()
