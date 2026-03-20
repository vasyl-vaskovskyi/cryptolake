from __future__ import annotations

import asyncio
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class StreamCheckpoint:
    """Per (exchange, symbol, stream) durable checkpoint updated after each durable write."""

    exchange: str
    symbol: str
    stream: str
    last_received_at: str
    last_collector_session_id: str
    last_gap_reason: str | None = None

    @property
    def checkpoint_key(self) -> tuple[str, str, str]:
        return (self.exchange, self.symbol, self.stream)


@dataclass
class ComponentRuntimeState:
    """Latest known lifecycle state per component instance.

    instance_id is unique per lifecycle (e.g., "collector-01_2026-03-18T21:00:00Z").
    Each restart creates a new instance_id, so the (component, instance_id) PK
    naturally separates lifecycle records without needing to update started_at on conflict.
    """

    component: str
    instance_id: str
    host_boot_id: str
    started_at: str
    last_heartbeat_at: str
    clean_shutdown_at: str | None = None
    planned_shutdown: bool = False
    maintenance_id: str | None = None


@dataclass
class MaintenanceIntent:
    """Durable operator intent for scheduled maintenance."""

    maintenance_id: str
    scope: str
    planned_by: str
    reason: str
    created_at: str
    expires_at: str
    consumed_at: str | None = None


@dataclass
class FileState:
    topic: str
    partition: int
    high_water_offset: int
    file_path: str
    file_byte_size: int

    @property
    def state_key(self) -> tuple[str, int, str]:
        return (self.topic, self.partition, self.file_path)


class StateManager:
    """Manages writer state in PostgreSQL for exactly-once archive semantics.

    Tracks the highest flushed Kafka offset and file byte size per (topic, partition, file_path).
    PK includes file_path so that multiple files for the same (topic, partition) — e.g.,
    different symbols or hours — each get independent state rows for correct truncation on restart.
    """

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS writer_file_state (
        topic TEXT NOT NULL,
        partition INTEGER NOT NULL,
        file_path TEXT NOT NULL,
        high_water_offset BIGINT NOT NULL,
        file_byte_size BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (topic, partition, file_path)
    );
    """

    UPSERT_SQL = """
    INSERT INTO writer_file_state (topic, partition, file_path, high_water_offset, file_byte_size, updated_at)
    VALUES (%(topic)s, %(partition)s, %(file_path)s, %(high_water_offset)s, %(file_byte_size)s, NOW())
    ON CONFLICT (topic, partition, file_path)
    DO UPDATE SET
        high_water_offset = GREATEST(writer_file_state.high_water_offset, EXCLUDED.high_water_offset),
        file_byte_size = EXCLUDED.file_byte_size,
        updated_at = NOW();
    """

    LOAD_ALL_SQL = """
    SELECT topic, partition, high_water_offset, file_path, file_byte_size
    FROM writer_file_state;
    """

    # ── stream_checkpoint table ──────────────────────────────────────────

    CREATE_STREAM_CHECKPOINT_SQL = """
    CREATE TABLE IF NOT EXISTS stream_checkpoint (
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        stream TEXT NOT NULL,
        last_received_at TIMESTAMPTZ NOT NULL,
        last_collector_session_id TEXT NOT NULL,
        last_gap_reason TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (exchange, symbol, stream)
    );
    """

    UPSERT_STREAM_CHECKPOINT_SQL = """
    INSERT INTO stream_checkpoint
        (exchange, symbol, stream, last_received_at, last_collector_session_id, last_gap_reason, updated_at)
    VALUES
        (%(exchange)s, %(symbol)s, %(stream)s, %(last_received_at)s,
         %(last_collector_session_id)s, %(last_gap_reason)s, NOW())
    ON CONFLICT (exchange, symbol, stream)
    DO UPDATE SET
        last_received_at = EXCLUDED.last_received_at,
        last_collector_session_id = EXCLUDED.last_collector_session_id,
        last_gap_reason = EXCLUDED.last_gap_reason,
        updated_at = NOW();
    """

    LOAD_STREAM_CHECKPOINTS_SQL = """
    SELECT exchange, symbol, stream, last_received_at, last_collector_session_id, last_gap_reason
    FROM stream_checkpoint;
    """

    # ── component_runtime_state table ────────────────────────────────────

    CREATE_COMPONENT_RUNTIME_STATE_SQL = """
    CREATE TABLE IF NOT EXISTS component_runtime_state (
        component TEXT NOT NULL,
        instance_id TEXT NOT NULL,
        host_boot_id TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        last_heartbeat_at TIMESTAMPTZ NOT NULL,
        clean_shutdown_at TIMESTAMPTZ,
        planned_shutdown BOOLEAN NOT NULL DEFAULT FALSE,
        maintenance_id TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (component, instance_id)
    );
    """

    UPSERT_COMPONENT_RUNTIME_SQL = """
    INSERT INTO component_runtime_state
        (component, instance_id, host_boot_id, started_at, last_heartbeat_at,
         clean_shutdown_at, planned_shutdown, maintenance_id, updated_at)
    VALUES
        (%(component)s, %(instance_id)s, %(host_boot_id)s, %(started_at)s,
         %(last_heartbeat_at)s, %(clean_shutdown_at)s, %(planned_shutdown)s,
         %(maintenance_id)s, NOW())
    ON CONFLICT (component, instance_id)
    DO UPDATE SET
        host_boot_id = EXCLUDED.host_boot_id,
        last_heartbeat_at = EXCLUDED.last_heartbeat_at,
        clean_shutdown_at = EXCLUDED.clean_shutdown_at,
        planned_shutdown = EXCLUDED.planned_shutdown,
        maintenance_id = EXCLUDED.maintenance_id,
        updated_at = NOW();
    """

    MARK_CLEAN_SHUTDOWN_SQL = """
    UPDATE component_runtime_state
    SET clean_shutdown_at = NOW(),
        planned_shutdown = %(planned_shutdown)s,
        maintenance_id = %(maintenance_id)s,
        updated_at = NOW()
    WHERE component = %(component)s AND instance_id = %(instance_id)s;
    """

    LOAD_LATEST_COMPONENT_STATES_SQL = """
    SELECT DISTINCT ON (component)
           component, instance_id, host_boot_id, started_at, last_heartbeat_at,
           clean_shutdown_at, planned_shutdown, maintenance_id
    FROM component_runtime_state
    ORDER BY component, started_at DESC;
    """

    # ── maintenance_intent table ─────────────────────────────────────────

    CREATE_MAINTENANCE_INTENT_SQL = """
    CREATE TABLE IF NOT EXISTS maintenance_intent (
        maintenance_id TEXT NOT NULL,
        scope TEXT NOT NULL,
        planned_by TEXT NOT NULL,
        reason TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        consumed_at TIMESTAMPTZ,
        PRIMARY KEY (maintenance_id)
    );
    """

    INSERT_MAINTENANCE_INTENT_SQL = """
    INSERT INTO maintenance_intent
        (maintenance_id, scope, planned_by, reason, created_at, expires_at)
    VALUES
        (%(maintenance_id)s, %(scope)s, %(planned_by)s, %(reason)s, %(created_at)s, %(expires_at)s);
    """

    CONSUME_MAINTENANCE_INTENT_SQL = """
    UPDATE maintenance_intent
    SET consumed_at = NOW()
    WHERE maintenance_id = %(maintenance_id)s AND consumed_at IS NULL;
    """

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._conn = None

    async def connect(self) -> None:
        import psycopg

        self._conn = await psycopg.AsyncConnection.connect(self._db_url, autocommit=True)
        for sql in (
            self.CREATE_TABLE_SQL,
            self.CREATE_STREAM_CHECKPOINT_SQL,
            self.CREATE_COMPONENT_RUNTIME_STATE_SQL,
            self.CREATE_MAINTENANCE_INTENT_SQL,
        ):
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(sql)
            except psycopg.errors.DuplicateObject:
                pass  # Table/type already exists from a previous run

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    async def load_all_states(self) -> dict[tuple[str, int, str], FileState]:
        assert self._conn is not None, "call connect() first"
        states: dict[tuple[str, int, str], FileState] = {}
        async with self._conn.cursor() as cur:
            await cur.execute(self.LOAD_ALL_SQL)
            rows = await cur.fetchall()
            for row in rows:
                state = FileState(
                    topic=row[0],
                    partition=row[1],
                    high_water_offset=row[2],
                    file_path=row[3],
                    file_byte_size=row[4],
                )
                states[state.state_key] = state
        return states

    async def _ensure_connected(self) -> None:
        """Reconnect to PostgreSQL if the connection is closed or broken."""
        import psycopg
        if self._conn is None:
            self._conn = await psycopg.AsyncConnection.connect(self._db_url, autocommit=True)
            return
        try:
            # Lightweight check — execute a no-op
            async with self._conn.cursor() as cur:
                await cur.execute("SELECT 1")
        except (psycopg.OperationalError, psycopg.InterfaceError):
            logger.warning("pg_reconnecting")
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = await psycopg.AsyncConnection.connect(self._db_url, autocommit=True)

    async def save_states(self, states: list[FileState]) -> None:
        """Atomically save multiple file states in a single transaction.

        Retries up to 3 times on connection errors. If all retries fail,
        raises the exception — the writer will crash and resume from PG state on restart.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._ensure_connected()
                assert self._conn is not None
                async with self._conn.transaction():
                    async with self._conn.cursor() as cur:
                        for state in states:
                            await cur.execute(self.UPSERT_SQL, {
                                "topic": state.topic,
                                "partition": state.partition,
                                "high_water_offset": state.high_water_offset,
                                "file_path": state.file_path,
                                "file_byte_size": state.file_byte_size,
                            })
                logger.debug("state_saved", count=len(states))
                return
            except Exception as e:
                logger.warning("pg_save_failed", attempt=attempt + 1, error=str(e))
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

    async def save_states_and_checkpoints(
        self,
        states: list[FileState],
        checkpoints: list[StreamCheckpoint],
    ) -> None:
        """Atomically save file states AND stream checkpoints in a single transaction.

        This ensures both are committed together so a crash between them
        cannot leave the two tables inconsistent.

        Retries up to 3 times on connection errors.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._ensure_connected()
                assert self._conn is not None
                async with self._conn.transaction():
                    async with self._conn.cursor() as cur:
                        for state in states:
                            await cur.execute(self.UPSERT_SQL, {
                                "topic": state.topic,
                                "partition": state.partition,
                                "high_water_offset": state.high_water_offset,
                                "file_path": state.file_path,
                                "file_byte_size": state.file_byte_size,
                            })
                        for cp in checkpoints:
                            await cur.execute(self.UPSERT_STREAM_CHECKPOINT_SQL, {
                                "exchange": cp.exchange,
                                "symbol": cp.symbol,
                                "stream": cp.stream,
                                "last_received_at": cp.last_received_at,
                                "last_collector_session_id": cp.last_collector_session_id,
                                "last_gap_reason": cp.last_gap_reason,
                            })
                logger.debug("states_and_checkpoints_saved",
                             states=len(states), checkpoints=len(checkpoints))
                return
            except Exception as e:
                logger.warning("pg_save_failed", attempt=attempt + 1, error=str(e))
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

    # ── stream checkpoint methods ────────────────────────────────────────

    async def load_stream_checkpoints(self) -> dict[tuple[str, str, str], StreamCheckpoint]:
        """Load all stream checkpoints from PostgreSQL."""
        assert self._conn is not None, "call connect() first"
        checkpoints: dict[tuple[str, str, str], StreamCheckpoint] = {}
        async with self._conn.cursor() as cur:
            await cur.execute(self.LOAD_STREAM_CHECKPOINTS_SQL)
            rows = await cur.fetchall()
            for row in rows:
                cp = StreamCheckpoint(
                    exchange=row[0],
                    symbol=row[1],
                    stream=row[2],
                    last_received_at=str(row[3]),
                    last_collector_session_id=row[4],
                    last_gap_reason=row[5],
                )
                checkpoints[cp.checkpoint_key] = cp
        return checkpoints

    async def save_stream_checkpoints(self, checkpoints: list[StreamCheckpoint]) -> None:
        """Atomically upsert multiple stream checkpoints.

        Retries up to 3 times on connection errors, matching save_states() behavior.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._ensure_connected()
                assert self._conn is not None
                async with self._conn.transaction():
                    async with self._conn.cursor() as cur:
                        for cp in checkpoints:
                            await cur.execute(self.UPSERT_STREAM_CHECKPOINT_SQL, {
                                "exchange": cp.exchange,
                                "symbol": cp.symbol,
                                "stream": cp.stream,
                                "last_received_at": cp.last_received_at,
                                "last_collector_session_id": cp.last_collector_session_id,
                                "last_gap_reason": cp.last_gap_reason,
                            })
                logger.debug("stream_checkpoints_saved", count=len(checkpoints))
                return
            except Exception as e:
                logger.warning("pg_checkpoint_save_failed", attempt=attempt + 1, error=str(e))
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

    # ── component runtime state methods ──────────────────────────────────

    async def upsert_component_runtime(self, state: ComponentRuntimeState) -> None:
        """Insert or update a component runtime state row."""
        await self._ensure_connected()
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(self.UPSERT_COMPONENT_RUNTIME_SQL, {
                "component": state.component,
                "instance_id": state.instance_id,
                "host_boot_id": state.host_boot_id,
                "started_at": state.started_at,
                "last_heartbeat_at": state.last_heartbeat_at,
                "clean_shutdown_at": state.clean_shutdown_at,
                "planned_shutdown": state.planned_shutdown,
                "maintenance_id": state.maintenance_id,
            })
        logger.debug("component_runtime_upserted",
                      component=state.component, instance_id=state.instance_id)

    async def mark_component_clean_shutdown(
        self,
        component: str,
        instance_id: str,
        planned_shutdown: bool = False,
        maintenance_id: str | None = None,
    ) -> None:
        """Mark a component instance as cleanly shut down."""
        await self._ensure_connected()
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(self.MARK_CLEAN_SHUTDOWN_SQL, {
                "component": component,
                "instance_id": instance_id,
                "planned_shutdown": planned_shutdown,
                "maintenance_id": maintenance_id,
            })
        logger.debug("component_clean_shutdown_marked",
                      component=component, instance_id=instance_id)

    async def load_latest_component_states(self) -> list[ComponentRuntimeState]:
        """Load all component runtime state rows."""
        assert self._conn is not None, "call connect() first"
        states: list[ComponentRuntimeState] = []
        async with self._conn.cursor() as cur:
            await cur.execute(self.LOAD_LATEST_COMPONENT_STATES_SQL)
            rows = await cur.fetchall()
            for row in rows:
                states.append(ComponentRuntimeState(
                    component=row[0],
                    instance_id=row[1],
                    host_boot_id=row[2],
                    started_at=str(row[3]),
                    last_heartbeat_at=str(row[4]),
                    clean_shutdown_at=str(row[5]) if row[5] else None,
                    planned_shutdown=row[6],
                    maintenance_id=row[7],
                ))
        return states

    # ── maintenance intent methods ───────────────────────────────────────

    async def create_maintenance_intent(self, intent: MaintenanceIntent) -> None:
        """Insert a new maintenance intent."""
        await self._ensure_connected()
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(self.INSERT_MAINTENANCE_INTENT_SQL, {
                "maintenance_id": intent.maintenance_id,
                "scope": intent.scope,
                "planned_by": intent.planned_by,
                "reason": intent.reason,
                "created_at": intent.created_at,
                "expires_at": intent.expires_at,
            })
        logger.debug("maintenance_intent_created", maintenance_id=intent.maintenance_id)

    async def load_active_maintenance_intent(self) -> MaintenanceIntent | None:
        """Load the most recent unconsumed, unexpired maintenance intent."""
        await self._ensure_connected()
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute("""
                SELECT maintenance_id, scope, planned_by, reason,
                       created_at, expires_at, consumed_at
                FROM maintenance_intent
                WHERE consumed_at IS NULL AND expires_at > NOW()
                ORDER BY created_at DESC
                LIMIT 1;
            """)
            row = await cur.fetchone()
            if row is None:
                return None
            return MaintenanceIntent(
                maintenance_id=row[0],
                scope=row[1],
                planned_by=row[2],
                reason=row[3],
                created_at=str(row[4]),
                expires_at=str(row[5]),
                consumed_at=str(row[6]) if row[6] else None,
            )

    async def consume_maintenance_intent(self, maintenance_id: str) -> None:
        """Mark a maintenance intent as consumed (set consumed_at = NOW())."""
        await self._ensure_connected()
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(self.CONSUME_MAINTENANCE_INTENT_SQL, {
                "maintenance_id": maintenance_id,
            })
        logger.debug("maintenance_intent_consumed", maintenance_id=maintenance_id)
