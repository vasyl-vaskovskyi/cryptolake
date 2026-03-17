from __future__ import annotations

import asyncio
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


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
        high_water_offset = EXCLUDED.high_water_offset,
        file_byte_size = EXCLUDED.file_byte_size,
        updated_at = NOW();
    """

    LOAD_ALL_SQL = """
    SELECT topic, partition, high_water_offset, file_path, file_byte_size
    FROM writer_file_state;
    """

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._conn = None

    async def connect(self) -> None:
        import psycopg

        self._conn = await psycopg.AsyncConnection.connect(self._db_url)
        async with self._conn.cursor() as cur:
            await cur.execute(self.CREATE_TABLE_SQL)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    async def load_all_states(self) -> dict[tuple[str, int, str], FileState]:
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
        try:
            # Lightweight check — execute a no-op
            async with self._conn.cursor() as cur:
                await cur.execute("SELECT 1")
        except (psycopg.OperationalError, psycopg.InterfaceError, Exception):
            logger.warning("pg_reconnecting")
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = await psycopg.AsyncConnection.connect(self._db_url)

    async def save_states(self, states: list[FileState]) -> None:
        """Atomically save multiple file states in a single transaction.

        Retries up to 3 times on connection errors. If all retries fail,
        raises the exception — the writer will crash and resume from PG state on restart.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._ensure_connected()
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
