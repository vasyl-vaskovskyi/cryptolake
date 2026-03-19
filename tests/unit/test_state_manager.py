class TestWriterState:
    def test_state_record_creation(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades",
            partition=0,
            high_water_offset=1000,
            file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
            file_byte_size=52428,
        )
        assert state.topic == "binance.trades"
        assert state.partition == 0
        assert state.high_water_offset == 1000
        assert state.file_byte_size == 52428

    def test_state_key(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades", partition=0,
            high_water_offset=100,
            file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
            file_byte_size=0,
        )
        assert state.state_key == ("binance.trades", 0, "/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst")

    def test_multiple_files_same_topic_partition(self):
        """Multiple files for same (topic, partition) must have distinct state keys."""
        from src.writer.state_manager import FileState

        s1 = FileState(topic="binance.trades", partition=0, high_water_offset=100,
                       file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
                       file_byte_size=1000)
        s2 = FileState(topic="binance.trades", partition=0, high_water_offset=200,
                       file_path="/data/binance/ethusdt/trades/2026-03-11/hour-14.jsonl.zst",
                       file_byte_size=2000)
        assert s1.state_key != s2.state_key


class TestStateManagerSQL:
    """These tests verify SQL generation. Full integration is in test_writer_rotation.py."""

    def test_create_table_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_TABLE_SQL
        assert "writer_file_state" in sql
        assert "topic" in sql
        assert "partition" in sql
        assert "file_path" in sql
        assert "high_water_offset" in sql
        assert "file_byte_size" in sql

    def test_upsert_sql_file_path_in_pk(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.UPSERT_SQL
        assert "INSERT" in sql
        assert "ON CONFLICT" in sql
        assert "file_path" in sql


class TestStreamCheckpoint:
    """Tests for StreamCheckpoint dataclass and related SQL."""

    def test_stream_checkpoint_creation(self):
        from src.writer.state_manager import StreamCheckpoint

        cp = StreamCheckpoint(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            last_received_at="2026-03-11T14:30:00Z",
            last_collector_session_id="session-abc-123",
            last_gap_reason="restart_gap",
        )
        assert cp.exchange == "binance"
        assert cp.symbol == "btcusdt"
        assert cp.stream == "trades"
        assert cp.last_received_at == "2026-03-11T14:30:00Z"
        assert cp.last_collector_session_id == "session-abc-123"
        assert cp.last_gap_reason == "restart_gap"

    def test_stream_checkpoint_key(self):
        from src.writer.state_manager import StreamCheckpoint

        cp = StreamCheckpoint(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            last_received_at="2026-03-11T14:30:00Z",
            last_collector_session_id="session-abc-123",
        )
        assert cp.checkpoint_key == ("binance", "btcusdt", "trades")

    def test_stream_checkpoint_optional_gap_reason(self):
        """last_gap_reason should default to None."""
        from src.writer.state_manager import StreamCheckpoint

        cp = StreamCheckpoint(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            last_received_at="2026-03-11T14:30:00Z",
            last_collector_session_id="session-abc-123",
        )
        assert cp.last_gap_reason is None

    def test_create_stream_checkpoint_table_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_STREAM_CHECKPOINT_SQL
        assert "stream_checkpoint" in sql
        assert "exchange" in sql
        assert "symbol" in sql
        assert "stream" in sql
        assert "last_received_at" in sql
        assert "last_collector_session_id" in sql
        assert "last_gap_reason" in sql
        assert "updated_at" in sql
        assert "PRIMARY KEY" in sql

    def test_upsert_stream_checkpoint_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.UPSERT_STREAM_CHECKPOINT_SQL
        assert "INSERT" in sql
        assert "stream_checkpoint" in sql
        assert "ON CONFLICT" in sql
        assert "exchange" in sql
        assert "symbol" in sql
        assert "stream" in sql

    def test_load_stream_checkpoints_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.LOAD_STREAM_CHECKPOINTS_SQL
        assert "SELECT" in sql
        assert "stream_checkpoint" in sql
        assert "exchange" in sql
        assert "symbol" in sql
        assert "stream" in sql


class TestComponentRuntimeState:
    """Tests for ComponentRuntimeState dataclass and related SQL."""

    def test_component_runtime_state_creation(self):
        from src.writer.state_manager import ComponentRuntimeState

        state = ComponentRuntimeState(
            component="collector",
            instance_id="collector-01",
            host_boot_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            started_at="2026-03-11T14:00:00Z",
            last_heartbeat_at="2026-03-11T14:30:00Z",
        )
        assert state.component == "collector"
        assert state.instance_id == "collector-01"
        assert state.host_boot_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert state.started_at == "2026-03-11T14:00:00Z"
        assert state.last_heartbeat_at == "2026-03-11T14:30:00Z"

    def test_component_runtime_state_optional_fields(self):
        """clean_shutdown_at, planned_shutdown, maintenance_id should be optional."""
        from src.writer.state_manager import ComponentRuntimeState

        state = ComponentRuntimeState(
            component="writer",
            instance_id="writer-01",
            host_boot_id="boot-id-xyz",
            started_at="2026-03-11T14:00:00Z",
            last_heartbeat_at="2026-03-11T14:30:00Z",
        )
        assert state.clean_shutdown_at is None
        assert state.planned_shutdown is False
        assert state.maintenance_id is None

    def test_component_runtime_state_with_shutdown(self):
        from src.writer.state_manager import ComponentRuntimeState

        state = ComponentRuntimeState(
            component="collector",
            instance_id="collector-01",
            host_boot_id="boot-id-123",
            started_at="2026-03-11T14:00:00Z",
            last_heartbeat_at="2026-03-11T15:00:00Z",
            clean_shutdown_at="2026-03-11T15:00:00Z",
            planned_shutdown=True,
            maintenance_id="maint-001",
        )
        assert state.clean_shutdown_at == "2026-03-11T15:00:00Z"
        assert state.planned_shutdown is True
        assert state.maintenance_id == "maint-001"

    def test_create_component_runtime_state_table_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_COMPONENT_RUNTIME_STATE_SQL
        assert "component_runtime_state" in sql
        assert "component" in sql
        assert "instance_id" in sql
        assert "host_boot_id" in sql
        assert "started_at" in sql
        assert "last_heartbeat_at" in sql
        assert "clean_shutdown_at" in sql
        assert "planned_shutdown" in sql
        assert "maintenance_id" in sql
        assert "updated_at" in sql
        assert "PRIMARY KEY" in sql

    def test_upsert_component_runtime_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.UPSERT_COMPONENT_RUNTIME_SQL
        assert "INSERT" in sql
        assert "component_runtime_state" in sql
        assert "ON CONFLICT" in sql

    def test_mark_clean_shutdown_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.MARK_CLEAN_SHUTDOWN_SQL
        assert "UPDATE" in sql
        assert "component_runtime_state" in sql
        assert "clean_shutdown_at" in sql

    def test_load_latest_component_states_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.LOAD_LATEST_COMPONENT_STATES_SQL
        assert "SELECT" in sql
        assert "component_runtime_state" in sql


class TestMaintenanceIntent:
    """Tests for MaintenanceIntent dataclass and related SQL."""

    def test_maintenance_intent_creation(self):
        from src.writer.state_manager import MaintenanceIntent

        intent = MaintenanceIntent(
            maintenance_id="maint-001",
            scope="collector",
            planned_by="operator",
            reason="Upgrade to v2.0",
            created_at="2026-03-11T14:00:00Z",
            expires_at="2026-03-11T16:00:00Z",
        )
        assert intent.maintenance_id == "maint-001"
        assert intent.scope == "collector"
        assert intent.planned_by == "operator"
        assert intent.reason == "Upgrade to v2.0"
        assert intent.created_at == "2026-03-11T14:00:00Z"
        assert intent.expires_at == "2026-03-11T16:00:00Z"

    def test_maintenance_intent_optional_consumed_at(self):
        """consumed_at should default to None."""
        from src.writer.state_manager import MaintenanceIntent

        intent = MaintenanceIntent(
            maintenance_id="maint-001",
            scope="all",
            planned_by="automation",
            reason="Scheduled restart",
            created_at="2026-03-11T14:00:00Z",
            expires_at="2026-03-11T16:00:00Z",
        )
        assert intent.consumed_at is None

    def test_maintenance_intent_with_consumed_at(self):
        from src.writer.state_manager import MaintenanceIntent

        intent = MaintenanceIntent(
            maintenance_id="maint-001",
            scope="writer",
            planned_by="operator",
            reason="Config change",
            created_at="2026-03-11T14:00:00Z",
            expires_at="2026-03-11T16:00:00Z",
            consumed_at="2026-03-11T14:05:00Z",
        )
        assert intent.consumed_at == "2026-03-11T14:05:00Z"

    def test_create_maintenance_intent_table_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_MAINTENANCE_INTENT_SQL
        assert "maintenance_intent" in sql
        assert "maintenance_id" in sql
        assert "scope" in sql
        assert "planned_by" in sql
        assert "reason" in sql
        assert "created_at" in sql
        assert "expires_at" in sql
        assert "consumed_at" in sql
        assert "PRIMARY KEY" in sql

    def test_create_maintenance_intent_insert_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.INSERT_MAINTENANCE_INTENT_SQL
        assert "INSERT" in sql
        assert "maintenance_intent" in sql

    def test_consume_maintenance_intent_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CONSUME_MAINTENANCE_INTENT_SQL
        assert "UPDATE" in sql
        assert "maintenance_intent" in sql
        assert "consumed_at" in sql


class TestExistingWriterFileStateSQLUnchanged:
    """Verify that existing writer_file_state SQL constants remain intact."""

    def test_create_table_sql_still_has_writer_file_state(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_TABLE_SQL
        assert "writer_file_state" in sql
        assert "topic" in sql
        assert "partition" in sql
        assert "file_path" in sql
        assert "high_water_offset" in sql
        assert "file_byte_size" in sql
        assert "PRIMARY KEY (topic, partition, file_path)" in sql

    def test_upsert_sql_still_has_writer_file_state(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.UPSERT_SQL
        assert "writer_file_state" in sql
        assert "INSERT" in sql
        assert "ON CONFLICT" in sql
        assert "GREATEST" in sql

    def test_load_all_sql_still_has_writer_file_state(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.LOAD_ALL_SQL
        assert "writer_file_state" in sql
        assert "SELECT" in sql

    def test_file_state_dataclass_unchanged(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades",
            partition=0,
            high_water_offset=1000,
            file_path="/data/test.jsonl.zst",
            file_byte_size=52428,
        )
        assert state.state_key == ("binance.trades", 0, "/data/test.jsonl.zst")
