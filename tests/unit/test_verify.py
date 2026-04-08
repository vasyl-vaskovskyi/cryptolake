import hashlib
import zstandard as zstd
import orjson


def _make_data_envelope(offset, raw_text='{"test":1}'):
    raw_sha = hashlib.sha256(raw_text.encode()).hexdigest()
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1741689600_000_000_000,
        "exchange_ts": 1741689600120,
        "collector_session_id": "s", "session_seq": offset,
        "raw_text": raw_text, "raw_sha256": raw_sha,
        "_topic": "binance.trades", "_partition": 0, "_offset": offset,
    }


def _make_archive(tmp_path, envelopes, exchange="binance", symbol="btcusdt",
                  stream="trades", date="2026-03-11", hour=14):
    dir_path = tmp_path / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    data_path = dir_path / f"hour-{hour:02d}.jsonl.zst"
    sc_path = dir_path / f"hour-{hour:02d}.jsonl.zst.sha256"
    cctx = zstd.ZstdCompressor(level=3)
    raw_lines = b"".join(orjson.dumps(e) + b"\n" for e in envelopes)
    compressed = cctx.compress(raw_lines)
    data_path.write_bytes(compressed)
    digest = hashlib.sha256(compressed).hexdigest()
    sc_path.write_text(f"{digest}  {data_path.name}\n")
    return data_path, sc_path


class TestChecksumVerification:
    def test_valid_checksum(self, tmp_path):
        from src.cli.verify import verify_checksum
        data_path, sc_path = _make_archive(tmp_path, [_make_data_envelope(0)])
        assert verify_checksum(data_path, sc_path) == []

    def test_corrupted_file(self, tmp_path):
        from src.cli.verify import verify_checksum
        data_path, sc_path = _make_archive(tmp_path, [_make_data_envelope(0)])
        with open(data_path, "r+b") as f:
            f.seek(5)
            f.write(b"\xff\xff")
        errors = verify_checksum(data_path, sc_path)
        assert len(errors) == 1 and "checksum mismatch" in errors[0].lower()

    def test_missing_sidecar(self, tmp_path):
        from src.cli.verify import verify_checksum
        data_path, sc_path = _make_archive(tmp_path, [_make_data_envelope(0)])
        sc_path.unlink()
        assert len(verify_checksum(data_path, sc_path)) == 1


class TestEnvelopeValidation:
    def test_valid(self):
        from src.cli.verify import verify_envelopes
        assert verify_envelopes([_make_data_envelope(i) for i in range(3)]) == []

    def test_sha256_mismatch(self):
        from src.cli.verify import verify_envelopes
        env = _make_data_envelope(0)
        env["raw_sha256"] = "dead" * 16
        assert len(verify_envelopes([env])) == 1

    def test_missing_field(self):
        from src.cli.verify import verify_envelopes
        env = _make_data_envelope(0)
        del env["raw_text"]
        assert len(verify_envelopes([env])) >= 1


class TestDuplicateOffsets:
    def test_no_duplicates(self):
        from src.cli.verify import check_duplicate_offsets
        assert check_duplicate_offsets([_make_data_envelope(i) for i in range(5)]) == []

    def test_duplicate(self):
        from src.cli.verify import check_duplicate_offsets
        errors = check_duplicate_offsets([_make_data_envelope(0), _make_data_envelope(0)])
        assert len(errors) == 1 and "duplicate" in errors[0].lower()


class TestGapReporting:
    def test_reports_gaps(self):
        from src.cli.verify import report_gaps
        gap = {"v": 1, "type": "gap", "reason": "ws_disconnect",
               "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
               "received_at": 0, "collector_session_id": "s", "session_seq": 0,
               "gap_start_ts": 0, "gap_end_ts": 1, "detail": "test",
               "_topic": "t", "_partition": 0, "_offset": 1}
        assert len(report_gaps([_make_data_envelope(0), gap])) == 1

    def test_no_gaps(self):
        from src.cli.verify import report_gaps
        assert report_gaps([_make_data_envelope(i) for i in range(3)]) == []

    def test_restart_gap_preserves_metadata(self):
        """report_gaps must preserve restart metadata fields."""
        from src.cli.verify import report_gaps
        gap = {"v": 1, "type": "gap", "reason": "restart_gap",
               "exchange": "binance", "symbol": "btcusdt", "stream": "depth",
               "received_at": 0, "collector_session_id": "s", "session_seq": 0,
               "gap_start_ts": 0, "gap_end_ts": 1, "detail": "restart",
               "component": "ws_collector", "cause": "oom_kill",
               "planned": False, "maintenance_id": None,
               "_topic": "t", "_partition": 0, "_offset": 1}
        gaps = report_gaps([_make_data_envelope(0), gap])
        assert len(gaps) == 1
        assert gaps[0]["component"] == "ws_collector"
        assert gaps[0]["cause"] == "oom_kill"
        assert gaps[0]["planned"] is False


class TestRestartGapEnvelopeValidation:
    def test_verify_envelopes_accepts_restart_gap(self):
        """verify_envelopes should not error on restart_gap envelopes with extra fields."""
        from src.cli.verify import verify_envelopes
        gap = {"v": 1, "type": "gap", "reason": "restart_gap",
               "exchange": "binance", "symbol": "btcusdt", "stream": "depth",
               "received_at": 0, "collector_session_id": "s", "session_seq": 0,
               "gap_start_ts": 0, "gap_end_ts": 1, "detail": "restart",
               "component": "ws_collector", "cause": "upgrade",
               "planned": True, "classifier": "rule:scheduled",
               "evidence": {"exit_code": 0},
               "maintenance_id": "maint-001",
               "_topic": "t", "_partition": 0, "_offset": 1}
        errors = verify_envelopes([gap])
        assert errors == []

    def test_verify_envelopes_accepts_restart_gap_without_optional(self):
        """A restart_gap with only required fields is valid."""
        from src.cli.verify import verify_envelopes
        gap = {"v": 1, "type": "gap", "reason": "restart_gap",
               "exchange": "binance", "symbol": "btcusdt", "stream": "depth",
               "received_at": 0, "collector_session_id": "s", "session_seq": 0,
               "gap_start_ts": 0, "gap_end_ts": 1, "detail": "restart",
               "_topic": "t", "_partition": 0, "_offset": 1}
        errors = verify_envelopes([gap])
        assert errors == []


class TestDepthReplay:
    """Tests use received_at for cross-topic ordering (not _offset, which is
    only meaningful within a single topic/partition -- spec 4.1.1)."""

    _base_ts = 1741689600_000_000_000  # base timestamp for ordering

    def _make_depth_env(self, seq, U, u, pu):
        raw = f'{{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":{U},"u":{u},"pu":{pu},"b":[],"a":[]}}'
        env = {**_make_data_envelope(seq, raw), "stream": "depth",
               "_topic": "binance.depth"}
        # Use seq to derive a monotonically increasing received_at
        env["received_at"] = self._base_ts + seq * 1_000_000
        return env

    def _make_snap_env(self, seq, last_update_id):
        raw = f'{{"lastUpdateId":{last_update_id},"bids":[],"asks":[]}}'
        env = {**_make_data_envelope(seq, raw), "stream": "depth_snapshot",
               "_topic": "binance.depth_snapshot"}
        env["received_at"] = self._base_ts + seq * 1_000_000
        return env

    def test_valid_chain_passes(self):
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1003, 1005, 1002),
        ]
        errors = verify_depth_replay(diffs, [snap], [])
        assert errors == []

    def test_pu_break_without_gap_flagged(self):
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1010, 1015, 1008),  # pu break
        ]
        errors = verify_depth_replay(diffs, [snap], [])
        assert len(errors) >= 1 and "pu chain break" in errors[0].lower()

    def test_pu_break_with_depth_gap_is_excused(self):
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        gap = {"type": "gap", "symbol": "btcusdt", "stream": "depth",
               "gap_start_ts": 0, "gap_end_ts": 9999999999_000_000_000,
               "received_at": 1741689600_000_000_000}
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1010, 1015, 1008),
        ]
        errors = verify_depth_replay(diffs, [snap], [gap])
        assert errors == []

    def test_non_depth_gap_does_not_excuse_depth_break(self):
        """A trades gap for the same symbol must NOT excuse a depth pu chain break."""
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        gap = {"type": "gap", "symbol": "btcusdt", "stream": "trades",
               "gap_start_ts": 0, "gap_end_ts": 9999999999_000_000_000,
               "received_at": 1741689600_000_000_000}
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1010, 1015, 1008),
        ]
        errors = verify_depth_replay(diffs, [snap], [gap])
        assert len(errors) >= 1 and "pu chain break" in errors[0].lower()

    def test_first_diff_must_span_sync_point(self):
        """First diff after snapshot must satisfy U <= lastUpdateId+1 <= u."""
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        # Diff that does NOT span lastUpdateId+1=1001: U=1010, u=1015
        diffs = [self._make_depth_env(1, 1010, 1015, 1009)]
        errors = verify_depth_replay(diffs, [snap], [])
        assert len(errors) >= 1 and "sync point" in errors[0].lower()

    def test_multi_symbol_isolated(self):
        """Each symbol's pu chain is validated independently."""
        from src.cli.verify import verify_depth_replay
        # btcusdt has valid chain
        snap_btc = {**self._make_snap_env(0, 1000), "symbol": "btcusdt"}
        diff_btc = {**self._make_depth_env(1, 999, 1002, 998), "symbol": "btcusdt"}
        # ethusdt has broken chain (no snapshot)
        diff_eth_raw = '{"e":"depthUpdate","E":100,"s":"ETHUSDT","U":500,"u":505,"pu":499,"b":[],"a":[]}'
        diff_eth = {**_make_data_envelope(2, diff_eth_raw), "symbol": "ethusdt",
                    "stream": "depth", "_topic": "binance.depth",
                    "received_at": self._base_ts + 2 * 1_000_000}
        errors = verify_depth_replay([diff_btc, diff_eth], [snap_btc], [])
        # btcusdt should pass, ethusdt should fail
        assert any("ethusdt" in e for e in errors)
        assert not any("btcusdt" in e for e in errors)

        def test_multiple_snapshots_picks_spannable_one(self):
        """When multiple snapshots exist (e.g., SnapshotScheduler publishes
        before depth_resync), verify must pick the snapshot whose lid is
        actually spanned by the live diffs, not the first one in time order."""
        from src.cli.verify import verify_depth_replay
        # Snapshot 1 (e.g., from SnapshotScheduler) — published first but
        # stale relative to the live ws state
        snap1 = self._make_snap_env(0, 500)
        # Snapshot 2 (e.g., from depth_resync) — also published before any
        # live diff, with the lid the depth_handler actually synced to
        snap2 = self._make_snap_env(1, 1000)
        # Live diffs — span snap2 (lid=1000), not snap1 (lid=500)
        diffs = [
            self._make_depth_env(2, 999, 1002, 998),
            self._make_depth_env(3, 1003, 1005, 1002),
        ]
        errors = verify_depth_replay(diffs, [snap1, snap2], [])
        assert errors == []

    def test_periodic_snapshot_does_not_break_chain(self):
        """A periodic snapshot published mid-stream must not require the
        next diff to span its lastUpdateId — periodic snapshots are
        reconstruction checkpoints, not live sync points."""
        from src.cli.verify import verify_depth_replay
        # Initial snapshot — synced to live
        snap_initial = self._make_snap_env(0, 1000)
        diff1 = self._make_depth_env(1, 999, 1002, 998)
        diff2 = self._make_depth_env(2, 1003, 1005, 1002)
        # Periodic snapshot mid-stream with stale lid (lid << live u)
        snap_periodic = self._make_snap_env(3, 1004)
        # Live diffs continue with normal pu chain
        diff3 = self._make_depth_env(4, 1006, 1010, 1005)
        diff4 = self._make_depth_env(5, 1011, 1015, 1010)
        errors = verify_depth_replay(
            [diff1, diff2, diff3, diff4],
            [snap_initial, snap_periodic],
            [],
        )
        assert errors == []


class TestGenerateManifest:
    def test_manifest_structure(self, tmp_path):
        from src.cli.verify import generate_manifest
        exchange = "binance"
        date = "2026-03-11"
        hour = 14
        envelopes = [_make_data_envelope(i) for i in range(5)]
        _make_archive(tmp_path, envelopes, exchange=exchange, symbol="btcusdt",
                      stream="trades", date=date, hour=hour)
        manifest = generate_manifest(tmp_path, exchange, date)
        assert manifest["date"] == date
        assert manifest["exchange"] == exchange
        assert "btcusdt" in manifest["symbols"]
        sym = manifest["symbols"]["btcusdt"]
        assert "streams" in sym
        assert "trades" in sym["streams"]
        stream_info = sym["streams"]["trades"]
        assert "hours" in stream_info
        assert hour in stream_info["hours"]
        assert "record_count" in stream_info
        assert stream_info["record_count"] == len(envelopes)
        assert "gaps" in stream_info
        assert stream_info["gaps"] == []

    def test_manifest_includes_backfill_and_late_files(self, tmp_path):
        """generate_manifest should recognize backfill and late files alongside regular files."""
        from src.cli.verify import generate_manifest
        import hashlib
        import zstandard as zstd

        exchange = "binance"
        symbol = "btcusdt"
        stream = "trades"
        date = "2026-03-11"

        # Create regular hour file
        dir_path = tmp_path / exchange / symbol / stream / date
        dir_path.mkdir(parents=True, exist_ok=True)

        envelopes = [_make_data_envelope(i) for i in range(3)]
        cctx = zstd.ZstdCompressor(level=3)
        raw_lines = b"".join(orjson.dumps(e) + b"\n" for e in envelopes)
        compressed = cctx.compress(raw_lines)

        # Write regular hour-14 file
        regular_path = dir_path / "hour-14.jsonl.zst"
        regular_path.write_bytes(compressed)
        digest = hashlib.sha256(compressed).hexdigest()
        (dir_path / "hour-14.jsonl.zst.sha256").write_text(f"{digest}  hour-14.jsonl.zst\n")

        # Write backfill hour-15.backfill-1 file
        backfill_path = dir_path / "hour-15.backfill-1.jsonl.zst"
        backfill_path.write_bytes(compressed)
        digest = hashlib.sha256(compressed).hexdigest()
        (dir_path / "hour-15.backfill-1.jsonl.zst.sha256").write_text(f"{digest}  hour-15.backfill-1.jsonl.zst\n")

        # Write late hour-16.late-100 file
        late_path = dir_path / "hour-16.late-100.jsonl.zst"
        late_path.write_bytes(compressed)
        digest = hashlib.sha256(compressed).hexdigest()
        (dir_path / "hour-16.late-100.jsonl.zst.sha256").write_text(f"{digest}  hour-16.late-100.jsonl.zst\n")

        manifest = generate_manifest(tmp_path, exchange, date)
        stream_info = manifest["symbols"][symbol]["streams"][stream]

        # All three hours should be present
        assert sorted(stream_info["hours"]) == [14, 15, 16]
        assert stream_info["record_count"] == 9  # 3 envelopes x 3 files

    def test_manifest_includes_restart_gap_metadata(self, tmp_path):
        """Manifest gap entries must include restart metadata when present."""
        from src.cli.verify import generate_manifest
        gap_env = {
            "v": 1, "type": "gap", "reason": "restart_gap",
            "exchange": "binance", "symbol": "btcusdt", "stream": "depth",
            "received_at": 0, "collector_session_id": "s", "session_seq": 0,
            "gap_start_ts": 100, "gap_end_ts": 200, "detail": "restart",
            "component": "ws_collector", "cause": "upgrade",
            "planned": True, "maintenance_id": "maint-001",
            "_topic": "t", "_partition": 0, "_offset": 0,
        }
        _make_archive(tmp_path, [gap_env], exchange="binance", symbol="btcusdt",
                      stream="depth", date="2026-03-11", hour=10)
        manifest = generate_manifest(tmp_path, "binance", "2026-03-11")
        gaps = manifest["symbols"]["btcusdt"]["streams"]["depth"]["gaps"]
        assert len(gaps) == 1
        gap = gaps[0]
        assert gap["reason"] == "restart_gap"
        assert gap["component"] == "ws_collector"
        assert gap["cause"] == "upgrade"
        assert gap["planned"] is True
        assert gap["maintenance_id"] == "maint-001"

    def test_manifest_non_restart_gap_no_extra_fields(self, tmp_path):
        """Non-restart gap manifest entries should have None for restart-specific fields."""
        from src.cli.verify import generate_manifest
        gap_env = {
            "v": 1, "type": "gap", "reason": "ws_disconnect",
            "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
            "received_at": 0, "collector_session_id": "s", "session_seq": 0,
            "gap_start_ts": 100, "gap_end_ts": 200, "detail": "disconnect",
            "_topic": "t", "_partition": 0, "_offset": 0,
        }
        _make_archive(tmp_path, [gap_env], exchange="binance", symbol="btcusdt",
                      stream="trades", date="2026-03-11", hour=10)
        manifest = generate_manifest(tmp_path, "binance", "2026-03-11")
        gaps = manifest["symbols"]["btcusdt"]["streams"]["trades"]["gaps"]
        assert len(gaps) == 1
        gap = gaps[0]
        assert gap["reason"] == "ws_disconnect"
        assert gap.get("component") is None
        assert gap.get("cause") is None
        assert gap.get("planned") is None
        assert gap.get("maintenance_id") is None


class TestMarkMaintenanceCLI:
    """Tests for the `mark-maintenance` CLI command."""

    def test_mark_maintenance_basic(self):
        """mark-maintenance should write a maintenance_intent row via StateManager."""
        from unittest.mock import AsyncMock, patch, MagicMock
        from click.testing import CliRunner
        from src.cli.verify import cli

        runner = CliRunner()

        mock_sm_instance = MagicMock()
        mock_sm_instance.connect = AsyncMock()
        mock_sm_instance.create_maintenance_intent = AsyncMock()
        mock_sm_instance.close = AsyncMock()

        with patch("src.cli.verify.StateManager", return_value=mock_sm_instance):
            result = runner.invoke(cli, [
                "mark-maintenance",
                "--db-url", "postgresql://fake:fake@localhost/fake",
                "--scope", "system",
                "--maintenance-id", "deploy-2026-03-18T21-00Z",
                "--reason", "scheduled deploy",
                "--ttl-minutes", "30",
            ])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        mock_sm_instance.connect.assert_called_once()
        mock_sm_instance.create_maintenance_intent.assert_called_once()

        # Verify the intent was created with correct fields
        intent = mock_sm_instance.create_maintenance_intent.call_args[0][0]
        assert intent.maintenance_id == "deploy-2026-03-18T21-00Z"
        assert intent.scope == "system"
        assert intent.reason == "scheduled deploy"
        assert intent.planned_by == "cli"
        # expires_at should be ~30 minutes after created_at
        assert intent.expires_at > intent.created_at

        mock_sm_instance.close.assert_called_once()

    def test_mark_maintenance_default_ttl(self):
        """Default TTL should be 60 minutes if not specified."""
        from unittest.mock import AsyncMock, patch, MagicMock
        from click.testing import CliRunner
        from src.cli.verify import cli

        runner = CliRunner()

        mock_sm_instance = MagicMock()
        mock_sm_instance.connect = AsyncMock()
        mock_sm_instance.create_maintenance_intent = AsyncMock()
        mock_sm_instance.close = AsyncMock()

        with patch("src.cli.verify.StateManager", return_value=mock_sm_instance):
            result = runner.invoke(cli, [
                "mark-maintenance",
                "--db-url", "postgresql://fake:fake@localhost/fake",
                "--scope", "collector",
                "--maintenance-id", "maint-001",
                "--reason", "config change",
            ])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        intent = mock_sm_instance.create_maintenance_intent.call_args[0][0]
        # With default TTL of 60 minutes, expires_at should be ~60 min after created_at
        from datetime import datetime
        created = datetime.fromisoformat(intent.created_at)
        expires = datetime.fromisoformat(intent.expires_at)
        delta = (expires - created).total_seconds()
        assert 3590 <= delta <= 3610  # ~60 minutes with tolerance

    def test_mark_maintenance_output_message(self):
        """CLI should output confirmation message."""
        from unittest.mock import AsyncMock, patch, MagicMock
        from click.testing import CliRunner
        from src.cli.verify import cli

        runner = CliRunner()

        mock_sm_instance = MagicMock()
        mock_sm_instance.connect = AsyncMock()
        mock_sm_instance.create_maintenance_intent = AsyncMock()
        mock_sm_instance.close = AsyncMock()

        with patch("src.cli.verify.StateManager", return_value=mock_sm_instance):
            result = runner.invoke(cli, [
                "mark-maintenance",
                "--db-url", "postgresql://fake:fake@localhost/fake",
                "--scope", "system",
                "--maintenance-id", "deploy-001",
                "--reason", "test",
            ])

        assert result.exit_code == 0
        assert "deploy-001" in result.output
        assert "maintenance" in result.output.lower()

    def test_mark_maintenance_requires_mandatory_args(self):
        """CLI should fail if required options are missing."""
        from click.testing import CliRunner
        from src.cli.verify import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["mark-maintenance"])
        assert result.exit_code != 0

    def test_mark_maintenance_does_not_shut_anything_down(self):
        """mark-maintenance should only write intent; it should NOT trigger shutdown."""
        from unittest.mock import AsyncMock, patch, MagicMock
        from click.testing import CliRunner
        from src.cli.verify import cli

        runner = CliRunner()

        mock_sm_instance = MagicMock()
        mock_sm_instance.connect = AsyncMock()
        mock_sm_instance.create_maintenance_intent = AsyncMock()
        mock_sm_instance.close = AsyncMock()

        with patch("src.cli.verify.StateManager", return_value=mock_sm_instance) as mock_sm_cls:
            result = runner.invoke(cli, [
                "mark-maintenance",
                "--db-url", "postgresql://fake:fake@localhost/fake",
                "--scope", "system",
                "--maintenance-id", "deploy-001",
                "--reason", "test",
            ])

        assert result.exit_code == 0
        # Only connect, create_maintenance_intent, and close should be called
        mock_sm_instance.connect.assert_called_once()
        mock_sm_instance.create_maintenance_intent.assert_called_once()
        mock_sm_instance.close.assert_called_once()
        # No consume, no shutdown methods
        mock_sm_instance.consume_maintenance_intent.assert_not_called()
        mock_sm_instance.mark_component_clean_shutdown.assert_not_called()
