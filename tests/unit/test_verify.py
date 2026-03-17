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
