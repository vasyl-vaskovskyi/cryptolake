class TestDepthPuChainValidator:
    def test_first_diff_after_sync_accepted(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        # First diff: U <= 1001 and u >= 1001
        result = det.validate_diff(U=999, u=1002, pu=998)
        assert result.valid is True
        assert result.gap is False

    def test_subsequent_diff_pu_chain_valid(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        det.validate_diff(U=999, u=1002, pu=998)  # sync
        result = det.validate_diff(U=1003, u=1005, pu=1002)
        assert result.valid is True

    def test_pu_chain_break_detected(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        det.validate_diff(U=999, u=1002, pu=998)  # sync
        result = det.validate_diff(U=1010, u=1015, pu=1008)  # gap: pu!=1002
        assert result.valid is False
        assert result.gap is True
        assert result.reason == "pu_chain_break"

    def test_stale_diff_before_sync_rejected(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        result = det.validate_diff(U=990, u=995, pu=989)  # u < lastUpdateId
        assert result.valid is False
        assert result.stale is True

    def test_no_sync_point_rejects(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        result = det.validate_diff(U=1, u=2, pu=0)
        assert result.valid is False
        assert result.reason == "no_sync_point"

    def test_reset_clears_state(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        det.validate_diff(U=999, u=1002, pu=998)
        det.reset()
        result = det.validate_diff(U=1003, u=1005, pu=1002)
        assert result.valid is False
        assert result.reason == "no_sync_point"


class TestSessionSeqTracker:
    def test_sequential_no_gap(self):
        from src.collector.gap_detector import SessionSeqTracker

        tracker = SessionSeqTracker()
        assert tracker.check(0) is None
        assert tracker.check(1) is None
        assert tracker.check(2) is None

    def test_gap_detected(self):
        from src.collector.gap_detector import SessionSeqTracker

        tracker = SessionSeqTracker()
        tracker.check(0)
        tracker.check(1)
        gap = tracker.check(5)  # skipped 2, 3, 4
        assert gap is not None
        assert gap.expected == 2
        assert gap.actual == 5

    def test_first_message_no_gap(self):
        from src.collector.gap_detector import SessionSeqTracker

        tracker = SessionSeqTracker()
        assert tracker.check(42) is None  # first message, any seq ok
