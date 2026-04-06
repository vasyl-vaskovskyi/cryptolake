"""Tests for smart depth resync: skip snapshot when backup has recent depth diffs."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.collector.connection import WebSocketManager
from src.collector.streams.depth import DepthHandler


def _make_ws_manager(*, topic_prefix=""):
    adapter = MagicMock()
    producer = MagicMock()
    producer.topic_prefix = topic_prefix
    producer.exchange = "binance"
    producer.is_healthy_for_resync.return_value = True

    depth_handler = MagicMock(spec=DepthHandler)
    depth_handler.detectors = {"btcusdt": MagicMock()}

    handlers = {"depth": depth_handler}

    ws = WebSocketManager(
        exchange="binance",
        collector_session_id="test-session",
        adapter=adapter,
        producer=producer,
        handlers=handlers,
        symbols=["btcusdt"],
        enabled_streams=["depth", "trades"],
        brokers=["localhost:9092"],
    )
    return ws, depth_handler, producer


class TestDepthResyncSkipsSnapshot:
    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_skips_snapshot_when_backup_has_recent_data(self, mock_read):
        mock_read.return_value = 12345
        ws, depth_handler, producer = _make_ws_manager()

        await ws._depth_resync("btcusdt")

        depth_handler.set_sync_point.assert_called_once_with("btcusdt", 12345)
        ws.adapter.build_snapshot_url.assert_not_called()
        producer.produce.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_falls_through_to_snapshot_when_no_backup_data(self, mock_read):
        mock_read.return_value = None
        ws, depth_handler, producer = _make_ws_manager()

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = AsyncMock(return_value='{"lastUpdateId": 99999}')
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("src.collector.connection.aiohttp.ClientSession", return_value=mock_session):
            ws.adapter.build_snapshot_url.return_value = "https://example.com/depth"
            ws.adapter.parse_snapshot_last_update_id.return_value = 99999

            await ws._depth_resync("btcusdt")

        ws.adapter.build_snapshot_url.assert_called_once_with("btcusdt")
        depth_handler.set_sync_point.assert_called_once_with("btcusdt", 99999)

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_backup_check_uses_correct_topic_for_primary(self, mock_read):
        mock_read.return_value = 500
        ws, depth_handler, _ = _make_ws_manager(topic_prefix="")

        await ws._depth_resync("btcusdt")

        mock_read.assert_called_once_with(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
            max_age_seconds=30,
        )

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_backup_check_uses_correct_topic_for_backup_collector(self, mock_read):
        mock_read.return_value = 500
        ws, depth_handler, _ = _make_ws_manager(topic_prefix="backup.")

        await ws._depth_resync("btcusdt")

        mock_read.assert_called_once_with(
            brokers=["localhost:9092"],
            topic="binance.depth",
            symbol="btcusdt",
            max_age_seconds=30,
        )

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_logs_skip_on_backup_hit(self, mock_read):
        mock_read.return_value = 12345
        ws, depth_handler, _ = _make_ws_manager()

        with patch("src.collector.connection.logger") as mock_logger:
            await ws._depth_resync("btcusdt")
            mock_logger.info.assert_any_call(
                "depth_resync_skipped_snapshot",
                symbol="btcusdt",
                backup_last_u=12345,
            )
