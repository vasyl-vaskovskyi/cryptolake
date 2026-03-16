from __future__ import annotations

import json


class TestStructuredLogging:
    def test_json_output(self, capsys) -> None:
        from src.common.logging import get_logger, setup_logging

        setup_logging()
        log = get_logger(component="test", symbol="btcusdt")
        log.info("test_event", session_seq=42)

        output = capsys.readouterr().out
        parsed = json.loads(output.strip())
        assert parsed["event"] == "test_event"
        assert parsed["component"] == "test"
        assert parsed["symbol"] == "btcusdt"
        assert parsed["session_seq"] == 42
        assert "timestamp" in parsed
        assert parsed["level"] == "info"
