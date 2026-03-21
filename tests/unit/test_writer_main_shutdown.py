"""Verify writer main() awaits shutdown completion before loop.close()."""
from pathlib import Path

def test_writer_main_awaits_shutdown_on_signal():
    source = Path("src/writer/main.py").read_text()
    assert "shutdown_task" in source, "Writer main() must capture shutdown_task like collector"
    assert "run_until_complete(shutdown_task)" in source, (
        "Writer main() must await shutdown_task in finally block"
    )
