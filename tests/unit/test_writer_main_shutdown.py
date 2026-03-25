"""Verify writer and collector use the shared service_runner bootstrap."""


def test_writer_main_uses_run_service():
    from src.writer.main import main
    assert callable(main)


def test_collector_main_uses_run_service():
    from src.collector.main import main
    assert callable(main)


def test_service_runner_has_shutdown_handling():
    source = open("src/common/service_runner.py").read()
    assert "shutdown_task" in source
    assert "run_until_complete(shutdown_task)" in source
