from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_kill_writer_script_resumes_writes_then_runs_verify() -> None:
    text = (PROJECT_ROOT / "tests/chaos/3_writer_crash_before_commit.sh").read_text()

    assert "wait_for_envelope_count_gt" in text
    assert "wait_for_writer_lag_below" in text
    assert "uv run cryptolake verify" in text


def test_common_helpers_can_poll_writer_lag() -> None:
    text = (PROJECT_ROOT / "tests/chaos/common.sh").read_text()

    assert "wait_for_writer_lag_below" in text
    assert "writer_consumer_lag" in text
