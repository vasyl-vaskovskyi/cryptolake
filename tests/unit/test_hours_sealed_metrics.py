from src.writer import metrics as writer_metrics

def test_hours_sealed_today_metric_exists():
    assert hasattr(writer_metrics, "hours_sealed_today")

def test_hours_sealed_previous_day_metric_exists():
    assert hasattr(writer_metrics, "hours_sealed_previous_day")

def test_hours_sealed_today_labels():
    assert set(writer_metrics.hours_sealed_today._labelnames) == {
        "exchange", "symbol", "stream",
    }

def test_hours_sealed_previous_day_labels():
    assert set(writer_metrics.hours_sealed_previous_day._labelnames) == {
        "exchange", "symbol", "stream",
    }
