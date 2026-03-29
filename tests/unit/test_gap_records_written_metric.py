from src.writer import metrics as writer_metrics

def test_gap_records_written_metric_exists():
    assert hasattr(writer_metrics, "gap_records_written_total")

def test_gap_records_written_has_correct_labels():
    label_names = writer_metrics.gap_records_written_total._labelnames
    assert set(label_names) == {"exchange", "symbol", "stream", "reason"}
