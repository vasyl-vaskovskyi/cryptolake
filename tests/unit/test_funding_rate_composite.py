from src.cli.gaps import _build_mark_price_update, _compute_next_funding_time

def test_build_mark_price_update():
    mark_kline = [1774828800000, "65973.40", "66053.80", "65973.40", "66019.80", "0", 1774828859999, "0", 60, "0", "0", "0"]
    index_kline = [1774828800000, "66012.22", "66068.94", "66012.22", "66059.43", "0", 1774828859999, "0", 60, "0", "0", "0"]
    premium_kline = [1774828800000, "-0.00014", "-0.00014", "-0.00074", "-0.00064", "0", 1774828859999, "0", 12, "0", "0", "0"]
    funding_rate = "0.00000300"
    result = _build_mark_price_update("BTCUSDT", mark_kline, index_kline, premium_kline, funding_rate)
    assert result["e"] == "markPriceUpdate"
    assert result["s"] == "BTCUSDT"
    assert result["E"] == 1774828860000
    assert result["p"] == "66019.80"
    assert result["ap"] == "66019.80"
    assert result["i"] == "66059.43"
    assert result["P"] == "-0.00064"
    assert result["r"] == "0.00000300"
    assert result["T"] == 1774857600000

def test_compute_next_funding_time_before_8():
    assert _compute_next_funding_time(1774839600000) == 1774857600000

def test_compute_next_funding_time_before_16():
    assert _compute_next_funding_time(1774864800000) == 1774886400000

def test_compute_next_funding_time_after_16():
    assert _compute_next_funding_time(1774900800000) == 1774915200000
