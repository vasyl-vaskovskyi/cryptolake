from src.exchanges.binance import BinanceAdapter


def _make_adapter():
    return BinanceAdapter(
        ws_base="wss://fstream.binance.com",
        rest_base="https://fapi.binance.com",
    )


def test_build_historical_trades_url():
    url = _make_adapter().build_historical_trades_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000"


def test_build_historical_trades_url_with_from_id():
    url = _make_adapter().build_historical_trades_url("btcusdt", from_id=12345)
    assert "fromId=12345" in url
    assert "startTime" not in url


def test_build_historical_funding_url():
    url = _make_adapter().build_historical_funding_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000"


def test_build_historical_liquidations_url():
    url = _make_adapter().build_historical_liquidations_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/allForceOrders?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000"


def test_build_historical_open_interest_url():
    url = _make_adapter().build_historical_open_interest_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&startTime=1000&endTime=2000&limit=500"


def test_build_mark_price_klines_url():
    adapter = _make_adapter()
    url = adapter.build_mark_price_klines_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&startTime=1000&endTime=2000&limit=1000"


def test_build_index_price_klines_url():
    adapter = _make_adapter()
    url = adapter.build_index_price_klines_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/indexPriceKlines?symbol=BTCUSDT&pair=BTCUSDT&interval=1m&startTime=1000&endTime=2000&limit=1000"


def test_build_premium_index_klines_url():
    adapter = _make_adapter()
    url = adapter.build_premium_index_klines_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol=BTCUSDT&interval=1m&startTime=1000&endTime=2000&limit=1000"
