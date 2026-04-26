package com.cryptolake.collector.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.collector.capture.FrameRoute;
import com.cryptolake.common.envelope.EnvelopeCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BinanceAdapter}.
 *
 * <p>New tests (design §8.1 table).
 */
class BinanceAdapterTest {

  private BinanceAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter =
        new BinanceAdapter(
            "wss://fstream.binance.com",
            "https://fapi.binance.com",
            EnvelopeCodec.newMapper());
  }

  @Test
  // ports: (new) BinanceAdapterTest::routeStreamExtractsRawTextVerbatim
  void routeStreamExtractsRawTextVerbatim() {
    String inner = "{\"e\":\"aggTrade\",\"E\":1234,\"a\":1}";
    String frame = "{\"stream\":\"btcusdt@aggTrade\",\"data\":" + inner + "}";
    FrameRoute route = adapter.routeStream(frame);
    assertThat(route).isNotNull();
    assertThat(route.streamType()).isEqualTo("trades");
    assertThat(route.symbol()).isEqualTo("btcusdt");
    assertThat(route.rawText()).isEqualTo(inner);
  }

  @Test
  // ports: (new) BinanceAdapterTest::parseDepthUpdateIdsReturnsLongs
  void parseDepthUpdateIdsReturnsLongs() {
    // Values exceeding Integer.MAX_VALUE (2^31 = 2147483648)
    long bigU = 3_000_000_000L;
    long bigPu = 2_999_999_999L;
    String rawText =
        String.format(
            "{\"U\":%d,\"u\":%d,\"pu\":%d}", bigU, bigU, bigPu);
    DepthUpdateIds ids = adapter.parseDepthUpdateIds(rawText);
    assertThat(ids.U()).isEqualTo(bigU);
    assertThat(ids.u()).isEqualTo(bigU);
    assertThat(ids.pu()).isEqualTo(bigPu);
  }

  @Test
  // ports: (new) BinanceAdapterTest::parseSnapshotLastUpdateIdReturnsLong
  void parseSnapshotLastUpdateIdReturnsLong() {
    long bigId = 10_415_934_605_920L;
    String rawText = String.format("{\"lastUpdateId\":%d,\"bids\":[],\"asks\":[]}", bigId);
    long result = adapter.parseSnapshotLastUpdateId(rawText);
    assertThat(result).isEqualTo(bigId);
  }

  @Test
  // ports: (new) BinanceAdapterTest::getWsUrlsSplitsPublicMarket
  void getWsUrlsSingleSocket() {
    java.util.List<String> symbols = java.util.List.of("btcusdt");
    java.util.List<String> streams = java.util.List.of("trades", "depth");
    java.util.Map<String, String> urls = adapter.getWsUrls(symbols, streams);
    assertThat(urls).containsKey("ws");
    assertThat(urls.get("ws")).endsWith("/stream");
  }

  @Test
  // ports: (new) BinanceAdapterTest::buildSnapshotUrlUpperCasesSymbol
  void buildSnapshotUrlUpperCasesSymbol() {
    String url = adapter.buildSnapshotUrl("btcusdt", 1000);
    assertThat(url).contains("symbol=BTCUSDT");
  }
}
