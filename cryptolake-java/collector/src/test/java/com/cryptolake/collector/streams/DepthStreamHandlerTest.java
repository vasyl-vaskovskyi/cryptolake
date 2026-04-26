package com.cryptolake.collector.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.producer.TestProducerBridge;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.ClockSupplier;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DepthStreamHandler}.
 *
 * <p>Ports depth handler tests from design §8.1 table.
 */
class DepthStreamHandlerTest {

  private TestProducerBridge producer;
  private GapEmitter gapEmitter;
  private BinanceAdapter adapter;
  private List<String> puBreakTriggers;
  private DepthStreamHandler handler;
  private final ClockSupplier clock = () -> 1_000_000_000_000L;

  @BeforeEach
  void setUp() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = new com.cryptolake.collector.metrics.CollectorMetrics(registry);
    producer = new TestProducerBridge();
    var session = new CollectorSession("test", "test_2026-01-01T00:00:00Z", Instant.now());
    gapEmitter = new GapEmitter("binance", session, producer, metrics, clock);
    adapter =
        new BinanceAdapter(
            "wss://fstream.binance.com", "https://fapi.binance.com", EnvelopeCodec.newMapper());
    puBreakTriggers = new ArrayList<>();
    handler =
        new DepthStreamHandler(
            "binance",
            session,
            adapter,
            producer,
            gapEmitter,
            clock,
            symbol -> puBreakTriggers.add(symbol));
  }

  private static String depthRaw(long U, long u, long pu) {
    return String.format(
        "{\"e\":\"depthUpdate\",\"E\":1234,\"s\":\"BTCUSDT\",\"U\":%d,\"u\":%d,\"pu\":%d,"
            + "\"b\":[],\"a\":[]}",
        U, u, pu);
  }

  @Test
  // ports: (new) DepthStreamHandlerTest::handleValidDiffProduces
  void handleValidDiffProduces() {
    // Set sync point first, then send a valid diff
    handler.setSyncPoint("btcusdt", 100L);
    String rawText = depthRaw(100L, 105L, 99L);
    handler.handle("btcusdt", rawText, 1234L, 0L);
    assertThat(producer.dataEnvelopes).hasSize(1);
    assertThat(puBreakTriggers).isEmpty();
  }

  @Test
  // ports: (new) DepthStreamHandlerTest::handleStaleDiffRecordsDrop
  void handleStaleDiffRecordsDrop() {
    handler.setSyncPoint("btcusdt", 100L);
    // u < lid (50 < 100) → stale, should be discarded
    handler.handle("btcusdt", depthRaw(45L, 50L, 44L), 1234L, 0L);
    assertThat(producer.dataEnvelopes).isEmpty();
    assertThat(puBreakTriggers).isEmpty();
  }

  @Test
  // ports: (new) DepthStreamHandlerTest::handlePuChainBreakTriggersResyncCallback
  void handlePuChainBreakTriggersResyncCallback() {
    handler.setSyncPoint("btcusdt", 100L);
    // Valid first diff
    handler.handle("btcusdt", depthRaw(100L, 105L, 99L), 1234L, 0L);
    // Now break the chain: pu should be 105 but we send 103
    handler.handle("btcusdt", depthRaw(106L, 110L, 103L), 1234L, 1L);
    assertThat(puBreakTriggers).containsExactly("btcusdt");
    assertThat(producer.gapEnvelopes).hasSize(1);
    assertThat(producer.gapEnvelopes.get(0).reason()).isEqualTo("pu_chain_break");
  }

  @Test
  // ports: (new) DepthStreamHandlerTest::handleBeforeSyncBuffersDiff
  void handleBeforeSyncBuffersDiff() {
    // No sync point: diffs should be buffered, not produced
    handler.handle("btcusdt", depthRaw(100L, 105L, 99L), 1234L, 0L);
    assertThat(producer.dataEnvelopes).isEmpty();
    assertThat(puBreakTriggers).isEmpty();
  }

  @Test
  // ports: (new) DepthStreamHandlerTest::setSyncPointReplaysBufferedDiffs
  void setSyncPointReplaysBufferedDiffs() {
    // Buffer a diff
    handler.handle("btcusdt", depthRaw(100L, 105L, 99L), 1234L, 0L);
    assertThat(producer.dataEnvelopes).isEmpty();

    // Set sync point with lastUpdateId=99 so U=100 <= 100 and u=105 >= 100
    handler.setSyncPoint("btcusdt", 99L);
    // The buffered diff should now have been replayed
    assertThat(producer.dataEnvelopes).hasSize(1);
  }

  @Test
  // ports: (new) DepthStreamHandlerTest::bufferFullRecordsDropAndTriggersResync
  void bufferFullRecordsDropAndTriggersResync() {
    // Fill the buffer beyond MAX_PENDING_DIFFS
    for (int i = 0; i <= DepthStreamHandler.MAX_PENDING_DIFFS; i++) {
      handler.handle("btcusdt", depthRaw(i + 1L, i + 5L, (long) i), null, (long) i);
    }
    // On overflow: resync triggered
    assertThat(puBreakTriggers).isNotEmpty();
  }
}
