package com.cryptolake.writer.buffer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Clocks;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BufferManager}.
 *
 * <p>Ports: Python's {@code test_buffer_manager.py} — routing, flush threshold, interval (Tier 5
 * A5, B2, F3, F4, M1, M9).
 */
class BufferManagerTest {

  private EnvelopeCodec codec;
  private BufferManager manager;

  @BeforeEach
  void setUp() {
    codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    // threshold=5, interval=60s
    manager = new BufferManager("/archive", 5, 60, codec);
  }

  private DataEnvelope makeEnvelope(String exchange, String symbol, String stream, long tsNs) {
    return new DataEnvelope(
        1, "data", exchange, symbol, stream, tsNs, tsNs, "collector_2024-01-15T14:00:00Z", 1L,
        "{}", "abc123");
  }

  // ports: Tier 5 M1 — symbol lowercased in route()
  @Test
  void route_uppercaseSymbol_lowercasedInTarget() {
    DataEnvelope env = makeEnvelope("binance", "BTCUSDT", "trades",
        1705329600_000_000_000L); // 2024-01-15 14:00:00 UTC
    FileTarget target = manager.route(env);

    assertThat(target.symbol()).isEqualTo("btcusdt");
  }

  // ports: Tier 5 F3 — UTC date/hour extraction
  @Test
  void route_derivesUtcDateAndHour() {
    // 2024-01-15 14:30:00 UTC in nanos
    long tsNs = 1705329600_000_000_000L; // 2024-01-15 14:00:00 UTC
    DataEnvelope env = makeEnvelope("binance", "btcusdt", "trades", tsNs);

    FileTarget target = manager.route(env);

    assertThat(target.date()).isEqualTo("2024-01-15");
    assertThat(target.hour()).isEqualTo(14);
  }

  // ports: Tier 5 B2 — append returns empty before threshold
  @Test
  void add_belowThreshold_returnsEmpty() {
    DataEnvelope env = makeEnvelope("binance", "btcusdt", "trades", 1705329600_000_000_000L);
    BrokerCoordinates coords = new BrokerCoordinates("binance.trades", 0, 100L);

    Optional<List<FlushResult>> result = manager.add(env, coords, "primary");

    assertThat(result).isEmpty();
  }

  // ports: design §4.3 — auto-flush at threshold
  @Test
  void add_atThreshold_autoFlushes() {
    long tsNs = 1705329600_000_000_000L;
    for (int i = 0; i < 4; i++) {
      DataEnvelope env = makeEnvelope("binance", "btcusdt", "trades", tsNs);
      BrokerCoordinates coords = new BrokerCoordinates("binance.trades", 0, (long) i);
      assertThat(manager.add(env, coords, "primary")).isEmpty();
    }

    // 5th record triggers flush
    DataEnvelope env = makeEnvelope("binance", "btcusdt", "trades", tsNs);
    BrokerCoordinates coords = new BrokerCoordinates("binance.trades", 0, 4L);
    Optional<List<FlushResult>> result = manager.add(env, coords, "primary");

    assertThat(result).isPresent();
    assertThat(result.get()).hasSize(1);
    assertThat(result.get().get(0).count()).isEqualTo(5);
  }

  // ports: Tier 5 M9 — synthetic offset -1L does not update high-water
  @Test
  void add_syntheticOffsetMinus1_doesNotUpdateHighWater() {
    long tsNs = 1705329600_000_000_000L;
    // Add real record with offset 100
    DataEnvelope env1 = makeEnvelope("binance", "btcusdt", "trades", tsNs);
    manager.add(env1, new BrokerCoordinates("binance.trades", 0, 100L), "primary");

    // Add synthetic with offset -1
    DataEnvelope env2 = makeEnvelope("binance", "btcusdt", "trades", tsNs);
    manager.add(env2, new BrokerCoordinates("binance.trades", 0, -1L), "primary");

    List<FlushResult> results = manager.flushAll();
    assertThat(results).hasSize(1);
    // High water should still be 100, not -1
    assertThat(results.get(0).highWaterOffset()).isEqualTo(100L);
  }

  // ports: Tier 5 F4 — shouldFlushByInterval uses nanoTime
  @Test
  void shouldFlushByInterval_freshManager_returnsFalse() {
    // interval is 60s; freshly constructed should not be due
    assertThat(manager.shouldFlushByInterval()).isFalse();
  }

  // ports: design §4.3 — flushAll empties all buffers
  @Test
  void flushAll_returnsResultsForAllBuffers() {
    long tsNs = 1705329600_000_000_000L;

    manager.add(makeEnvelope("binance", "btcusdt", "trades", tsNs),
        new BrokerCoordinates("binance.trades", 0, 1L), "primary");
    manager.add(makeEnvelope("binance", "ethusdt", "trades", tsNs),
        new BrokerCoordinates("binance.trades", 1, 1L), "primary");

    List<FlushResult> results = manager.flushAll();

    assertThat(results).hasSize(2);
  }

  // ports: design §4.3 — flushKey flushes only matching stream
  @Test
  void flushKey_flushesOnlyMatchingStream() {
    long tsNs = 1705329600_000_000_000L;

    manager.add(makeEnvelope("binance", "btcusdt", "trades", tsNs),
        new BrokerCoordinates("binance.trades", 0, 1L), "primary");
    manager.add(makeEnvelope("binance", "btcusdt", "depth", tsNs),
        new BrokerCoordinates("binance.depth", 0, 1L), "primary");

    List<FlushResult> results =
        manager.flushKey(new com.cryptolake.writer.StreamKey("binance", "btcusdt", "trades"));

    assertThat(results).hasSize(1);
    assertThat(results.get(0).target().stream()).isEqualTo("trades");
  }

  // ports: Tier 5 B2 — each line ends with 0x0A newline
  @Test
  void add_lineBytes_endsWithNewline() {
    long tsNs = 1705329600_000_000_000L;
    manager.add(makeEnvelope("binance", "btcusdt", "trades", tsNs),
        new BrokerCoordinates("binance.trades", 0, 1L), "primary");

    List<FlushResult> results = manager.flushAll();
    byte[] line = results.get(0).lines().get(0);
    assertThat(line[line.length - 1]).isEqualTo((byte) 0x0A);
  }
}
