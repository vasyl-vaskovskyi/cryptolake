package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SessionChangeDetector}.
 *
 * <p>Ports: Python's {@code test_session_change_detector.py} — session gap detection (design §2.2).
 */
class SessionChangeDetectorTest {

  private SessionChangeDetector detector;

  @BeforeEach
  void setUp() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    CoverageFilter coverage = new CoverageFilter(5.0, 10.0, metrics, Clocks.systemNanoClock());
    // GapEmitter needs several dependencies; use a no-op stub via null since
    // SessionChangeDetector only calls observe() → returns Optional; it does NOT call gaps.emit()
    // (that's done by the caller RecordHandler). So gaps can be null for this unit test.
    detector = new SessionChangeDetector(null, coverage, metrics, Clocks.systemNanoClock());
  }

  private DataEnvelope makeEnv(
      String exchange, String symbol, String stream, String sessionId, long seq) {
    return new DataEnvelope(
        1,
        "data",
        exchange,
        symbol,
        stream,
        System.nanoTime(),
        System.nanoTime(),
        sessionId,
        seq,
        "{}",
        "abc");
  }

  // ports: design §2.2 — first envelope for stream → no gap
  @Test
  void observe_firstEnvelope_noGap() {
    DataEnvelope env = makeEnv("binance", "btcusdt", "trades", "col_2024-01-15T14:00:00Z", 1L);

    Optional<GapEnvelope> gap = detector.observe(env, "primary");

    assertThat(gap).isEmpty();
  }

  // ports: design §2.2 — same session, sequential seq → no gap
  @Test
  void observe_sameSameSession_noGap() {
    DataEnvelope env1 = makeEnv("binance", "btcusdt", "trades", "col_2024-01-15T14:00:00Z", 1L);
    DataEnvelope env2 = makeEnv("binance", "btcusdt", "trades", "col_2024-01-15T14:00:00Z", 2L);

    detector.observe(env1, "primary");
    Optional<GapEnvelope> gap = detector.observe(env2, "primary");

    assertThat(gap).isEmpty();
  }

  // ports: design §2.2 — different session → collector_restart gap
  @Test
  void observe_differentSession_emitsGap() {
    DataEnvelope env1 = makeEnv("binance", "btcusdt", "trades", "col_2024-01-15T14:00:00Z", 1L);
    DataEnvelope env2 = makeEnv("binance", "btcusdt", "trades", "col_2024-01-15T15:00:00Z", 1L);

    detector.observe(env1, "primary");
    Optional<GapEnvelope> gap = detector.observe(env2, "primary");

    assertThat(gap).isPresent();
    assertThat(gap.get().reason()).isEqualTo("collector_restart");
    assertThat(gap.get().exchange()).isEqualTo("binance");
    assertThat(gap.get().symbol()).isEqualTo("btcusdt");
    assertThat(gap.get().stream()).isEqualTo("trades");
  }

  // ports: Tier 5 M10 — gap sessionSeq = -1 (writer-injected sentinel)
  @Test
  void observe_sessionChange_gapSessionSeqIsMinus1() {
    DataEnvelope env1 = makeEnv("binance", "btcusdt", "trades", "col_sess_A", 1L);
    DataEnvelope env2 = makeEnv("binance", "btcusdt", "trades", "col_sess_B", 1L);

    detector.observe(env1, "primary");
    Optional<GapEnvelope> gap = detector.observe(env2, "primary");

    assertThat(gap).isPresent();
    assertThat(gap.get().sessionSeq()).isEqualTo(-1L);
  }

  // ports: design §2.2 — different streams tracked independently
  @Test
  void observe_differentStreams_independentTracking() {
    DataEnvelope tradesEnv = makeEnv("binance", "btcusdt", "trades", "col_sess_A", 1L);
    DataEnvelope depthEnv = makeEnv("binance", "btcusdt", "depth", "col_sess_B", 1L);

    detector.observe(tradesEnv, "primary");
    Optional<GapEnvelope> gap = detector.observe(depthEnv, "primary"); // first for depth

    // First envelope for depth stream — no gap
    assertThat(gap).isEmpty();
  }

  // ports: design §2.2 — same symbol, different exchange → no cross-stream confusion
  @Test
  void observe_differentExchanges_trackedSeparately() {
    DataEnvelope binance = makeEnv("binance", "btcusdt", "trades", "col_sess_A", 1L);
    DataEnvelope okx = makeEnv("okx", "btcusdt", "trades", "col_sess_B", 1L);

    detector.observe(binance, "primary");
    Optional<GapEnvelope> gap = detector.observe(okx, "primary"); // first for okx

    assertThat(gap).isEmpty(); // first envelope for okx, not a session change
  }
}
