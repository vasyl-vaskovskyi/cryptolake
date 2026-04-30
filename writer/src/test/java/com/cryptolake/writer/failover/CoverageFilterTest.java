package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoverageFilter}.
 *
 * <p>Ports: Python's {@code test_coverage_filter.py} — park/suppress/archive logic (design §4.6;
 * Tier 2 §12).
 */
class CoverageFilterTest {

  private WriterMetrics metrics;
  private AtomicLong fakeClock;
  private CoverageFilter filter;

  @BeforeEach
  void setUp() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new WriterMetrics(registry);
    fakeClock = new AtomicLong(1_000_000_000_000L); // 1000s in nanos
    filter = new CoverageFilter(5.0, 10.0, metrics, fakeClock::get);
  }

  private DataEnvelope makeData(String exchange, String symbol, String stream) {
    return new DataEnvelope(
        1,
        "data",
        exchange,
        symbol,
        stream,
        fakeClock.get(),
        fakeClock.get(),
        "col_sess",
        1L,
        "{}",
        "abc");
  }

  private GapEnvelope makeGap(
      String exchange, String symbol, String stream, long startTs, long endTs) {
    return new GapEnvelope(
        1,
        "gap",
        exchange,
        symbol,
        stream,
        fakeClock.get(),
        "col_sess",
        -1L,
        startTs,
        endTs,
        "ws_disconnect",
        "test",
        null,
        null,
        null,
        null,
        null,
        null);
  }

  // ports: design §4.6 — filter disabled when only one source seen
  @Test
  void handleGap_singleSource_immediatelyAccepted() {
    // Only primary seen
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));

    GapEnvelope gap = makeGap("binance", "btcusdt", "trades", 100L, 200L);
    boolean accepted = filter.handleGap("primary", gap);

    assertThat(accepted).isTrue();
    assertThat(filter.enabled()).isFalse();
  }

  // ports: design §4.6 — both sources → filter enabled
  @Test
  void handleData_bothSources_enablesFilter() {
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));

    assertThat(filter.enabled()).isTrue();
  }

  // ports: design §4.6 — gap parked when other source covers
  @Test
  void handleGap_otherSourceCovers_parks() {
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));

    GapEnvelope gap = makeGap("binance", "btcusdt", "trades", 100L, 200L);
    boolean accepted = filter.handleGap("primary", gap);

    assertThat(accepted).isFalse();
    assertThat(filter.pendingSize()).isEqualTo(1);
  }

  // ports: design §4.6 — grace period expiry → archived
  @Test
  void sweepExpired_graceElapsed_noOtherCoverage_archives() {
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));

    GapEnvelope gap = makeGap("binance", "btcusdt", "trades", 100L, 200L);
    filter.handleGap("primary", gap);
    assertThat(filter.pendingSize()).isEqualTo(1);

    // Advance clock past grace period (5s = 5_000_000_000 ns) and also past the other-source TTL
    fakeClock.addAndGet(10_000_000_000L); // 10s forward

    List<GapEnvelope> expired = filter.sweepExpired();

    assertThat(expired).hasSize(1);
    assertThat(filter.pendingSize()).isEqualTo(0);
  }

  // ports: Tier 2 §12 — coalescing creates new immutable record
  @Test
  void handleGap_coalescesSamePendingGap_maxEndTs() {
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));

    GapEnvelope gap1 = makeGap("binance", "btcusdt", "trades", 100L, 200L);
    GapEnvelope gap2 =
        makeGap("binance", "btcusdt", "trades", 100L, 300L); // same start, higher end

    filter.handleGap("primary", gap1);
    filter.handleGap("primary", gap2);

    // Still just 1 pending (coalesced)
    assertThat(filter.pendingSize()).isEqualTo(1);

    // Advance and sweep
    fakeClock.addAndGet(20_000_000_000L);
    List<GapEnvelope> expired = filter.sweepExpired();

    assertThat(expired).hasSize(1);
    assertThat(expired.get(0).gapEndTs()).isEqualTo(300L); // max end
  }

  // ports: design §3.4 — flushAllPending returns all parked gaps on shutdown
  @Test
  void flushAllPending_returnsAllParkedGaps() {
    // Register data on BOTH streams from BOTH sources so the per-stream coverage
    // check finds backup covering each gap's specific stream.
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));
    filter.handleData("primary", makeData("binance", "btcusdt", "depth"));
    filter.handleData("backup", makeData("binance", "btcusdt", "depth"));

    filter.handleGap("primary", makeGap("binance", "btcusdt", "trades", 100L, 200L));
    filter.handleGap("primary", makeGap("binance", "btcusdt", "depth", 100L, 200L));

    List<GapEnvelope> all = filter.flushAllPending();

    assertThat(all).hasSize(2);
    assertThat(filter.pendingSize()).isEqualTo(0);
  }

  // TWO-COLLECTOR rule — coverage check is PER STREAM, not per source globally.
  // A sparse stream like open_interest must not be falsely "uncovered" just
  // because backup last published a record on a different (faster) stream.
  @Test
  void handleGap_perStreamCoverage_doesNotConfuseSparseStreams() {
    // Both sources active, both delivered "trades" recently.
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));
    // Only PRIMARY has ever delivered open_interest (sparse stream); backup
    // hasn't fired open_interest yet.
    filter.handleData("primary", makeData("binance", "btcusdt", "open_interest"));

    // Gap arises on primary's open_interest stream. Backup has NEVER delivered
    // open_interest, so per-stream coverage is missing → archive immediately.
    GapEnvelope oiGap =
        makeGap("binance", "btcusdt", "open_interest", 100L, 200L);
    boolean accepted = filter.handleGap("primary", oiGap);
    assertThat(accepted)
        .as("backup never delivered open_interest, so per-stream coverage absent")
        .isTrue();

    // But a gap on trades — where backup HAS delivered — must be parked.
    GapEnvelope tradesGap = makeGap("binance", "btcusdt", "trades", 100L, 200L);
    boolean tradesAccepted = filter.handleGap("primary", tradesGap);
    assertThat(tradesAccepted)
        .as("backup recently delivered trades, so per-stream coverage present → park")
        .isFalse();
    assertThat(filter.pendingSize()).isEqualTo(1);
  }
}
