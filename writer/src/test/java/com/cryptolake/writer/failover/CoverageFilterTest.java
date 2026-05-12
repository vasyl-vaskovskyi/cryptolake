package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.envelope.GapReason;
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
        GapReason.WS_DISCONNECT,
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
    // Both sources deliver at t=1000s; backup's last delivery is now 1000s_ns.
    filter.handleData("primary", makeData("binance", "btcusdt", "trades"));
    filter.handleData("backup", makeData("binance", "btcusdt", "trades"));
    long backupLastTs = fakeClock.get();

    // Gap starts AFTER backup's last delivery — backup did not bridge it.
    long gapStart = backupLastTs + 1_000_000_000L; // gap begins 1s after backup's last
    long gapEnd = backupLastTs + 2_000_000_000L;
    GapEnvelope gap = makeGap("binance", "btcusdt", "trades", gapStart, gapEnd);
    fakeClock.set(gapEnd); // gap arrives at the end of its window
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
    long backupLastTs = fakeClock.get();

    // Gap starts AFTER backup's last delivery so it is not auto-suppressed by
    // the gap-window coverage check; both gaps share gap_start, different gap_ends.
    long gapStart = backupLastTs + 1_000_000_000L;
    GapEnvelope gap1 = makeGap("binance", "btcusdt", "trades", gapStart, gapStart + 100L);
    GapEnvelope gap2 =
        makeGap(
            "binance", "btcusdt", "trades", gapStart, gapStart + 200L); // same start, higher end

    fakeClock.set(gapStart + 100L);
    filter.handleGap("primary", gap1);
    fakeClock.set(gapStart + 200L);
    filter.handleGap("primary", gap2);

    // Still just 1 pending (coalesced)
    assertThat(filter.pendingSize()).isEqualTo(1);

    // Advance and sweep
    fakeClock.addAndGet(20_000_000_000L);
    List<GapEnvelope> expired = filter.sweepExpired();

    assertThat(expired).hasSize(1);
    assertThat(expired.get(0).gapEndTs()).isEqualTo(gapStart + 200L); // max end
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

  // BUG A regression — chaos test 01: a session-change gap from primary on a sparse
  // stream (open_interest, ~once/min) was being archived as "no coverage" even though
  // backup delivered that stream during the down window, just because backup's last
  // open_interest delivery was older than the 5s grace window. The right check is
  // "did backup deliver on this stream AFTER the gap window began?", not
  // "is backup's last delivery within grace of NOW?".
  @Test
  void handleGap_otherSourceDeliveredDuringGapWindow_parks() {
    // t=1000s: both sources deliver open_interest (sparse stream).
    filter.handleData("primary", makeData("binance", "btcusdt", "open_interest"));
    filter.handleData("backup", makeData("binance", "btcusdt", "open_interest"));
    long primaryLastDataTs = fakeClock.get();

    // t=1010s: primary stops delivering (simulates SIGKILL primary).
    fakeClock.addAndGet(10_000_000_000L);
    // t=1020s: backup delivers open_interest (covers primary's gap).
    fakeClock.addAndGet(10_000_000_000L);
    filter.handleData("backup", makeData("binance", "btcusdt", "open_interest"));

    // t=1090s: primary restarts and delivers open_interest. By now backup's last
    // open_interest delivery is 70s old — well outside the 5s grace window. The OLD
    // check would say "backup not fresh" and archive. The NEW check looks at whether
    // backup delivered AFTER the gap began (it did at t=1020s), so it parks instead.
    fakeClock.addAndGet(70_000_000_000L);
    filter.handleData("primary", makeData("binance", "btcusdt", "open_interest"));

    // Construct a session-change-style gap: gap_start = primary's last pre-kill
    // record at t=1000s, gap_end = primary's first post-restart record at t=1090s.
    GapEnvelope sessionChangeGap =
        makeGap("binance", "btcusdt", "open_interest", primaryLastDataTs, fakeClock.get());
    boolean accepted = filter.handleGap("primary", sessionChangeGap);

    assertThat(accepted)
        .as(
            "backup delivered open_interest at t=1020s, AFTER primary's last record at"
                + " t=1000s — gap window covered, must be parked, not archived")
        .isFalse();
    assertThat(filter.pendingSize()).isEqualTo(1);
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
    GapEnvelope oiGap = makeGap("binance", "btcusdt", "open_interest", 100L, 200L);
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
