package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Regression test for chaos test 04 (writer disk-full brief) — the false-positive
 * {@code pu_chain_break} gap caused by the writer's backup consumer NOT polling when failover was
 * inactive.
 *
 * <p>Before the dual-source-tailing fix, the writer only consumed the backup topic on demand
 * during failover, so {@code lastDataTsByStream[(stream, "backup")]} was {@code null} when a
 * primary-emitted {@code pu_chain_break} arrived shortly after a brief failover flap. The
 * coverage check then concluded "backup did not cover" → archived → false positive.
 *
 * <p>After Tasks 3 and 4 of the {@code continuous-dual-source-tailing} plan, the writer tails
 * the backup topic continuously, so {@code lastDataTsByStream} stays fresh during a flap and
 * the gap correctly parks → suppresses on grace expiry.
 *
 * <p>This test reproduces the SYMPTOM logically at the {@link CoverageFilter} level (no chaos
 * infra, no Kafka): backup data arrives throughout the flap window, primary emits a
 * {@code pu_chain_break} after the flap, the gap is parked, grace elapses, and
 * {@link CoverageFilter#sweepExpired()} returns nothing because backup's per-(stream, source)
 * coverage timestamp covers the gap window.
 */
class CoverageFilterFailoverFlapTest {

  private DataEnvelope makeData(
      String exchange, String symbol, String stream, long receivedAtNs) {
    return new DataEnvelope(
        1,
        "data",
        exchange,
        symbol,
        stream,
        receivedAtNs,
        receivedAtNs,
        "col_sess",
        1L,
        "{}",
        "abc");
  }

  private GapEnvelope makePuChainBreakGap(
      String exchange,
      String symbol,
      String stream,
      long gapStartTs,
      long gapEndTs,
      long receivedAtNs) {
    return new GapEnvelope(
        1,
        "gap",
        exchange,
        symbol,
        stream,
        receivedAtNs,
        "col_sess",
        -1L,
        gapStartTs,
        gapEndTs,
        "pu_chain_break",
        "regression: backup tailed through flap",
        null,
        null,
        null,
        null,
        null,
        null);
  }

  // Regression: chaos test 04 leaked false-positive pu_chain_break gaps because the backup
  // consumer was not polling when failover was inactive. With continuous dual-source tailing
  // (Tasks 3 and 4), the per-(stream, source) coverage map stays fresh and the gap is
  // suppressed on grace expiry.
  @Test
  void puChainBreak_isSuppressed_whenBackupTailedThroughFlap() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    AtomicLong clock = new AtomicLong(1_000_000_000_000L); // t = 1000s in ns
    CoverageFilter coverage = new CoverageFilter(10.0, 30.0, metrics, clock::get);

    // t=1000s: primary delivers a depth record — only one source seen, filter still off.
    coverage.handleData("primary", makeData("binance", "btcusdt", "depth", clock.get()));
    assertThat(coverage.enabled()).isFalse();

    // t=1001s: backup delivers a depth record — both sources seen, coverage filter activates.
    clock.addAndGet(1_000_000_000L);
    coverage.handleData("backup", makeData("binance", "btcusdt", "depth", clock.get()));
    assertThat(coverage.enabled()).isTrue();

    long flapStartNs = clock.get();

    // t=1006s: backup keeps delivering depth records DURING the flap window — this is the
    // step that the old (on-demand) backup consumer was failing to do, leaving the per-stream
    // coverage map stale.
    clock.addAndGet(5_000_000_000L);
    coverage.handleData("backup", makeData("binance", "btcusdt", "depth", clock.get()));

    // t=1007s: backup keeps delivering — last backup-on-depth ts is now ~1007s.
    clock.addAndGet(1_000_000_000L);
    coverage.handleData("backup", makeData("binance", "btcusdt", "depth", clock.get()));
    long backupLastDepthTs = clock.get();

    // t=1012s: primary emits a pu_chain_break gap whose window begins at the start of the
    // flap (t=1001s). Backup's last depth delivery (~t=1007s) is INSIDE the gap window
    // (gap_start=1001s), so coverage applies → must park, not archive.
    clock.addAndGet(5_000_000_000L);
    long gapEndTs = clock.get();
    GapEnvelope puGap =
        makePuChainBreakGap("binance", "btcusdt", "depth", flapStartNs, gapEndTs, clock.get());

    boolean acceptedNow = coverage.handleGap("primary", puGap);

    assertThat(acceptedNow)
        .as(
            "backup delivered depth at t=%dns (during the flap window starting at t=%dns) — "
                + "coverage applies, the gap MUST be parked, not archived",
            backupLastDepthTs,
            flapStartNs)
        .isFalse();
    assertThat(coverage.pendingSize()).isEqualTo(1);

    // t=1023s: grace period (10s) has elapsed since the gap was parked at t=1012s.
    // sweepExpired must observe that backup's per-stream coverage covers the gap window
    // (backupLastDepthTs > gap_start_ts) and SUPPRESS the gap — i.e. return empty.
    clock.addAndGet(11_000_000_000L);
    List<GapEnvelope> archive = coverage.sweepExpired();

    assertThat(archive)
        .as(
            "with continuous dual-source tailing, backup's per-stream last-data ts (%dns) "
                + "covers the gap window (gap_start=%dns) — sweepExpired must return empty "
                + "(this is the chaos-04 regression assertion)",
            backupLastDepthTs,
            flapStartNs)
        .isEmpty();
    assertThat(coverage.pendingSize()).isEqualTo(0);
  }
}
