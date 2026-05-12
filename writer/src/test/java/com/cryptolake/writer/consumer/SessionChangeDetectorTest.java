package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.envelope.GapReason;
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
    assertThat(gap.get().reason()).isEqualTo(GapReason.COLLECTOR_RESTART);
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

  // TWO-COLLECTOR rule — primary→backup switch is failover, NOT a session change.
  // Both collectors have independent sessions; switching between them is the writer's
  // failover mechanism working as designed. Per the TWO-COLLECTOR rule (gap iff BOTH
  // collectors fail simultaneously), this MUST NOT produce a gap.
  @Test
  void observe_crossSourceSwitchPrimaryToBackup_noGap() {
    DataEnvelope primaryEnv =
        makeEnv("binance", "btcusdt", "trades", "binance-collector-01_2026-04-30T05:07:13Z", 1L);
    DataEnvelope backupEnv =
        makeEnv(
            "binance", "btcusdt", "trades", "binance-collector-backup_2026-04-30T05:07:13Z", 1L);

    detector.observe(primaryEnv, "primary");
    Optional<GapEnvelope> gap = detector.observe(backupEnv, "backup"); // failover

    assertThat(gap).isEmpty();
  }

  // TWO-COLLECTOR rule — backup→primary switchback is also not a session change.
  // After primary recovers, the writer switches back. Same logic: not a data event.
  @Test
  void observe_crossSourceSwitchBackupToPrimary_noGap() {
    DataEnvelope backupEnv =
        makeEnv(
            "binance", "btcusdt", "trades", "binance-collector-backup_2026-04-30T05:07:13Z", 1L);
    DataEnvelope primaryEnv =
        makeEnv("binance", "btcusdt", "trades", "binance-collector-01_2026-04-30T05:09:15Z", 1L);

    detector.observe(backupEnv, "backup");
    Optional<GapEnvelope> gap = detector.observe(primaryEnv, "primary"); // switchback

    assertThat(gap).isEmpty();
  }

  // TWO-COLLECTOR rule — full failover-and-recovery loop produces zero gaps from the
  // session detector. Sequence: primary A → backup B → primary A' (primary actually
  // restarted with a new session_id while it was down). All three transitions are
  // either cross-source (1, 2) or within-source-but-covered-by-the-other (3). The
  // detector emits a gap candidate for transition 3 (within-primary), but per the
  // unit-test contract here we ONLY assert the cross-source switches return empty.
  @Test
  void observe_fullFailoverLoop_crossSourceSwitchesAreNoGap() {
    DataEnvelope primaryA =
        makeEnv("binance", "btcusdt", "trades", "binance-collector-01_2026-04-30T05:07:13Z", 1L);
    DataEnvelope backupB =
        makeEnv(
            "binance", "btcusdt", "trades", "binance-collector-backup_2026-04-30T05:07:13Z", 1L);
    DataEnvelope primaryAPrime =
        makeEnv("binance", "btcusdt", "trades", "binance-collector-01_2026-04-30T05:09:15Z", 1L);

    detector.observe(primaryA, "primary");
    Optional<GapEnvelope> gap1 = detector.observe(backupB, "backup"); // primary→backup
    Optional<GapEnvelope> gap2 = detector.observe(primaryAPrime, "primary"); // backup→primary

    assertThat(gap1).as("primary→backup cross-source switch must not emit a gap").isEmpty();
    assertThat(gap2)
        .as(
            "backup→primary switchback is a within-PRIMARY session change "
                + "(A → A'), so it IS a gap candidate that the coverage filter "
                + "decides on. Detector returns it; archival is filtered downstream.")
        .isPresent();
    assertThat(gap2.get().reason()).isEqualTo(GapReason.COLLECTOR_RESTART);
  }

  // TWO-COLLECTOR rule — within-source session change (the SAME source's session_id
  // changes between two of its envelopes) IS a gap candidate. This is the only case
  // SessionChangeDetector should fire on.
  @Test
  void observe_withinSourcePrimaryRestart_emitsGap() {
    DataEnvelope before =
        makeEnv("binance", "btcusdt", "trades", "binance-collector-01_2026-04-30T05:07:13Z", 1L);
    DataEnvelope after =
        makeEnv("binance", "btcusdt", "trades", "binance-collector-01_2026-04-30T05:09:15Z", 1L);

    detector.observe(before, "primary");
    Optional<GapEnvelope> gap = detector.observe(after, "primary"); // primary restarted

    assertThat(gap).isPresent();
    assertThat(gap.get().reason()).isEqualTo(GapReason.COLLECTOR_RESTART);
  }

  // Symmetric: backup-only restart also fires a within-backup gap candidate.
  @Test
  void observe_withinSourceBackupRestart_emitsGap() {
    DataEnvelope before =
        makeEnv(
            "binance", "btcusdt", "trades", "binance-collector-backup_2026-04-30T05:07:13Z", 1L);
    DataEnvelope after =
        makeEnv(
            "binance", "btcusdt", "trades", "binance-collector-backup_2026-04-30T05:09:15Z", 1L);

    detector.observe(before, "backup");
    Optional<GapEnvelope> gap = detector.observe(after, "backup"); // backup restarted

    assertThat(gap).isPresent();
    assertThat(gap.get().reason()).isEqualTo(GapReason.COLLECTOR_RESTART);
  }
}
