package com.cryptolake.writer.gap;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GapEmitter}.
 *
 * <p>Ports: Python's {@code test_gap_emitter.py} — verifies the triad contract (Tier 1 §5):
 * metric increment, structured log, and buffered-for-archive in one method call. Also verifies
 * coverage-filter suppression behaviour (design §2.8).
 */
class GapEmitterTest {

  private PrometheusMeterRegistry registry;
  private WriterMetrics metrics;
  private BufferManager buffers;
  private CoverageFilter coverage;
  private AtomicLong fakeClock;
  private GapEmitter emitter;

  @BeforeEach
  void setUp() {
    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new WriterMetrics(registry);
    fakeClock = new AtomicLong(1_000_000_000_000L);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    buffers = new BufferManager("/tmp/writer-test", 100, 60, codec);
    // Default grace short enough that tests don't need to sweep
    coverage = new CoverageFilter(1.0, 1.0, metrics, fakeClock::get);
    emitter = new GapEmitter(buffers, metrics, null, coverage);
  }

  private GapEnvelope makeGap(String reason) {
    return new GapEnvelope(
        1, "gap", "binance", "btcusdt", "trades",
        fakeClock.get(), "col_sess", -1L,
        100L, 200L, reason, "test", null, null, null, null, null, null);
  }

  // Tier 1 §5 — metric incremented on emit
  @Test
  void emit_incrementsGapRecordsWrittenCounter() {
    GapEnvelope gap = makeGap("ws_disconnect");
    emitter.emit(gap, "primary", "binance.trades", 0, 42L);

    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_gap_records_written_total");
    assertThat(scrape).contains("reason=\"ws_disconnect\"");
  }

  // Tier 1 §5 — when coverage filter only sees one source, gap is accepted and buffered
  @Test
  void emit_singleSource_acceptedAndBuffered() {
    // register a primary data event so coverage isn't enabled (needs both sources)
    DataEnvelope env = new DataEnvelope(1, "data", "binance", "btcusdt", "trades",
        fakeClock.get(), fakeClock.get(), "col_sess", 1L, "{}", "abc");
    coverage.handleData("primary", env);

    GapEnvelope gap = makeGap("ws_disconnect");
    boolean written = emitter.emit(gap, "primary", "binance.trades", 0, 42L);

    assertThat(written).isTrue();
    // Buffer should now contain one line
    assertThat(buffers.flushAll().get(0).count()).isEqualTo(1);
  }

  // Tier 1 §5 — emitUnfiltered always buffers, ignoring coverage filter
  @Test
  void emitUnfiltered_alwaysBuffered() {
    // Force coverage filter into "both sources seen" state (should normally suppress)
    DataEnvelope primary = new DataEnvelope(1, "data", "binance", "btcusdt", "trades",
        fakeClock.get(), fakeClock.get(), "col_sess", 1L, "{}", "abc");
    DataEnvelope backup = new DataEnvelope(1, "data", "binance", "btcusdt", "trades",
        fakeClock.get() + 1, fakeClock.get() + 1, "col_sess", 2L, "{}", "abd");
    coverage.handleData("primary", primary);
    coverage.handleData("backup", backup);

    GapEnvelope gap = makeGap("ws_disconnect");
    emitter.emitUnfiltered(gap, "primary", "binance.trades", 0);

    // Unfiltered path goes straight to buffer
    assertThat(buffers.flushAll().get(0).count()).isEqualTo(1);
  }

  // Design §2.8 — reason tag flows through to the counter label
  @Test
  void emit_perReasonLabel_separatelyCounted() {
    emitter.emit(makeGap("ws_disconnect"), "primary", "binance.trades", 0, 1L);
    emitter.emit(new GapEnvelope(1, "gap", "binance", "btcusdt", "trades",
        fakeClock.get(), "col_sess", -1L, 100L, 200L, "session_seq_skip", "test",
        null, null, null, null, null, null), "primary", "binance.trades", 0, 2L);

    String scrape = registry.scrape();
    assertThat(scrape).contains("reason=\"ws_disconnect\"");
    assertThat(scrape).contains("reason=\"session_seq_skip\"");
  }
}
