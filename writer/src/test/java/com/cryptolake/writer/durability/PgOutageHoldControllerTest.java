package com.cryptolake.writer.durability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Unit tests for PgOutageHoldController using functional seams. */
class PgOutageHoldControllerTest {

  record GapEmission(
      String symbol,
      String stream,
      long sessionSeq,
      com.cryptolake.common.envelope.GapReason reason,
      String detail,
      long gapStartTs,
      long gapEndTs) {}

  private PgOutageHoldController buildController(
      AtomicLong clock,
      AtomicBoolean pgAlive,
      List<GapEmission> emissions,
      List<PgOutageHoldController.SymbolStream> ss) {
    return new PgOutageHoldController(
        clock::get,
        pgAlive::get,
        (sym, str, seq, reason, detail, start, end) ->
            emissions.add(new GapEmission(sym, str, seq, reason, detail, start, end)),
        ss);
  }

  // ── test: 3 consecutive failures trigger hold + emit opening gap ──────────

  @Test
  void threeConsecutiveFailuresEnterHoldAndEmitGap() {
    AtomicLong clock = new AtomicLong(1_000_000_000L);
    AtomicBoolean pgAlive = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    List<PgOutageHoldController.SymbolStream> ss =
        List.of(new PgOutageHoldController.SymbolStream("btcusdt", "depth"));

    PgOutageHoldController controller = buildController(clock, pgAlive, emissions, ss);

    // First two failures: below threshold
    controller.recordPgFailure();
    assertThat(controller.isHoldActive()).isFalse();
    controller.recordPgFailure();
    assertThat(controller.isHoldActive()).isFalse();

    // Third failure: threshold reached → hold
    controller.recordPgFailure();
    assertThat(controller.isHoldActive()).isTrue();
    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).reason())
        .isEqualTo(com.cryptolake.common.envelope.GapReason.PG_OUTAGE_HOLD);
    assertThat(emissions.get(0).symbol()).isEqualTo("btcusdt");
  }

  // ── test: recovery exits hold + emits closing gap ────────────────────────

  @Test
  void recoveryExitsHoldAndEmitsClosingGap() {
    AtomicLong clock = new AtomicLong(1_000_000_000L);
    AtomicBoolean pgAlive = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    List<PgOutageHoldController.SymbolStream> ss =
        List.of(new PgOutageHoldController.SymbolStream("btcusdt", "depth"));

    PgOutageHoldController controller = buildController(clock, pgAlive, emissions, ss);

    // Enter hold
    for (int i = 0; i < PgOutageHoldController.FAILURE_THRESHOLD; i++) {
      controller.recordPgFailure();
    }
    assertThat(controller.isHoldActive()).isTrue();
    long openingEmissions = emissions.size();

    // Advance clock and recover
    clock.set(2_000_000_000L);
    controller.recordPgSuccess();

    assertThat(controller.isHoldActive()).isFalse();
    // One more gap for the closing event
    assertThat(emissions).hasSize((int) openingEmissions + 1);
    GapEmission closing = emissions.get(emissions.size() - 1);
    assertThat(closing.reason()).isEqualTo(com.cryptolake.common.envelope.GapReason.PG_OUTAGE_HOLD);
    assertThat(closing.gapEndTs()).isEqualTo(2_000_000_000L);
  }

  // ── test: success after single failure doesn't enter hold ────────────────

  @Test
  void successAfterSingleFailureDoesNotEnterHold() {
    AtomicLong clock = new AtomicLong(0L);
    AtomicBoolean pgAlive = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    PgOutageHoldController controller =
        buildController(
            clock,
            pgAlive,
            emissions,
            List.of(new PgOutageHoldController.SymbolStream("btcusdt", "depth")));

    controller.recordPgFailure();
    controller.recordPgSuccess(); // success resets counter

    assertThat(controller.isHoldActive()).isFalse();
    assertThat(emissions).isEmpty(); // threshold never reached
  }

  // ── test: hold not entered if failures are interleaved with success ───────

  @Test
  void interleavedFailuresAndSuccessesNeverEnterHold() {
    AtomicLong clock = new AtomicLong(0L);
    AtomicBoolean pgAlive = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    PgOutageHoldController controller =
        buildController(
            clock,
            pgAlive,
            emissions,
            List.of(new PgOutageHoldController.SymbolStream("btcusdt", "depth")));

    controller.recordPgFailure();
    controller.recordPgFailure();
    controller.recordPgSuccess(); // reset counter

    controller.recordPgFailure();
    controller.recordPgFailure();
    controller.recordPgSuccess(); // reset again

    assertThat(controller.isHoldActive()).isFalse();
    assertThat(emissions).isEmpty();
  }

  // ── test: start/stop lifecycle ───────────────────────────────────────────

  @Test
  void startStopDoesNotThrow() throws InterruptedException {
    AtomicLong clock = new AtomicLong(0L);
    AtomicBoolean pgAlive = new AtomicBoolean(true);
    List<GapEmission> emissions = new ArrayList<>();

    PgOutageHoldController controller =
        buildController(
            clock,
            pgAlive,
            emissions,
            List.of(new PgOutageHoldController.SymbolStream("btcusdt", "depth")));

    controller.start();
    Thread.sleep(20);
    controller.stop();

    assertThat(emissions).isEmpty();
  }
}
