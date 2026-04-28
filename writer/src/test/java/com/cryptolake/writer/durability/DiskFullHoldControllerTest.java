package com.cryptolake.writer.durability;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Unit tests for DiskFullHoldController using functional seams. */
class DiskFullHoldControllerTest {

  record GapEmission(
      String symbol,
      String stream,
      long sessionSeq,
      String reason,
      String detail,
      long gapStartTs,
      long gapEndTs) {}

  private DiskFullHoldController buildController(
      AtomicLong clock,
      AtomicBoolean diskOk,
      List<GapEmission> emissions,
      List<DiskFullHoldController.SymbolStream> ss) {
    return new DiskFullHoldController(
        clock::get,
        diskOk::get,
        (sym, str, seq, reason, detail, start, end) ->
            emissions.add(new GapEmission(sym, str, seq, reason, detail, start, end)),
        ss);
  }

  // ── test: ENOSPC triggers hold + emits gap ────────────────────────────────

  @Test
  void enospcWriteErrorEntersHoldAndEmitsGap() {
    AtomicLong clock = new AtomicLong(1_000_000_000L);
    AtomicBoolean diskOk = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    List<DiskFullHoldController.SymbolStream> ss =
        List.of(new DiskFullHoldController.SymbolStream("btcusdt", "depth"));

    DiskFullHoldController controller = buildController(clock, diskOk, emissions, ss);

    IOException enospc = new IOException("No space left on device");
    controller.onWriteError(enospc);

    assertThat(controller.isHoldActive()).isTrue();
    assertThat(emissions).hasSize(1);
    assertThat(emissions.get(0).reason()).isEqualTo("disk_full_hold");
    assertThat(emissions.get(0).symbol()).isEqualTo("btcusdt");
  }

  // ── test: non-ENOSPC errors don't trigger hold ────────────────────────────

  @Test
  void nonEnospcErrorDoesNotEnterHold() {
    AtomicLong clock = new AtomicLong(0L);
    AtomicBoolean diskOk = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    DiskFullHoldController controller =
        buildController(
            clock,
            diskOk,
            emissions,
            List.of(new DiskFullHoldController.SymbolStream("btcusdt", "depth")));

    IOException notEnospc = new IOException("Permission denied");
    controller.onWriteError(notEnospc);

    assertThat(controller.isHoldActive()).isFalse();
    assertThat(emissions).isEmpty();
  }

  // ── test: recovery exits hold + emits closing gap ─────────────────────────

  @Test
  void recoveryExitsHoldAndEmitsClosingGap() {
    AtomicLong clock = new AtomicLong(1_000_000_000L);
    AtomicBoolean diskOk = new AtomicBoolean(false);
    List<GapEmission> emissions = new ArrayList<>();

    DiskFullHoldController controller =
        buildController(
            clock,
            diskOk,
            emissions,
            List.of(new DiskFullHoldController.SymbolStream("btcusdt", "depth")));

    // Enter hold
    controller.onWriteError(new IOException("No space left on device"));
    assertThat(controller.isHoldActive()).isTrue();

    // Advance clock and recover
    clock.set(2_000_000_000L);
    controller.onRecovery();

    assertThat(controller.isHoldActive()).isFalse();
    assertThat(emissions).hasSize(2); // opening + closing
    assertThat(emissions.get(1).reason()).isEqualTo("disk_full_hold");
    assertThat(emissions.get(1).gapEndTs()).isEqualTo(2_000_000_000L);
  }

  // ── test: isEnospc detects ENOSPC message ────────────────────────────────

  @Test
  void isEnospcDetectsEnospcMessage() {
    assertThat(DiskFullHoldController.isEnospc(new IOException("No space left on device")))
        .isTrue();
    assertThat(DiskFullHoldController.isEnospc(new IOException("Permission denied"))).isFalse();
    assertThat(DiskFullHoldController.isEnospc(null)).isFalse();
  }

  // ── test: start/stop lifecycle ───────────────────────────────────────────

  @Test
  void startStopDoesNotThrow() throws InterruptedException {
    AtomicLong clock = new AtomicLong(0L);
    AtomicBoolean diskOk = new AtomicBoolean(true);
    List<GapEmission> emissions = new ArrayList<>();

    DiskFullHoldController controller =
        buildController(
            clock,
            diskOk,
            emissions,
            List.of(new DiskFullHoldController.SymbolStream("btcusdt", "depth")));

    controller.start();
    Thread.sleep(20);
    controller.stop();

    assertThat(emissions).isEmpty();
  }
}
