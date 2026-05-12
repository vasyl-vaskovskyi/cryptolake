package com.cryptolake.writer.durability;

import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.gap.GapEmitter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * State machine that detects disk-full conditions (ENOSPC) and enters a "hold" mode.
 *
 * <p>Same shape as {@link PgOutageHoldController} but triggered by {@code IOException} with {@code
 * ENOSPC} on archive writes. Per spec §5.4 #29 / §6.3 DiskFullHoldController.
 *
 * <p>Hold semantics (spec §12 Q4): pauses Kafka commits ONLY. Archive flushes keep running (but
 * will fail with ENOSPC — those failures are expected; the hold is a safety net so at least the
 * last committed offset is preserved).
 *
 * <p>Thread safety: {@link #isHoldActive()} and {@link #onWriteError(IOException)} are called from
 * the consumer-loop / file-rotator thread (T1). The retry loop runs on a virtual thread.
 */
public final class DiskFullHoldController {

  private static final StructuredLogger log = StructuredLogger.of(DiskFullHoldController.class);

  /** Error message fragment that indicates ENOSPC on Linux. */
  private static final String ENOSPC_MESSAGE = "No space left on device";

  /** How often to probe for available disk space while in hold mode. */
  static final long RETRY_INTERVAL_NS = 30_000_000_000L; // 30 s

  /**
   * A {@code (symbol, stream)} pair for which gap envelopes are emitted during hold mode.
   *
   * @param symbol exchange symbol
   * @param stream stream name
   */
  public record SymbolStream(String symbol, String stream) {}

  /** Functional interface for emitting gap envelopes (seam for testing). */
  @FunctionalInterface
  public interface GapEmitAction {
    void emitWithTimestamps(
        String symbol,
        String stream,
        long sessionSeq,
        com.cryptolake.common.envelope.GapReason reason,
        String detail,
        long gapStartTs,
        long gapEndTs);
  }

  private final ClockSupplier clock;
  private final BooleanSupplier diskProbe;
  private final GapEmitAction gapEmitAction;
  private final List<SymbolStream> symbolStreams;

  private final AtomicBoolean holdActive = new AtomicBoolean(false);
  private final AtomicLong holdStartedNs = new AtomicLong(0L);

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread retryThread;

  /**
   * Constructs a {@code DiskFullHoldController}.
   *
   * @param clock nanosecond clock
   * @param diskProbe returns {@code true} if disk space is now available (for recovery check)
   * @param gapEmitAction callback to emit gap envelopes
   * @param symbolStreams set of {@code (symbol, stream)} pairs to emit gaps for
   */
  public DiskFullHoldController(
      ClockSupplier clock,
      BooleanSupplier diskProbe,
      GapEmitAction gapEmitAction,
      List<SymbolStream> symbolStreams) {
    this.clock = clock;
    this.diskProbe = diskProbe;
    this.gapEmitAction = gapEmitAction;
    this.symbolStreams = List.copyOf(symbolStreams);
  }

  /**
   * Convenience factory for production use — wraps the writer's {@link GapEmitter#emitUnfiltered}.
   *
   * @param clock nanosecond clock
   * @param diskProbe disk-space availability probe
   * @param gapEmitter writer-side gap emitter
   * @param exchange exchange name
   * @param symbolStreams set of {@code (symbol, stream)} pairs
   */
  public static DiskFullHoldController of(
      ClockSupplier clock,
      BooleanSupplier diskProbe,
      GapEmitter gapEmitter,
      String exchange,
      List<SymbolStream> symbolStreams) {
    return new DiskFullHoldController(
        clock,
        diskProbe,
        (symbol, stream, sessionSeq, reason, detail, gapStart, gapEnd) -> {
          GapEnvelope gap =
              GapEnvelope.create(
                  exchange,
                  symbol,
                  stream,
                  "synthetic",
                  sessionSeq,
                  gapStart,
                  gapEnd,
                  reason,
                  detail,
                  clock);
          gapEmitter.emitUnfiltered(gap, "primary", "synthetic", -1);
        },
        symbolStreams);
  }

  /** Starts the retry loop virtual thread. Idempotent. */
  public void start() {
    if (running.compareAndSet(false, true)) {
      retryThread = Thread.ofVirtual().name("disk-full-hold-controller").start(this::retryLoop);
      log.info("disk_full_hold_controller_started");
    }
  }

  /** Stops the retry loop. Safe to call from any thread. */
  public void stop() {
    running.set(false);
    Thread t = retryThread;
    if (t != null) {
      t.interrupt();
    }
    log.info("disk_full_hold_controller_stopped");
  }

  /**
   * Returns {@code true} if hold mode is active — Kafka commits should be paused.
   *
   * <p>Called by the consume loop before each offset commit.
   */
  public boolean isHoldActive() {
    return holdActive.get();
  }

  /**
   * Called by {@code FileRotator} when an archive write fails.
   *
   * <p>If the exception indicates ENOSPC, enters hold mode and emits the opening {@code
   * disk_full_hold} gap.
   *
   * @param e the I/O exception from the archive write
   */
  public void onWriteError(IOException e) {
    if (!isEnospc(e)) {
      return; // Not a disk-full error — don't enter hold
    }
    if (holdActive.compareAndSet(false, true)) {
      long holdStart = clock.nowNs();
      holdStartedNs.set(holdStart);
      log.info("disk_full_hold_entered", "error", e.getMessage());
      log.info(
          "LIFECYCLE WRITER_DISK_FULL_HOLD_ENTERED: Disk is full — writer cannot write"
              + " archives. Pausing Kafka commits and freezing the archive until disk is"
              + " freed (real loss begins now).",
          "error",
          e.getMessage());
      emitHoldGaps("disk_full_hold_started", holdStart, holdStart);
    }
  }

  /**
   * Manually exits hold mode (for testing or external recovery detection).
   *
   * <p>Emits the closing {@code disk_full_hold} gap.
   */
  public void onRecovery() {
    if (holdActive.compareAndSet(true, false)) {
      long holdStart = holdStartedNs.get();
      long holdEnd = clock.nowNs();
      log.info("disk_full_hold_exited", "hold_duration_ns", holdEnd - holdStart);
      log.info(
          "LIFECYCLE WRITER_DISK_FULL_HOLD_EXITED: Disk space is available again —"
              + " writer is resuming archive writes and Kafka commits.",
          "hold_duration_ns",
          holdEnd - holdStart);
      emitHoldGaps("disk_full_hold_ended", holdStart, holdEnd);
    }
  }

  // ── private ───────────────────────────────────────────────────────────────

  /** Returns {@code true} if the exception or its cause indicates ENOSPC. */
  public static boolean isEnospc(IOException e) {
    if (e == null) return false;
    if (e.getMessage() != null && e.getMessage().contains(ENOSPC_MESSAGE)) return true;
    Throwable cause = e.getCause();
    if (cause instanceof IOException) return isEnospc((IOException) cause);
    if (cause != null && cause.getMessage() != null && cause.getMessage().contains(ENOSPC_MESSAGE))
      return true;
    return false;
  }

  private void emitHoldGaps(String detail, long gapStart, long gapEnd) {
    for (SymbolStream ss : symbolStreams) {
      gapEmitAction.emitWithTimestamps(
          ss.symbol(),
          ss.stream(),
          -1L,
          com.cryptolake.common.envelope.GapReason.DISK_FULL_HOLD,
          detail,
          gapStart,
          gapEnd);
    }
  }

  private void retryLoop() {
    while (running.get()) {
      sleep(RETRY_INTERVAL_NS / 1_000_000L);
      if (!running.get()) break;

      if (holdActive.get()) {
        boolean recovered = diskProbe.getAsBoolean();
        if (recovered) {
          log.info("disk_full_hold_probe_succeeded");
          onRecovery();
        } else {
          log.info("disk_full_hold_probe_failed");
        }
      }
    }
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
