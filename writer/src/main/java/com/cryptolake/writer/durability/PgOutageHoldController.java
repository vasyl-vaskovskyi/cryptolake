package com.cryptolake.writer.durability;

import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.envelope.GapReason;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.gap.GapEmitter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * State machine that detects prolonged PostgreSQL unavailability and enters a "hold" mode.
 *
 * <p>State machine (spec §8.2):
 *
 * <ul>
 *   <li>{@code healthy} — PG reachable; each {@link #recordPgFailure()} increments failure counter
 *   <li>{@code hold} — 3 consecutive failures; Kafka commits paused; archives still flushed; {@code
 *       pg_outage_hold} gap emitted; PG probed every {@link #RETRY_INTERVAL_NS}
 *   <li>{@code hold → healthy} — probe succeeds; commits resumed; closing gap emitted
 * </ul>
 *
 * <p>Per spec §12 Q4: only Kafka commits are paused in hold mode. Archive writes and sidecar
 * generation continue uninterrupted (data durability is independent of PG availability).
 *
 * <p>Thread safety: {@link #isHoldActive()} is safe to call from the consume-loop thread (T1).
 * {@link #recordPgFailure()} and {@link #recordPgSuccess()} are called from T1 only. The retry loop
 * runs on a virtual thread launched by {@link #start()}.
 */
public final class PgOutageHoldController {

  private static final StructuredLogger log = StructuredLogger.of(PgOutageHoldController.class);

  /** Number of consecutive PG failures before entering hold mode. */
  static final int FAILURE_THRESHOLD = 3;

  /** How often to probe PG recovery while in hold mode. */
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
        GapReason reason,
        String detail,
        long gapStartTs,
        long gapEndTs);
  }

  private final ClockSupplier clock;
  private final BooleanSupplier pgProbe;
  private final GapEmitAction gapEmitAction;
  private final List<SymbolStream> symbolStreams;

  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final AtomicBoolean holdActive = new AtomicBoolean(false);
  private final AtomicLong holdStartedNs = new AtomicLong(0L);

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread retryThread;

  /**
   * Constructs a {@code PgOutageHoldController}.
   *
   * @param clock nanosecond clock
   * @param pgProbe returns {@code true} if PG is reachable (used for recovery probe)
   * @param gapEmitAction callback to emit gap envelopes
   * @param symbolStreams set of {@code (symbol, stream)} pairs to emit gaps for
   */
  public PgOutageHoldController(
      ClockSupplier clock,
      BooleanSupplier pgProbe,
      GapEmitAction gapEmitAction,
      List<SymbolStream> symbolStreams) {
    this.clock = clock;
    this.pgProbe = pgProbe;
    this.gapEmitAction = gapEmitAction;
    this.symbolStreams = List.copyOf(symbolStreams);
  }

  /**
   * Convenience factory for production use — wraps the writer's {@link GapEmitter#emitUnfiltered}.
   *
   * @param clock nanosecond clock
   * @param pgProbe PG reachability probe
   * @param gapEmitter writer-side gap emitter
   * @param exchange exchange name
   * @param symbolStreams set of {@code (symbol, stream)} pairs
   */
  public static PgOutageHoldController of(
      ClockSupplier clock,
      BooleanSupplier pgProbe,
      GapEmitter gapEmitter,
      String exchange,
      List<SymbolStream> symbolStreams) {
    return new PgOutageHoldController(
        clock,
        pgProbe,
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
      retryThread = Thread.ofVirtual().name("pg-outage-hold-controller").start(this::retryLoop);
      log.info("pg_outage_hold_controller_started");
    }
  }

  /** Stops the retry loop. Safe to call from any thread. */
  public void stop() {
    running.set(false);
    Thread t = retryThread;
    if (t != null) {
      t.interrupt();
    }
    log.info("pg_outage_hold_controller_stopped");
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
   * Records one PG failure. If consecutive failures reach {@link #FAILURE_THRESHOLD}, enters hold
   * mode and emits the opening {@code pg_outage_hold} gap.
   *
   * <p>Called by the consumer loop when a PG call throws.
   */
  public void recordPgFailure() {
    int failures = consecutiveFailures.incrementAndGet();
    if (failures >= FAILURE_THRESHOLD && holdActive.compareAndSet(false, true)) {
      long holdStart = clock.nowNs();
      holdStartedNs.set(holdStart);
      log.info("pg_outage_hold_entered", "consecutive_failures", failures);
      log.info(
          "LIFECYCLE WRITER_PG_OUTAGE_HOLD_ENTERED: Postgres is unreachable — pausing"
              + " Kafka offset commits and lifecycle persistence, but archive flushes"
              + " continue normally (no data loss, redundancy via Kafka retention).",
          "consecutive_failures",
          failures);
      emitHoldGaps("pg_outage_hold_started", holdStart, holdStart);
    }
  }

  /**
   * Records a successful PG call. Resets the failure counter. If hold was active, exits hold mode
   * and emits the closing gap.
   *
   * <p>Called by the consumer loop when a PG call succeeds.
   */
  public void recordPgSuccess() {
    consecutiveFailures.set(0);
    if (holdActive.compareAndSet(true, false)) {
      long holdStart = holdStartedNs.get();
      long holdEnd = clock.nowNs();
      log.info("pg_outage_hold_exited", "hold_duration_ns", holdEnd - holdStart);
      log.info(
          "LIFECYCLE WRITER_PG_OUTAGE_HOLD_EXITED: Postgres is reachable again —"
              + " writer is resuming Kafka offset commits and lifecycle persistence.",
          "hold_duration_ns",
          holdEnd - holdStart);
      emitHoldGaps("pg_outage_hold_ended", holdStart, holdEnd);
    }
  }

  // ── private ───────────────────────────────────────────────────────────────

  private void emitHoldGaps(String detail, long gapStart, long gapEnd) {
    for (SymbolStream ss : symbolStreams) {
      gapEmitAction.emitWithTimestamps(
          ss.symbol(), ss.stream(), -1L, GapReason.PG_OUTAGE_HOLD, detail, gapStart, gapEnd);
    }
  }

  private void retryLoop() {
    while (running.get()) {
      sleep(RETRY_INTERVAL_NS / 1_000_000L);
      if (!running.get()) break;

      if (holdActive.get()) {
        boolean recovered = pgProbe.getAsBoolean();
        if (recovered) {
          log.info("pg_outage_hold_probe_succeeded");
          recordPgSuccess();
        } else {
          log.info("pg_outage_hold_probe_failed");
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
