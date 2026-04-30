package com.cryptolake.writer.validation;

import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.gap.GapEmitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Synthetic gap emitter for both-collectors-silent periods.
 *
 * <p>Virtual-thread monitor wakes every {@link #CHECK_INTERVAL_NS} (10s). For each {@code (symbol,
 * stream)} pair: if BOTH sources have {@code last_data_at_ns} older than {@link
 * #DATA_STALE_THRESHOLD_NS} (30s) AND last heartbeat older than {@link
 * #HEARTBEAT_STALE_THRESHOLD_NS} (15s), emits a {@code both_collectors_silent} gap.
 *
 * <p>Liquidations are exempt (no data is normal there).
 *
 * <p>Spec §8.3 / HOLE 4 / Task A3.7.
 *
 * <p>Thread safety: {@link CoverageFilter} methods are called from the monitor's virtual thread;
 * those methods are T1-owned but are read-only here (no writes from the monitor). {@link #stop()}
 * is safe from any thread.
 */
public final class SilenceInferredGapEmitter {

  private static final StructuredLogger log = StructuredLogger.of(SilenceInferredGapEmitter.class);

  /** How often the monitor wakes up. */
  static final long CHECK_INTERVAL_NS = 10_000_000_000L; // 10 s

  /** How long without data from both sources before declaring silence. */
  static final long DATA_STALE_THRESHOLD_NS = 30_000_000_000L; // 30 s

  /** How long without heartbeat from both sources before allowing silence emission. */
  static final long HEARTBEAT_STALE_THRESHOLD_NS = 15_000_000_000L; // 15 s

  /** Streams that are exempt from silence detection (no data is normal). */
  private static final java.util.Set<String> EXEMPT_STREAMS = java.util.Set.of("liquidations");

  /**
   * A {@code (symbol, stream)} pair to monitor.
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
        String reason,
        String detail,
        long gapStartTs,
        long gapEndTs);
  }

  private final ClockSupplier clock;
  private final CoverageFilter coverageFilter;
  private final GapEmitAction gapEmitAction;
  private final List<SymbolStream> symbolStreams;

  /** Tracks whether a both_collectors_silent gap is already in-flight per (symbol, stream). */
  private final Map<String, Boolean> silenceActive = new HashMap<>();

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread monitorThread;

  /**
   * Constructs a {@code SilenceInferredGapEmitter}.
   *
   * @param clock nanosecond clock
   * @param coverageFilter source of per-source data/heartbeat timestamps
   * @param gapEmitAction callback to emit gap envelopes
   * @param symbolStreams the set of {@code (symbol, stream)} pairs to monitor
   */
  public SilenceInferredGapEmitter(
      ClockSupplier clock,
      CoverageFilter coverageFilter,
      GapEmitAction gapEmitAction,
      List<SymbolStream> symbolStreams) {
    this.clock = clock;
    this.coverageFilter = coverageFilter;
    this.gapEmitAction = gapEmitAction;
    this.symbolStreams = List.copyOf(symbolStreams);
  }

  /**
   * Convenience factory for production use — wraps {@link GapEmitter#emitUnfiltered}.
   *
   * @param clock nanosecond clock
   * @param coverageFilter source of per-source timestamps
   * @param gapEmitter writer-side gap emitter
   * @param exchange exchange name
   * @param symbolStreams the set of {@code (symbol, stream)} pairs to monitor
   */
  public static SilenceInferredGapEmitter of(
      ClockSupplier clock,
      CoverageFilter coverageFilter,
      GapEmitter gapEmitter,
      String exchange,
      List<SymbolStream> symbolStreams) {
    return new SilenceInferredGapEmitter(
        clock,
        coverageFilter,
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

  /** Starts the monitor virtual thread. Idempotent. */
  public void start() {
    if (running.compareAndSet(false, true)) {
      monitorThread =
          Thread.ofVirtual().name("silence-inferred-gap-emitter").start(this::monitorLoop);
      log.info("silence_inferred_gap_emitter_started");
    }
  }

  /** Stops the monitor. Safe to call from any thread. Idempotent. */
  public void stop() {
    running.set(false);
    Thread t = monitorThread;
    if (t != null) {
      t.interrupt();
    }
    log.info("silence_inferred_gap_emitter_stopped");
  }

  // ── private ───────────────────────────────────────────────────────────────

  private void monitorLoop() {
    while (running.get()) {
      sleep(CHECK_INTERVAL_NS / 1_000_000L);
      if (!running.get()) break;

      long now = clock.nowNs();
      long primaryDataAge = now - coverageFilter.getLastDataTs("primary");
      long backupDataAge = now - coverageFilter.getLastDataTs("backup");
      long primaryHbAge = now - coverageFilter.getLastHeartbeatTs("primary");
      long backupHbAge = now - coverageFilter.getLastHeartbeatTs("backup");

      boolean bothDataStale =
          primaryDataAge > DATA_STALE_THRESHOLD_NS && backupDataAge > DATA_STALE_THRESHOLD_NS;
      boolean bothHeartbeatStale =
          primaryHbAge > HEARTBEAT_STALE_THRESHOLD_NS && backupHbAge > HEARTBEAT_STALE_THRESHOLD_NS;

      if (bothDataStale && bothHeartbeatStale) {
        long gapStart =
            Math.max(
                coverageFilter.getLastDataTs("primary"), coverageFilter.getLastDataTs("backup"));
        if (gapStart == 0L) gapStart = now - DATA_STALE_THRESHOLD_NS;

        for (SymbolStream ss : symbolStreams) {
          if (EXEMPT_STREAMS.contains(ss.stream())) continue;

          String key = ss.symbol() + "|" + ss.stream();
          if (!silenceActive.getOrDefault(key, false)) {
            silenceActive.put(key, true);
            String detail =
                "Both primary and backup silent: primary_data_age_ms="
                    + primaryDataAge / 1_000_000L
                    + " backup_data_age_ms="
                    + backupDataAge / 1_000_000L;
            log.info("both_collectors_silent", "symbol", ss.symbol(), "stream", ss.stream());
            log.info(
                "LIFECYCLE BOTH_COLLECTORS_SILENT: Neither main nor backup is delivering"
                    + " data for {} {} — this is real data loss; emitting a gap envelope.",
                ss.symbol(),
                ss.stream());
            gapEmitAction.emitWithTimestamps(
                ss.symbol(), ss.stream(), -1L, "both_collectors_silent", detail, gapStart, now);
          }
        }
      } else {
        // Data flowing again — clear silence state
        for (SymbolStream ss : symbolStreams) {
          String key = ss.symbol() + "|" + ss.stream();
          if (silenceActive.getOrDefault(key, false)) {
            silenceActive.put(key, false);
            log.info("both_collectors_recovered", "symbol", ss.symbol(), "stream", ss.stream());
            log.info(
                "LIFECYCLE BOTH_COLLECTORS_RECOVERED: Data is flowing again for {} {} —"
                    + " at least one of main/backup is delivering; gap window closed.",
                ss.symbol(),
                ss.stream());
          }
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
