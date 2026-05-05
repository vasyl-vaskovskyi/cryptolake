package com.cryptolake.collector.durability;

import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Virtual-thread monitor that detects prolonged Kafka producer unavailability.
 *
 * <p>State machine (spec §8.1):
 *
 * <ul>
 *   <li>{@code healthy} — broker reachable; probes every {@link #PROBE_INTERVAL_NS}
 *   <li>{@code degraded} — at least one probe failed; failure counter increments each probe cycle
 *   <li>{@code paused} — first probe failure ≥30s ago; writes {@link KafkaOutageJournal}
 *   <li>{@code paused → healthy} — probe succeeds; reads journal, emits {@code
 *       kafka_producer_outage} gap per {@code (symbol, stream)}, truncates journal
 * </ul>
 *
 * <p>Probe mechanism: {@link KafkaProducerBridge#probeHealth()} inside the virtual thread; a
 * non-throwing return means healthy, any exception means degraded.
 *
 * <p>Thread safety: all mutable state lives on the single virtual thread (the probe loop). The
 * {@link #stop()} method is safe to call from any thread.
 */
public final class KafkaProducerHealthMonitor {

  private static final StructuredLogger log = StructuredLogger.of(KafkaProducerHealthMonitor.class);

  /** How often to probe the producer. */
  static final long PROBE_INTERVAL_NS = 5_000_000_000L; // 5 s

  /** Duration of sustained failure before entering paused state. */
  static final long PAUSED_THRESHOLD_NS = 30_000_000_000L; // 30 s

  private enum State {
    HEALTHY,
    DEGRADED,
    PAUSED
  }

  /** Functional interface for emitting a gap with explicit timestamps (seam for testing). */
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

  /**
   * A {@code (symbol, stream)} pair for which gap envelopes are emitted on Kafka recovery.
   *
   * @param symbol exchange symbol (e.g. {@code "btcusdt"})
   * @param stream stream name (e.g. {@code "depth"})
   */
  public record SymbolStream(String symbol, String stream) {}

  private final ClockSupplier clock;
  private final BooleanSupplier healthProbe;
  private final KafkaOutageJournal journal;
  private final GapEmitAction gapEmitAction;
  private final List<SymbolStream> symbolStreams;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread probeThread;

  /**
   * Primary constructor — uses functional seams so the class is testable without subclassing.
   *
   * @param clock nanosecond clock
   * @param healthProbe returns {@code true} if the Kafka broker is reachable
   * @param journal durable record of outage start time
   * @param gapEmitAction emits {@code kafka_producer_outage} gap envelopes on recovery
   * @param symbolStreams the set of {@code (symbol, stream)} pairs to emit gaps for
   */
  public KafkaProducerHealthMonitor(
      ClockSupplier clock,
      BooleanSupplier healthProbe,
      KafkaOutageJournal journal,
      GapEmitAction gapEmitAction,
      List<SymbolStream> symbolStreams) {
    this.clock = clock;
    this.healthProbe = healthProbe;
    this.journal = journal;
    this.gapEmitAction = gapEmitAction;
    this.symbolStreams = List.copyOf(symbolStreams);
  }

  /**
   * Convenience factory for production use — wraps {@link KafkaProducerBridge#probeHealth()} and
   * {@link GapEmitter#emitWithTimestamps}.
   *
   * @param clock nanosecond clock
   * @param producer bridge whose {@link KafkaProducerBridge#probeHealth()} is used as the probe
   * @param journal durable record of outage start time
   * @param gapEmitter emits gap envelopes on Kafka recovery
   * @param symbolStreams the set of {@code (symbol, stream)} pairs to emit gaps for
   */
  public static KafkaProducerHealthMonitor of(
      ClockSupplier clock,
      KafkaProducerBridge producer,
      KafkaOutageJournal journal,
      GapEmitter gapEmitter,
      List<SymbolStream> symbolStreams) {
    return new KafkaProducerHealthMonitor(
        clock, producer::probeHealth, journal, gapEmitter::emitWithTimestamps, symbolStreams);
  }

  /** Starts the probe loop on a new virtual thread. Idempotent. */
  public void start() {
    if (running.compareAndSet(false, true)) {
      // Check for unresolved outage from a prior process run
      checkPriorOutageOnStartup();
      probeThread = Thread.ofVirtual().name("kafka-health-monitor").start(this::probeLoop);
      log.info("kafka_health_monitor_started");
    }
  }

  /** Stops the probe loop. Safe to call from any thread. Idempotent. */
  public void stop() {
    running.set(false);
    Thread t = probeThread;
    if (t != null) {
      t.interrupt();
    }
    log.info("kafka_health_monitor_stopped");
  }

  // ── private ───────────────────────────────────────────────────────────────

  /**
   * On startup, if a previous {@code KafkaOutageJournal} entry exists (because the prior process
   * crashed during an outage), emit the bridging gap envelopes immediately before normal data flow.
   */
  private void checkPriorOutageOnStartup() {
    OptionalLong priorOutage = journal.readOutageStart();
    if (priorOutage.isPresent()) {
      long gapEnd = clock.nowNs();
      long outageStart = priorOutage.getAsLong();
      log.info(
          "kafka_prior_outage_detected_on_startup",
          "outage_start_ns",
          outageStart,
          "gap_end_ns",
          gapEnd);
      emitOutageGaps(outageStart, gapEnd);
      journal.truncate();
    }
  }

  private void probeLoop() {
    State state = State.HEALTHY;
    long degradedSinceNs = 0L;

    while (running.get()) {
      boolean healthy = healthProbe.getAsBoolean();

      switch (state) {
        case HEALTHY -> {
          if (!healthy) {
            state = State.DEGRADED;
            degradedSinceNs = clock.nowNs();
            log.info("kafka_producer_degraded");
          }
        }
        case DEGRADED -> {
          if (healthy) {
            state = State.HEALTHY;
            log.info("kafka_producer_recovered_from_degraded");
          } else {
            long degradedDurationNs = clock.nowNs() - degradedSinceNs;
            if (degradedDurationNs >= PAUSED_THRESHOLD_NS) {
              state = State.PAUSED;
              journal.recordOutageStart(degradedSinceNs);
              log.info("kafka_outage_started", "outage_start_ns", degradedSinceNs);
              log.info(
                  "LIFECYCLE COLLECTOR_KAFKA_OUTAGE_ENTERED: This collector cannot publish"
                      + " to Kafka — its producer path is broken. The other collector"
                      + " should keep delivering if its producer is healthy.",
                  "outage_start_ns",
                  degradedSinceNs);
            }
          }
        }
        case PAUSED -> {
          if (healthy) {
            // Recovery: emit gap envelopes and truncate journal
            OptionalLong outageStartOpt = journal.readOutageStart();
            long gapEnd = clock.nowNs();
            if (outageStartOpt.isPresent()) {
              long outageStart = outageStartOpt.getAsLong();
              emitOutageGaps(outageStart, gapEnd);
              journal.truncate();
            }
            state = State.HEALTHY;
            log.info("kafka_outage_resolved", "gap_end_ns", gapEnd);
            log.info(
                "LIFECYCLE COLLECTOR_KAFKA_OUTAGE_EXITED: This collector's Kafka producer"
                    + " is healthy again. Replaying kafka_producer_outage gap envelopes"
                    + " for the down window so the writer knows what was lost.",
                "gap_end_ns",
                gapEnd);
          }
          // If still unhealthy: stay paused, keep probing
        }
      }

      sleepUntilNextProbe();
    }
  }

  private void emitOutageGaps(long outageStartNs, long gapEndNs) {
    String detail =
        "Kafka producer unreachable from "
            + outageStartNs
            + " to "
            + gapEndNs
            + " (duration_ns="
            + (gapEndNs - outageStartNs)
            + ")";
    for (SymbolStream ss : symbolStreams) {
      gapEmitAction.emitWithTimestamps(
          ss.symbol(), ss.stream(), -1L, "kafka_producer_outage", detail, outageStartNs, gapEndNs);
    }
  }

  private void sleepUntilNextProbe() {
    try {
      long sleepMs = PROBE_INTERVAL_NS / 1_000_000L;
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
