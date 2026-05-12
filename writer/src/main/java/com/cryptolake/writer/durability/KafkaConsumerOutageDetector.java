package com.cryptolake.writer.durability;

import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.gap.GapEmitter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Detects prolonged Kafka consumer silence and emits {@code kafka_consumer_outage} gap envelopes.
 *
 * <p>Tracks {@code lastPollWithRecordsAt} (updated by the consumer loop on each non-empty poll). A
 * virtual-thread monitor wakes every {@link #CHECK_INTERVAL_NS} (10s). If:
 *
 * <ul>
 *   <li>{@code now - lastPollWithRecordsAt > SILENCE_THRESHOLD_NS} (30s), AND
 *   <li>the writer's lifecycle heartbeat has fired recently (writer is alive — not itself shut
 *       down)
 * </ul>
 *
 * then a {@code kafka_consumer_outage} gap is emitted per {@code (symbol, stream)} covering {@code
 * [lastPollWithRecordsAt, now]}.
 *
 * <p>The detector re-arms when {@link #recordPollWithRecords()} is called.
 *
 * <p>Corresponds to spec §5.3 #15 / HOLE 1.
 *
 * <p>Thread safety: {@code lastPollWithRecordsAt} is an {@link AtomicLong}. The monitor loop and
 * the consumer-loop thread both access it safely. {@link #stop()} is safe to call from any thread.
 */
public final class KafkaConsumerOutageDetector {

  private static final StructuredLogger log =
      StructuredLogger.of(KafkaConsumerOutageDetector.class);

  /** How often the monitor wakes up. */
  static final long CHECK_INTERVAL_NS = 10_000_000_000L; // 10 s

  /** How long without records before we declare an outage. */
  static final long SILENCE_THRESHOLD_NS = 30_000_000_000L; // 30 s

  /**
   * A {@code (symbol, stream)} pair for which gap envelopes are emitted during outage.
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
  private final BooleanSupplier writerHeartbeatAlive;
  private final GapEmitAction gapEmitAction;
  private final List<SymbolStream> symbolStreams;

  /** Nanosecond timestamp of the most recent non-empty poll. 0 = never polled. */
  private final AtomicLong lastPollWithRecordsAt = new AtomicLong(0L);

  /** Whether an outage gap is currently in-flight (prevents duplicate emissions). */
  private final AtomicBoolean outageActive = new AtomicBoolean(false);

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread monitorThread;

  /**
   * Constructs a {@code KafkaConsumerOutageDetector}.
   *
   * @param clock nanosecond clock
   * @param writerHeartbeatAlive supplier that returns {@code true} if the writer's lifecycle
   *     heartbeat has been seen recently (i.e. the writer process itself is alive)
   * @param gapEmitAction callback to emit gap envelopes
   * @param symbolStreams set of {@code (symbol, stream)} pairs to emit gaps for
   */
  public KafkaConsumerOutageDetector(
      ClockSupplier clock,
      BooleanSupplier writerHeartbeatAlive,
      GapEmitAction gapEmitAction,
      List<SymbolStream> symbolStreams) {
    this.clock = clock;
    this.writerHeartbeatAlive = writerHeartbeatAlive;
    this.gapEmitAction = gapEmitAction;
    this.symbolStreams = List.copyOf(symbolStreams);
  }

  /**
   * Convenience factory for production use — wraps the writer's {@link GapEmitter#emitUnfiltered}
   * to emit synthetic {@code kafka_consumer_outage} gap envelopes.
   *
   * @param clock nanosecond clock
   * @param writerHeartbeatAlive supplier that returns {@code true} when writer is alive
   * @param gapEmitter writer-side gap emitter
   * @param exchange exchange name (e.g. {@code "binance"})
   * @param symbolStreams set of {@code (symbol, stream)} pairs
   */
  public static KafkaConsumerOutageDetector of(
      ClockSupplier clock,
      BooleanSupplier writerHeartbeatAlive,
      GapEmitter gapEmitter,
      String exchange,
      List<SymbolStream> symbolStreams) {
    return new KafkaConsumerOutageDetector(
        clock,
        writerHeartbeatAlive,
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

  /** Initializes the detector with the current time as the first baseline. Call before starting. */
  public void initialize() {
    lastPollWithRecordsAt.set(clock.nowNs());
  }

  /** Starts the monitor virtual thread. Idempotent. */
  public void start() {
    if (running.compareAndSet(false, true)) {
      if (lastPollWithRecordsAt.get() == 0L) {
        lastPollWithRecordsAt.set(clock.nowNs());
      }
      monitorThread = Thread.ofVirtual().name("kafka-consumer-outage-detector").start(this::loop);
      log.info("kafka_consumer_outage_detector_started");
    }
  }

  /** Stops the monitor. Safe to call from any thread. Idempotent. */
  public void stop() {
    running.set(false);
    Thread t = monitorThread;
    if (t != null) {
      t.interrupt();
    }
    log.info("kafka_consumer_outage_detector_stopped");
  }

  /**
   * Called by the consumer loop each time a poll returns at least one record.
   *
   * <p>Updates the baseline and clears any active outage state.
   */
  public void recordPollWithRecords() {
    lastPollWithRecordsAt.set(clock.nowNs());
    if (outageActive.compareAndSet(true, false)) {
      log.info("kafka_consumer_outage_recovered");
    }
  }

  // ── private ───────────────────────────────────────────────────────────────

  private void loop() {
    while (running.get()) {
      sleepInterval();
      if (!running.get()) break;

      long now = clock.nowNs();
      long lastAt = lastPollWithRecordsAt.get();
      long silenceDuration = now - lastAt;

      if (silenceDuration > SILENCE_THRESHOLD_NS
          && writerHeartbeatAlive.getAsBoolean()
          && !outageActive.get()) {
        outageActive.set(true);
        String detail =
            "No Kafka records for "
                + (silenceDuration / 1_000_000L)
                + "ms (last_poll_ns="
                + lastAt
                + ")";
        log.info("kafka_consumer_outage_detected", "silence_ms", silenceDuration / 1_000_000L);
        for (SymbolStream ss : symbolStreams) {
          gapEmitAction.emitWithTimestamps(
              ss.symbol(),
              ss.stream(),
              -1L,
              com.cryptolake.common.envelope.GapReason.KAFKA_CONSUMER_OUTAGE,
              detail,
              lastAt,
              now);
        }
      }
    }
  }

  private void sleepInterval() {
    try {
      Thread.sleep(CHECK_INTERVAL_NS / 1_000_000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
