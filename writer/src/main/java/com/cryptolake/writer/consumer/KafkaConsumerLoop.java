package com.cryptolake.writer.consumer;

import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.durability.KafkaConsumerOutageDetector;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the {@link KafkaConsumer} for the primary topic set. Runs the single consume loop on a
 * virtual thread. Dispatches records to {@link RecordHandler}. Polls the continuous {@link
 * BackupTailConsumer} every iteration for backup-source liveness (plan 2026-05-03).
 *
 * <p>Ports Python's {@code WriterConsumer.consume_loop()} (design §2.2; design §4.2).
 *
 * <p>Thread safety: SINGLE virtual thread owns this class (T1 — design §3.1). The only shared state
 * visible to other threads is:
 *
 * <ul>
 *   <li>{@code stopRequested} — {@code volatile boolean} set by SIGTERM hook (T3)
 *   <li>{@code assignedPartitions} — swapped atomically (volatile reference) for Ready check (T2)
 * </ul>
 *
 * <p>No {@code synchronized}, no locks (Tier 2 §9; Tier 5 A5).
 */
public final class KafkaConsumerLoop implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLoop.class);

  // shorter than primary poll: tail is liveness-only, must not stall the loop
  private static final Duration BACKUP_TAIL_POLL_TIMEOUT = Duration.ofMillis(100);

  private final KafkaConsumer<byte[], byte[]> primary;
  private final BackupTailConsumer backupTail;
  private final List<String> enabledTopics;
  private final RecordHandler recordHandler;
  private final FailoverController failover;
  private final OffsetCommitCoordinator committer;
  private final RecoveryCoordinator recovery;
  private final HourRotationScheduler rotator;
  private final BufferManager buffers;
  private final CoverageFilter coverage;
  private final GapEmitter gaps;
  private final WriterMetrics metrics;

  /**
   * Optional outage detector that tracks the last non-empty poll timestamp. Null if not wired
   * (default — for backward compatibility with existing tests).
   */
  private volatile KafkaConsumerOutageDetector outageDetector;

  /** Volatile flag set by SIGTERM hook to stop the consume loop. No synchronized needed. */
  private volatile boolean stopRequested = false;

  /**
   * Tracks whether the previous {@link #applyHoldPauseState()} call observed an active hold. Used
   * to emit the LIFECYCLE pause/resume log lines on edge-changes only (not every iteration). Read
   * and written by T1 only — no synchronization needed.
   */
  private boolean lastKnownHeld = false;

  /**
   * Volatile set of currently assigned partitions. Written by T1 (listener), read by T2 (health).
   * Uses an unmodifiable copy swap pattern — no race (single writer; T2 reads reference
   * atomically).
   */
  private volatile Set<TopicPartition> assignedPartitions = Set.of();

  /** Mutable backing set used by the listener; swapped out when updated. */
  private final Set<TopicPartition> assignedSink = new HashSet<>();

  public KafkaConsumerLoop(
      KafkaConsumer<byte[], byte[]> primary,
      BackupTailConsumer backupTail,
      List<String> enabledTopics,
      RecordHandler recordHandler,
      FailoverController failover,
      OffsetCommitCoordinator committer,
      RecoveryCoordinator recovery,
      HourRotationScheduler rotator,
      BufferManager buffers,
      CoverageFilter coverage,
      GapEmitter gaps,
      WriterMetrics metrics) {
    this.primary = primary;
    this.backupTail = backupTail;
    this.enabledTopics = List.copyOf(enabledTopics);
    this.recordHandler = recordHandler;
    this.failover = failover;
    this.committer = committer;
    this.recovery = recovery;
    this.rotator = rotator;
    this.buffers = buffers;
    this.coverage = coverage;
    this.gaps = gaps;
    this.metrics = metrics;
  }

  /**
   * Runs the consume loop — blocking; called via {@code executor.submit(this)} in {@link
   * com.cryptolake.writer.Main} (virtual thread, design §3.1 T1).
   *
   * <p>Ports {@code WriterConsumer.consume_loop()} and {@code WriterConsumer.start()}.
   */
  @Override
  public void run() {
    // Subscribe to enabled topics only (Tier 1 §3 — disabled streams NOT in this list)
    PartitionAssignmentListener listener =
        new PartitionAssignmentListener(primary, recovery, committer, assignedSink);
    primary.subscribe(enabledTopics, listener);
    log.info("consume_loop_started", "topics", enabledTopics.toString());

    // Continuous dual-source tailing (plan 2026-05-03): always tail the backup topic so
    // CoverageFilter has fresh per-source liveness data even when failover is inactive.
    if (backupTail != null) {
      backupTail.start();
      log.info("backup_tail_started");
    }

    long lastLagUpdateNs = System.nanoTime(); // Tier 5 F4

    while (!stopRequested) {
      try {
        // Pause/resume primary consumption based on hold state. When either disk-full or
        // pg-outage hold is active, primary partitions are paused so records remain in Kafka
        // rather than accumulating in BufferManager (no max-bytes cap → OOM risk under long
        // holds). Backup-tail is unaffected (liveness only).
        applyHoldPauseState();

        // Poll primary (Tier 5 A2 — blocking call on virtual thread; no run_in_executor wrapper)
        ConsumerRecords<byte[], byte[]> records = primary.poll(Duration.ofSeconds(1));
        if (!records.isEmpty()) {
          KafkaConsumerOutageDetector det = outageDetector;
          if (det != null) det.recordPollWithRecords();
        }
        for (ConsumerRecord<byte[], byte[]> rec : records) { // Tier 5 C2 — batch-first
          recordHandler.handle(rec, false);
          // Bug B: every primary record advances the recovery observation window.
          failover.markPrimaryDelivered();
        }

        // Bug B: hysteresis deactivation. After processing primary records, ask the
        // controller whether primary has been delivering continuously for the
        // recovery stability window. If yes, deactivate failover so MAIN_RECOVERED
        // / WRITER_NOW_ARCHIVING_FROM=MAIN LIFECYCLE events fire and the writer
        // stops polling backup.
        if (failover.shouldDeactivate()) {
          failover.deactivate();
        }

        // Sync the assigned set (listener updates assignedSink on T1; publish snapshot)
        if (!assignedSink.isEmpty()) {
          assignedPartitions = Collections.unmodifiableSet(new HashSet<>(assignedSink));
        }

        // Continuous backup-topic tailing (plan 2026-05-03). Poll the backup tail
        // unconditionally so CoverageFilter sees backup records regardless of failover state;
        // RecordHandler decides whether to archive vs. drop based on failover.isActive().
        //
        // ISOLATION BOUNDARY: the tail is best-effort liveness — primary archiving MUST NOT
        // be coupled to its health. Wrap the poll + per-record handle loop in its own
        // try/catch so a WakeupException, broker disconnect, auth failure, or any other
        // exception on the tail does not stop the writer from continuing to consume the
        // primary topic. Increment the writer_backup_tail_errors_total counter on each caught
        // exception so we can alert on tail health without losing the writer.
        if (backupTail != null) {
          try {
            ConsumerRecords<byte[], byte[]> backupRecords =
                backupTail.poll(BACKUP_TAIL_POLL_TIMEOUT);
            for (ConsumerRecord<byte[], byte[]> rec : backupRecords) {
              recordHandler.handle(rec, true);
            }
          } catch (Exception backupErr) {
            // Broad on purpose: this is an isolation boundary. Log at WARN and continue.
            metrics.backupTailErrors().increment();
            log.warn(
                "backup_tail_poll_error",
                "error",
                backupErr.getClass().getSimpleName() + ": " + backupErr.getMessage());
          }
        }
        if (!failover.isActive() && failover.shouldActivate()) {
          failover.activate();
        }

        // Periodic flush
        if (buffers.shouldFlushByInterval()) {
          committer.flushAndCommit(buffers);
          updateLagGauge(lastLagUpdateNs); // Q3 preferred — update on flushAndCommit (design §11)
          lastLagUpdateNs = System.nanoTime();
        }

        // Sweep expired coverage gaps and emit them
        for (com.cryptolake.common.envelope.GapEnvelope expiredGap : coverage.sweepExpired()) {
          gaps.emitUnfiltered(expiredGap, "primary", "internal", -1);
        }

      } catch (org.apache.kafka.common.errors.WakeupException e) {
        // Expected during shutdown
        break;
      } catch (OffsetOutOfRangeException e) {
        // Consumer auto-reset triggered (auto.offset.reset=earliest/latest). Emit a
        // kafka_offset_reset gap for all affected (symbol, stream) pairs.
        // After this catch, Kafka's consumer will have already reset the offsets.
        handleOffsetReset(e);
      } catch (Exception e) {
        // Log and continue — a single poll cycle failure must not kill the loop (design §7.2)
        log.error("consume_loop_error", "error", e.getMessage());
        // Non-Error exceptions are recoverable; Error types propagate (design §7.2)
      }
    }

    // Shutdown sequence (design §3.4)
    shutdownSequence();
  }

  /**
   * Wires an outage detector to be notified on each non-empty poll. Must be called before {@link
   * #run()}.
   *
   * @param detector the detector to notify; null disables notifications
   */
  public void setOutageDetector(KafkaConsumerOutageDetector detector) {
    this.outageDetector = detector;
  }

  /** Signals the consume loop to stop. Called from the SIGTERM handler (T3 — volatile write). */
  public void requestShutdown() {
    this.stopRequested = true;
    log.info("consume_loop_shutdown_requested");
  }

  /**
   * Pauses or resumes primary Kafka consumption based on the committer's hold state.
   *
   * <p>Called at the top of every consume-loop iteration BEFORE {@code primary.poll}. When any hold
   * is active, primary partitions are paused so records remain in Kafka rather than accumulating in
   * {@link com.cryptolake.writer.buffer.BufferManager} (which has no max-bytes cap and would OOM
   * during long holds).
   *
   * <p>The pause/resume is edge-triggered for log emission (single LIFECYCLE line per state
   * change), but the underlying {@code primary.pause(...)} call is repeated each iteration while
   * held to cover partition reassignment during a hold (Kafka client treats pause on already-
   * paused partitions as a no-op).
   *
   * <p>Package-private for unit-test access; production callers go through {@link #run()}.
   */
  void applyHoldPauseState() {
    boolean nowHeld = committer.isAnyHoldActive();
    if (nowHeld != lastKnownHeld) {
      try {
        if (nowHeld) {
          primary.pause(primary.assignment());
          log.info(
              "LIFECYCLE WRITER_KAFKA_CONSUMPTION_PAUSED: Hold is active — the writer is"
                  + " pausing primary Kafka consumption so records remain in Kafka rather than"
                  + " accumulating in the in-memory buffer. They will replay when the hold"
                  + " exits.");
        } else {
          primary.resume(primary.assignment());
          log.info(
              "LIFECYCLE WRITER_KAFKA_CONSUMPTION_RESUMED: Hold has cleared — the writer is"
                  + " resuming primary Kafka consumption; records that accumulated in Kafka"
                  + " during the hold will now flow through.");
        }
      } catch (Exception e) {
        // Pause/resume can throw IllegalStateException if the consumer is already closed
        // (only happens during shutdown; defensive).
        log.warn("hold_pause_state_apply_failed", "now_held", nowHeld, "error", e.getMessage());
      }
      lastKnownHeld = nowHeld;
    } else if (nowHeld) {
      // Re-pause the current assignment every iteration while held so a rebalance during
      // hold does not leak unpaused partitions.
      try {
        primary.pause(primary.assignment());
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }

  /**
   * Test-only: returns the current {@link #lastKnownHeld} value. Package-private accessor exposed
   * for assertions in {@link KafkaConsumerLoopHoldPauseTest}.
   */
  boolean isLastKnownHeldForTest() {
    return lastKnownHeld;
  }

  /**
   * Returns {@code true} if at least one partition is currently assigned (used by {@link
   * com.cryptolake.writer.health.WriterReadyCheck}).
   */
  public boolean isConnected() {
    return !assignedPartitions.isEmpty();
  }

  // ── Private helpers ──────────────────────────────────────────────────────────────────────────

  /**
   * Handles an {@link OffsetOutOfRangeException} from the consumer poll loop.
   *
   * <p>Emits one {@code kafka_offset_reset} gap per affected topic-partition. The Kafka consumer
   * has already auto-reset its position (via {@code auto.offset.reset}); this gap records the event
   * for the verify CLI.
   *
   * <p>Corresponds to spec §5.4 #30 / Task A3.5.
   */
  private void handleOffsetReset(OffsetOutOfRangeException e) {
    Map<TopicPartition, Long> offsetsMap = e.offsetOutOfRangePartitions();
    log.info(
        "kafka_offset_reset_detected",
        "partitions",
        offsetsMap.keySet().toString(),
        "old_offsets",
        offsetsMap.values().toString());

    long now = System.nanoTime();
    String detail =
        "Kafka auto-reset from OUT_OF_RANGE: partitions="
            + offsetsMap.keySet()
            + " old_offsets="
            + offsetsMap.values();

    // Emit one gap per affected topic-partition
    for (Map.Entry<TopicPartition, Long> entry : offsetsMap.entrySet()) {
      TopicPartition tp = entry.getKey();
      long oldOffset = entry.getValue();
      String partitionDetail =
          detail
              + " topic="
              + tp.topic()
              + " partition="
              + tp.partition()
              + " old_offset="
              + oldOffset;
      // Emit a synthetic gap — we don't have per-symbol/stream here so use topic name as detail
      com.cryptolake.common.envelope.GapEnvelope gap =
          com.cryptolake.common.envelope.GapEnvelope.create(
              "binance", // exchange — best-effort; topic-partition doesn't carry symbol info
              tp.topic(), // symbol field — use topic as placeholder
              "kafka_offset_reset", // stream placeholder
              "synthetic",
              -1L,
              now,
              now,
              "kafka_offset_reset",
              partitionDetail,
              () -> now);
      gaps.emitUnfiltered(gap, "primary", tp.topic(), tp.partition());
    }
  }

  private void shutdownSequence() {
    log.info("consume_loop_shutting_down");

    // Flush pending coverage gaps
    for (com.cryptolake.common.envelope.GapEnvelope pendingGap : coverage.flushAllPending()) {
      try {
        gaps.emitUnfiltered(pendingGap, "primary", "internal", -1);
      } catch (Exception ignored) {
        // best-effort shutdown
      }
    }

    // Rotate all non-current hours
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    String currentDate = DateTimeFormatter.ISO_LOCAL_DATE.format(now);
    int currentHour = now.getHour();
    rotator.rotateAllOnShutdown(currentDate, currentHour);

    // Final flush + commit
    committer.shutdownCommit(buffers);

    // Write sidecars for any archive files that still lack one — including the
    // current-hour files that were written by periodic flushes but never sealed
    // (Python's _rotate_hour scans all *.jsonl.zst at shutdown; Tier 5 I6).
    rotator.writeMissingSidecarsOnShutdown();

    // Close backup tail consumer.
    if (backupTail != null) {
      try {
        backupTail.close();
      } catch (Exception ignored) {
        // best-effort shutdown
      }
    }

    // Close primary consumer
    try {
      primary.close(Duration.ofSeconds(30));
    } catch (Exception ignored) {
      // best-effort
    }

    log.info("consume_loop_terminated");
  }

  private void updateLagGauge(long lastUpdateNs) {
    // Tier 5 C6: consumer.endOffsets + consumer.position for lag calculation
    try {
      for (TopicPartition tp : primary.assignment()) {
        java.util.Map<TopicPartition, Long> ends =
            primary.endOffsets(java.util.Collections.singleton(tp));
        long endOffset = ends.getOrDefault(tp, 0L);
        long position = primary.position(tp);
        long lag = Math.max(0, endOffset - position);
        // Extract exchange and stream from topic name (topic format: exchange.stream)
        String[] parts = tp.topic().split("\\.");
        String exchange = parts.length > 0 ? parts[0] : tp.topic();
        String stream = parts.length > 1 ? parts[1] : "unknown";
        metrics.setConsumerLag(exchange, stream, lag);
      }
    } catch (Exception e) {
      log.debug("lag_gauge_update_failed", "error", e.getMessage());
    }
  }
}
