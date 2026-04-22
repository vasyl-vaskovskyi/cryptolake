package com.cryptolake.writer.consumer;

import com.cryptolake.writer.buffer.BufferManager;
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
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the {@link KafkaConsumer} for the primary topic set. Runs the single consume loop on a
 * virtual thread. Dispatches records to {@link RecordHandler}. Delegates backup-consumer drain to
 * {@link FailoverController}.
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

  private final KafkaConsumer<byte[], byte[]> primary;
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

  /** Volatile flag set by SIGTERM hook to stop the consume loop. No synchronized needed. */
  private volatile boolean stopRequested = false;

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

    long lastLagUpdateNs = System.nanoTime(); // Tier 5 F4

    while (!stopRequested) {
      try {
        // Poll primary (Tier 5 A2 — blocking call on virtual thread; no run_in_executor wrapper)
        ConsumerRecords<byte[], byte[]> records = primary.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<byte[], byte[]> rec : records) { // Tier 5 C2 — batch-first
          recordHandler.handle(rec, false);
        }

        // Sync the assigned set (listener updates assignedSink on T1; publish snapshot)
        if (!assignedSink.isEmpty()) {
          assignedPartitions = Collections.unmodifiableSet(new HashSet<>(assignedSink));
        }

        // Poll backup if failover is active
        if (failover.isActive()) {
          ConsumerRecords<byte[], byte[]> backupRecords =
              failover.pollBackup(Duration.ofMillis(500));
          for (ConsumerRecord<byte[], byte[]> rec : backupRecords) {
            recordHandler.handle(rec, true);
          }
        } else if (failover.shouldActivate()) {
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
      } catch (Exception e) {
        // Log and continue — a single poll cycle failure must not kill the loop (design §7.2)
        log.error("consume_loop_error", "error", e.getMessage());
        // Non-Error exceptions are recoverable; Error types propagate (design §7.2)
      }
    }

    // Shutdown sequence (design §3.4)
    shutdownSequence();
  }

  /** Signals the consume loop to stop. Called from the SIGTERM handler (T3 — volatile write). */
  public void requestShutdown() {
    this.stopRequested = true;
    log.info("consume_loop_shutdown_requested");
  }

  /**
   * Returns {@code true} if at least one partition is currently assigned (used by {@link
   * com.cryptolake.writer.health.WriterReadyCheck}).
   */
  public boolean isConnected() {
    return !assignedPartitions.isEmpty();
  }

  // ── Private helpers ──────────────────────────────────────────────────────────────────────────

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

    // Close backup consumer
    try {
      failover.cleanup();
    } catch (Exception ignored) {
      // best-effort shutdown; never block main shutdown path
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
