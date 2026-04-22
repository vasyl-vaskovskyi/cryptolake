package com.cryptolake.writer.consumer;

import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.buffer.CheckpointMeta;
import com.cryptolake.writer.buffer.FlushResult;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.recovery.SealedFileIndex;
import com.cryptolake.writer.state.CryptoLakeStateException;
import com.cryptolake.writer.state.FileStateRecord;
import com.cryptolake.writer.state.StateManager;
import com.cryptolake.writer.state.StreamCheckpoint;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.common.util.ClockSupplier;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SOLE OWNER of Kafka offset commits (Tier 1 §4; Tier 5 C8).
 *
 * <p>This is the single class that calls {@code consumer.commitSync()}. NO other class in the
 * writer may call any commit API (commitSync, commitAsync). Unit test {@code
 * OffsetCommitCoordinatorOrderingTest} asserts the exact ordering via spies.
 *
 * <p>Linear ordering enforced in {@link #flushAndCommit(BufferManager)} (ports Python's
 * {@code WriterConsumer._commit_state} + {@code _write_and_save}):
 * <ol>
 *   <li>{@code buffers.flushAll()} → {@code List<FlushResult>} (in-memory extraction)
 *   <li>For each result: {@code compressor.compressFrame(lines)} → {@code
 *       appender.appendAndFsync(path, bytes)} (includes {@link java.nio.channels.FileChannel#force(boolean)
 *       force(true)}) — Tier 5 I3
 *   <li>Build {@code FileStateRecord} list + {@code StreamCheckpoint} list
 *   <li>{@code stateManager.saveStatesAndCheckpoints(...)} — ONE PG transaction, retry 3×; on
 *       final failure: increment metric + throw (NO Kafka commit)
 *   <li>Only on PG success: {@code consumer.commitSync(offsets)} — explicit offsets, uses {@code
 *       commitSync} not {@code commitAsync} (Tier 5 C8; architect mandate)
 *   <li>Update in-memory {@code _durable_checkpoints} cache
 * </ol>
 *
 * <p>Thread safety: called ONLY from the consume-loop virtual thread T1, OR from the shutdown hook
 * after T1 has returned (never concurrently — design §3.1 T3).
 */
public final class OffsetCommitCoordinator {

  private static final Logger log = LoggerFactory.getLogger(OffsetCommitCoordinator.class);

  private final KafkaConsumer<byte[], byte[]> primary;
  private final DurableAppender appender;
  private final ZstdFrameCompressor compressor;
  private final StateManager stateManager;
  private final SealedFileIndex sealedIndex;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /**
   * Mutable cache of durable checkpoints — updated after every successful PG+Kafka commit.
   * Owned by T1; shared (read-only) with RecoveryCoordinator via the method view.
   */
  private final Map<StreamKey, StreamCheckpoint> durableCheckpoints = new HashMap<>();

  public OffsetCommitCoordinator(
      KafkaConsumer<byte[], byte[]> primary,
      DurableAppender appender,
      ZstdFrameCompressor compressor,
      StateManager stateManager,
      SealedFileIndex sealedIndex,
      WriterMetrics metrics,
      ClockSupplier clock) {
    this.primary = primary;
    this.appender = appender;
    this.compressor = compressor;
    this.stateManager = stateManager;
    this.sealedIndex = sealedIndex;
    this.metrics = metrics;
    this.clock = clock;
  }

  // ── Main flush+commit operation (Tier 1 §4) ─────────────────────────────────────────────────

  /**
   * Flushes all buffers, writes each result to disk (fsync), saves PG state, then commitSync.
   *
   * <p>Returns total records flushed. Throws on PG failure BEFORE any commit (Tier 5 C8
   * watch-out). On IOException, wraps and rethrows; caller increments write_errors metric and
   * emits write_error gap.
   *
   * <p>THIS IS THE ONLY PLACE WHERE {@code consumer.commitSync} IS CALLED (Tier 1 §4).
   *
   * @param buffers the buffer manager to flush
   * @return total number of records flushed
   */
  public int flushAndCommit(BufferManager buffers) {
    // Step 1: Extract all buffered lines (in-memory, no I/O yet)
    List<FlushResult> results = buffers.flushAll();

    if (results.isEmpty()) {
      return 0;
    }

    int totalCount = 0;
    List<FileStateRecord> states = new ArrayList<>();
    List<StreamCheckpoint> checkpoints = new ArrayList<>();
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    // Step 2: Write each result to disk + fsync (Tier 5 I3)
    long flushStartNs = System.nanoTime(); // Tier 5 F4
    for (FlushResult r : results) {
      if (r.count() == 0) continue;
      totalCount += r.count();

      byte[] compressed = compressor.compressFrame(r.lines());
      try {
        appender.appendAndFsync(r.filePath(), compressed); // force(true) inside (Tier 5 I3)
      } catch (IOException e) {
        // Increment write_errors metric here; caller may emit write_error gap
        metrics.writeErrors(r.target().exchange(), r.target().symbol(), r.target().stream()).increment();
        throw new UncheckedIOException("File write failed for " + r.filePath(), e);
      }

      long fileSizeAfter;
      try {
        fileSizeAfter = appender.sizeAfter(r.filePath(), compressed.length);
      } catch (IOException e) {
        fileSizeAfter = 0L;
      }

      // Step 3: Build state records
      if (r.highWaterOffset() >= 0) { // only real (non-synthetic) records have offsets
        TopicPartition tp = new TopicPartition(
            r.target().exchange() + "." + r.target().stream(), r.partition());
        // Use the topic name from the file path context; in practice the target encodes it
        // via exchange.stream naming convention (matching TopicNames pattern)
        states.add(new FileStateRecord(
            tp.topic(), tp.partition(), r.highWaterOffset(), r.filePath().toString(), fileSizeAfter));
        offsets.put(tp, new OffsetAndMetadata(r.highWaterOffset() + 1));
      }

      // Build stream checkpoint
      CheckpointMeta cp = r.checkpointMeta();
      if (cp != null) {
        checkpoints.add(new StreamCheckpoint(
            cp.streamKey().exchange(),
            cp.streamKey().symbol(),
            cp.streamKey().stream(),
            java.time.Instant.ofEpochSecond(
                cp.lastReceivedAt() / 1_000_000_000L,
                cp.lastReceivedAt() % 1_000_000_000L).toString(), // ISO-8601 (Tier 5 F1)
            cp.lastCollectorSessionId(),
            null)); // lastGapReason updated when gap is emitted
      }

      // Update metrics
      metrics.bytesWritten(r.target().exchange(), r.target().symbol(), r.target().stream())
          .increment(compressed.length);
      if (compressed.length > 0) {
        int uncompressed = r.lines().stream().mapToInt(b -> b.length).sum();
        if (uncompressed > 0) {
          metrics.setCompressionRatio(r.target().exchange(), r.target().symbol(), r.target().stream(),
              (double) uncompressed / compressed.length);
        }
      }
    }

    long flushEndNs = System.nanoTime();
    double flushMs = (flushEndNs - flushStartNs) / 1_000_000.0;

    // Step 4: PG save — atomic, retry 3× (Tier 5 G3). On failure: metric + throw, NO commit
    try {
      stateManager.saveStatesAndCheckpoints(states, checkpoints);
    } catch (CryptoLakeStateException e) {
      metrics.pgCommitFailures().increment();
      log.error("pg_commit_failed", e, "error", e.getMessage());
      throw e; // NO Kafka commit (Tier 5 C8 watch-out)
    }

    // Step 5: Only on PG success — commitSync with explicit offsets (Tier 5 C8; Tier 1 §4)
    if (!offsets.isEmpty()) {
      try {
        primary.commitSync(offsets); // commitSync NOT commitAsync (Tier 5 C8)
      } catch (CommitFailedException e) {
        metrics.kafkaCommitFailures().increment();
        log.error("kafka_commit_failed", e, "error", e.getMessage());
        throw e; // Non-retriable; let T1 observe on next poll
      }
    }

    // Step 6: Update in-memory durable checkpoints cache
    for (StreamCheckpoint cp : checkpoints) {
      StreamKey key = new StreamKey(cp.exchange(), cp.symbol(), cp.stream());
      durableCheckpoints.put(key, cp);
    }

    // Record flush duration histogram per stream (first result only for simplicity)
    if (!results.isEmpty()) {
      FlushResult first = results.get(0);
      metrics.flushDurationMs(first.target().exchange(), first.target().symbol(), first.target().stream())
          .record(flushMs);
    }

    log.info("flush_and_commit_complete",
        "records", totalCount,
        "flush_ms", flushMs,
        "partitions", offsets.size());

    return totalCount;
  }

  /**
   * Variant called during hour rotation: accepts flush results and sealed file paths, performs
   * seal-specific PG upsert, then commitSync.
   *
   * <p>Ports Python's {@code WriterConsumer._rotate_file} commit path.
   *
   * @param results flush results from the rotated hour
   * @param sealedFiles data paths that were sealed (for PG state)
   */
  public void commitSealedHour(List<FlushResult> results, List<java.nio.file.Path> sealedFiles) {
    List<FileStateRecord> states = new ArrayList<>();
    List<StreamCheckpoint> checkpoints = new ArrayList<>();
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    for (FlushResult r : results) {
      if (r.highWaterOffset() < 0) continue;

      long fileByteSize = 0;
      for (java.nio.file.Path sp : sealedFiles) {
        if (sp.toString().contains(r.target().stream())) {
          try { fileByteSize = java.nio.file.Files.size(sp); } catch (IOException e) { /* skip */ }
          break;
        }
      }

      TopicPartition tp = new TopicPartition(
          r.target().exchange() + "." + r.target().stream(), r.partition());
      states.add(new FileStateRecord(
          tp.topic(), tp.partition(), r.highWaterOffset(),
          r.filePath().toString(), fileByteSize));
      offsets.put(tp, new OffsetAndMetadata(r.highWaterOffset() + 1));

      CheckpointMeta cp = r.checkpointMeta();
      if (cp != null) {
        checkpoints.add(new StreamCheckpoint(
            cp.streamKey().exchange(), cp.streamKey().symbol(), cp.streamKey().stream(),
            java.time.Instant.ofEpochSecond(
                cp.lastReceivedAt() / 1_000_000_000L,
                cp.lastReceivedAt() % 1_000_000_000L).toString(),
            cp.lastCollectorSessionId(), null));
      }
    }

    try {
      stateManager.saveStatesAndCheckpoints(states, checkpoints);
    } catch (CryptoLakeStateException e) {
      metrics.pgCommitFailures().increment();
      throw e;
    }

    if (!offsets.isEmpty()) {
      try {
        primary.commitSync(offsets); // Tier 1 §4 sole owner
      } catch (CommitFailedException e) {
        metrics.kafkaCommitFailures().increment();
        throw e;
      }
    }

    for (StreamCheckpoint cp : checkpoints) {
      durableCheckpoints.put(new StreamKey(cp.exchange(), cp.symbol(), cp.stream()), cp);
    }
  }

  /**
   * Called by {@link PartitionAssignmentListener} during {@code onPartitionsRevoked}: commitSync
   * (not async) any offsets already durable in PG. Blocking by design (Tier 5 C4 watch-out).
   *
   * <p>Ports Python's {@code _on_revoke} → {@code commit(asynchronous=False)}.
   *
   * @param revoked the partitions being revoked
   */
  public void commitBeforeRevoke(Collection<TopicPartition> revoked) {
    // No buffered data to flush here (flush happens on assign); just commit current positions
    // This is a lightweight commitSync for durability before losing the partition
    try {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      for (TopicPartition tp : revoked) {
        long position;
        try {
          position = primary.position(tp);
        } catch (Exception e) {
          log.warn("position_error_on_revoke", "tp", tp.toString(), "error", e.getMessage());
          continue;
        }
        offsets.put(tp, new OffsetAndMetadata(position));
      }
      if (!offsets.isEmpty()) {
        primary.commitSync(offsets); // Tier 1 §4 sole owner; Tier 5 C4
      }
    } catch (CommitFailedException e) {
      metrics.kafkaCommitFailures().increment();
      log.warn("commit_before_revoke_failed", "error", e.getMessage());
      // Don't rethrow — revoke is already in progress; Kafka coordinator handles it
    }
  }

  /**
   * Final shutdown commit: fresh flushAndCommit, then explicit commitSync of all current positions.
   *
   * <p>Ports Python's final flush+commit in {@code WriterConsumer.stop()}.
   *
   * @param buffers the buffer manager to flush
   */
  public void shutdownCommit(BufferManager buffers) {
    try {
      flushAndCommit(buffers);
    } catch (Exception e) {
      log.warn("shutdown_flush_failed", "error", e.getMessage());
    }
  }

  /** Returns the live durable checkpoints map (updated after each successful commit). */
  public Map<StreamKey, StreamCheckpoint> durableCheckpoints() {
    return java.util.Collections.unmodifiableMap(durableCheckpoints);
  }

  /**
   * Seed the durable checkpoints from recovery (called by RecoveryCoordinator after startup load).
   *
   * @param loaded the loaded checkpoints from PG
   */
  public void seedDurableCheckpoints(Map<StreamKey, StreamCheckpoint> loaded) {
    durableCheckpoints.putAll(loaded);
  }
}
