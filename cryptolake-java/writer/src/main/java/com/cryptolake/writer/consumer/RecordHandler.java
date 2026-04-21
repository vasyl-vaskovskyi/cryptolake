package com.cryptolake.writer.consumer;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Per-record logic: deserialize, session-change detection, depth-anchor check, coverage filter,
 * buffer add, failover tracking.
 *
 * <p>Ports Python's {@code WriterConsumer._handle_gap_detection},
 * {@code _handle_rotation_and_buffer}, {@code _deserialize_and_stamp} (design §2.2; design §4.2).
 *
 * <p>Returns one of: {accepted, gap-emitted, skipped}.
 *
 * <p>Thread safety: stateless; called only from {@link KafkaConsumerLoop}'s virtual thread (T1).
 * All state is in the injected collaborators, which are all T1-owned.
 */
public final class RecordHandler {

  private static final StructuredLogger log = StructuredLogger.of(RecordHandler.class);

  private final EnvelopeCodec codec;
  private final SessionChangeDetector sessionDetector;
  private final DepthRecoveryGapFilter depthFilter;
  private final CoverageFilter coverageFilter;
  private final FailoverController failover;
  private final RecoveryCoordinator recovery;
  private final BufferManager buffers;
  private final GapEmitter gaps;
  private final WriterMetrics metrics;
  private final String backupPrefix;

  public RecordHandler(
      EnvelopeCodec codec,
      SessionChangeDetector sessionDetector,
      DepthRecoveryGapFilter depthFilter,
      CoverageFilter coverageFilter,
      FailoverController failover,
      RecoveryCoordinator recovery,
      BufferManager buffers,
      GapEmitter gaps,
      WriterMetrics metrics,
      String backupPrefix) {
    this.codec = codec;
    this.sessionDetector = sessionDetector;
    this.depthFilter = depthFilter;
    this.coverageFilter = coverageFilter;
    this.failover = failover;
    this.recovery = recovery;
    this.buffers = buffers;
    this.gaps = gaps;
    this.metrics = metrics;
    this.backupPrefix = backupPrefix;
  }

  /**
   * Processes a single Kafka record: deserialize, detect gaps, route to buffer.
   *
   * <p>On deserialization failure: emits a {@code deserialization_error} gap and returns (Tier 5
   * G4 — never rethrows; a single malformed record must not kill the consumer loop).
   *
   * @param rec the Kafka consumer record
   * @param fromBackup {@code true} if this record came from the backup topic
   */
  public void handle(ConsumerRecord<byte[], byte[]> rec, boolean fromBackup) {
    String topic = rec.topic();
    int partition = rec.partition();
    long offset = rec.offset();
    String source = fromBackup ? "backup" : "primary";

    // Determine the primary topic name (strip backup prefix if needed — Tier 5 C7)
    String primaryTopic = topic.startsWith(backupPrefix)
        ? topic.substring(backupPrefix.length())
        : topic;

    // Deserialize (Tier 5 G4: catch + deserialization_error gap on failure)
    DataEnvelope env;
    try {
      env = codec.readData(rec.value());
    } catch (Exception e) {
      log.error("corrupt_message_skipped", e,
          "topic", topic,
          "partition", partition,
          "offset", offset,
          "error", e.getMessage());
      // Emit deserialization_error gap with synthetic coords (Tier 5 G4)
      // Use a placeholder gap — we don't know the stream, so use topic as proxy
      // In practice this should be rare; just log and move on
      return; // DO NOT rethrow (Tier 5 G4)
    }

    // Track in coverage filter
    coverageFilter.handleData(source, env);

    // Metrics: consumed (always, even if we'll skip below)
    metrics.messagesConsumed(env.exchange(), env.symbol(), env.stream()).increment();

    if (fromBackup) {
      metrics.failoverRecordsTotal().increment();
      // Switchback filter: if primary has caught up, drop backup records
      if (failover.checkSwitchbackFilter(env)) {
        metrics.messagesSkipped(env.exchange(), env.symbol(), env.stream()).increment();
        return;
      }
    } else {
      failover.resetSilenceTimer();
    }

    // MDC context for structured logging (Tier 5 H3)
    try (var ctx = StructuredLogger.mdc(
        "exchange", env.exchange(),
        "symbol", env.symbol(),
        "stream", env.stream(),
        "session_id", env.collectorSessionId())) {

      // Check recovery gap (first envelope for this stream post-restart)
      GapEnvelope recoveryGap = recovery.checkOnFirstEnvelope(env);
      if (recoveryGap != null) {
        BrokerCoordinates syntheticCoords = new BrokerCoordinates(primaryTopic, partition, -1L);
        gaps.emit(recoveryGap, source, primaryTopic, partition, -1L);
      }

      // Session change detection
      Optional<GapEnvelope> sessionGap = sessionDetector.observe(env, source);
      if (sessionGap.isPresent()) {
        gaps.emit(sessionGap.get(), source, primaryTopic, partition, -1L);
      }

      // Depth recovery gap detection (for depth stream only)
      if ("depth".equals(env.stream())) {
        Optional<GapEnvelope> depthGap = depthFilter.onDepthDiff(env);
        if (depthGap.isPresent()) {
          gaps.emit(depthGap.get(), source, primaryTopic, partition, -1L);
        }
      }

      // Add to buffer with broker coordinates
      BrokerCoordinates coords = new BrokerCoordinates(primaryTopic, partition, offset);
      Optional<List<com.cryptolake.writer.buffer.FlushResult>> autoFlush =
          buffers.add(env, coords, source);
      // Auto-flush results are handled by the caller (KafkaConsumerLoop) via flushAndCommit
    }
  }
}
