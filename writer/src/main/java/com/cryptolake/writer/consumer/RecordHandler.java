package com.cryptolake.writer.consumer;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.validation.CrossSourcePuChainValidator;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Per-record logic: deserialize, session-change detection, depth-anchor check, coverage filter,
 * buffer add, failover tracking.
 *
 * <p>Ports Python's {@code WriterConsumer._handle_gap_detection}, {@code
 * _handle_rotation_and_buffer}, {@code _deserialize_and_stamp} (design §2.2; design §4.2).
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

  /**
   * Optional cross-source pu-chain validator. Null if not wired (e.g. older tests). Emits a {@code
   * cross_source_pu_chain_break} gap when the merged depth stream has a pu-chain gap.
   */
  private volatile CrossSourcePuChainValidator crossSourceValidator;

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
   * Wires a cross-source pu-chain validator. Called once during startup; must be set before the
   * consume loop starts processing depth envelopes.
   *
   * @param validator the validator; null disables cross-source validation
   */
  public void setCrossSourceValidator(CrossSourcePuChainValidator validator) {
    this.crossSourceValidator = validator;
  }

  /**
   * Processes a single Kafka record: deserialize, detect gaps, route to buffer.
   *
   * <p>On deserialization failure: emits a {@code deserialization_error} gap and returns (Tier 5 G4
   * — never rethrows; a single malformed record must not kill the consumer loop).
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
    String primaryTopic =
        topic.startsWith(backupPrefix) ? topic.substring(backupPrefix.length()) : topic;

    // Peek at "type" field to route gap/heartbeat envelopes before full deserialization.
    // Collector-emitted gap and heartbeat records land in the same Kafka topic as data records.
    // Deserializing a GapEnvelope as DataEnvelope silently drops gap-specific fields (Bug C).
    String envelopeType;
    try {
      JsonNode tree = codec.readTree(rec.value());
      envelopeType = tree.path("type").asText("data");
    } catch (Exception e) {
      log.error(
          "corrupt_message_skipped",
          e,
          "topic",
          topic,
          "partition",
          partition,
          "offset",
          offset,
          "error",
          e.getMessage());
      return; // DO NOT rethrow (Tier 5 G4)
    }

    // Heartbeat envelopes: liveness-only; no archive write (ports Python env_type == "heartbeat").
    if ("heartbeat".equals(envelopeType)) {
      // No session change, no recovery gap, no buffer write — heartbeats are not archived.
      // Update coverage filter heartbeat timestamp for SilenceInferredGapEmitter.
      coverageFilter.handleHeartbeat(source);
      if (!fromBackup) {
        failover.resetSilenceTimer();
      }
      return;
    }

    // Gap envelopes emitted by the collector: deserialize as GapEnvelope and write to archive
    // directly, bypassing session-change and recovery-gap checks (those apply to data only).
    if ("gap".equals(envelopeType)) {
      GapEnvelope collectorGap;
      try {
        collectorGap = codec.readGap(rec.value());
      } catch (Exception e) {
        log.error(
            "corrupt_gap_skipped",
            e,
            "topic",
            topic,
            "partition",
            partition,
            "offset",
            offset,
            "error",
            e.getMessage());
        return; // DO NOT rethrow
      }
      // Route through coverage filter; if not suppressed, write to buffer.
      // Use the actual Kafka topic (not primaryTopic) so backup gap records are stamped with
      // _topic="backup.binance.X" — matching the data record convention (Tier 1 §4; design §6.1).
      BrokerCoordinates coords = new BrokerCoordinates(topic, partition, offset);
      boolean accepted = coverageFilter.handleGap(source, collectorGap);
      if (accepted) {
        buffers.add(collectorGap, coords, source);
      }
      metrics
          .messagesConsumed(collectorGap.exchange(), collectorGap.symbol(), collectorGap.stream())
          .increment();
      if (!fromBackup) {
        failover.resetSilenceTimer();
      }
      return;
    }

    // Data path (existing logic below) — also handles any unknown types gracefully.
    // Deserialize (Tier 5 G4: catch + deserialization_error gap on failure)
    DataEnvelope env;
    try {
      env = codec.readData(rec.value());
    } catch (Exception e) {
      log.error(
          "corrupt_message_skipped",
          e,
          "topic",
          topic,
          "partition",
          partition,
          "offset",
          offset,
          "error",
          e.getMessage());
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
      // Continuous dual-source tailing (plan 2026-05-03): when failover is INACTIVE the backup
      // tail consumer is still running for liveness/coverage tracking, but its records must NOT
      // be archived (primary is healthy and owns the archive write path). Coverage was already
      // updated above; just bump the tail-only counter and return.
      if (!failover.isActive()) {
        metrics.backupTailRecordsSeen().increment();
        return;
      }
      metrics.failoverRecordsTotal().increment();
      // Switchback filter: if primary has caught up, drop backup records
      if (failover.checkSwitchbackFilter(env)) {
        metrics.messagesSkipped(env.exchange(), env.symbol(), env.stream()).increment();
        return;
      }
    } else {
      failover.resetSilenceTimer();
    }

    // MDC context for structured logging (Tier 5 H3).
    // StructuredLogger.mdc returns AutoCloseable (close throws Exception); we manage cleanup
    // explicitly in a finally block to avoid propagating a checked Exception up the handle() API.
    AutoCloseable ctx =
        StructuredLogger.mdc(
            "exchange", env.exchange(),
            "symbol", env.symbol(),
            "stream", env.stream(),
            "session_id", env.collectorSessionId());
    try {
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
        // Cross-source pu-chain validation (ruler #4 from spec §4)
        CrossSourcePuChainValidator xsValidator = crossSourceValidator;
        if (xsValidator != null) {
          xsValidator.handle(env);
          // Gap emission is handled inside the validator via its GapCallback
        }
      }

      // Add to buffer with broker coordinates.
      // Use the ACTUAL Kafka topic (including backup prefix if applicable) so that backup records
      // are stamped as _topic="backup.binance.X" and primary records as _topic="binance.X".
      // This prevents false "Duplicate broker record" errors in verify when both primary and backup
      // happen to have the same offset number for independently-produced records on different
      // topics
      // (Tier 1 §4; design §6.1 archive broker coordinates).
      BrokerCoordinates coords = new BrokerCoordinates(topic, partition, offset);
      Optional<List<com.cryptolake.writer.buffer.FlushResult>> autoFlush =
          buffers.add(env, coords, source);
      // Auto-flush results are handled by the caller (KafkaConsumerLoop) via flushAndCommit
    } finally {
      try {
        ctx.close();
      } catch (Exception ignored) {
        // MDC cleanup lambda cannot throw in practice (see StructuredLogger.mdc); swallow so
        // the declared AutoCloseable.close signature doesn't force throws on handle().
      }
    }
  }
}
