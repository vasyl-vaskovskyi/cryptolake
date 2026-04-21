package com.cryptolake.writer.consumer;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.failover.HostLifecycleEvidence;
import com.cryptolake.writer.failover.RestartGapClassifier;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.recovery.LastEnvelopeReader;
import com.cryptolake.writer.recovery.SealedFileIndex;
import com.cryptolake.writer.state.MaintenanceIntent;
import com.cryptolake.writer.state.StateManager;
import com.cryptolake.writer.state.StreamCheckpoint;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time startup recovery coordinator. Computes pending Kafka seeks and loads durable
 * checkpoints. Also handles the first-envelope restart-gap injection per stream.
 *
 * <p>Ports Python's {@code WriterConsumer._check_recovery_gap} and startup-recovery logic (design
 * §2.2; design §4.2; Tier 1 §6 — replay over reconstruction).
 *
 * <p>Thread safety: construction-time heavy; {@link #checkOnFirstEnvelope(DataEnvelope)} called
 * from consume-loop thread only. Dicts owned by this class, never shared.
 */
public final class RecoveryCoordinator {

  private static final Logger log = LoggerFactory.getLogger(RecoveryCoordinator.class);

  private final StateManager stateManager;
  private final SealedFileIndex sealedIndex;
  private final LastEnvelopeReader envelopeReader;
  private final HostLifecycleEvidence hostEvidence;
  private final RestartGapClassifier classifier; // unused directly — static utility, passed for testability
  private final GapEmitter gaps;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;
  private final String currentBootId;
  private final String currentInstanceId;

  /**
   * Streams for which recovery is complete (seek applied + gap optionally emitted).
   */
  private final Set<StreamKey> recoveryDone = new HashSet<>();

  /**
   * Streams that have had their restart gap emitted.
   */
  private final Set<StreamKey> recoveryGapEmitted = new HashSet<>();

  /**
   * Pending Kafka seeks: populated during {@link #runOnStartup()}, drained by
   * {@link PartitionAssignmentListener}.
   */
  private final Map<TopicPartition, Long> pendingSeeks = new HashMap<>();

  /**
   * Durable checkpoints loaded from PG (live view — updated by OffsetCommitCoordinator).
   */
  private final Map<StreamKey, StreamCheckpoint> durableCheckpoints = new HashMap<>();

  /**
   * Recovery high-water offsets per (topic, partition, filePath): tracks which offset we've
   * replayed through (Tier 1 §6 — skip records already in archive).
   */
  private final Map<RecoveryKey, Long> recoveryHighWater = new HashMap<>();

  public RecoveryCoordinator(
      StateManager stateManager,
      SealedFileIndex sealedIndex,
      LastEnvelopeReader envelopeReader,
      HostLifecycleEvidence hostEvidence,
      RestartGapClassifier ignoredClassifier, // static utility; param preserved for testability
      GapEmitter gaps,
      WriterMetrics metrics,
      ClockSupplier clock,
      String currentBootId,
      String currentInstanceId) {
    this.stateManager = stateManager;
    this.sealedIndex = sealedIndex;
    this.envelopeReader = envelopeReader;
    this.hostEvidence = hostEvidence;
    this.classifier = ignoredClassifier;
    this.gaps = gaps;
    this.metrics = metrics;
    this.clock = clock;
    this.currentBootId = currentBootId;
    this.currentInstanceId = currentInstanceId;
  }

  /**
   * Runs once at startup. Loads PG state, computes pending seeks, populates durable checkpoints.
   *
   * <p>Ports Python's {@code WriterConsumer.start()} recovery section and
   * {@code _check_recovery_gap} on startup.
   *
   * @return {@link RecoveryResult} with pending seeks and durable checkpoints
   */
  public RecoveryResult runOnStartup() {
    // Load all file states indexed by (topic, partition)
    Map<TopicPartition, List<com.cryptolake.writer.state.FileStateRecord>> pgFileStates =
        stateManager.loadAllFileStates();

    // Load stream checkpoints
    Map<StreamKey, StreamCheckpoint> loadedCheckpoints = stateManager.loadStreamCheckpoints();
    durableCheckpoints.putAll(loadedCheckpoints);

    // Scan and reconcile files (truncate oversized, delete uncommitted — Tier 5 I3, I7)
    SealedFileIndex.ScanResult scanResult = sealedIndex.scanAndReconcile(pgFileStates);
    log.info("startup_recovery_scan",
        "sealed", scanResult.sealed().size(),
        "deleted_uncommitted", scanResult.deletedUncommitted().size(),
        "truncated", scanResult.truncated().size());

    // Compute pending seeks from checkpoints (Tier 1 §6 — replay over reconstruction)
    for (Map.Entry<TopicPartition, List<com.cryptolake.writer.state.FileStateRecord>> entry :
        pgFileStates.entrySet()) {
      TopicPartition tp = entry.getKey();
      long maxHighWater = entry.getValue().stream()
          .mapToLong(com.cryptolake.writer.state.FileStateRecord::highWaterOffset)
          .max()
          .orElse(-1L);
      if (maxHighWater >= 0) {
        long seekTo = maxHighWater + 1;
        pendingSeeks.put(tp, seekTo);
        log.info("recovery_seek_planned",
            "topic", tp.topic(),
            "partition", tp.partition(),
            "seek_to", seekTo);
      }
    }

    // Load maintenance intent for restart gap classification
    Optional<MaintenanceIntent> intent = stateManager.loadActiveMaintenanceIntent();

    log.info("recovery_state_loaded",
        "checkpoints", durableCheckpoints.size(),
        "pending_seeks", pendingSeeks.size(),
        "current_boot_id", currentBootId,
        "current_instance_id", currentInstanceId);

    return new RecoveryResult(Map.copyOf(pendingSeeks), Map.copyOf(durableCheckpoints));
  }

  /**
   * Called by {@link RecordHandler} on the first envelope for a stream after startup. Checks
   * whether a restart gap should be emitted based on the loaded checkpoint.
   *
   * <p>Ports Python's {@code WriterConsumer._check_recovery_gap}.
   *
   * @param env the first incoming envelope for this stream
   * @return the restart gap envelope if one should be emitted, otherwise null
   */
  public GapEnvelope checkOnFirstEnvelope(DataEnvelope env) {
    StreamKey key = new StreamKey(env.exchange(), env.symbol(), env.stream());

    if (recoveryDone.contains(key)) {
      return null; // Already processed for this stream
    }
    recoveryDone.add(key);

    StreamCheckpoint checkpoint = durableCheckpoints.get(key);
    if (checkpoint == null) {
      // No checkpoint for this stream — this is a fresh start; no gap to emit
      log.info("recovery_no_checkpoint",
          "exchange", env.exchange(), "symbol", env.symbol(), "stream", env.stream());
      return null;
    }

    // We have a checkpoint: compute restart gap
    if (recoveryGapEmitted.contains(key)) {
      return null;
    }
    recoveryGapEmitted.add(key);

    // Use RestartGapClassifier (static utility) to classify the restart
    Map<String, com.cryptolake.writer.state.ComponentRuntimeState> compStates =
        stateManager.loadLatestComponentStates();
    Optional<MaintenanceIntent> intent = stateManager.loadActiveMaintenanceIntent();

    com.cryptolake.writer.state.ComponentRuntimeState collectorState = compStates.get("collector");
    com.cryptolake.writer.state.ComponentRuntimeState writerState = compStates.get("writer");

    boolean collectorCleanShutdown = collectorState != null && collectorState.cleanShutdownAt() != null;
    boolean systemCleanShutdown = writerState != null && writerState.cleanShutdownAt() != null;
    String previousBootId = writerState != null ? writerState.hostBootId() : null;
    String previousSessionId = checkpoint.lastCollectorSessionId();

    RestartGapClassifier.Classification classification = RestartGapClassifier.classify(
        previousBootId,
        currentBootId,
        previousSessionId,
        env.collectorSessionId(),
        collectorCleanShutdown,
        systemCleanShutdown,
        intent.orElse(null),
        hostEvidence,
        clock);

    // Build evidence for GapEnvelope — Q1 preferred: evidence is List<String> passed as
    // Map<String,Object> with key "evidence_list" (design §11 Q1 preferred, using Object field)
    // The GapEnvelope.evidence field is Map<String,Object>; we adapt by using a map wrapper
    Map<String, Object> evidenceMap = new HashMap<>();
    evidenceMap.put("evidence_list", classification.evidence()); // Q1 adaptation

    // Parse the checkpoint's last_received_at for gap_start_ts
    long gapStartTs;
    try {
      gapStartTs = java.time.OffsetDateTime.parse(checkpoint.lastReceivedAt()).toInstant()
          .toEpochMilli() * 1_000_000L; // Convert ms to ns for gap_start_ts
    } catch (Exception e) {
      gapStartTs = clock.nowNs() - 1_000_000_000L; // 1 second ago as fallback
    }

    GapEnvelope gapEnv = GapEnvelope.createWithRestartMetadata(
        env.exchange(),
        env.symbol(),
        env.stream(),
        env.collectorSessionId(),
        -1L, // writer-injected (Tier 5 M10)
        gapStartTs,
        env.receivedAt(),
        "restart_gap",
        "writer restart detected",
        clock,
        classification.component(),
        classification.cause(),
        classification.planned(),
        classification.classifier(),
        evidenceMap,
        classification.maintenanceId());

    metrics.sessionGapsDetected(env.exchange(), env.symbol(), env.stream()).increment();
    log.info("recovery_gap_emitted",
        "exchange", env.exchange(),
        "symbol", env.symbol(),
        "stream", env.stream(),
        "component", classification.component(),
        "cause", classification.cause(),
        "planned", classification.planned());

    return gapEnv;
  }

  /** Returns an immutable view of pending seeks (for PartitionAssignmentListener). */
  public Map<TopicPartition, Long> pendingSeeks() {
    return Map.copyOf(pendingSeeks);
  }

  /** Returns a live view of durable checkpoints (updated by OffsetCommitCoordinator). */
  public Map<StreamKey, StreamCheckpoint> durableCheckpoints() {
    return durableCheckpoints;
  }

  /** Clears a pending seek after it has been applied (called by PartitionAssignmentListener). */
  public void clearPendingSeek(TopicPartition tp) {
    pendingSeeks.remove(tp);
  }
}
