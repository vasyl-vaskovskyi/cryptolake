package com.cryptolake.collector.snapshot;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.backup.BackupChainReader;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.collector.streams.DepthStreamHandler;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Depth snapshot resync state machine (design §2.6).
 *
 * <p>Phases per {@link #start(String)}:
 *
 * <ol>
 *   <li>Wait up to 60s for producer to be healthy (Tier 5 A3).
 *   <li>Try backup-chain reader first (Tier 1 §6 — replay preferred over reconstruction).
 *   <li>Fall back to REST snapshot fetch.
 *   <li>Apply sync point to {@link DepthStreamHandler}.
 * </ol>
 *
 * <p>Per-symbol serialization via {@link ReentrantLock} — {@link #start(String)} uses {@link
 * ReentrantLock#tryLock()} so a concurrent resync trigger (from reconnect + pu-break) skips if one
 * is already in progress (design §11 Q4 preferred path).
 *
 * <p>Thread safety: per-symbol {@link ReentrantLock} (Tier 5 A5); blocking I/O (REST, Kafka)
 * happens outside the lock.
 */
public final class DepthSnapshotResync {

  private static final StructuredLogger log = StructuredLogger.of(DepthSnapshotResync.class);

  private static final long PRODUCER_HEALTHY_POLL_SECONDS = 2L;
  private static final long PRODUCER_HEALTHY_TIMEOUT_SECONDS = 60L;

  private final String exchange;
  private final CollectorSession session;
  private final SnapshotFetcher snapshotFetcher;
  private final BackupChainReader backupChainReader;
  private volatile DepthStreamHandler depthHandler;
  private final KafkaProducerBridge producer;
  private final GapEmitter gapEmitter;
  private final ClockSupplier clock;
  private final List<String> brokers;
  private final String topicPrefix;

  /** Per-symbol locks — serialize resync attempts (design §3.2). */
  private final ConcurrentHashMap<String, ReentrantLock> perSymbolLocks = new ConcurrentHashMap<>();

  /** Sets the depth stream handler post-construction (breaks circular dep with Main wiring). */
  public void setDepthHandler(DepthStreamHandler handler) {
    this.depthHandler = handler;
  }

  /** Stop latch — set by Main.shutdownAll() to interrupt waiting loops. */
  private final CountDownLatch globalStop;

  public DepthSnapshotResync(
      String exchange,
      CollectorSession session,
      SnapshotFetcher snapshotFetcher,
      BackupChainReader backupChainReader,
      DepthStreamHandler depthHandler,
      KafkaProducerBridge producer,
      GapEmitter gapEmitter,
      ClockSupplier clock,
      List<String> brokers,
      String topicPrefix,
      CountDownLatch globalStop) {
    this.exchange = exchange;
    this.session = session;
    this.snapshotFetcher = snapshotFetcher;
    this.backupChainReader = backupChainReader;
    this.depthHandler = depthHandler;
    this.producer = producer;
    this.gapEmitter = gapEmitter;
    this.clock = clock;
    this.brokers = brokers;
    this.topicPrefix = topicPrefix;
    this.globalStop = globalStop;
  }

  /**
   * Starts a resync for the given symbol. Uses {@code tryLock()} — if a resync is already in
   * progress for this symbol, returns immediately (the in-progress resync covers this trigger).
   *
   * <p>Runs on the calling virtual thread (blocking I/O is fine on VT — Tier 5 A2).
   */
  public void start(String symbol) {
    ReentrantLock lock = perSymbolLocks.computeIfAbsent(symbol, k -> new ReentrantLock());
    if (!lock.tryLock()) {
      log.info("resync_already_in_progress", "symbol", symbol);
      return;
    }
    try {
      doResync(symbol);
    } finally {
      lock.unlock();
    }
  }

  private void doResync(String symbol) {
    log.info("depth_resync_started", "symbol", symbol);
    depthHandler.reset(symbol);

    // Phase 1: wait for producer to be healthy (max 60s)
    if (!waitProducerHealthy(symbol)) {
      return; // emitted pu_chain_break gap inside
    }

    // Phase 2: try backup chain reader (replay preferred — Tier 1 §6)
    Optional<Long> lastU = tryBackupChainReader(symbol);
    if (lastU.isPresent()) {
      depthHandler.setSyncPoint(symbol, lastU.get());
      log.info("depth_resync_via_backup", "symbol", symbol, "last_u", lastU.get());
      return;
    }

    // Phase 3: fetch REST snapshot
    Optional<String> rawText = snapshotFetcher.fetch(symbol);
    if (rawText.isEmpty()) {
      log.warn("depth_resync_snapshot_failed", "symbol", symbol);
      gapEmitter.emit(
          symbol, "depth", -1L, "pu_chain_break", "Snapshot exhausted (3 retries) during resync");
      return;
    }

    // Phase 4: apply sync point
    applySnapshot(symbol, rawText.get());
  }

  private boolean waitProducerHealthy(String symbol) {
    long started = System.nanoTime();
    long timeoutNs = PRODUCER_HEALTHY_TIMEOUT_SECONDS * 1_000_000_000L;
    while (!producer.isHealthyForResync()) {
      if (System.nanoTime() - started > timeoutNs) {
        log.warn("resync_producer_unhealthy_timeout", "symbol", symbol);
        gapEmitter.emit(
            symbol, "depth", -1L, "pu_chain_break", "Resync aborted: producer unhealthy after 60s");
        return false;
      }
      try {
        if (globalStop.await(PRODUCER_HEALTHY_POLL_SECONDS, TimeUnit.SECONDS)) {
          return false; // shutdown
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  private Optional<Long> tryBackupChainReader(String symbol) {
    try {
      String backupTopic = BackupChainReader.otherDepthTopic(topicPrefix, exchange);
      return backupChainReader.readLastDepthUpdateId(
          brokers, backupTopic, symbol, Duration.ofSeconds(30));
    } catch (Exception e) {
      log.warn("backup_chain_reader_failed", "symbol", symbol, "error", e.getMessage());
      return Optional.empty();
    }
  }

  private void applySnapshot(String symbol, String rawText) {
    try {
      long lastUpdateId = snapshotFetcher.adapter().parseSnapshotLastUpdateId(rawText);
      depthHandler.setSyncPoint(symbol, lastUpdateId);

      // Produce the depth_snapshot envelope
      DataEnvelope env =
          DataEnvelope.create(
              exchange,
              symbol,
              "depth_snapshot",
              rawText,
              extractSnapshotTs(rawText),
              session.sessionId(),
              -1L,
              clock);
      producer.produce(env);
      log.info("depth_resync_snapshot_applied", "symbol", symbol, "last_update_id", lastUpdateId);
    } catch (Exception e) {
      log.error("depth_resync_apply_failed", e, "symbol", symbol, "error", e.getMessage());
      gapEmitter.emit(
          symbol, "depth", -1L, "pu_chain_break", "Failed to apply snapshot: " + e.getMessage());
    }
  }

  private long extractSnapshotTs(String rawText) {
    // Extract E field if present, else return 0
    try {
      com.fasterxml.jackson.databind.JsonNode node =
          com.cryptolake.common.envelope.EnvelopeCodec.newMapper().readTree(rawText);
      com.fasterxml.jackson.databind.JsonNode e = node.path("E");
      return e.isMissingNode() ? 0L : e.asLong();
    } catch (Exception e) {
      return 0L;
    }
  }
}
