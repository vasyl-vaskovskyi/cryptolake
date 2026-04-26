package com.cryptolake.collector.streams;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.adapter.DepthUpdateIds;
import com.cryptolake.collector.gap.DepthGapDetector;
import com.cryptolake.collector.gap.DiffValidationResult;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.gap.SeqGap;
import com.cryptolake.collector.gap.SessionSeqTracker;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Handles the {@code depth} stream: diff buffering, pu-chain validation, and resync triggering.
 *
 * <p>Ports {@code DepthHandler} from {@code src/collector/streams/depth.py}.
 *
 * <p>State per symbol:
 *
 * <ul>
 *   <li>{@code pendingDiffs} — buffered when not yet synced (up to {@link #MAX_PENDING_DIFFS}).
 *   <li>{@code detectors} — per-symbol pu-chain validator.
 *   <li>{@code trackers} — per-symbol session-seq tracker.
 *   <li>{@code dropCounters} — drop count for deferred gap emission.
 * </ul>
 *
 * <p>Thread safety: confined to the single virtual thread that drives the WebSocket listener.
 * {@link #setSyncPoint(String, long)} may be called from the resync virtual thread — the caller
 * must ensure it is not called concurrently with {@link #handle(String, String, Long, long)} for
 * the same symbol (the per-symbol ReentrantLock in {@code DepthSnapshotResync} enforces this).
 */
public final class DepthStreamHandler implements StreamHandler {

  private static final StructuredLogger log = StructuredLogger.of(DepthStreamHandler.class);

  /** Maximum pending diffs buffered while awaiting a sync point (design §2.5). */
  public static final int MAX_PENDING_DIFFS = 5000;

  private final String exchange;
  private final CollectorSession session;
  private final BinanceAdapter adapter;
  private final KafkaProducerBridge producer;
  private final GapEmitter gapEmitter;
  private final ClockSupplier clock;
  private final Consumer<String> onPuChainBreak;

  private final Map<String, DepthGapDetector> detectors = new HashMap<>();
  private final Map<String, SessionSeqTracker> trackers = new HashMap<>();
  private final Map<String, List<PendingDiff>> pendingDiffs = new HashMap<>();
  private final Map<String, Integer> dropCounters = new HashMap<>();
  private final Map<String, Long> firstDropTsNs = new HashMap<>();

  /**
   * @param onPuChainBreak callback invoked when a pu-chain break is detected; receives the symbol;
   *     should trigger {@code DepthSnapshotResync.start(symbol)} on a new virtual thread
   */
  public DepthStreamHandler(
      String exchange,
      CollectorSession session,
      BinanceAdapter adapter,
      KafkaProducerBridge producer,
      GapEmitter gapEmitter,
      ClockSupplier clock,
      Consumer<String> onPuChainBreak) {
    this.exchange = exchange;
    this.session = session;
    this.adapter = adapter;
    this.producer = producer;
    this.gapEmitter = gapEmitter;
    this.clock = clock;
    this.onPuChainBreak = onPuChainBreak;
  }

  @Override
  public void handle(String symbol, String rawText, Long exchangeTs, long sessionSeq) {
    DepthGapDetector detector = detectors.computeIfAbsent(symbol, k -> new DepthGapDetector());
    SessionSeqTracker tracker = trackers.computeIfAbsent(symbol, k -> new SessionSeqTracker());
    List<PendingDiff> pending = pendingDiffs.computeIfAbsent(symbol, k -> new ArrayList<>());

    // Check session seq
    Optional<SeqGap> seqGap = tracker.check(sessionSeq);
    if (seqGap.isPresent()) {
      SeqGap g = seqGap.get();
      gapEmitter.emit(
          symbol,
          "depth",
          sessionSeq,
          "session_seq_skip",
          "Expected seq " + g.expected() + " got " + g.actual());
    }

    // Parse update IDs
    DepthUpdateIds ids;
    try {
      ids = adapter.parseDepthUpdateIds(rawText);
    } catch (Exception e) {
      log.warn("depth_parse_failed", "symbol", symbol, "error", e.getMessage());
      return;
    }

    if (!detector.isSynced()) {
      // Buffer pending diffs until we have a sync point
      if (pending.size() >= MAX_PENDING_DIFFS) {
        recordDrop(symbol, sessionSeq);
        return;
      }
      pending.add(new PendingDiff(rawText, exchangeTs, sessionSeq));
      return;
    }

    DiffValidationResult result = detector.validateDiff(ids.U(), ids.u(), ids.pu());

    if (result.isStale()) {
      // Stale diff: discard silently (before sync point)
      return;
    }

    if (!result.valid()) {
      if (result.isGap()) {
        // pu-chain break: emit gap and trigger resync
        log.info("pu_chain_break", "symbol", symbol, "reason", result.reason());
        gapEmitter.emit(symbol, "depth", sessionSeq, "pu_chain_break", result.reason());
        detector.reset();
        onPuChainBreak.accept(symbol);
      } else {
        // Not synced / no sync diff found: buffer
        if (pending.size() >= MAX_PENDING_DIFFS) {
          recordDrop(symbol, sessionSeq);
          return;
        }
        pending.add(new PendingDiff(rawText, exchangeTs, sessionSeq));
      }
      return;
    }

    // Valid diff: produce
    DataEnvelope env =
        DataEnvelope.create(
            exchange,
            symbol,
            "depth",
            rawText,
            exchangeTs != null ? exchangeTs : 0L,
            session.sessionId(),
            sessionSeq,
            clock);
    producer.produce(env);
  }

  /**
   * Sets the depth sync point from a snapshot's {@code lastUpdateId}. Replays any buffered pending
   * diffs — diffs that pass validation are produced; the first pu-chain break in the replay emits a
   * gap and triggers another resync.
   *
   * <p>This is the replay-over-reconstruction path (Tier 1 §6).
   */
  public void setSyncPoint(String symbol, long lastUpdateId) {
    DepthGapDetector detector = detectors.computeIfAbsent(symbol, k -> new DepthGapDetector());
    detector.setSyncPoint(lastUpdateId);

    // Emit deferred drop gap if drops accumulated while we were buffering
    Integer drops = dropCounters.remove(symbol);
    Long firstDrop = firstDropTsNs.remove(symbol);
    if (drops != null && drops > 0 && firstDrop != null) {
      gapEmitter.emitWithTimestamps(
          symbol,
          "depth",
          -1L,
          "pu_chain_break",
          "Dropped " + drops + " diffs while buffering (buffer full)",
          firstDrop,
          clock.nowNs());
    }

    // Replay buffered diffs
    List<PendingDiff> pending = pendingDiffs.remove(symbol);
    if (pending == null) return;

    for (PendingDiff diff : pending) {
      DepthUpdateIds ids;
      try {
        ids = adapter.parseDepthUpdateIds(diff.rawText());
      } catch (Exception e) {
        log.warn("depth_replay_parse_failed", "symbol", symbol, "error", e.getMessage());
        continue;
      }

      DiffValidationResult result = detector.validateDiff(ids.U(), ids.u(), ids.pu());
      if (result.isStale()) continue;
      if (!result.valid()) {
        if (result.isGap()) {
          // pu-chain break during replay — emit gap and trigger resync
          gapEmitter.emit(
              symbol,
              "depth",
              diff.sessionSeq(),
              "pu_chain_break",
              "pu chain break during snapshot replay: " + result.reason());
          detector.reset();
          onPuChainBreak.accept(symbol);
          return; // stop replay
        }
        continue;
      }

      DataEnvelope env =
          DataEnvelope.create(
              exchange,
              symbol,
              "depth",
              diff.rawText(),
              diff.exchangeTs() != null ? diff.exchangeTs() : 0L,
              session.sessionId(),
              diff.sessionSeq(),
              clock);
      producer.produce(env);
    }
  }

  /** Resets the detector state for a symbol (e.g. on reconnect). */
  public void reset(String symbol) {
    DepthGapDetector detector = detectors.get(symbol);
    if (detector != null) detector.reset();
    pendingDiffs.remove(symbol);
    dropCounters.remove(symbol);
    firstDropTsNs.remove(symbol);
  }

  private void recordDrop(String symbol, long sessionSeq) {
    dropCounters.merge(symbol, 1, Integer::sum);
    firstDropTsNs.putIfAbsent(symbol, clock.nowNs());
    log.info("depth_diff_dropped", "symbol", symbol, "pending_full", MAX_PENDING_DIFFS);
    // Trigger resync since we're losing data
    onPuChainBreak.accept(symbol);
  }
}
