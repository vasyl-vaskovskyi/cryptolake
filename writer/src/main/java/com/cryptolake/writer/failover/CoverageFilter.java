package com.cryptolake.writer.failover;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.metrics.WriterMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-source coverage map with pending-gap parking and grace period.
 *
 * <p>Ports Python's {@code CoverageFilter} (design §2.7; design §4.6). When a gap arrives on one
 * source but the other source is still receiving data (coverage), the gap is parked for a grace
 * period before deciding whether to archive or suppress it.
 *
 * <p>Coalescing: when an incoming gap shares {@code gap_start_ts} with a pending entry, the pending
 * record is REPLACED with a new {@code GapEnvelope} whose {@code gap_end_ts} is the {@code max()} —
 * records are immutable so a new record is constructed (Tier 2 §12).
 *
 * <p>Thread safety: consume-loop thread only (T1). No synchronization (Tier 5 A5).
 */
public final class CoverageFilter {

  private static final Logger log = LoggerFactory.getLogger(CoverageFilter.class);

  private final double gracePeriodSeconds;
  private final double snapshotMissGraceSeconds;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /** Whether coverage filtering is enabled (requires at least 2 sources). */
  private boolean filterEnabled = false;

  /**
   * Aggregate per-source last-data timestamp (ns). Tracks "did this source deliver ANY data
   * recently" — used by {@link com.cryptolake.writer.validation.SilenceInferredGapEmitter} to
   * detect both-collectors-silent globally. Key: "primary" or "backup"; value: ns timestamp.
   */
  private final Map<String, Long> lastDataTs = new HashMap<>();

  /**
   * Per-(stream, source) last-data timestamp (ns). The coverage decision for a gap on stream X
   * from source S asks: "did the OTHER source deliver data on stream X within the grace
   * window?". Without this stream-scoped granularity, sparse streams (e.g. {@code open_interest}
   * polled once a minute) would falsely look "uncovered" just because the other source happens
   * to have last published a different stream's record.
   */
  private final Map<StreamSourceKey, Long> lastDataTsByStream = new HashMap<>();

  /**
   * Per-source last-heartbeat timestamp (ns). Key: "primary" or "backup"; value: nanosecond
   * timestamp. Updated by {@link #handleHeartbeat(String)}.
   */
  private final Map<String, Long> lastHeartbeatTs = new HashMap<>();

  /** Pending gaps waiting for grace period: key is gapKey, value is pending entry. */
  private final Map<String, PendingGap> pendingGaps = new HashMap<>();

  private record PendingGap(GapEnvelope gap, String source, long firstSeenNs) {}

  /** Composite key for per-(stream, source) last-data tracking. */
  private record StreamSourceKey(String exchange, String symbol, String stream, String source) {}

  public CoverageFilter(
      double gracePeriodSeconds,
      double snapshotMissGraceSeconds,
      WriterMetrics metrics,
      ClockSupplier clock) {
    this.gracePeriodSeconds = gracePeriodSeconds;
    this.snapshotMissGraceSeconds = snapshotMissGraceSeconds;
    this.metrics = metrics;
    this.clock = clock;
  }

  // ── Public API ───────────────────────────────────────────────────────────────────────────────

  /**
   * Records that a data envelope was received from the given source. Activates filter when both
   * sources have been seen.
   *
   * <p>Ports {@code CoverageFilter.handle_data(source, env)}.
   */
  public void handleData(String source, DataEnvelope env) {
    long now = clock.nowNs();
    lastDataTs.put(source, now);
    lastDataTsByStream.put(
        new StreamSourceKey(env.exchange(), env.symbol(), env.stream(), source), now);
    if (!filterEnabled && lastDataTs.size() >= 2) {
      filterEnabled = true;
      log.info(
          "LIFECYCLE COVERAGE_FILTER_ACTIVATED: Both main and backup collectors have"
              + " delivered data — redundancy is now active and spurious gaps will be"
              + " suppressed per-stream when one source covers for the other.");
    }
  }

  /**
   * Handles a gap from a source. If the other source covers the gap, parks it for the grace period.
   * Returns {@code true} if the gap should be archived now; {@code false} if parked or suppressed.
   *
   * <p>Ports {@code CoverageFilter.handle_gap(source, gap)}.
   */
  public boolean handleGap(String source, GapEnvelope gap) {
    if (!filterEnabled) {
      return true; // No coverage filter when only one source seen
    }

    String otherSource = "primary".equals(source) ? "backup" : "primary";
    StreamSourceKey otherStreamKey =
        new StreamSourceKey(gap.exchange(), gap.symbol(), gap.stream(), otherSource);
    Long otherLastTs = lastDataTsByStream.get(otherStreamKey);
    long thresholdNs = (long) (gracePeriodSeconds * 1_000_000_000L);
    boolean otherCovers =
        otherLastTs != null && (clock.nowNs() - otherLastTs) < thresholdNs;

    String gapKey =
        gap.exchange() + "|" + gap.symbol() + "|" + gap.stream() + "|" + gap.gapStartTs();

    PendingGap existing = pendingGaps.get(gapKey);
    if (existing != null) {
      // Coalesce: replace with new envelope having max gap_end_ts (Tier 2 §12 — records immutable)
      if (gap.gapEndTs() > existing.gap().gapEndTs()) {
        GapEnvelope coalesced = coalesceGap(existing.gap(), gap);
        pendingGaps.put(
            gapKey, new PendingGap(coalesced, existing.source(), existing.firstSeenNs()));
        metrics.gapCoalesced(source).increment();
        log.debug("gap_coalesced", "key", gapKey, "source", source);
      }
      return false; // Still parked
    }

    if (otherCovers) {
      // Park for grace period
      pendingGaps.put(gapKey, new PendingGap(gap, source, clock.nowNs()));
      metrics.setGapPendingSize(pendingGaps.size());
      log.debug("gap_parked", "key", gapKey, "source", source);
      log.info(
          "LIFECYCLE GAP_PARKED: Possible gap detected on {} {} from {} (reason={}); the {}"
              + " collector delivered data on this same stream within the {}s grace window,"
              + " so this gap is held for {}s before deciding (will be discarded if {} keeps"
              + " delivering, archived otherwise).",
          gap.symbol(),
          gap.stream(),
          source,
          gap.reason(),
          otherSource,
          gracePeriodSeconds,
          gracePeriodSeconds,
          otherSource);
      return false;
    }

    // No coverage on THIS stream from the other source — archive immediately.
    log.info(
        "LIFECYCLE GAP_ACCEPTED_NO_COVERAGE: Gap detected on {} {} from {} (reason={}); the"
            + " {} collector has NOT delivered data on this stream within the {}s grace"
            + " window, so this is treated as real data loss and recorded immediately."
            + " (Note: this only checks stream {}; data on other streams may still be"
            + " flowing on {}.)",
        gap.symbol(),
        gap.stream(),
        source,
        gap.reason(),
        otherSource,
        gracePeriodSeconds,
        gap.stream(),
        otherSource);
    return true;
  }

  /**
   * Sweeps pending gaps whose grace period has expired and returns those that should now be
   * archived (the other source did not cover during the grace period).
   *
   * <p>Ports {@code CoverageFilter.sweep_expired()}.
   */
  public List<GapEnvelope> sweepExpired() {
    List<GapEnvelope> toArchive = new ArrayList<>();
    long nowNs = clock.nowNs();
    Iterator<Map.Entry<String, PendingGap>> it = pendingGaps.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, PendingGap> entry = it.next();
      PendingGap pg = entry.getValue();
      long ageNs = nowNs - pg.firstSeenNs();
      long thresholdNs = (long) (gracePeriodSeconds * 1_000_000_000L);
      if (ageNs >= thresholdNs) {
        // Check per-stream coverage from the other source
        GapEnvelope g = pg.gap();
        String otherSource = "primary".equals(pg.source()) ? "backup" : "primary";
        StreamSourceKey otherStreamKey =
            new StreamSourceKey(g.exchange(), g.symbol(), g.stream(), otherSource);
        Long otherLastTs = lastDataTsByStream.get(otherStreamKey);
        boolean nowCovered = otherLastTs != null && (nowNs - otherLastTs) < thresholdNs;
        if (nowCovered) {
          // Suppress: other source kept delivering on this same stream
          metrics.gapEnvelopesSuppressed(pg.source(), "covered").increment();
          log.debug("gap_suppressed_by_coverage", "source", pg.source());
          log.info(
              "LIFECYCLE GAP_SUPPRESSED_BY_COVERAGE: The {} collector kept delivering data"
                  + " on {} {} throughout the {}s grace period — no real data loss"
                  + " occurred, so the parked gap from {} is discarded (TWO-COLLECTOR rule"
                  + " worked). reason={}",
              otherSource,
              g.symbol(),
              g.stream(),
              gracePeriodSeconds,
              pg.source(),
              g.reason());
        } else {
          // Archive: grace expired, the other source did not cover THIS stream
          toArchive.add(pg.gap());
          log.info(
              "LIFECYCLE GAP_ARCHIVED: The {}s grace period expired and the {} collector"
                  + " never delivered data on {} {} during it — confirmed real data loss"
                  + " on this stream; writing gap envelope to archive. source={} reason={}",
              gracePeriodSeconds,
              otherSource,
              g.symbol(),
              g.stream(),
              pg.source(),
              g.reason());
        }
        it.remove();
      }
    }
    metrics.setGapPendingSize(pendingGaps.size());
    return toArchive;
  }

  /**
   * Flushes all pending gaps for archival (called on shutdown — design §3.4).
   *
   * <p>Ports {@code CoverageFilter.flush_all_pending()}.
   */
  public List<GapEnvelope> flushAllPending() {
    List<GapEnvelope> all = new ArrayList<>();
    for (PendingGap pg : pendingGaps.values()) {
      all.add(pg.gap());
    }
    pendingGaps.clear();
    metrics.setGapPendingSize(0);
    return all;
  }

  /** Returns the current number of pending gap envelopes. */
  public int pendingSize() {
    return pendingGaps.size();
  }

  /** Returns whether coverage filtering is enabled (both sources seen). */
  public boolean enabled() {
    return filterEnabled;
  }

  /**
   * Records that a heartbeat was received from the given source.
   *
   * <p>Called by {@code RecordHandler} when a {@code heartbeat} envelope arrives. Used by {@code
   * SilenceInferredGapEmitter} to distinguish "collector is alive but no data" from "both
   * collectors silent".
   *
   * @param source "primary" or "backup"
   */
  public void handleHeartbeat(String source) {
    lastHeartbeatTs.put(source, clock.nowNs());
  }

  /**
   * Returns the last-data timestamp (ns) for the given source, or 0 if never seen.
   *
   * @param source "primary" or "backup"
   */
  public long getLastDataTs(String source) {
    return lastDataTs.getOrDefault(source, 0L);
  }

  /**
   * Returns the last-heartbeat timestamp (ns) for the given source, or 0 if never seen.
   *
   * @param source "primary" or "backup"
   */
  public long getLastHeartbeatTs(String source) {
    return lastHeartbeatTs.getOrDefault(source, 0L);
  }

  // ── Private helpers ──────────────────────────────────────────────────────────────────────────

  /**
   * Creates a coalesced {@link GapEnvelope} with the maximum {@code gap_end_ts} of the two
   * envelopes. Records are immutable; a new record is constructed (Tier 2 §12).
   */
  private static GapEnvelope coalesceGap(GapEnvelope existing, GapEnvelope incoming) {
    long mergedEndTs = Math.max(existing.gapEndTs(), incoming.gapEndTs());
    return new GapEnvelope(
        existing.v(),
        existing.type(),
        existing.exchange(),
        existing.symbol(),
        existing.stream(),
        existing.receivedAt(),
        existing.collectorSessionId(),
        existing.sessionSeq(),
        existing.gapStartTs(),
        mergedEndTs,
        existing.reason(),
        existing.detail(),
        existing.component(),
        existing.cause(),
        existing.planned(),
        existing.classifier(),
        existing.evidence(),
        existing.maintenanceId());
  }
}
