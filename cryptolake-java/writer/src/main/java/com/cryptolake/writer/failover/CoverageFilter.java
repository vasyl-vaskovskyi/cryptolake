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
   * Per-source last-data timestamp (ns) for coverage detection. Key: "primary" or "backup"; value:
   * nanosecond timestamp.
   */
  private final Map<String, Long> lastDataTs = new HashMap<>();

  /** Pending gaps waiting for grace period: key is gapKey, value is pending entry. */
  private final Map<String, PendingGap> pendingGaps = new HashMap<>();

  private record PendingGap(GapEnvelope gap, String source, long firstSeenNs) {}

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
    lastDataTs.put(source, clock.nowNs());
    if (lastDataTs.size() >= 2) {
      filterEnabled = true;
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
    Long otherLastTs = lastDataTs.get(otherSource);
    boolean otherCovers =
        otherLastTs != null
            && (clock.nowNs() - otherLastTs) < (long) (gracePeriodSeconds * 1_000_000_000L);

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
      return false;
    }

    // No coverage — accept immediately
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
        // Check if the other source covered during the grace period
        String otherSource = "primary".equals(pg.source()) ? "backup" : "primary";
        Long otherLastTs = lastDataTs.get(otherSource);
        boolean nowCovered = otherLastTs != null && (nowNs - otherLastTs) < thresholdNs;
        if (nowCovered) {
          // Suppress: other source covered
          metrics.gapEnvelopesSuppressed(pg.source(), "covered").increment();
          log.debug("gap_suppressed_by_coverage", "source", pg.source());
        } else {
          // Archive: grace expired, still no coverage
          toArchive.add(pg.gap());
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
