package com.cryptopanner.monitor;

/**
 * Outcome of one {@link NodeScraper#poll} attempt against a node. Either a success carrying the
 * parsed {@link StatusSnapshot} + {@link MetricsSnapshot}, or a failure carrying a human-readable
 * reason (timeout, connection refused, non-2xx, parse error). The {@link NodeStateTracker} counts
 * consecutive failures toward the §13.a node-unreachable Critical alert.
 */
public record ScrapeResult(
    boolean ok, StatusSnapshot status, MetricsSnapshot metrics, String error) {

  public static ScrapeResult success(StatusSnapshot status, MetricsSnapshot metrics) {
    return new ScrapeResult(true, status, metrics, null);
  }

  public static ScrapeResult failure(String error) {
    return new ScrapeResult(false, null, null, error);
  }
}
