package com.cryptolake.collector.snapshot;

import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.common.logging.StructuredLogger;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;

/**
 * One-shot REST fetcher for Binance depth snapshots ({@code GET /fapi/v1/depth}).
 *
 * <p>Ports {@code SnapshotScheduler.fetch_snapshot} from {@code src/collector/snapshot.py}. Returns
 * the raw JSON body as a {@code String} on success; {@link Optional#empty()} after 3 failed
 * attempts.
 *
 * <p>HTTP semantics (Tier 5 D4, D5):
 *
 * <ul>
 *   <li>429: read {@code Retry-After} header, sleep that long, retry.
 *   <li>Other non-2xx: log, retry up to 3 times with 1s/2s exp-backoff.
 *   <li>Bytes decoded as UTF-8 explicitly ({@link BodyHandlers#ofByteArray()} — Tier 5 D5).
 * </ul>
 *
 * <p>Thread safety: {@link HttpClient} is thread-safe; method is pure. May be called from any
 * virtual thread (Tier 5 A2 — blocking is free on VT).
 */
public final class SnapshotFetcher {

  private static final StructuredLogger log = StructuredLogger.of(SnapshotFetcher.class);

  private static final int MAX_RETRIES = 3;
  private static final long[] BACKOFF_NS = {1_000_000_000L, 2_000_000_000L};
  private static final int SNAPSHOT_LIMIT = 1000;

  private final HttpClient httpClient;
  private final BinanceAdapter adapter;
  private final String exchange;
  private final CollectorMetrics metrics;

  public SnapshotFetcher(
      HttpClient httpClient, BinanceAdapter adapter, String exchange, CollectorMetrics metrics) {
    this.httpClient = httpClient;
    this.adapter = adapter;
    this.exchange = exchange;
    this.metrics = metrics;
  }

  /** Exposes the adapter for callers that need to parse the returned raw text. */
  public BinanceAdapter adapter() {
    return adapter;
  }

  /**
   * Fetches a depth snapshot for {@code symbol}. Returns the raw JSON body on success or {@link
   * Optional#empty()} after {@link #MAX_RETRIES} failures.
   */
  public Optional<String> fetch(String symbol) {
    String url = adapter.buildSnapshotUrl(symbol, SNAPSHOT_LIMIT);
    HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();

    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        HttpResponse<byte[]> response = httpClient.send(request, BodyHandlers.ofByteArray());

        if (response.statusCode() == 429) {
          long retryAfterSec = response.headers().firstValueAsLong("Retry-After").orElse(5L);
          log.warn("snapshot_rate_limited", "symbol", symbol, "retry_after_sec", retryAfterSec);
          LockSupport.parkNanos(
              retryAfterSec * 1_000_000_000L); // (Tier 5 D4; not Thread.sleep — Tier 2 §10)
          // Don't count as a retry attempt
          attempt--;
          continue;
        }

        if (response.statusCode() >= 400) {
          log.warn(
              "snapshot_http_error",
              "symbol",
              symbol,
              "attempt",
              attempt,
              "status",
              response.statusCode());
          if (attempt < MAX_RETRIES) {
            int backoffIdx = Math.min(attempt - 1, BACKOFF_NS.length - 1);
            LockSupport.parkNanos(BACKOFF_NS[backoffIdx]); // (Tier 2 §10)
          }
          continue;
        }

        // Success
        String rawText = new String(response.body(), StandardCharsets.UTF_8); // (Tier 5 D5)
        metrics.snapshotsTaken(exchange, symbol).increment();
        log.info("snapshot_fetched", "symbol", symbol);
        return Optional.of(rawText);

      } catch (IOException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          return Optional.empty();
        }
        log.warn(
            "snapshot_fetch_error", "symbol", symbol, "attempt", attempt, "error", e.getMessage());
        if (attempt < MAX_RETRIES) {
          int backoffIdx = Math.min(attempt - 1, BACKOFF_NS.length - 1);
          LockSupport.parkNanos(BACKOFF_NS[backoffIdx]); // (Tier 2 §10)
        }
      }
    }

    metrics.snapshotsFailed(exchange, symbol).increment();
    log.warn("snapshot_all_retries_failed", "symbol", symbol);
    return Optional.empty();
  }
}
