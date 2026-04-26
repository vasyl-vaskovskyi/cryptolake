package com.cryptolake.collector.streams;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Virtual-thread-based per-symbol open-interest REST poller.
 *
 * <p>Ports {@code OpenInterestPoller} from {@code src/collector/streams/open_interest.py}. One
 * virtual thread per symbol; each thread polls at the configured interval using
 * {@link CountDownLatch#await(long, TimeUnit)} for interruptible waits (Tier 5 A3).
 *
 * <p>HTTP semantics: 429 honors {@code Retry-After}; other errors retry up to 3 times with
 * exp-backoff 1s/2s (Tier 5 D4). On exhaustion, emits {@code snapshot_poll_miss} gap.
 *
 * <p>Thread safety: one virtual thread per symbol; stop coordinated via shared {@link CountDownLatch}.
 */
public final class OpenInterestPoller {

  private static final StructuredLogger log = StructuredLogger.of(OpenInterestPoller.class);

  private static final int MAX_RETRIES = 3;
  private static final long[] BACKOFF_NS = {1_000_000_000L, 2_000_000_000L};
  private static final long DEFAULT_INTERVAL_SECONDS = 60L;

  private final String exchange;
  private final CollectorSession session;
  private final BinanceAdapter adapter;
  private final HttpClient httpClient;
  private final KafkaProducerBridge producer;
  private final GapEmitter gapEmitter;
  private final ClockSupplier clock;
  private final CollectorMetrics metrics;
  private final List<String> symbols;
  private final long intervalSeconds;

  private final CountDownLatch stopLatch = new CountDownLatch(1);
  private final AtomicBoolean running = new AtomicBoolean(false);

  public OpenInterestPoller(
      String exchange,
      CollectorSession session,
      BinanceAdapter adapter,
      HttpClient httpClient,
      KafkaProducerBridge producer,
      GapEmitter gapEmitter,
      ClockSupplier clock,
      CollectorMetrics metrics,
      List<String> symbols,
      long intervalSeconds) {
    this.exchange = exchange;
    this.session = session;
    this.adapter = adapter;
    this.httpClient = httpClient;
    this.producer = producer;
    this.gapEmitter = gapEmitter;
    this.clock = clock;
    this.metrics = metrics;
    this.symbols = symbols;
    this.intervalSeconds = intervalSeconds > 0 ? intervalSeconds : DEFAULT_INTERVAL_SECONDS;
  }

  /** Starts per-symbol poll loops on virtual threads. */
  public void start() {
    running.set(true);
    for (String symbol : symbols) {
      Thread.ofVirtual()
          .name("oi-poller-" + symbol)
          .start(() -> pollLoop(symbol));
    }
  }

  /** Stops all poll loops. */
  public void stop() {
    running.set(false);
    stopLatch.countDown();
  }

  public boolean isRunning() {
    return running.get();
  }

  private void pollLoop(String symbol) {
    log.info("oi_poller_started", "symbol", symbol, "interval_sec", intervalSeconds);
    while (running.get()) {
      pollOnce(symbol);
      try {
        if (stopLatch.await(intervalSeconds, TimeUnit.SECONDS)) break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    log.info("oi_poller_stopped", "symbol", symbol);
  }

  private void pollOnce(String symbol) {
    String url = adapter.buildOpenInterestUrl(symbol);
    HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();

    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        HttpResponse<byte[]> response = httpClient.send(request, BodyHandlers.ofByteArray());

        if (response.statusCode() == 429) {
          long retryAfterSec = response.headers()
              .firstValueAsLong("Retry-After").orElse(5L);
          log.warn("oi_rate_limited", "symbol", symbol, "retry_after_sec", retryAfterSec);
          LockSupport.parkNanos(retryAfterSec * 1_000_000_000L); // (Tier 5 D4; Tier 2 §10)
          attempt--;
          continue;
        }

        if (response.statusCode() >= 400) {
          log.warn("oi_http_error", "symbol", symbol, "attempt", attempt,
              "status", response.statusCode());
          if (attempt < MAX_RETRIES) {
            int backoffIdx = Math.min(attempt - 1, BACKOFF_NS.length - 1);
            LockSupport.parkNanos(BACKOFF_NS[backoffIdx]);
          }
          continue;
        }

        // Success
        String rawText = new String(response.body(), StandardCharsets.UTF_8); // (Tier 5 D5)
        Long exchangeTs = adapter.extractExchangeTs("open_interest", rawText);
        DataEnvelope env = DataEnvelope.create(
            exchange, symbol, "open_interest", rawText,
            exchangeTs != null ? exchangeTs : 0L,
            session.sessionId(), -1L, clock);
        producer.produce(env);
        log.info("oi_produced", "symbol", symbol);
        return;

      } catch (IOException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          return;
        }
        log.warn("oi_fetch_error", "symbol", symbol, "attempt", attempt, "error", e.getMessage());
        if (attempt < MAX_RETRIES) {
          int backoffIdx = Math.min(attempt - 1, BACKOFF_NS.length - 1);
          LockSupport.parkNanos(BACKOFF_NS[backoffIdx]);
        }
      }
    }

    // All retries exhausted
    log.warn("oi_all_retries_failed", "symbol", symbol);
    gapEmitter.emit(symbol, "open_interest", -1L, "snapshot_poll_miss",
        "Open interest fetch failed after " + MAX_RETRIES + " retries");
  }
}
