package com.cryptolake.backfill;

import com.cryptolake.common.health.HealthServer;
import com.cryptolake.verify.gaps.BinanceRestClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long-running backfill scheduler.
 *
 * <p>Ports {@code main} from {@code backfill_scheduler.py}. Runs a backfill cycle every 6 hours
 * (configurable). Serves a {@code /ready} health endpoint plus Prometheus {@code /metrics} on
 * {@code healthPort} (default {@value #DEFAULT_HEALTH_PORT}); the same port is the scrape target
 * named {@code backfill:8000} in {@code infra/prometheus/prometheus.yml}.
 *
 * <p>Tier 5 A3 — {@code stopLatch.await(6 * 3600, SECONDS)} for interruptible sleep. Tier 5 A2, A4.
 *
 * <p>Thread safety: designed to run on a single virtual thread; {@code stopLatch} is thread-safe.
 */
public final class BackfillScheduler {

  private static final Logger log = LoggerFactory.getLogger(BackfillScheduler.class);
  private static final long DEFAULT_INTERVAL_SECONDS = 6L * 3600L; // 6 hours
  // Matches infra/prometheus/prometheus.yml's `backfill:8000` scrape job. Aligned with
  // collector / writer / consolidation, all of which bind 8000 (or 8003) inside the container
  // and let docker-compose publish a distinct host port.
  private static final int DEFAULT_HEALTH_PORT = 8000;

  private final Path baseDir;
  private final long intervalSeconds;
  private final int healthPort;
  private final ObjectMapper mapper;
  private final CountDownLatch stopLatch = new CountDownLatch(1); // Tier 5 A3

  public BackfillScheduler(
      Path baseDir, long intervalSeconds, int healthPort, ObjectMapper mapper) {
    this.baseDir = baseDir;
    this.intervalSeconds = intervalSeconds;
    this.healthPort = healthPort;
    this.mapper = mapper;
  }

  public BackfillScheduler(Path baseDir, long intervalSeconds, ObjectMapper mapper) {
    this(baseDir, intervalSeconds, DEFAULT_HEALTH_PORT, mapper);
  }

  public BackfillScheduler(Path baseDir, ObjectMapper mapper) {
    this(baseDir, DEFAULT_INTERVAL_SECONDS, DEFAULT_HEALTH_PORT, mapper);
  }

  /**
   * Starts the scheduler loop; blocks until {@link #stop()} is called or an interrupt is received.
   */
  public void run() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    BackfillMetrics metrics = new BackfillMetrics(registry);

    // /ready always reports the scheduler healthy once the loop is running; /metrics serves the
    // Prometheus scrape (job=backfill in prometheus.yml). Started BEFORE the first await so the
    // scrape target goes UP immediately rather than only after the first 6-hour cycle elapses.
    HealthServer health =
        new HealthServer(
            healthPort,
            () -> Map.of("scheduler", true),
            () -> registry.scrape().getBytes(StandardCharsets.UTF_8));
    health.start();

    // Single shared HttpClient (Tier 5 D3)
    HttpClient httpClient = HttpClient.newHttpClient();
    BinanceRestClient restClient = new BinanceRestClient(httpClient, mapper);

    log.info(
        "backfill_scheduler_started",
        "interval_seconds",
        intervalSeconds,
        "health_port",
        healthPort);

    try {
      runLoop(metrics, restClient);
    } finally {
      health.stop();
    }
  }

  private void runLoop(BackfillMetrics metrics, BinanceRestClient restClient) {
    while (true) {
      try {
        // Tier 5 A3: await with timeout; returns true on stop signal
        if (stopLatch.await(intervalSeconds, TimeUnit.SECONDS)) {
          log.info("backfill_scheduler_stopping");
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Tier 5 A4
        log.info("backfill_scheduler_interrupted");
        return;
      }

      // Run the backfill cycle
      long cycleStart = System.nanoTime();
      metrics.incRuns();
      try {
        BackfillCycle.CycleSummary summary = BackfillCycle.run(baseDir, restClient, mapper);
        metrics.setGapsFound(summary.gapsFound());
        metrics.setRecordsWritten(summary.recordsWritten());
        metrics.setLastRunSuccess(1);
      } catch (Exception e) {
        metrics.setLastRunSuccess(0);
        log.error("backfill_scheduler_cycle_error", "error", e.getMessage());
      }
      long cycleEnd = System.nanoTime();
      metrics.setLastRunDuration((cycleEnd - cycleStart) / 1_000_000_000.0);
      metrics.setLastRunTimestamp(System.currentTimeMillis() / 1_000.0);
    }
  }

  /** Signals the scheduler to stop. */
  public void stop() {
    stopLatch.countDown();
  }
}
