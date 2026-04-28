package com.cryptolake.consolidation.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long-running daily consolidation scheduler.
 *
 * <p>Ports {@code main} from {@code consolidation_scheduler.py}. Sleeps until the configured daily
 * run time (default 02:30 UTC), runs the consolidation cycle, then repeats.
 *
 * <p>Tier 5 A3 — {@code stopLatch.await(timeout)} for interruptible sleep. Tier 5 A2 — cycle runs
 * as direct blocking call. Tier 5 A4 — restores interrupt flag on {@link InterruptedException}.
 *
 * <p>Thread safety: designed to run on a single virtual thread; {@code stopLatch} is thread-safe.
 */
public final class ConsolidationScheduler {

  private static final Logger log = LoggerFactory.getLogger(ConsolidationScheduler.class);
  private static final int DEFAULT_START_HOUR_UTC = 2; // 02:30 UTC

  private final Path baseDir;
  private final int startHourUtc;
  private final ObjectMapper mapper;
  private final CountDownLatch stopLatch = new CountDownLatch(1); // Tier 5 A3

  public ConsolidationScheduler(Path baseDir, int startHourUtc, ObjectMapper mapper) {
    this.baseDir = baseDir;
    this.startHourUtc = startHourUtc;
    this.mapper = mapper;
  }

  public ConsolidationScheduler(Path baseDir, ObjectMapper mapper) {
    this(baseDir, DEFAULT_START_HOUR_UTC, mapper);
  }

  /**
   * Starts the scheduler loop; blocks until {@link #stop()} is called or an interrupt is received.
   */
  public void run() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    ConsolidationMetrics metrics = new ConsolidationMetrics(registry);

    log.info("consolidation_scheduler_started", "start_hour_utc", startHourUtc);

    while (true) {
      Instant nextRun = ScheduleClock.nextRunInstant(Instant.now(), startHourUtc);
      long sleepNanos = Duration.between(Instant.now(), nextRun).toNanos();
      if (sleepNanos < 0L) {
        sleepNanos = 0L;
      }

      try {
        // Tier 5 A3: await with timeout; returns true on stop signal
        if (stopLatch.await(sleepNanos, TimeUnit.NANOSECONDS)) {
          log.info("consolidation_scheduler_stopping");
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Tier 5 A4
        log.info("consolidation_scheduler_interrupted");
        return;
      }

      // Run the cycle
      long cycleStart = System.nanoTime();
      metrics.incRuns();
      try {
        ConsolidationCycle.CycleSummary summary =
            ConsolidationCycle.run(baseDir, null, metrics, mapper);
        metrics.setLastRunSuccess(summary.anyFailed() ? 0 : 1);
      } catch (Exception e) {
        metrics.setLastRunSuccess(0);
        log.error("consolidation_scheduler_cycle_error", "error", e.getMessage());
      }
      long cycleEnd = System.nanoTime();
      double durationSec = (cycleEnd - cycleStart) / 1_000_000_000.0;
      metrics.setLastRunDuration(durationSec);
      metrics.setLastRunTimestamp(System.currentTimeMillis() / 1_000.0);
    }
  }

  /** Signals the scheduler to stop after the current (or next) cycle completes. */
  public void stop() {
    stopLatch.countDown();
  }

  /** Returns the Prometheus scrape output for the health endpoint. */
  public static String createPrometheusScrape(MeterRegistry registry) {
    if (registry instanceof PrometheusMeterRegistry prom) {
      return prom.scrape();
    }
    return "";
  }
}
