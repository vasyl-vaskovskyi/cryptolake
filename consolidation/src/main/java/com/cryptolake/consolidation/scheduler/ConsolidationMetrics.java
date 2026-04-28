package com.cryptolake.consolidation.scheduler;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Prometheus meters for the consolidation scheduler.
 *
 * <p>8 meters matching Python's {@code consolidation_scheduler.py} metric names (Tier 3 §18; design
 * §9.1). {@code NamingConvention.identity} prevents Micrometer's default camelCase transformation
 * from mangling the names (Tier 5 H4).
 *
 * <p>Gauges are backed by {@link AtomicReference} holders so they are strongly reachable (Tier 5 H6
 * — gauge supplier must not be GC'd before the registry is closed).
 *
 * <p>Thread safety: meter values are updated from the scheduler loop thread only; meter objects
 * themselves are thread-safe (Micrometer-internal).
 */
public final class ConsolidationMetrics {

  // Gauge backing fields (AtomicReference for strong reachability — Tier 5 H6)
  private final AtomicReference<Double> lastRunTimestampRef = new AtomicReference<>(0.0);
  private final AtomicReference<Double> lastRunDurationRef = new AtomicReference<>(0.0);
  private final AtomicReference<Double> lastRunSuccessRef = new AtomicReference<>(0.0);

  // Counters
  private final Counter runsTotal;
  private final Counter daysProcessed;
  private final Counter filesConsolidated;
  private final Counter verificationFailures;
  private final Counter missingHoursTotal;

  public ConsolidationMetrics(MeterRegistry registry) {
    // Tier 5 H4: NamingConvention.identity — names already include _total suffix where applicable
    registry.config().namingConvention(NamingConvention.identity);

    Gauge.builder(
            "consolidation_last_run_timestamp_seconds", lastRunTimestampRef, AtomicReference::get)
        .register(registry);
    Gauge.builder(
            "consolidation_last_run_duration_seconds", lastRunDurationRef, AtomicReference::get)
        .register(registry);
    Gauge.builder("consolidation_last_run_success", lastRunSuccessRef, AtomicReference::get)
        .register(registry);

    // Counters: bare name without _total (Micrometer appends _total on scrape)
    runsTotal = Counter.builder("consolidation_runs_total").register(registry);
    daysProcessed = Counter.builder("consolidation_days_processed").register(registry);
    filesConsolidated = Counter.builder("consolidation_files_consolidated").register(registry);
    verificationFailures =
        Counter.builder("consolidation_verification_failures_total").register(registry);
    missingHoursTotal = Counter.builder("consolidation_missing_hours_total").register(registry);
  }

  public void setLastRunTimestamp(double seconds) {
    lastRunTimestampRef.set(seconds);
  }

  public void setLastRunDuration(double seconds) {
    lastRunDurationRef.set(seconds);
  }

  public void setLastRunSuccess(int value) {
    lastRunSuccessRef.set((double) value);
  }

  public void incRuns() {
    runsTotal.increment();
  }

  public void incDaysProcessed(double n) {
    daysProcessed.increment(n);
  }

  public void incFilesConsolidated(double n) {
    filesConsolidated.increment(n);
  }

  public void incVerificationFailures() {
    verificationFailures.increment();
  }

  public void incMissingHours(double n) {
    missingHoursTotal.increment(n);
  }
}
