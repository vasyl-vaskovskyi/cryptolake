package com.cryptolake.backfill;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Prometheus meters for the backfill scheduler.
 *
 * <p>6 meters matching Python's {@code backfill_scheduler.py} metric names (Tier 3 §18; design
 * §9.2). {@code NamingConvention.identity} prevents Micrometer name mangling (Tier 5 H4).
 *
 * <p>Gauge holders are {@link AtomicReference} fields for strong reachability (Tier 5 H6).
 */
public final class BackfillMetrics {

  private final AtomicReference<Double> lastRunTimestampRef = new AtomicReference<>(0.0);
  private final AtomicReference<Double> lastRunDurationRef = new AtomicReference<>(0.0);
  private final AtomicReference<Double> gapsFoundRef = new AtomicReference<>(0.0);
  private final AtomicReference<Double> recordsWrittenRef = new AtomicReference<>(0.0);
  private final AtomicReference<Double> lastRunSuccessRef = new AtomicReference<>(0.0);

  private final Counter runsTotal;

  public BackfillMetrics(MeterRegistry registry) {
    registry.config().namingConvention(NamingConvention.identity); // Tier 5 H4

    Gauge.builder("backfill_last_run_timestamp_seconds", lastRunTimestampRef, AtomicReference::get)
        .register(registry);
    Gauge.builder("backfill_last_run_duration_seconds", lastRunDurationRef, AtomicReference::get)
        .register(registry);
    Gauge.builder("backfill_gaps_found", gapsFoundRef, AtomicReference::get).register(registry);
    Gauge.builder("backfill_records_written", recordsWrittenRef, AtomicReference::get)
        .register(registry);
    Gauge.builder("backfill_last_run_success", lastRunSuccessRef, AtomicReference::get)
        .register(registry);

    runsTotal = Counter.builder("backfill_runs_total").register(registry);
  }

  public void setLastRunTimestamp(double seconds) {
    lastRunTimestampRef.set(seconds);
  }

  public void setLastRunDuration(double seconds) {
    lastRunDurationRef.set(seconds);
  }

  public void setGapsFound(double n) {
    gapsFoundRef.set(n);
  }

  public void setRecordsWritten(double n) {
    recordsWrittenRef.set(n);
  }

  public void setLastRunSuccess(int value) {
    lastRunSuccessRef.set((double) value);
  }

  public void incRuns() {
    runsTotal.increment();
  }
}
