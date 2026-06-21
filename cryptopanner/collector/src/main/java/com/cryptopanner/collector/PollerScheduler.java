package com.cryptopanner.collector;

import com.cryptopanner.common.RestPoller;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the executor that fires {@link RestPoller#pollOnce()} on cadence. One scheduler per
 * Collector. Each poller fires immediately on start, then re-schedules itself: a <b>successful</b>
 * poll schedules the next at the configured cadence; a <b>failed</b> poll schedules a retry sooner,
 * with exponential backoff (1s, 2s, 4s, …) capped at the cadence and ±25% jitter (master spec
 * §8.d). Both the failure envelope and the eventual success envelope are written by the poller.
 */
public final class PollerScheduler implements AutoCloseable {

  private final ScheduledExecutorService exec =
      Executors.newScheduledThreadPool(
          1,
          r -> {
            Thread t = new Thread(r, "rest-poller");
            t.setDaemon(true);
            return t;
          });

  private final List<Entry> entries = new ArrayList<>();

  private static final class Entry {
    final RestPoller poller;
    final int cadenceSeconds;
    final String label;
    final AtomicInteger attempt = new AtomicInteger(0);

    Entry(RestPoller poller, int cadenceSeconds, String label) {
      this.poller = poller;
      this.cadenceSeconds = cadenceSeconds;
      this.label = label;
    }
  }

  public void add(RestPoller poller, int cadenceSeconds, String label) {
    entries.add(new Entry(poller, cadenceSeconds, label));
  }

  public void start() {
    for (Entry e : entries) {
      System.out.println(
          "[collector] REST poller scheduled: " + e.label + " every " + e.cadenceSeconds + "s");
      scheduleIn(e, 0);
    }
  }

  private void scheduleIn(Entry e, long delaySeconds) {
    try {
      exec.schedule(() -> runOnce(e), delaySeconds, TimeUnit.SECONDS);
    } catch (RejectedExecutionException ignored) {
      // Executor shut down by close() — stop rescheduling.
    }
  }

  private void runOnce(Entry e) {
    boolean ok;
    try {
      ok = e.poller.pollOnce();
    } catch (Exception ex) {
      System.err.println("[collector] poll " + e.label + " threw: " + ex.getMessage());
      ok = false;
    }
    if (ok) {
      e.attempt.set(0);
      scheduleIn(e, e.cadenceSeconds);
    } else {
      scheduleIn(e, retryDelaySeconds(e.attempt.getAndIncrement(), e.cadenceSeconds));
    }
  }

  /**
   * Retry delay for a failed poll: exponential base {@code min(60, 2^attempt)} clamped to the
   * cadence, with ±25% uniform jitter, floored at 1s. Package-private for test access.
   */
  static long retryDelaySeconds(int attempt, int cadenceSeconds) {
    long base = Math.min(cadenceSeconds, Math.min(60L, 1L << Math.min(attempt, 6)));
    double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
    return Math.max(1L, Math.round(base * jitter));
  }

  @Override
  public void close() {
    exec.shutdownNow();
  }
}
