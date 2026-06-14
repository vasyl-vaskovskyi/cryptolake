package com.cryptopanner.collector;

import com.cryptopanner.common.RestPoller;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns the executor that fires {@link RestPoller#pollOnce()} on cadence. One scheduler per
 * Collector. Each poller fires immediately on start (so we never wait an entire cadence for the
 * first sample), then at fixed rate.
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

  public record Entry(RestPoller poller, int cadenceSeconds, String label) {}

  public void add(RestPoller poller, int cadenceSeconds, String label) {
    entries.add(new Entry(poller, cadenceSeconds, label));
  }

  public void start() {
    for (Entry e : entries) {
      System.out.println(
          "[collector] REST poller scheduled: " + e.label() + " every " + e.cadenceSeconds() + "s");
      exec.scheduleAtFixedRate(
          () -> {
            try {
              e.poller().pollOnce();
            } catch (Exception ex) {
              System.err.println("[collector] poll " + e.label() + " threw: " + ex.getMessage());
            }
          },
          0,
          e.cadenceSeconds(),
          TimeUnit.SECONDS);
    }
  }

  @Override
  public void close() {
    exec.shutdownNow();
  }
}
