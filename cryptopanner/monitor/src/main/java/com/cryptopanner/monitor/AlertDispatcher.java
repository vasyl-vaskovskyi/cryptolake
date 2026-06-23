package com.cryptopanner.monitor;

import com.cryptopanner.monitor.Alert.AlertType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the per-cycle set of currently-active alerts into the messages to actually send (§13.e):
 *
 * <ul>
 *   <li><b>Dedup</b> — a repeat firing of the same {@code (node, component, type)} key within
 *       {@code dedup_ttl} is suppressed.
 *   <li><b>Correlation</b> — when ≥ {@code threshold_nodes} distinct nodes fire the same alert type
 *       in one cycle (a region-wide event within the correlation window), they collapse into one
 *       combined message instead of N pages.
 *   <li><b>Recovery</b> — when a previously-active key is absent this cycle, a one-shot {@code
 *       recovered} message is emitted and its dedup memory cleared so a re-occurrence pages
 *       immediately.
 * </ul>
 *
 * <p>Stateful across cycles (dedup timestamps + active set) but driven entirely by the injected
 * {@code now}, so it is deterministic in tests. Caller passes the full active-alert list each
 * cycle.
 */
public final class AlertDispatcher {

  private final Duration dedupTtl;
  private final int correlationThreshold;
  private final Duration correlationWindow;

  private final Map<String, Instant> lastFired = new HashMap<>();
  private final Map<String, Alert> active = new HashMap<>();

  public AlertDispatcher(Duration dedupTtl, int correlationThreshold, Duration correlationWindow) {
    this.dedupTtl = dedupTtl;
    this.correlationThreshold = correlationThreshold;
    this.correlationWindow = correlationWindow;
  }

  public List<DispatchedMessage> dispatch(List<Alert> current, Instant now) {
    List<DispatchedMessage> out = new ArrayList<>();

    Map<String, Alert> byKey = new LinkedHashMap<>();
    for (Alert a : current) {
      byKey.put(a.dedupKey(), a);
    }

    // Recoveries: keys that were active but are not present this cycle.
    for (String key : new ArrayList<>(active.keySet())) {
      if (!byKey.containsKey(key)) {
        out.add(DispatchedMessage.recovered(active.get(key)));
        active.remove(key);
        lastFired.remove(key);
      }
    }

    // Determine which alerts are fireable (past the dedup TTL) and update memory.
    List<Alert> fireable = new ArrayList<>();
    for (Alert a : current) {
      String key = a.dedupKey();
      active.put(key, a);
      Instant lf = lastFired.get(key);
      boolean fire = lf == null || !Duration.between(lf, now).minus(dedupTtl).isNegative();
      if (fire) {
        fireable.add(a);
        lastFired.put(key, now);
      }
    }

    // Correlation: group fireable alerts by type; collapse region-wide events.
    Map<AlertType, List<Alert>> byType = new LinkedHashMap<>();
    for (Alert a : fireable) {
      byType.computeIfAbsent(a.type(), k -> new ArrayList<>()).add(a);
    }
    for (var e : byType.entrySet()) {
      List<Alert> list = e.getValue();
      long distinctNodes = list.stream().map(Alert::node).distinct().count();
      if (distinctNodes >= correlationThreshold) {
        out.add(DispatchedMessage.correlated(e.getKey(), list));
      } else {
        for (Alert a : list) {
          out.add(DispatchedMessage.single(a));
        }
      }
    }
    return out;
  }

  /** Exposed for the dashboard / future cross-cycle correlation tuning. */
  public Duration correlationWindow() {
    return correlationWindow;
  }
}
