package com.cryptopanner.monitor;

import com.cryptopanner.common.config.ConfigParse;
import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.Alert.AlertType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decides and drives component restarts (master spec §13.b/d). Each scrape cycle, {@link
 * #reconcile} scans a node's status and, for every restart-eligible component, applies:
 *
 * <ul>
 *   <li><b>Gate (§13.d):</b> only {@code down}/{@code stuck} components restart. {@code running}
 *       and {@code degraded} never do (degraded is observation-only) — except the cliff exception.
 *       The inactive collector slot is never restarted (§13.b active-slot targeting); its {@code
 *       down} is normal.
 *   <li><b>Cliff exception (§13.d):</b> a {@code degraded} component holding {@code .fs-heavy.lock}
 *       is restarted to release the lock when a collector connection age has crossed the cliff.
 *   <li><b>Backoff (§13.b):</b> attempts for one component are spaced by the configured schedule
 *       ({@code 5s→15s→60s→300s}).
 *   <li><b>Circuit breaker (§13.b):</b> after {@code failure_count} failed attempts within {@code
 *       window}, the (node, component) pair trips — no further attempts, and a one-shot {@link
 *       AlertType#CIRCUIT_BREAKER_TRIPPED} Critical alert is returned for the dispatcher.
 * </ul>
 *
 * <p>When a component recovers (no longer eligible), its restart state resets, so its backoff
 * starts fresh and a tripped breaker clears.
 */
public final class RestartOrchestrator {

  private static final class RestartState {
    int attempts;
    Instant lastAttemptAt;
    final Deque<Instant> failures = new ArrayDeque<>();
    boolean tripped;
  }

  private final List<Duration> backoff;
  private final int breakerFailureCount;
  private final Duration breakerWindow;
  private final Duration wsConnectionCliff;
  private final RestartClient client;
  private final Map<String, RestartState> states = new ConcurrentHashMap<>();

  public RestartOrchestrator(
      List<String> backoff,
      int breakerFailureCount,
      Duration breakerWindow,
      Duration wsConnectionCliff,
      RestartClient client) {
    this.backoff = backoff.stream().map(ConfigParse::duration).toList();
    this.breakerFailureCount = breakerFailureCount;
    this.breakerWindow = breakerWindow;
    this.wsConnectionCliff = wsConnectionCliff;
    this.client = client;
  }

  /** Apply restart policy for one node's current status; returns any breaker-trip alerts. */
  public List<Alert> reconcile(MonitorConfig.Node node, StatusSnapshot s, Instant now) {
    List<Alert> alerts = new ArrayList<>();
    String activeCollector = s.activeCollectorComponent();

    Set<String> targets = new LinkedHashSet<>();
    for (var e : s.components().entrySet()) {
      String name = e.getKey();
      if (isCollectorSlot(name) && !name.equals(activeCollector)) {
        continue; // never restart the inactive slot
      }
      String st = e.getValue().state();
      if ("down".equals(st) || "stuck".equals(st)) {
        targets.add(name);
      }
    }

    // Cliff exception: a degraded lock-holder blocking a rotation approaching the cliff.
    String holder = s.fsHeavyLockHeldBy();
    if (holder != null) {
      StatusSnapshot.Component hc = s.components().get(holder);
      Double age = s.currentConnectionAgeS();
      if (hc != null
          && "degraded".equals(hc.state())
          && age != null
          && age > wsConnectionCliff.toSeconds()) {
        targets.add(holder);
      }
    }

    // Reset state for previously-tracked components on this node that have recovered.
    String prefix = node.id() + "|";
    for (String key : List.copyOf(states.keySet())) {
      if (key.startsWith(prefix) && !targets.contains(key.substring(prefix.length()))) {
        states.remove(key);
      }
    }

    for (String component : targets) {
      if (attempt(node, component, now)) {
        alerts.add(
            Alert.of(
                node.id(),
                component,
                AlertType.CIRCUIT_BREAKER_TRIPPED,
                "restart circuit breaker tripped for "
                    + component
                    + " after "
                    + breakerFailureCount
                    + " failures",
                now));
      }
    }
    return alerts;
  }

  /** Returns true iff this attempt tripped the breaker (so the caller emits the one-shot alert). */
  private boolean attempt(MonitorConfig.Node node, String component, Instant now) {
    RestartState st = states.computeIfAbsent(node.id() + "|" + component, k -> new RestartState());
    if (st.tripped) {
      return false;
    }
    if (st.attempts > 0) {
      Duration required = backoff.get(Math.min(st.attempts - 1, backoff.size() - 1));
      if (Duration.between(st.lastAttemptAt, now).compareTo(required) < 0) {
        return false; // still backing off
      }
    }

    st.attempts++;
    st.lastAttemptAt = now;
    boolean ok = client.restart(node, component);
    if (ok) {
      return false;
    }

    st.failures.addLast(now);
    while (!st.failures.isEmpty()
        && Duration.between(st.failures.peekFirst(), now).compareTo(breakerWindow) > 0) {
      st.failures.pollFirst();
    }
    if (st.failures.size() >= breakerFailureCount) {
      st.tripped = true;
      return true;
    }
    return false;
  }

  private static boolean isCollectorSlot(String name) {
    return name.startsWith("cryptopanner-collector@");
  }
}
