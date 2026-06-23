package com.cryptopanner.monitor;

import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.Alert.AlertType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The §13.a alert rules engine. Given one scrape result, the node's {@link NodeStateTracker} memory
 * (already updated for this scrape), and the scrape {@code now}, it returns the alert conditions
 * currently true for that node. Pure read over its inputs — all temporal state lives in the tracker
 * — so every rule is exercised in isolation with synthetic snapshots.
 *
 * <p>Coverage in this pass is the set of conditions whose signal the {@code /status}+{@code
 * /metrics} contract carries. Conditions blocked on a node-side signal not yet exposed remotely
 * (precise upload-backlog age, REST poll rate, clock-skew degree, ws-disconnect duration,
 * rotation-verify-fail counts) are intentionally absent and marked DEFERRED so they slot in as new
 * rules without touching the loop.
 */
public final class AlertEvaluator {

  private final MonitorConfig.Alert thresholds;
  private final Duration wsConnectionCliff;

  public AlertEvaluator(MonitorConfig.Alert thresholds, Duration wsConnectionCliff) {
    this.thresholds = thresholds;
    this.wsConnectionCliff = wsConnectionCliff;
  }

  public List<Alert> evaluate(String node, ScrapeResult r, NodeStateTracker tracker, Instant now) {
    List<Alert> alerts = new ArrayList<>();

    if (!r.ok()) {
      // §13.a Critical: node unreachable after 3 consecutive scrape failures.
      if (tracker.consecutiveFailures(node) >= 3) {
        alerts.add(
            Alert.of(
                node,
                null,
                AlertType.NODE_UNREACHABLE,
                "node unreachable: "
                    + tracker.consecutiveFailures(node)
                    + " consecutive scrape failures",
                now));
      }
      return alerts;
    }

    StatusSnapshot s = r.status();
    String activeCollector = s.activeCollectorComponent();

    // Per-component states. The inactive collector slot is expected `down` in steady state
    // (§11.b) — only the active slot and the singleton components alert.
    for (var e : s.components().entrySet()) {
      String name = e.getKey();
      String state = e.getValue().state();
      if (isCollectorSlot(name) && !name.equals(activeCollector)) {
        continue;
      }
      switch (state == null ? "" : state) {
        case "down" ->
            alerts.add(Alert.of(node, name, AlertType.COMPONENT_DOWN, name + " is down", now));
        case "stuck" ->
            alerts.add(
                Alert.of(node, name, AlertType.COMPONENT_STUCK, name + " heartbeat stuck", now));
        case "degraded" -> {
          Instant since = tracker.degradedSince(node, name);
          if (since != null && !elapsed(since, now).minus(degradedPersisting()).isNegative()) {
            alerts.add(
                Alert.of(
                    node,
                    name,
                    AlertType.COMPONENT_DEGRADED,
                    name + " degraded for " + elapsed(since, now).toSeconds() + "s",
                    now));
          }
        }
        default -> {
          // running: no alert
        }
      }
    }

    // Disk pressure on /data.
    Double disk = s.diskPercent("/data");
    if (disk != null) {
      if (disk > thresholds.critical().diskDataPct()) {
        alerts.add(
            Alert.of(node, null, AlertType.DISK_PRESSURE_CRITICAL, "/data at " + disk + "%", now));
      } else if (disk > thresholds.warning().diskDataPct()) {
        alerts.add(
            Alert.of(node, null, AlertType.DISK_PRESSURE_WARNING, "/data at " + disk + "%", now));
      }
    }

    // Deploy state machine stuck (no progression).
    Instant deploySince = tracker.deploySince(node);
    if (deploySince != null && !elapsed(deploySince, now).minus(deployStuck()).isNegative()) {
      alerts.add(
          Alert.of(
              node,
              null,
              AlertType.DEPLOY_STUCK,
              "deploy stuck in "
                  + s.deployState()
                  + " for "
                  + elapsed(deploySince, now).toMinutes()
                  + "m",
              now));
    }

    // Active-slot mismatch: the active slot is down while the other slot runs (§12.m).
    String otherCollector = "cryptopanner-collector@" + ("a".equals(s.activeSlot()) ? "b" : "a");
    StatusSnapshot.Component ac = s.components().get(activeCollector);
    StatusSnapshot.Component oc = s.components().get(otherCollector);
    boolean activeDown = ac == null || "down".equals(ac.state());
    boolean otherRunning = oc != null && "running".equals(oc.state());
    if (activeDown && otherRunning) {
      alerts.add(
          Alert.of(
              node,
              null,
              AlertType.ACTIVE_SLOT_MISMATCH,
              "active_slot=" + s.activeSlot() + " is down but other slot is running",
              now));
    }

    // WS connection-age cliff (approaching Binance's 24h forced close).
    Double age = s.currentConnectionAgeS();
    if (age != null && age > wsConnectionCliff.toSeconds()) {
      alerts.add(
          Alert.of(
              node,
              null,
              AlertType.WS_CONNECTION_AGE_CLIFF,
              "WS connection age "
                  + age.longValue()
                  + "s exceeds cliff "
                  + wsConnectionCliff.toSeconds()
                  + "s",
              now));
    }

    // Upload backlog sustained beyond the age threshold (proxy via pending-count > 0).
    Instant backlogSince = tracker.backlogSince(node);
    if (backlogSince != null
        && !elapsed(backlogSince, now).minus(uploadBacklogAge()).isNegative()) {
      alerts.add(
          Alert.of(
              node,
              null,
              AlertType.UPLOAD_BACKLOG,
              "upload backlog non-empty for " + elapsed(backlogSince, now).toMinutes() + "m",
              now));
    }

    // DEFERRED(§13.a): clock-skew degree, REST failed-poll rate, ws-disconnect duration, and
    // rotation-verify-fail counts need a node-side signal not yet exposed remotely. Each becomes a
    // new rule here once the signal lands; nothing else in the loop changes.

    return alerts;
  }

  private static boolean isCollectorSlot(String name) {
    return name.startsWith("cryptopanner-collector@");
  }

  private static Duration elapsed(Instant since, Instant now) {
    return Duration.between(since, now);
  }

  private Duration degradedPersisting() {
    return thresholds.warning().degradedPersistingDuration();
  }

  private Duration deployStuck() {
    return thresholds.warning().deployStuckDuration();
  }

  private Duration uploadBacklogAge() {
    return thresholds.warning().uploadBacklogAgeDuration();
  }
}
