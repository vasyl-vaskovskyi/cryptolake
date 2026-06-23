package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.Alert.AlertType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §13.a — the alert rules engine over scraped snapshots. */
class AlertEvaluatorTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();
  private static final String NODE = "vps-fra-1";
  private static final Instant T0 = Instant.parse("2026-06-23T00:00:00Z");
  private static final Duration CLIFF = Duration.ofHours(23).plusMinutes(30);

  private NodeStateTracker tracker;
  private AlertEvaluator evaluator;

  @BeforeEach
  void setUp() {
    MonitorConfig.Alert thresholds =
        new MonitorConfig.Alert(
            "1h",
            new MonitorConfig.Correlation(3, "1m"),
            new MonitorConfig.Warning("2m", "30m", "1h", 10, "10m", 80, 1),
            new MonitorConfig.Critical("5m", 2, 95, 5));
    tracker = new NodeStateTracker();
    evaluator = new AlertEvaluator(thresholds, CLIFF);
  }

  // Update the tracker then evaluate — exactly the MonitorCore cycle.
  private List<AlertType> eval(ScrapeResult r, Instant now) {
    tracker.update(NODE, r, now);
    return evaluator.evaluate(NODE, r, tracker, now).stream()
        .map(Alert::type)
        .collect(Collectors.toList());
  }

  private ScrapeResult ok(
      String activeSlot,
      Map<String, String> comps,
      Double diskData,
      String deploy,
      Double connAge) {
    return ok(activeSlot, comps, diskData, deploy, connAge, 0);
  }

  private ScrapeResult ok(
      String activeSlot,
      Map<String, String> comps,
      Double diskData,
      String deploy,
      Double connAge,
      int pendingUploads) {
    StringBuilder c = new StringBuilder();
    boolean first = true;
    for (var e : comps.entrySet()) {
      if (!first) c.append(",");
      first = false;
      boolean down = e.getValue().equals("down");
      c.append("\"")
          .append(e.getKey())
          .append("\": {\"state\":\"")
          .append(e.getValue())
          .append("\",\"pid\":")
          .append(down ? "null" : "5")
          .append(",\"heartbeat_age_s\":")
          .append(down ? "null" : "1.0")
          .append(",\"uptime_s\":10}");
    }
    String json =
        "{\"node\":\""
            + NODE
            + "\",\"components\":{"
            + c
            + "},\"active_slot\":\""
            + activeSlot
            + "\",\"fs_heavy_lock\":{\"held_by\":null},\"deploy\":{\"state\":\""
            + deploy
            + "\"},\"rotation\":{\"state\":\"IDLE\",\"current_connection_age_s\":"
            + connAge
            + "},\"vps\":{\"disk\":{\"/data\":{\"percent\":"
            + diskData
            + ",\"free_bytes\":1}}}}";
    try {
      StatusSnapshot s = StatusSnapshot.parse(json, MAPPER);
      MetricsSnapshot m =
          MetricsSnapshot.parse(
              "cryptopanner_sealed_files_pending_upload " + pendingUploads + "\n");
      return ScrapeResult.success(s, m);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private Map<String, String> healthy() {
    return Map.of(
        "cryptopanner-collector@a", "running",
        "cryptopanner-collector@b", "down",
        "cryptopanner-sealer", "running",
        "cryptopanner-uploader", "running",
        "cryptopanner-agent", "running");
  }

  @Test
  void healthyNodeProducesNoAlerts() {
    assertTrue(eval(ok("a", healthy(), 50.0, "IDLE", 100.0), T0).isEmpty());
  }

  @Test
  void nodeUnreachableOnlyAfterThreeConsecutiveFailures() {
    assertFalse(eval(ScrapeResult.failure("x"), T0).contains(AlertType.NODE_UNREACHABLE));
    assertFalse(
        eval(ScrapeResult.failure("x"), T0.plusSeconds(5)).contains(AlertType.NODE_UNREACHABLE));
    assertTrue(
        eval(ScrapeResult.failure("x"), T0.plusSeconds(10)).contains(AlertType.NODE_UNREACHABLE));
  }

  @Test
  void successResetsFailureCounter() {
    eval(ScrapeResult.failure("x"), T0);
    eval(ScrapeResult.failure("x"), T0.plusSeconds(5));
    eval(ok("a", healthy(), 50.0, "IDLE", 100.0), T0.plusSeconds(10));
    assertFalse(
        eval(ScrapeResult.failure("x"), T0.plusSeconds(15)).contains(AlertType.NODE_UNREACHABLE));
  }

  @Test
  void singletonComponentDownAlertsCritical() {
    Map<String, String> comps = new java.util.HashMap<>(healthy());
    comps.put("cryptopanner-sealer", "down");
    assertTrue(eval(ok("a", comps, 50.0, "IDLE", 100.0), T0).contains(AlertType.COMPONENT_DOWN));
  }

  @Test
  void inactiveCollectorSlotDownIsNotAlerted() {
    // collector@b is down but a is active — that is the normal steady state (§11.b).
    assertFalse(
        eval(ok("a", healthy(), 50.0, "IDLE", 100.0), T0).contains(AlertType.COMPONENT_DOWN));
  }

  @Test
  void componentStuckAlertsWarning() {
    Map<String, String> comps = new java.util.HashMap<>(healthy());
    comps.put("cryptopanner-uploader", "stuck");
    assertTrue(eval(ok("a", comps, 50.0, "IDLE", 100.0), T0).contains(AlertType.COMPONENT_STUCK));
  }

  @Test
  void degradedAlertsOnlyAfterPersistingThreshold() {
    Map<String, String> comps = new java.util.HashMap<>(healthy());
    comps.put("cryptopanner-sealer", "degraded");
    assertFalse(
        eval(ok("a", comps, 50.0, "IDLE", 100.0), T0).contains(AlertType.COMPONENT_DEGRADED));
    // still degraded 1 min later — under the 2m threshold
    assertFalse(
        eval(ok("a", comps, 50.0, "IDLE", 100.0), T0.plusSeconds(60))
            .contains(AlertType.COMPONENT_DEGRADED));
    // 2m1s later — over threshold
    assertTrue(
        eval(ok("a", comps, 50.0, "IDLE", 100.0), T0.plusSeconds(121))
            .contains(AlertType.COMPONENT_DEGRADED));
  }

  @Test
  void degradedTimerResetsWhenComponentRecovers() {
    Map<String, String> degraded = new java.util.HashMap<>(healthy());
    degraded.put("cryptopanner-sealer", "degraded");
    eval(ok("a", degraded, 50.0, "IDLE", 100.0), T0);
    eval(ok("a", healthy(), 50.0, "IDLE", 100.0), T0.plusSeconds(60)); // recovered
    // degraded again — timer restarts, so 90s after T0 is only 30s of new degradation
    assertFalse(
        eval(ok("a", degraded, 50.0, "IDLE", 100.0), T0.plusSeconds(90))
            .contains(AlertType.COMPONENT_DEGRADED));
  }

  @Test
  void diskPressureWarningAndCritical() {
    assertFalse(
        eval(ok("a", healthy(), 79.0, "IDLE", 100.0), T0)
            .contains(AlertType.DISK_PRESSURE_WARNING));
    assertTrue(
        eval(ok("a", healthy(), 85.0, "IDLE", 100.0), T0)
            .contains(AlertType.DISK_PRESSURE_WARNING));
    List<AlertType> crit = eval(ok("a", healthy(), 96.0, "IDLE", 100.0), T0);
    assertTrue(crit.contains(AlertType.DISK_PRESSURE_CRITICAL));
    assertFalse(crit.contains(AlertType.DISK_PRESSURE_WARNING));
  }

  @Test
  void deployStuckAfterOneHour() {
    assertFalse(
        eval(ok("a", healthy(), 50.0, "STAGED", 100.0), T0).contains(AlertType.DEPLOY_STUCK));
    assertFalse(
        eval(ok("a", healthy(), 50.0, "STAGED", 100.0), T0.plusSeconds(3599))
            .contains(AlertType.DEPLOY_STUCK));
    assertTrue(
        eval(ok("a", healthy(), 50.0, "STAGED", 100.0), T0.plusSeconds(3601))
            .contains(AlertType.DEPLOY_STUCK));
  }

  @Test
  void deployStuckTimerResetsOnStateProgression() {
    eval(ok("a", healthy(), 50.0, "STAGED", 100.0), T0);
    eval(ok("a", healthy(), 50.0, "VERIFIED", 100.0), T0.plusSeconds(1800)); // progressed
    assertFalse(
        eval(ok("a", healthy(), 50.0, "VERIFIED", 100.0), T0.plusSeconds(3600))
            .contains(AlertType.DEPLOY_STUCK));
  }

  @Test
  void wsConnectionAgeCliffCritical() {
    double underCliff = CLIFF.toSeconds() - 10;
    double overCliff = CLIFF.toSeconds() + 10;
    assertFalse(
        eval(ok("a", healthy(), 50.0, "IDLE", underCliff), T0)
            .contains(AlertType.WS_CONNECTION_AGE_CLIFF));
    assertTrue(
        eval(ok("a", healthy(), 50.0, "IDLE", overCliff), T0)
            .contains(AlertType.WS_CONNECTION_AGE_CLIFF));
  }

  @Test
  void activeSlotMismatchWhenActiveSlotDownButOtherRunning() {
    Map<String, String> comps =
        Map.of(
            "cryptopanner-collector@a", "down",
            "cryptopanner-collector@b", "running",
            "cryptopanner-sealer", "running",
            "cryptopanner-uploader", "running",
            "cryptopanner-agent", "running");
    // active_slot says a, but a is down and b is running → mismatch
    assertTrue(
        eval(ok("a", comps, 50.0, "IDLE", 100.0), T0).contains(AlertType.ACTIVE_SLOT_MISMATCH));
  }

  @Test
  void uploadBacklogAfterSustainedThreshold() {
    assertFalse(
        eval(ok("a", healthy(), 50.0, "IDLE", 100.0, 3), T0).contains(AlertType.UPLOAD_BACKLOG));
    assertFalse(
        eval(ok("a", healthy(), 50.0, "IDLE", 100.0, 3), T0.plusSeconds(1799))
            .contains(AlertType.UPLOAD_BACKLOG));
    assertTrue(
        eval(ok("a", healthy(), 50.0, "IDLE", 100.0, 3), T0.plusSeconds(1801))
            .contains(AlertType.UPLOAD_BACKLOG));
  }

  @Test
  void uploadBacklogClearsWhenDrained() {
    eval(ok("a", healthy(), 50.0, "IDLE", 100.0, 3), T0);
    eval(ok("a", healthy(), 50.0, "IDLE", 100.0, 0), T0.plusSeconds(60)); // drained
    assertFalse(
        eval(ok("a", healthy(), 50.0, "IDLE", 100.0, 3), T0.plusSeconds(120))
            .contains(AlertType.UPLOAD_BACKLOG));
  }

  @Test
  void criticalSeverityIsIntrinsicToType() {
    assertEquals(Alert.Severity.CRITICAL, AlertType.COMPONENT_DOWN.severity());
    assertEquals(Alert.Severity.WARNING, AlertType.COMPONENT_DEGRADED.severity());
  }
}
