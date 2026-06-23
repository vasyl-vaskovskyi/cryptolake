package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.Alert.AlertType;
import com.cryptopanner.monitor.testutil.Snapshots;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * §13.b/d — restart decisioning: gate, backoff, breaker, active-slot targeting, cliff exception.
 */
class RestartOrchestratorTest {

  private static final String NODE = "vps-fra-1";
  private static final Instant T0 = Instant.parse("2026-06-23T00:00:00Z");
  private static final Duration CLIFF = Duration.ofHours(23).plusMinutes(30);

  // Records every restart call and returns a scripted outcome.
  private static final class FakeClient implements RestartClient {
    final List<String> calls = new CopyOnWriteArrayList<>();
    volatile boolean succeed = true;

    @Override
    public boolean restart(MonitorConfig.Node node, String component) {
      calls.add(component);
      return succeed;
    }
  }

  private FakeClient client;
  private RestartOrchestrator orch;
  private final MonitorConfig.Node node = new MonitorConfig.Node(NODE, "127.0.0.1:9", null);

  @BeforeEach
  void setUp() {
    client = new FakeClient();
    orch =
        new RestartOrchestrator(
            List.of("5s", "15s", "60s", "300s"), 3, Duration.ofMinutes(5), CLIFF, client);
  }

  private Map<String, String> states(String collectorA, String collectorB, String sealer) {
    return Map.of(
        "cryptopanner-collector@a", collectorA,
        "cryptopanner-collector@b", collectorB,
        "cryptopanner-sealer", sealer,
        "cryptopanner-uploader", "running",
        "cryptopanner-agent", "running");
  }

  private List<Alert> reconcile(StatusSnapshot s, Instant now) {
    return orch.reconcile(node, s, now);
  }

  @Test
  void downComponentIsRestarted() {
    reconcile(Snapshots.status(NODE, "a", states("running", "down", "down"), null, 100), T0);
    assertEquals(List.of("cryptopanner-sealer"), client.calls);
  }

  @Test
  void runningComponentIsNeverRestarted() {
    reconcile(Snapshots.status(NODE, "a", states("running", "down", "running"), null, 100), T0);
    assertTrue(client.calls.isEmpty());
  }

  @Test
  void degradedComponentIsNotRestartedNormally() {
    reconcile(Snapshots.status(NODE, "a", states("running", "down", "degraded"), null, 100), T0);
    assertTrue(client.calls.isEmpty());
  }

  @Test
  void inactiveCollectorSlotIsNeverRestarted() {
    // collector@b is down but a is active — that down is normal, never restart it.
    reconcile(Snapshots.status(NODE, "a", states("running", "down", "running"), null, 100), T0);
    assertTrue(client.calls.isEmpty());
  }

  @Test
  void activeCollectorSlotIsRestartedWhenDown() {
    reconcile(Snapshots.status(NODE, "a", states("down", "down", "running"), null, 100), T0);
    assertEquals(List.of("cryptopanner-collector@a"), client.calls);
  }

  @Test
  void backoffPreventsImmediateRetry() {
    StatusSnapshot s = Snapshots.status(NODE, "a", states("running", "down", "down"), null, 100);
    reconcile(s, T0); // attempt 1 at T0
    reconcile(s, T0.plusSeconds(3)); // < 5s backoff → no new attempt
    assertEquals(1, client.calls.size());
    reconcile(s, T0.plusSeconds(6)); // > 5s → attempt 2
    assertEquals(2, client.calls.size());
  }

  @Test
  void backoffEscalatesThroughTheSchedule() {
    client.succeed = true;
    StatusSnapshot s = Snapshots.status(NODE, "a", states("running", "down", "down"), null, 100);
    reconcile(s, T0); // attempt 1
    reconcile(s, T0.plusSeconds(6)); // attempt 2 (after 5s)
    reconcile(s, T0.plusSeconds(6 + 16)); // attempt 3 (after 15s)
    assertEquals(3, client.calls.size());
    // 4th waits 60s: at +50s from last → no attempt
    reconcile(s, T0.plusSeconds(6 + 16 + 50));
    assertEquals(3, client.calls.size());
    reconcile(s, T0.plusSeconds(6 + 16 + 61)); // after 60s → attempt 4
    assertEquals(4, client.calls.size());
  }

  @Test
  void circuitBreakerTripsAfterThreeFailures() {
    client.succeed = false;
    StatusSnapshot s = Snapshots.status(NODE, "a", states("running", "down", "down"), null, 100);
    reconcile(s, T0); // fail 1
    reconcile(s, T0.plusSeconds(6)); // fail 2
    List<Alert> alerts = reconcile(s, T0.plusSeconds(6 + 16)); // fail 3 → trip
    assertTrue(alerts.stream().anyMatch(a -> a.type() == AlertType.CIRCUIT_BREAKER_TRIPPED));
    assertEquals(3, client.calls.size());
    // tripped: no further attempts even long after
    reconcile(s, T0.plusSeconds(10_000));
    assertEquals(3, client.calls.size());
  }

  @Test
  void recoveryResetsBackoff() {
    StatusSnapshot down = Snapshots.status(NODE, "a", states("running", "down", "down"), null, 100);
    StatusSnapshot up =
        Snapshots.status(NODE, "a", states("running", "down", "running"), null, 100);
    reconcile(down, T0); // attempt 1
    reconcile(up, T0.plusSeconds(2)); // recovered → reset
    reconcile(down, T0.plusSeconds(4)); // down again → immediate attempt (backoff reset)
    assertEquals(2, client.calls.size());
  }

  @Test
  void cliffExceptionRestartsDegradedLockHolder() {
    // sealer holds the lock and is degraded, connection age past the cliff → restart it anyway.
    double pastCliff = CLIFF.toSeconds() + 60;
    StatusSnapshot s =
        Snapshots.status(
            NODE, "a", states("running", "down", "degraded"), "cryptopanner-sealer", pastCliff);
    reconcile(s, T0);
    assertEquals(List.of("cryptopanner-sealer"), client.calls);
  }

  @Test
  void noCliffExceptionWhenConnectionAgeUnderCliff() {
    double underCliff = CLIFF.toSeconds() - 60;
    StatusSnapshot s =
        Snapshots.status(
            NODE, "a", states("running", "down", "degraded"), "cryptopanner-sealer", underCliff);
    reconcile(s, T0);
    assertTrue(client.calls.isEmpty());
  }
}
