package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.DispatchedMessage.Kind;
import com.cryptopanner.monitor.testutil.StubHttpServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** End-to-end smoke of one scrape cycle wiring all monitor units against a stub Node Agent. */
class MonitorCoreTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();
  private static final Duration CLIFF = Duration.ofHours(23).plusMinutes(30);
  private static final Instant T0 = Instant.parse("2026-06-23T12:00:00Z");

  private StubHttpServer agent;

  @AfterEach
  void tearDown() {
    if (agent != null) {
      agent.close();
    }
  }

  private static final class RecordingChannel implements AlertChannel {
    final List<DispatchedMessage> sent = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
      return "rec";
    }

    @Override
    public boolean enabled() {
      return true;
    }

    @Override
    public boolean send(DispatchedMessage m) {
      sent.add(m);
      return true;
    }
  }

  private MonitorConfig.Alert thresholds() {
    return new MonitorConfig.Alert(
        "1h",
        new MonitorConfig.Correlation(3, "1m"),
        new MonitorConfig.Warning("2m", "30m", "1h", 10, "10m", 80, 1),
        new MonitorConfig.Critical("5m", 2, 95, 5));
  }

  private MonitorCore core(
      MonitorConfig.Node node,
      RecordingChannel channel,
      AtomicReference<List<NodeView>> view,
      Path tokenFile) {
    NodeScraper scraper = new NodeScraper(NodeScraper.newHttpClient(), MAPPER);
    AlertEvaluator evaluator = new AlertEvaluator(thresholds(), CLIFF);
    AlertDispatcher dispatcher = new AlertDispatcher(Duration.ofHours(1), 3, Duration.ofMinutes(1));
    RestartOrchestrator orchestrator =
        new RestartOrchestrator(
            List.of("5s", "15s", "60s", "300s"),
            3,
            Duration.ofMinutes(5),
            CLIFF,
            new AgentRestartClient(NodeScraper.newHttpClient(), () -> T0));
    // Self-test time after T0 (12:00) so the dead-man doesn't fire during these cycle smokes.
    DeadMansSwitch deadMan =
        new DeadMansSwitch(
            NodeScraper.newHttpClient(),
            "",
            Duration.ofSeconds(60),
            java.time.LocalTime.of(23, 0),
            List.of(channel));
    return new MonitorCore(
        List.of(node),
        scraper,
        new NodeStateTracker(),
        evaluator,
        orchestrator,
        dispatcher,
        List.of(channel),
        deadMan,
        view);
  }

  private String statusJson(String sealerState) {
    return "{\"node\":\"vps-fra-1\",\"components\":{"
        + "\"cryptopanner-collector@a\":{\"state\":\"running\",\"pid\":5,\"heartbeat_age_s\":1.0,\"uptime_s\":10},"
        + "\"cryptopanner-sealer\":{\"state\":\""
        + sealerState
        + "\",\"pid\":"
        + (sealerState.equals("down") ? "null" : "6")
        + ",\"heartbeat_age_s\":"
        + (sealerState.equals("down") ? "null" : "1.0")
        + ",\"uptime_s\":10}},"
        + "\"active_slot\":\"a\",\"fs_heavy_lock\":{\"held_by\":null},\"deploy\":{\"state\":\"IDLE\"},"
        + "\"rotation\":{\"state\":\"IDLE\",\"current_connection_age_s\":100},"
        + "\"vps\":{\"disk\":{\"/data\":{\"percent\":50.0,\"free_bytes\":1}}}}";
  }

  @Test
  void healthyNodeProducesViewAndNoAlerts() throws Exception {
    agent =
        new StubHttpServer()
            .route("/status", 200, statusJson("running"))
            .route("/metrics", 200, "cryptopanner_sealed_files_pending_upload 0\n");
    RecordingChannel channel = new RecordingChannel();
    AtomicReference<List<NodeView>> view = new AtomicReference<>(List.of());
    MonitorConfig.Node node = new MonitorConfig.Node("vps-fra-1", agent.endpoint(), null);

    core(node, channel, view, null).runCycle(T0);

    assertEquals(1, view.get().size());
    assertTrue(view.get().get(0).reachable());
    assertTrue(channel.sent.isEmpty());
  }

  @Test
  void downComponentRestartsAndAlerts(@TempDir Path dir) throws Exception {
    Path token = dir.resolve("token");
    Files.writeString(token, "tok");
    agent =
        new StubHttpServer()
            .route("/status", 200, statusJson("down"))
            .route("/metrics", 200, "cryptopanner_sealed_files_pending_upload 0\n")
            .route("/restart/cryptopanner-sealer", 200, "ok\n");
    RecordingChannel channel = new RecordingChannel();
    AtomicReference<List<NodeView>> view = new AtomicReference<>(List.of());
    MonitorConfig.Node node = new MonitorConfig.Node("vps-fra-1", agent.endpoint(), token);

    core(node, channel, view, token).runCycle(T0);

    // a restart was attempted against the agent
    assertTrue(
        agent.received().stream().anyMatch(r -> r.path().equals("/restart/cryptopanner-sealer")),
        agent.received().toString());
    // a COMPONENT_DOWN alert was delivered to the channel
    assertTrue(
        channel.sent.stream()
            .anyMatch(m -> m.kind() == Kind.ALERT && m.body().contains("cryptopanner-sealer")),
        channel.sent.toString());
  }

  @Test
  void unreachableNodeYieldsUnreachableView() {
    AtomicReference<List<NodeView>> view = new AtomicReference<>(List.of());
    RecordingChannel channel = new RecordingChannel();
    MonitorConfig.Node node = new MonitorConfig.Node("vps-dead", "127.0.0.1:1", null);

    core(node, channel, view, null).runCycle(T0);

    assertEquals(1, view.get().size());
    assertFalse(view.get().get(0).reachable());
  }
}
