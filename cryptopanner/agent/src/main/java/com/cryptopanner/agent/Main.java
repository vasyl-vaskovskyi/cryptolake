package com.cryptopanner.agent;

import com.cryptopanner.common.Heartbeat;
import com.cryptopanner.common.SlotManager;
import com.cryptopanner.common.config.NodeConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Node Agent entry point (composition root, master spec §11.c). Wires {@link AgentServer} with a
 * {@link StatusBuilder} over the node's component heartbeats, a {@link Systemctl}-backed restart
 * handler, and bearer auth from the agent token file. Touches its own heartbeat every 5s.
 *
 * <p>Wiring only — the unit-tested behavior lives in StatusBuilder/HeartbeatState/BearerAuth/
 * AgentServer/Systemctl.unit.
 */
public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    NodeConfig cfg = NodeConfig.load(configPath);
    ObjectMapper mapper = new ObjectMapper();

    NodeConfig.Agent agentCfg = cfg.agent();
    int port = listenPort(agentCfg);
    String token = loadToken(agentCfg);
    Duration degraded =
        Duration.ofSeconds(
            agentCfg != null && agentCfg.heartbeat() != null
                ? agentCfg.heartbeat().degradedThresholdS()
                : 15);
    Duration stuck =
        Duration.ofSeconds(
            agentCfg != null && agentCfg.heartbeat() != null
                ? agentCfg.heartbeat().stuckThresholdS()
                : 60);

    SlotManager slots = new SlotManager(cfg.paths().deploy().resolve("active-slot"));
    StatusBuilder statusBuilder = new StatusBuilder(mapper, cfg.nodeId(), degraded, stuck);

    Path agentHeartbeat = Path.of("/tmp/cryptopanner-agent.heartbeat");

    AgentServer server =
        new AgentServer(
            port,
            new BearerAuth(token),
            () -> status(statusBuilder, slots),
            () -> metrics(agentHeartbeat),
            component -> Systemctl.restart(Systemctl.unit(component)),
            () -> {
              // Operator-triggered rotation (§5.4) signals the active collector; wired once the
              // WsConnectionManager exposes a trigger channel. For now report not-yet-available.
              System.err.println("[agent] /rotation/trigger not yet wired to the collector");
              return false;
            });

    ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
    ticker.scheduleAtFixedRate(
        () -> {
          try {
            Heartbeat.touch(agentHeartbeat);
          } catch (Exception e) {
            System.err.println("[agent] heartbeat touch failed: " + e.getMessage());
          }
        },
        0,
        5,
        TimeUnit.SECONDS);

    System.out.println("[agent] listening on :" + port + " for node " + cfg.nodeId());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  ticker.shutdownNow();
                  server.close();
                },
                "agent-shutdown"));
  }

  private static String status(StatusBuilder builder, SlotManager slots) {
    try {
      SlotManager.Slot active = slots.active();
      List<StatusBuilder.Component> components =
          List.of(
              collector(slots, "a"),
              collector(slots, "b"),
              component("cryptopanner-sealer"),
              component("cryptopanner-uploader"),
              component("cryptopanner-agent"));
      return builder.build(components, active, Instant.now());
    } catch (Exception e) {
      return "{\"error\":\"" + e.getMessage() + "\"}";
    }
  }

  private static StatusBuilder.Component collector(SlotManager slots, String slot) {
    String unit = "cryptopanner-collector@" + slot;
    return new StatusBuilder.Component(
        unit,
        Path.of("/tmp/cryptopanner-collector@" + slot + ".heartbeat"),
        Systemctl.isActive(unit + ".service"));
  }

  private static StatusBuilder.Component component(String name) {
    return new StatusBuilder.Component(
        name, Path.of("/tmp/" + name + ".heartbeat"), Systemctl.isActive(name + ".service"));
  }

  /** Minimal OpenMetrics: agent liveness gauge. Richer metrics (§11.c) layer on per-component. */
  private static String metrics(Path agentHeartbeat) {
    long ageS = 0;
    try {
      ageS = Heartbeat.age(agentHeartbeat, Instant.now()).map(Duration::toSeconds).orElse(0L);
    } catch (Exception ignored) {
      // best-effort
    }
    return "# TYPE cryptopanner_agent_up gauge\ncryptopanner_agent_up 1\n"
        + "# TYPE cryptopanner_heartbeat_age_seconds gauge\n"
        + "cryptopanner_heartbeat_age_seconds{component=\"cryptopanner-agent\"} "
        + ageS
        + "\n";
  }

  private static int listenPort(NodeConfig.Agent agentCfg) {
    if (agentCfg != null && agentCfg.listenAddress() != null) {
      String addr = agentCfg.listenAddress();
      int colon = addr.lastIndexOf(':');
      if (colon >= 0) {
        return Integer.parseInt(addr.substring(colon + 1));
      }
    }
    return 9100;
  }

  private static String loadToken(NodeConfig.Agent agentCfg) throws Exception {
    String env = System.getenv("CRYPTOPANNER_AGENT_TOKEN");
    if (env != null && !env.isBlank()) {
      return env;
    }
    if (agentCfg != null && agentCfg.tokenFile() != null && Files.exists(agentCfg.tokenFile())) {
      return Files.readString(agentCfg.tokenFile()).trim();
    }
    throw new IllegalStateException(
        "agent token not found: set CRYPTOPANNER_AGENT_TOKEN or agent.token_file");
  }
}
