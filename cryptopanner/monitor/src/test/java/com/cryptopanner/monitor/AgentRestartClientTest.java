package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.testutil.StubHttpServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** §13.b — the authenticated {@code POST /restart/<component>} call against a Node Agent. */
class AgentRestartClientTest {

  private StubHttpServer agent;
  private Path tokenFile;

  @BeforeEach
  void setUp(@TempDir Path dir) throws Exception {
    agent = new StubHttpServer().route("/restart/cryptopanner-sealer", 200, "restarted\n");
    tokenFile = dir.resolve("token");
    Files.writeString(tokenFile, "s3cr3t-token");
  }

  @AfterEach
  void tearDown() {
    agent.close();
  }

  private MonitorConfig.Node node() {
    return new MonitorConfig.Node("vps-fra-1", agent.endpoint(), tokenFile);
  }

  @Test
  void postsRestartWithBearerAndTimestamp() {
    AgentRestartClient client =
        new AgentRestartClient(
            NodeScraper.newHttpClient(), () -> Instant.parse("2026-06-23T00:00:00Z"));
    assertTrue(client.restart(node(), "cryptopanner-sealer"));

    StubHttpServer.Received r = agent.received().get(0);
    assertTrue(r.path().equals("/restart/cryptopanner-sealer"));
    // com.sun.net.httpserver normalizes header display keys (e.g. "X-timestamp"); match
    // case-insensitively as the agent's Headers map does.
    assertTrue(
        headerIgnoreCase(r, "Authorization").equals("Bearer s3cr3t-token"), r.headers().toString());
    assertTrue(headerIgnoreCase(r, "X-Timestamp").length() > 0, r.headers().toString());
  }

  private static String headerIgnoreCase(StubHttpServer.Received r, String name) {
    return r.headers().entrySet().stream()
        .filter(e -> e.getKey().equalsIgnoreCase(name))
        .map(java.util.Map.Entry::getValue)
        .findFirst()
        .orElse("");
  }

  @Test
  void nonOkResponseReturnsFalse() {
    agent.route("/restart/cryptopanner-sealer", 503, "no");
    AgentRestartClient client = new AgentRestartClient(NodeScraper.newHttpClient(), Instant::now);
    assertFalse(client.restart(node(), "cryptopanner-sealer"));
  }

  @Test
  void unreachableAgentReturnsFalse() {
    MonitorConfig.Node dead = new MonitorConfig.Node("x", "127.0.0.1:1", tokenFile);
    AgentRestartClient client = new AgentRestartClient(NodeScraper.newHttpClient(), Instant::now);
    assertFalse(client.restart(dead, "cryptopanner-sealer"));
  }
}
