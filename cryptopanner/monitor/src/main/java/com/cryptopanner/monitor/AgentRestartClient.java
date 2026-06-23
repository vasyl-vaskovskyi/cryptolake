package com.cryptopanner.monitor;

import com.cryptopanner.common.config.MonitorConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * The real {@link RestartClient}: {@code POST http://<endpoint>/restart/<component>} authenticated
 * with the node's bearer token (read from {@link MonitorConfig.Node#tokenFile()}) and an {@code
 * X-Timestamp} the Node Agent's {@code BearerAuth} validates within a ±30s window (§6.f). The clock
 * is injected so the timestamp is deterministic in tests. Any failure (missing token, transport
 * error, non-2xx) returns false so the orchestrator records it as a failed attempt.
 */
public final class AgentRestartClient implements RestartClient {

  private final HttpClient http;
  private final Supplier<Instant> clock;

  public AgentRestartClient(HttpClient http, Supplier<Instant> clock) {
    this.http = http;
    this.clock = clock;
  }

  @Override
  public boolean restart(MonitorConfig.Node node, String component) {
    try {
      String token = node.tokenFile() == null ? "" : Files.readString(node.tokenFile()).strip();
      HttpRequest req =
          HttpRequest.newBuilder(URI.create("http://" + node.endpoint() + "/restart/" + component))
              .timeout(Duration.ofSeconds(10))
              .header("Authorization", "Bearer " + token)
              .header("X-Timestamp", clock.get().toString())
              .POST(HttpRequest.BodyPublishers.noBody())
              .build();
      HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
      return resp.statusCode() / 100 == 2;
    } catch (Exception e) {
      return false;
    }
  }
}
