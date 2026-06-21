package com.cryptopanner.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AgentServerTest {

  private static final String TOKEN = "test-token";
  private AgentServer server;
  private final CopyOnWriteArrayList<String> restarts = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> rotations = new CopyOnWriteArrayList<>();
  private final HttpClient http = HttpClient.newHttpClient();

  @BeforeEach
  void setUp() throws Exception {
    server =
        new AgentServer(
            0,
            new BearerAuth(TOKEN),
            () -> "{\"node\":\"test\",\"components\":{}}",
            () -> "# TYPE cryptopanner_up gauge\ncryptopanner_up 1\n",
            component -> {
              restarts.add(component);
              return true;
            },
            () -> {
              rotations.add("triggered");
              return true;
            });
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  private URI uri(String path) {
    return URI.create("http://127.0.0.1:" + server.port() + path);
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> post(String path, String token, String timestamp) throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody());
    if (token != null) b.header("Authorization", "Bearer " + token);
    if (timestamp != null) b.header("X-Timestamp", timestamp);
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void statusIsServedWithoutAuth() throws Exception {
    HttpResponse<String> r = get("/status");
    assertEquals(200, r.statusCode());
    assertTrue(r.body().contains("\"node\":\"test\""));
  }

  @Test
  void metricsIsServedWithoutAuth() throws Exception {
    HttpResponse<String> r = get("/metrics");
    assertEquals(200, r.statusCode());
    assertTrue(r.body().contains("cryptopanner_up"));
  }

  @Test
  void restartRequiresAuth() throws Exception {
    HttpResponse<String> r = post("/restart/sealer", null, null);
    assertEquals(401, r.statusCode());
    assertEquals(List.of(), restarts, "no restart invoked without auth");
  }

  @Test
  void restartWithBadTokenIsRejected() throws Exception {
    HttpResponse<String> r = post("/restart/sealer", "wrong", Instant.now().toString());
    assertEquals(401, r.statusCode());
    assertEquals(List.of(), restarts);
  }

  @Test
  void restartWithValidAuthInvokesHandler() throws Exception {
    HttpResponse<String> r = post("/restart/sealer", TOKEN, Instant.now().toString());
    assertEquals(200, r.statusCode());
    assertEquals(List.of("sealer"), restarts);
  }

  @Test
  void restartParsesCollectorSlotComponent() throws Exception {
    post("/restart/collector/a", TOKEN, Instant.now().toString());
    assertEquals(List.of("collector/a"), restarts);
  }

  @Test
  void rotationTriggerWithValidAuthInvokesHandler() throws Exception {
    HttpResponse<String> r = post("/rotation/trigger", TOKEN, Instant.now().toString());
    assertEquals(200, r.statusCode());
    assertEquals(List.of("triggered"), rotations);
  }
}
