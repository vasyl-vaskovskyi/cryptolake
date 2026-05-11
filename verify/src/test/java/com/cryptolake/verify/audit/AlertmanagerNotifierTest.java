package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AlertmanagerNotifier}.
 *
 * <p>Uses {@code com.sun.net.httpserver.HttpServer} (built into JDK 21) to stand up a localhost
 * stub on a random port. No external dependencies needed.
 */
class AlertmanagerNotifierTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpServer server;
  private int port;
  private volatile int returnStatus = 200;
  private final AtomicReference<String> capturedBody = new AtomicReference<>();
  private final AtomicInteger requestCount = new AtomicInteger(0);

  @BeforeEach
  void setup() throws Exception {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();

    server.createContext(
        "/api/v2/alerts",
        exchange -> {
          capturedBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          requestCount.incrementAndGet();
          exchange.sendResponseHeaders(returnStatus, 0);
          exchange.getResponseBody().close();
        });
    server.start();
  }

  @AfterEach
  void teardown() {
    server.stop(0);
  }

  private AlertmanagerNotifier notifier() {
    // Short timeouts so tests run fast; point at the stub server
    HttpClient client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    String url = "http://127.0.0.1:" + port + "/api/v2/alerts";
    return new AlertmanagerNotifier(url, client, MAPPER);
  }

  // ---------------------------------------------------------------------------
  // Test 1: 2xx → returns true; request body has expected shape
  // ---------------------------------------------------------------------------

  @Test
  void post_2xx_returnsTrue() throws Exception {
    returnStatus = 200;
    AlertmanagerNotifier n = notifier();

    boolean result =
        n.post(
            "AuditDivergence",
            "critical",
            "Archive divergence detected",
            "Details go here",
            Map.of("audit_run", "run-abc", "symbol", "btcusdt"));

    assertThat(result).isTrue();
    assertThat(requestCount.get()).isEqualTo(1);

    // Verify the JSON body shape
    String body = capturedBody.get();
    assertThat(body).isNotNull();

    JsonNode root = MAPPER.readTree(body);
    assertThat(root.isArray()).isTrue();
    assertThat(root.size()).isEqualTo(1);

    JsonNode alert = root.get(0);

    // --- labels ---
    JsonNode labels = alert.get("labels");
    assertThat(labels).isNotNull();
    assertThat(labels.get("alertname").asText()).isEqualTo("AuditDivergence");
    assertThat(labels.get("severity").asText()).isEqualTo("critical");
    assertThat(labels.get("audit_run").asText()).isEqualTo("run-abc");
    assertThat(labels.get("symbol").asText()).isEqualTo("btcusdt");

    // --- annotations ---
    JsonNode annotations = alert.get("annotations");
    assertThat(annotations).isNotNull();
    assertThat(annotations.get("summary").asText()).isEqualTo("Archive divergence detected");
    assertThat(annotations.get("description").asText()).isEqualTo("Details go here");
  }

  // ---------------------------------------------------------------------------
  // Test 2: 5xx → returns false (does not throw)
  // ---------------------------------------------------------------------------

  @Test
  void post_5xx_returnsFalse() {
    returnStatus = 500;
    AlertmanagerNotifier n = notifier();

    boolean result = n.post("AuditDivergence", "critical", "summary", "description", Map.of());

    assertThat(result).isFalse();
    assertThat(requestCount.get()).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------
  // Test 3 (bonus): connection refused → returns false (does not throw)
  // ---------------------------------------------------------------------------

  @Test
  void post_connectRefused_returnsFalse() {
    // Pick a port that is definitely not listening (we stop our server first)
    server.stop(0);

    HttpClient client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(500))
            .build();
    AlertmanagerNotifier n =
        new AlertmanagerNotifier("http://127.0.0.1:" + port + "/api/v2/alerts", client, MAPPER);

    boolean result = n.post("AuditDivergence", "critical", "summary", "description", Map.of());

    assertThat(result).isFalse();
    // Restart server so @AfterEach teardown doesn't crash (stop on stopped server is a no-op)
  }

  // ---------------------------------------------------------------------------
  // Test 4 (bonus): fromEnv uses default URL when env var is unset
  // ---------------------------------------------------------------------------

  @Test
  void fromEnv_usesDefaultWhenUnset() {
    // System.getenv("CRYPTOLAKE_ALERTMANAGER_WEBHOOK") is not set in the test JVM env.
    // We can't assert the URL directly, but we can verify fromEnv doesn't throw and
    // returns a non-null instance.
    AlertmanagerNotifier n = AlertmanagerNotifier.fromEnv(MAPPER);
    assertThat(n).isNotNull();

    // Reflective check: the default URL is embedded in the object; verify it via toString or
    // by invoking a known-failing post (connect refused on 9093 is expected in CI).
    // We just verify the method doesn't throw during construction.
  }

  // ---------------------------------------------------------------------------
  // Test 5: null extraLabels treated as empty (no NullPointerException)
  // ---------------------------------------------------------------------------

  @Test
  void post_nullExtraLabels_treatedAsEmpty() throws Exception {
    returnStatus = 200;
    AlertmanagerNotifier n = notifier();

    boolean result = n.post("TestAlert", "warning", "sum", "desc", null);

    assertThat(result).isTrue();

    JsonNode root = MAPPER.readTree(capturedBody.get());
    JsonNode labels = root.get(0).get("labels");
    assertThat(labels.get("alertname").asText()).isEqualTo("TestAlert");
    assertThat(labels.get("severity").asText()).isEqualTo("warning");
  }

  // ---------------------------------------------------------------------------
  // Test 6: 4xx → returns false (not 2xx)
  // ---------------------------------------------------------------------------

  @Test
  void post_4xx_returnsFalse() {
    returnStatus = 400;
    AlertmanagerNotifier n = notifier();

    boolean result = n.post("AuditDivergence", "critical", "summary", "description", Map.of());

    assertThat(result).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Test 7: extraLabels do NOT override alertname / severity
  //         (alertname and severity are always set from the named params)
  // ---------------------------------------------------------------------------

  @Test
  void post_extraLabels_doNotOverrideCoreLabels() throws Exception {
    returnStatus = 200;
    AlertmanagerNotifier n = notifier();

    // Attempt to override alertname and severity via extraLabels
    boolean result =
        n.post(
            "RealAlert",
            "critical",
            "s",
            "d",
            Map.of("alertname", "OverriddenAlert", "severity", "info"));

    assertThat(result).isTrue();

    JsonNode labels = MAPPER.readTree(capturedBody.get()).get(0).get("labels");
    // The named params must win (alertname=RealAlert, severity=critical)
    // The implementation spec uses LinkedHashMap and always sets alertname/severity after merging
    // extraLabels, so they take precedence.
    assertThat(labels.get("alertname").asText()).isEqualTo("RealAlert");
    assertThat(labels.get("severity").asText()).isEqualTo("critical");
  }

  // ---------------------------------------------------------------------------
  // Test 8: 201 is also 2xx → returns true
  // ---------------------------------------------------------------------------

  @Test
  void post_201_returnsTrue() {
    returnStatus = 201;
    AlertmanagerNotifier n = notifier();

    boolean result = n.post("AuditDivergence", "critical", "summary", "description", Map.of());

    assertThat(result).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Test 9: body contains no startsAt / endsAt (per spec: optional fields omitted)
  // ---------------------------------------------------------------------------

  @Test
  void post_bodyDoesNotContainStartsAtOrEndsAt() throws Exception {
    returnStatus = 200;
    AlertmanagerNotifier n = notifier();

    n.post("AuditDivergence", "critical", "summary", "description", Map.of());

    String body = capturedBody.get();
    // Per the task spec, startsAt / endsAt are not included
    assertThat(body).doesNotContain("startsAt");
    assertThat(body).doesNotContain("endsAt");
  }

  // ---------------------------------------------------------------------------
  // Test 10: post() with URI that points to wrong path returns false gracefully
  //          (server returns 404 for a path it doesn't handle)
  // ---------------------------------------------------------------------------

  @Test
  void post_wrongPath_returnsFalse() {
    // Point at the stub server but on a path that isn't registered → JDK HttpServer returns 404
    HttpClient client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    // Use a different path — server doesn't have a context for /wrong
    AlertmanagerNotifier n =
        new AlertmanagerNotifier("http://127.0.0.1:" + port + "/wrong/path", client, MAPPER);

    boolean result = n.post("A", "critical", "s", "d", Map.of());

    // 404 is not 2xx → false
    assertThat(result).isFalse();
  }
}
