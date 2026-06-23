package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.monitor.testutil.Snapshots;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §11.d — the Monitor's dashboard + /api/nodes HTTP surface. */
class MonitorServerTest {

  private MonitorServer server;
  private final AtomicReference<List<NodeView>> view = new AtomicReference<>(List.of());
  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void setUp() throws Exception {
    server = new MonitorServer(0, EnvelopeCodec.newMapper(), view::get, 5);
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  private HttpResponse<String> get(String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
            .GET()
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private NodeView healthy(String id) {
    StatusSnapshot s =
        Snapshots.status(
            id,
            "a",
            Map.of("cryptopanner-collector@a", "running", "cryptopanner-sealer", "running"),
            null,
            100);
    return NodeView.of(id, s, false, List.of());
  }

  @Test
  void apiNodesReturnsJsonForEachNode() throws Exception {
    view.set(List.of(healthy("vps-fra-1"), NodeView.unreachable("vps-tyo-1", "timeout")));
    HttpResponse<String> r = get("/api/nodes");
    assertEquals(200, r.statusCode());
    assertTrue(r.body().contains("vps-fra-1"), r.body());
    assertTrue(r.body().contains("vps-tyo-1"), r.body());
    assertTrue(r.body().contains("\"reachable\":false"), r.body());
  }

  @Test
  void dashboardRendersHtmlWithRefreshAndNodes() throws Exception {
    view.set(List.of(healthy("vps-fra-1")));
    HttpResponse<String> r = get("/dashboard");
    assertEquals(200, r.statusCode());
    assertTrue(
        r.headers().firstValue("Content-Type").orElse("").contains("text/html"),
        r.headers().toString());
    assertTrue(r.body().contains("http-equiv=\"refresh\""), r.body());
    assertTrue(r.body().contains("content=\"5\""), r.body());
    assertTrue(r.body().contains("vps-fra-1"), r.body());
  }

  @Test
  void dashboardHighlightsUnreachableNode() throws Exception {
    view.set(List.of(NodeView.unreachable("vps-tyo-1", "timeout")));
    HttpResponse<String> r = get("/dashboard");
    assertTrue(r.body().contains("UNREACHABLE"), r.body());
    assertTrue(r.body().contains("vps-tyo-1"), r.body());
  }

  @Test
  void dashboardShowsDeployRotationBanner() throws Exception {
    StatusSnapshot rotating = Snapshots.statusWithDeployRotation("vps-fra-1", "OVERLAP_VERIFYING");
    view.set(List.of(NodeView.of("vps-fra-1", rotating, false, List.of())));
    HttpResponse<String> r = get("/dashboard");
    assertTrue(r.body().contains("OVERLAP_VERIFYING"), r.body());
  }

  @Test
  void ownStatusEndpointReturnsOk() throws Exception {
    assertEquals(200, get("/status").statusCode());
  }
}
