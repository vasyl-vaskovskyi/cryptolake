package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.testutil.StubHttpServer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §11.c — the Monitor scraping one node's {@code /status} + {@code /metrics}. */
class NodeScraperTest {

  private static final String STATUS =
      """
      {
        "node": "vps-fra-1",
        "components": { "cryptopanner-sealer": { "state": "running", "pid": 7, "heartbeat_age_s": 1.0, "uptime_s": 10 } },
        "active_slot": "a",
        "fs_heavy_lock": { "held_by": null },
        "deploy": { "state": "IDLE" },
        "rotation": { "state": "IDLE", "current_connection_age_s": 100 },
        "vps": { "disk": { "/data": { "percent": 50.0, "free_bytes": 1 } } }
      }
      """;
  private static final String METRICS = "cryptopanner_sealed_files_pending_upload 2\n";

  private StubHttpServer agent;
  private NodeScraper scraper;

  @BeforeEach
  void setUp() throws Exception {
    agent = new StubHttpServer().route("/status", 200, STATUS).route("/metrics", 200, METRICS);
    scraper = new NodeScraper(NodeScraper.newHttpClient(), EnvelopeCodec.newMapper());
  }

  @AfterEach
  void tearDown() {
    agent.close();
  }

  private MonitorConfig.Node node(String endpoint) {
    return new MonitorConfig.Node("vps-fra-1", endpoint, Path.of("/dev/null"));
  }

  @Test
  void successfulScrapeParsesStatusAndMetrics() {
    ScrapeResult r = scraper.poll(node(agent.endpoint()));
    assertTrue(r.ok());
    assertNotNull(r.status());
    assertEquals("vps-fra-1", r.status().node());
    assertEquals(2.0, r.metrics().sealedFilesPendingUpload().orElseThrow());
  }

  @Test
  void nonOkStatusCodeIsFailure() {
    agent.route("/status", 500, "boom");
    ScrapeResult r = scraper.poll(node(agent.endpoint()));
    assertFalse(r.ok());
    assertNotNull(r.error());
  }

  @Test
  void unreachableEndpointIsFailure() {
    // Nothing is listening on this port.
    ScrapeResult r = scraper.poll(node("127.0.0.1:1"));
    assertFalse(r.ok());
    assertNotNull(r.error());
  }
}
