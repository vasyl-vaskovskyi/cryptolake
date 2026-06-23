package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/** §11.c — parsing the Node Agent's {@code /status} JSON into a typed snapshot. */
class StatusSnapshotTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();

  // The §11.c example payload verbatim.
  private static final String STATUS =
      """
      {
        "node": "vps-fra-1",
        "scraped_at": "2026-06-12T14:23:50Z",
        "components": {
          "cryptopanner-collector@a": { "state": "running",  "pid": 1234, "heartbeat_age_s": 2.1, "uptime_s": 83412 },
          "cryptopanner-collector@b": { "state": "down",     "pid": null, "heartbeat_age_s": null, "uptime_s": 0 },
          "cryptopanner-sealer":      { "state": "running",  "pid": 1235, "heartbeat_age_s": 3.4, "uptime_s": 83410 },
          "cryptopanner-uploader":    { "state": "running",  "pid": 1236, "heartbeat_age_s": 1.9, "uptime_s": 83410 },
          "cryptopanner-agent":       { "state": "running",  "pid": 1237, "heartbeat_age_s": 0.5, "uptime_s": 83410 }
        },
        "active_slot": "a",
        "fs_heavy_lock": { "held_by": null },
        "deploy":       { "state": "IDLE" },
        "rotation":     { "state": "IDLE", "current_connection_age_s": 76800 },
        "vps": {
          "cpu_percent": 14.2,
          "memory_percent": 38.0,
          "load_average_1m": 0.42,
          "disk": {
            "/":     { "percent": 22.3, "free_bytes": 30400000000 },
            "/data": { "percent": 41.8, "free_bytes": 41200000000 }
          }
        }
      }
      """;

  @Test
  void parsesTopLevelIdentityAndSlot() throws Exception {
    StatusSnapshot s = StatusSnapshot.parse(STATUS, MAPPER);
    assertEquals("vps-fra-1", s.node());
    assertEquals("a", s.activeSlot());
    assertNull(s.fsHeavyLockHeldBy());
    assertEquals("IDLE", s.deployState());
    assertEquals("IDLE", s.rotationState());
    assertEquals(76800.0, s.currentConnectionAgeS());
  }

  @Test
  void parsesComponents() throws Exception {
    StatusSnapshot s = StatusSnapshot.parse(STATUS, MAPPER);
    assertEquals(5, s.components().size());

    StatusSnapshot.Component a = s.components().get("cryptopanner-collector@a");
    assertEquals("running", a.state());
    assertEquals(1234, a.pid());
    assertEquals(2.1, a.heartbeatAgeS());

    StatusSnapshot.Component b = s.components().get("cryptopanner-collector@b");
    assertEquals("down", b.state());
    assertNull(b.pid());
    assertNull(b.heartbeatAgeS());
  }

  @Test
  void parsesDiskPressureForData() throws Exception {
    StatusSnapshot s = StatusSnapshot.parse(STATUS, MAPPER);
    assertEquals(41.8, s.diskPercent("/data"));
    assertEquals(22.3, s.diskPercent("/"));
  }

  @Test
  void heldByLockSurfacesHolderName() throws Exception {
    String json = STATUS.replace("\"held_by\": null", "\"held_by\": \"cryptopanner-sealer\"");
    StatusSnapshot s = StatusSnapshot.parse(json, MAPPER);
    assertEquals("cryptopanner-sealer", s.fsHeavyLockHeldBy());
  }

  @Test
  void activeCollectorComponentResolvesFromActiveSlot() throws Exception {
    StatusSnapshot s = StatusSnapshot.parse(STATUS, MAPPER);
    assertEquals("cryptopanner-collector@a", s.activeCollectorComponent());
    assertTrue(s.components().containsKey(s.activeCollectorComponent()));
  }
}
