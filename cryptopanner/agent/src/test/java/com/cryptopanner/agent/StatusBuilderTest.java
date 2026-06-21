package com.cryptopanner.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.Heartbeat;
import com.cryptopanner.common.SlotManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StatusBuilderTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final StatusBuilder builder =
      new StatusBuilder(mapper, "vps-fra-1", Duration.ofSeconds(15), Duration.ofSeconds(60));

  @Test
  void buildsNodeStatusWithComponentStatesAndActiveSlot(@TempDir Path dir) throws Exception {
    Path sealerHb = dir.resolve("sealer.heartbeat");
    Heartbeat.touch(sealerHb); // fresh → running
    Path collHb = dir.resolve("collector.heartbeat"); // never touched, but systemd active → stuck

    String json =
        builder.build(
            List.of(
                new StatusBuilder.Component("cryptopanner-sealer", sealerHb, true),
                new StatusBuilder.Component("cryptopanner-collector@a", collHb, true),
                new StatusBuilder.Component(
                    "cryptopanner-uploader", dir.resolve("u.heartbeat"), false)),
            SlotManager.Slot.A,
            Instant.now());

    JsonNode root = mapper.readTree(json);
    assertEquals("vps-fra-1", root.get("node").asText());
    assertEquals("a", root.get("active_slot").asText());
    JsonNode comps = root.get("components");
    assertEquals("running", comps.get("cryptopanner-sealer").get("state").asText());
    assertEquals("stuck", comps.get("cryptopanner-collector@a").get("state").asText());
    assertEquals("down", comps.get("cryptopanner-uploader").get("state").asText());
    assertTrue(comps.get("cryptopanner-sealer").get("heartbeat_age_s").isNumber());
    assertTrue(
        comps.get("cryptopanner-uploader").get("heartbeat_age_s").isNull(),
        "a down/no-heartbeat component reports null age");
  }
}
