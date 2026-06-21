package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RotationsLogTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void appendsEventAsJsonLineCreatingFileAndParent(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("deploy/rotations.jsonl");
    RotationsLog.append(
        log,
        mapper,
        new RotationsLog.RotationEvent(
            "rot-1",
            "SCHEDULED",
            22.93,
            Instant.parse("2026-06-09T02:33:00Z"),
            List.of(32),
            "PASS"));

    assertTrue(Files.exists(log));
    List<String> lines = Files.readAllLines(log);
    assertEquals(1, lines.size());
    JsonNode e = mapper.readTree(lines.get(0));
    assertEquals("rot-1", e.get("rotation_id").asText());
    assertEquals("SCHEDULED", e.get("reason").asText());
    assertEquals("2026-06-09T02:33:00Z", e.get("promoted_at").asText());
    assertEquals(32, e.get("minutes_merged").get(0).asInt());
    assertEquals("PASS", e.get("verify_result").asText());
  }

  @Test
  void appendsAreAdditive(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("rotations.jsonl");
    RotationsLog.append(
        log,
        mapper,
        new RotationsLog.RotationEvent(
            "rot-1",
            "SCHEDULED",
            23.0,
            Instant.parse("2026-06-09T02:33:00Z"),
            List.of(32),
            "PASS"));
    RotationsLog.append(
        log,
        mapper,
        new RotationsLog.RotationEvent(
            "rot-2",
            "EMERGENCY",
            23.8,
            Instant.parse("2026-06-09T03:50:00Z"),
            List.of(50),
            "FORCED"));
    assertEquals(2, Files.readAllLines(log).size());
  }
}
