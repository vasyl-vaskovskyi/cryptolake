package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StructuredLogTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void writesJsonLineWithRequiredFieldsPlusSlotAndDomainFields(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("logs/cryptopanner-collector@a.jsonl");
    StructuredLog logger =
        new StructuredLog(
            log,
            "cryptopanner-collector",
            "a",
            mapper,
            () -> Instant.parse("2026-06-23T14:23:47.512Z"));

    logger.info("ws_connect", Map.of("stream_count", 141, "endpoint", "wss://x/public"));

    JsonNode e = mapper.readTree(Files.readAllLines(log).get(0));
    assertEquals("2026-06-23T14:23:47.512Z", e.get("ts").asText());
    assertEquals("cryptopanner-collector", e.get("component").asText());
    assertEquals("a", e.get("slot").asText(), "slot required for collector logs (§11.a)");
    assertEquals("ws_connect", e.get("event").asText());
    assertEquals("INFO", e.get("level").asText());
    assertEquals(141, e.get("stream_count").asInt());
    assertEquals("wss://x/public", e.get("endpoint").asText());
  }

  @Test
  void omitsSlotWhenNull(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("cryptopanner-sealer.jsonl");
    StructuredLog logger =
        new StructuredLog(log, "cryptopanner-sealer", null, mapper, () -> Instant.EPOCH);

    logger.info("hour_sealed", Map.of());

    JsonNode e = mapper.readTree(Files.readAllLines(log).get(0));
    assertFalse(e.has("slot"), "non-collector components omit slot");
  }

  @Test
  void errorLevelIsMirroredToCriticalEventsSidecar(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("logs/cryptopanner-collector@a.jsonl");
    StructuredLog logger =
        new StructuredLog(log, "cryptopanner-collector", "a", mapper, () -> Instant.EPOCH);

    logger.error("rotation_verify_failed", Map.of("rotation_id", "rot-9"));

    Path critical = log.resolveSibling("critical-events.jsonl");
    assertTrue(Files.exists(critical), "ERROR entries mirror to critical-events.jsonl (§11.a)");
    JsonNode e = mapper.readTree(Files.readAllLines(critical).get(0));
    assertEquals("rotation_verify_failed", e.get("event").asText());
    assertEquals("ERROR", e.get("level").asText());
  }

  @Test
  void appendsOneLinePerEvent(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("c.jsonl");
    StructuredLog logger = new StructuredLog(log, "c", null, mapper, () -> Instant.EPOCH);
    logger.info("a", Map.of());
    logger.info("b", Map.of());
    List<String> lines = Files.readAllLines(log);
    assertEquals(2, lines.size());
  }
}
