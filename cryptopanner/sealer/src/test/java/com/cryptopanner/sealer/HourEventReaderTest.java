package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HourEventReaderTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final Instant hour14 = Instant.parse("2026-06-09T14:00:00Z");

  @Test
  void emptyWhenFileAbsent(@TempDir Path dir) throws Exception {
    assertEquals(
        List.of(), HourEventReader.readWithinHour(dir.resolve("rotations.jsonl"), hour14, mapper));
  }

  @Test
  void returnsOnlyEventsWithPromotedAtInsideTheHour(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("rotations.jsonl");
    Files.writeString(
        log,
        "{\"rotation_id\":\"r1\",\"promoted_at\":\"2026-06-09T13:59:59Z\"}\n" // before hour
            + "{\"rotation_id\":\"r2\",\"promoted_at\":\"2026-06-09T14:33:00Z\"}\n" // in hour
            + "{\"rotation_id\":\"r3\",\"promoted_at\":\"2026-06-09T15:00:00Z\"}\n"); // next hour

    List<JsonNode> events = HourEventReader.readWithinHour(log, hour14, mapper);
    assertEquals(1, events.size());
    assertEquals("r2", events.get(0).get("rotation_id").asText());
  }

  @Test
  void skipsUnparseableLines(@TempDir Path dir) throws Exception {
    Path log = dir.resolve("history.jsonl");
    Files.writeString(
        log, "not json\n{\"deploy_id\":\"d1\",\"promoted_at\":\"2026-06-09T14:10:00Z\"}\n");
    List<JsonNode> events = HourEventReader.readWithinHour(log, hour14, mapper);
    assertEquals(1, events.size());
    assertEquals("d1", events.get(0).get("deploy_id").asText());
  }
}
