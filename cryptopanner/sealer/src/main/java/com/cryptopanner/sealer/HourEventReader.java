package com.cryptopanner.sealer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads deploy/rotation event records (JSON Lines) and returns those whose {@code promoted_at}
 * falls inside a given hour. The Sealer uses this to fold {@code deploy_events[]} and {@code
 * connection_rotation_events[]} into the hour's manifest (design doc §6, master spec §9.b.6). Lines
 * that don't parse or lack a valid {@code promoted_at} are skipped (best-effort, log-tolerant).
 */
public final class HourEventReader {

  private HourEventReader() {}

  public static List<JsonNode> readWithinHour(Path jsonl, Instant hourStart, ObjectMapper mapper)
      throws IOException {
    List<JsonNode> out = new ArrayList<>();
    if (!Files.exists(jsonl)) {
      return out;
    }
    Instant hourEnd = hourStart.plus(Duration.ofHours(1));
    for (String line : Files.readAllLines(jsonl)) {
      if (line.isBlank()) {
        continue;
      }
      JsonNode node;
      try {
        node = mapper.readTree(line);
      } catch (Exception e) {
        continue; // skip malformed line
      }
      JsonNode promotedAt = node.get("promoted_at");
      if (promotedAt == null || !promotedAt.isTextual()) {
        continue;
      }
      Instant ts;
      try {
        ts = Instant.parse(promotedAt.asText());
      } catch (Exception e) {
        continue;
      }
      if (!ts.isBefore(hourStart) && ts.isBefore(hourEnd)) {
        out.add(node);
      }
    }
    return out;
  }
}
