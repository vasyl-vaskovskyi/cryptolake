package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManifestWriterTest {

  @Test
  void writesPrettyPrintedManifestWithExpectedFields(@TempDir Path tmp) throws IOException {
    Path target = tmp.resolve("hour-14.manifest.json");
    HourMerger.Result merge =
        new HourMerger.Result(
            tmp.resolve("hour-14.jsonl.zst"), "deadbeef".repeat(8), 12345L, 42L, List.of(0, 1, 2));

    ManifestWriter.write(
        target,
        "dev-node",
        "btcusdt",
        "trade",
        Instant.parse("2026-06-14T14:00:00Z"),
        merge,
        Instant.parse("2026-06-14T15:02:08Z"));

    String contents = Files.readString(target);
    assertTrue(contents.contains("\n"), "must be pretty-printed");
    JsonNode root = new ObjectMapper().readTree(contents);
    assertEquals(1, root.get("manifest_schema_version").asInt());
    assertEquals("dev-node", root.get("node").asText());
    assertEquals("btcusdt", root.get("symbol").asText());
    assertEquals("trade", root.get("stream").asText());
    assertEquals("2026-06-14", root.get("date").asText());
    assertEquals(14, root.get("hour").asInt());
    assertEquals(3, root.get("minutes_present").size());
    assertEquals(42L, root.get("record_count").asLong());
  }
}
