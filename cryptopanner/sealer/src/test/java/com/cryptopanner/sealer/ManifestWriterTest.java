package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
  void writesPrettyPrintedManifestWithBackfillFields(@TempDir Path tmp) throws IOException {
    Path target = tmp.resolve("hour-14.manifest.json");
    HourMerger.Result merge =
        new HourMerger.Result(
            tmp.resolve("hour-14.jsonl.zst"),
            "deadbeef".repeat(8),
            12345L,
            42L,
            List.of(0, 1, 2),
            new SequenceAnalyzer.Analysis(
                100L, 142L, List.of(new SequenceAnalyzer.Gap(110L, 110L, 1L))),
            List.of(RestBackfiller.Outcome.FILLED),
            List.of(
                new BackfillAttempt(
                    "/fapi/v1/historicalTrades",
                    110L,
                    110L,
                    1,
                    200,
                    1,
                    RestBackfiller.Outcome.FILLED,
                    null)));

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
    assertEquals("btcusdt", root.get("symbol").asText());
    assertEquals("trade", root.get("stream").asText());
    assertEquals(42L, root.get("record_count").asLong());

    JsonNode range = root.get("sequence_id_range");
    assertEquals(100L, range.get("first").asLong());
    assertEquals(142L, range.get("last").asLong());

    JsonNode gaps = root.get("sequence_gaps");
    assertEquals(1, gaps.size());
    assertEquals(110L, gaps.get(0).get("from").asLong());
    assertEquals(110L, gaps.get(0).get("to").asLong());
    assertEquals(1L, gaps.get(0).get("count").asLong());
    assertEquals("FILLED", gaps.get(0).get("backfill_outcome").asText());

    JsonNode attempts = root.get("backfill_attempts");
    assertEquals(1, attempts.size());
    JsonNode att = attempts.get(0);
    assertEquals("/fapi/v1/historicalTrades", att.get("endpoint").asText());
    assertEquals(110L, att.get("from_id").asLong());
    assertEquals(110L, att.get("to_id").asLong());
    assertEquals(1, att.get("attempts").asInt());
    assertEquals(200, att.get("http_status").asInt());
    assertEquals(1, att.get("records_inserted").asInt());
    assertEquals("FILLED", att.get("outcome").asText());
    assertFalse(att.has("error"));
  }

  @Test
  void notAttemptedWhenBackfillerAbsent(@TempDir Path tmp) throws IOException {
    Path target = tmp.resolve("hour-14.manifest.json");
    HourMerger.Result merge =
        new HourMerger.Result(
            tmp.resolve("hour-14.jsonl.zst"),
            "deadbeef".repeat(8),
            12345L,
            42L,
            List.of(0),
            new SequenceAnalyzer.Analysis(
                100L, 142L, List.of(new SequenceAnalyzer.Gap(110L, 110L, 1L))),
            List.of(RestBackfiller.Outcome.NOT_ATTEMPTED),
            List.of());

    ManifestWriter.write(
        target,
        "dev-node",
        "btcusdt",
        "trade",
        Instant.parse("2026-06-14T14:00:00Z"),
        merge,
        Instant.parse("2026-06-14T15:02:08Z"));

    JsonNode root = new ObjectMapper().readTree(Files.readString(target));
    assertEquals(
        "NOT_ATTEMPTED", root.get("sequence_gaps").get(0).get("backfill_outcome").asText());
    assertEquals(0, root.get("backfill_attempts").size());
  }

  @Test
  void attachesDeployAndRotationEventsWhenPresentAndOmitsWhenEmpty(@TempDir Path tmp)
      throws IOException {
    ObjectMapper m = new ObjectMapper();
    HourMerger.Result merge =
        new HourMerger.Result(
            tmp.resolve("hour-14.jsonl.zst"),
            "ab".repeat(32),
            1L,
            1L,
            List.of(0),
            null,
            List.of(),
            List.of());

    // With events → arrays present.
    Path withEvents = tmp.resolve("hour-14.manifest.json");
    JsonNode rot = m.readTree("{\"rotation_id\":\"rot-1\",\"reason\":\"SCHEDULED\"}");
    JsonNode dep = m.readTree("{\"deploy_id\":\"d-1\",\"verify_result\":\"PASS\"}");
    ManifestWriter.write(
        withEvents,
        "dev-node",
        "btcusdt",
        "ticker",
        Instant.parse("2026-06-14T14:00:00Z"),
        merge,
        Instant.parse("2026-06-14T15:02:08Z"),
        List.of(dep),
        List.of(rot));
    JsonNode root = m.readTree(Files.readString(withEvents));
    assertEquals("d-1", root.get("deploy_events").get(0).get("deploy_id").asText());
    assertEquals(
        "rot-1", root.get("connection_rotation_events").get(0).get("rotation_id").asText());

    // Without events (empty lists) → arrays absent, not empty (§6).
    Path noEvents = tmp.resolve("hour-15.manifest.json");
    ManifestWriter.write(
        noEvents,
        "dev-node",
        "btcusdt",
        "ticker",
        Instant.parse("2026-06-14T14:00:00Z"),
        merge,
        Instant.parse("2026-06-14T15:02:08Z"),
        List.of(),
        List.of());
    JsonNode root2 = m.readTree(Files.readString(noEvents));
    assertFalse(root2.has("deploy_events"));
    assertFalse(root2.has("connection_rotation_events"));
  }

  @Test
  void omitsSequenceFieldsForNonIdBearingStream(@TempDir Path tmp) throws IOException {
    Path target = tmp.resolve("hour-14.manifest.json");
    HourMerger.Result merge =
        new HourMerger.Result(
            tmp.resolve("hour-14.jsonl.zst"),
            "deadbeef".repeat(8),
            999L,
            7L,
            List.of(0),
            null,
            List.of(),
            List.of());

    ManifestWriter.write(
        target,
        "dev-node",
        "btcusdt",
        "ticker",
        Instant.parse("2026-06-14T14:00:00Z"),
        merge,
        Instant.parse("2026-06-14T15:02:08Z"));

    JsonNode root = new ObjectMapper().readTree(Files.readString(target));
    assertEquals("ticker", root.get("stream").asText());
    assertFalse(root.has("sequence_id_range"));
    assertFalse(root.has("sequence_gaps"));
    assertFalse(root.has("backfill_attempts"));
  }
}
