package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.DurableSegment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RotationExecutorTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final RotationExecutor executor = new RotationExecutor(mapper);

  private String trade(long id) {
    return CaptureEnvelope.wsFrame(
            mapper,
            "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":" + id + ",\"T\":" + id + "}}",
            Instant.EPOCH)
        + "\n";
  }

  private TreeSet<Long> idsIn(Path zst) throws Exception {
    TreeSet<Long> ids = new TreeSet<>();
    for (String line : DurableSegment.readLines(zst)) {
      ids.add(CaptureEnvelope.unwrap(mapper, mapper.readTree(line)).get("data").get("t").asLong());
    }
    return ids;
  }

  @Test
  void passingEquivalenceMergesPromotesAndLogs(@TempDir Path dir) throws Exception {
    Path primary = dir.resolve("minute-14-23.jsonl.zst");
    Path shadow = dir.resolve("minute-14-23.shadow.jsonl.zst");
    Path rotations = dir.resolve("deploy/rotations.jsonl");
    DurableSegment.writeLines(primary, List.of(trade(1), trade(2)));
    DurableSegment.writeLines(shadow, List.of(trade(2), trade(3))); // edge straddle → PASS

    RotationExecutor.Outcome out =
        executor.cutoverMinute(
            "trade",
            primary,
            shadow,
            rotations,
            "rot-1",
            "SCHEDULED",
            23.0,
            Instant.parse("2026-06-14T14:24:00Z"),
            23,
            false);

    assertEquals("PASS", out.verifyResult());
    assertEquals(new TreeSet<>(List.of(1L, 2L, 3L)), idsIn(primary), "primary holds the union");
    assertFalse(Files.exists(shadow), "shadow consumed");
    JsonNode ev = mapper.readTree(Files.readAllLines(rotations).get(0));
    assertEquals("rot-1", ev.get("rotation_id").asText());
    assertEquals("PASS", ev.get("verify_result").asText());
  }

  @Test
  void failingEquivalenceWithoutForceDoesNotMerge(@TempDir Path dir) throws Exception {
    Path primary = dir.resolve("minute-14-23.jsonl.zst");
    Path shadow = dir.resolve("minute-14-23.shadow.jsonl.zst");
    Path rotations = dir.resolve("rotations.jsonl");
    DurableSegment.writeLines(primary, List.of(trade(1), trade(2), trade(3)));
    DurableSegment.writeLines(shadow, List.of(trade(1), trade(3))); // interior hole at 2 → FAIL

    RotationExecutor.Outcome out =
        executor.cutoverMinute(
            "trade",
            primary,
            shadow,
            rotations,
            "rot-2",
            "SCHEDULED",
            23.0,
            Instant.parse("2026-06-14T14:24:00Z"),
            23,
            false);

    assertEquals("FAIL", out.verifyResult());
    assertEquals(new TreeSet<>(List.of(1L, 2L, 3L)), idsIn(primary), "primary untouched");
    assertTrue(Files.exists(shadow), "shadow kept for re-verify");
    assertFalse(Files.exists(rotations), "no event logged on a deferred fail");
  }

  @Test
  void failingEquivalenceWithForceMergesAsForced(@TempDir Path dir) throws Exception {
    Path primary = dir.resolve("minute-14-23.jsonl.zst");
    Path shadow = dir.resolve("minute-14-23.shadow.jsonl.zst");
    Path rotations = dir.resolve("rotations.jsonl");
    DurableSegment.writeLines(primary, List.of(trade(1), trade(2), trade(3)));
    DurableSegment.writeLines(shadow, List.of(trade(1), trade(3)));

    RotationExecutor.Outcome out =
        executor.cutoverMinute(
            "trade",
            primary,
            shadow,
            rotations,
            "rot-3",
            "SCHEDULED",
            23.0,
            Instant.parse("2026-06-14T14:24:00Z"),
            23,
            true);

    assertEquals("FORCED", out.verifyResult());
    assertFalse(Files.exists(shadow), "shadow consumed on forced cutover");
    JsonNode ev = mapper.readTree(Files.readAllLines(rotations).get(0));
    assertEquals("FORCED", ev.get("verify_result").asText());
  }
}
