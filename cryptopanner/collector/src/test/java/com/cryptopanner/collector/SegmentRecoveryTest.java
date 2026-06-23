package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.DurableSegment;
import com.cryptopanner.common.Sha256Sidecar;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentRecoveryTest {

  private Path seg(Path root, String name) throws Exception {
    Path dir = root.resolve("btcusdt/trade/2026-06-21");
    Files.createDirectories(dir);
    Path f = dir.resolve(name);
    Files.writeString(f, "payload-" + name);
    return f;
  }

  @Test
  void deletesLeftoverTmpFiles(@TempDir Path root) throws Exception {
    Path tmp = seg(root, "minute-14-23.jsonl.zst.tmp");
    SegmentRecovery.Result r = SegmentRecovery.recover(root);
    assertFalse(Files.exists(tmp), "incomplete .tmp must be deleted");
    assertEquals(1, r.tmpDeleted());
  }

  @Test
  void writesMissingSidecar(@TempDir Path root) throws Exception {
    Path data = seg(root, "minute-14-23.jsonl.zst");
    SegmentRecovery.Result r = SegmentRecovery.recover(root);
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    assertTrue(Files.exists(sidecar), "missing sidecar must be recreated");
    assertEquals(Sha256Sidecar.sha256Hex(data), Sha256Sidecar.readHash(sidecar));
    assertEquals(1, r.sidecarsWritten());
  }

  @Test
  void recomputesMismatchedSidecar(@TempDir Path root) throws Exception {
    Path data = seg(root, "minute-14-23.jsonl.zst");
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    Files.writeString(sidecar, "0".repeat(64) + "  " + data.getFileName() + "\n"); // stale/wrong

    SegmentRecovery.Result r = SegmentRecovery.recover(root);
    assertEquals(Sha256Sidecar.sha256Hex(data), Sha256Sidecar.readHash(sidecar), "corrected");
    assertEquals(1, r.sidecarsWritten());
  }

  @Test
  void leavesCorrectSidecarUntouched(@TempDir Path root) throws Exception {
    Path data = seg(root, "minute-14-23.jsonl.zst");
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    Sha256Sidecar.computeAndWrite(data, sidecar);

    SegmentRecovery.Result r = SegmentRecovery.recover(root);
    assertEquals(0, r.sidecarsWritten(), "a correct sidecar is not rewritten");
  }

  // ── §5.6 step 3: leftover shadow handling ──────────────────────────────────

  private final ObjectMapper mapper = new ObjectMapper();

  private String tradeLine(long id) {
    // Each on-disk line carries its trailing LF, exactly as MinuteSegmentWriter writes them.
    return CaptureEnvelope.wsFrame(
            mapper,
            "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":" + id + ",\"T\":" + id + "}}",
            Instant.EPOCH)
        + "\n";
  }

  private Path tradeDir(Path root) {
    return root.resolve("btcusdt/trade/2026-06-21");
  }

  private TreeSet<Long> idsIn(Path zst) throws Exception {
    TreeSet<Long> ids = new TreeSet<>();
    for (String line : DurableSegment.readLines(zst)) {
      JsonNode data = CaptureEnvelope.unwrap(mapper, mapper.readTree(line)).get("data");
      ids.add(data.get("t").asLong());
    }
    return ids;
  }

  @Test
  void mergesShadowAlongsidePrimary(@TempDir Path root) throws Exception {
    Path primary = tradeDir(root).resolve("minute-14-23.jsonl.zst");
    Path shadow = tradeDir(root).resolve("minute-14-23.shadow.jsonl.zst");
    DurableSegment.writeLines(primary, List.of(tradeLine(1), tradeLine(2)));
    DurableSegment.writeLines(shadow, List.of(tradeLine(2), tradeLine(3)));

    SegmentRecovery.Result r = SegmentRecovery.recover(root, mapper);

    assertEquals(1, r.shadowsMerged());
    assertEquals(new TreeSet<>(List.of(1L, 2L, 3L)), idsIn(primary), "primary holds the union");
    assertFalse(Files.exists(shadow), "shadow consumed by the merge");
  }

  @Test
  void writesRecoveredAtStartupRotationEventWhenShadowsRecovered(@TempDir Path root)
      throws Exception {
    Path primary = tradeDir(root).resolve("minute-14-23.jsonl.zst");
    Path shadow = tradeDir(root).resolve("minute-14-23.shadow.jsonl.zst");
    DurableSegment.writeLines(primary, List.of(tradeLine(1), tradeLine(2)));
    DurableSegment.writeLines(shadow, List.of(tradeLine(2), tradeLine(3)));
    Path rotations = root.resolve("deploy/rotations.jsonl");

    SegmentRecovery.Result r =
        SegmentRecovery.recover(
            root, mapper, rotations, "recovery-1", Instant.parse("2026-06-21T15:00:00Z"));

    assertEquals(1, r.shadowsMerged());
    JsonNode ev = mapper.readTree(Files.readAllLines(rotations).get(0));
    assertEquals(
        "RECOVERED_AT_STARTUP", ev.get("verify_result").asText(), "§10.d recovery verify_result");
    assertEquals("recovery-1", ev.get("rotation_id").asText());
    assertTrue(ev.get("minutes_merged").toString().contains("23"), "records the recovered minute");
  }

  @Test
  void writesNoRecoveryEventWhenNothingRecovered(@TempDir Path root) throws Exception {
    DurableSegment.writeLines(
        tradeDir(root).resolve("minute-14-23.jsonl.zst"), List.of(tradeLine(1)));
    Path rotations = root.resolve("deploy/rotations.jsonl");

    SegmentRecovery.recover(
        root, mapper, rotations, "recovery-2", Instant.parse("2026-06-21T15:00:00Z"));

    assertFalse(Files.exists(rotations), "no rotation event when no shadows were recovered");
  }

  @Test
  void promotesOrphanShadowWhenNoPrimary(@TempDir Path root) throws Exception {
    Path primary = tradeDir(root).resolve("minute-14-23.jsonl.zst");
    Path shadow = tradeDir(root).resolve("minute-14-23.shadow.jsonl.zst");
    DurableSegment.writeLines(shadow, List.of(tradeLine(5)));

    SegmentRecovery.Result r = SegmentRecovery.recover(root, mapper);

    assertEquals(1, r.shadowsPromoted());
    assertTrue(Files.exists(primary), "orphan shadow promoted to primary");
    assertFalse(Files.exists(shadow));
    assertEquals(new TreeSet<>(List.of(5L)), idsIn(primary));
    Path sidecar = primary.resolveSibling(primary.getFileName() + ".sha256");
    assertEquals(
        Sha256Sidecar.sha256Hex(primary), Sha256Sidecar.readHash(sidecar), "valid sidecar");
  }
}
