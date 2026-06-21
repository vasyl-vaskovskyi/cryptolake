package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.Sha256Sidecar;
import java.nio.file.Files;
import java.nio.file.Path;
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
}
