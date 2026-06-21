package com.cryptopanner.collector;

import com.cryptopanner.common.Sha256Sidecar;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Startup recovery over the {@code segments/} tree (design doc §3.2 recovery rules, §5.6). Applied
 * before the Collector resumes writing so a crash mid-write can never leave inconsistent state:
 *
 * <ul>
 *   <li>Any {@code *.tmp} file → delete (an atomic rename never completed).
 *   <li>A {@code *.jsonl.zst} whose {@code .sha256} sidecar is missing or mismatched → recompute
 *       the sidecar atomically from the file's current bytes.
 * </ul>
 *
 * <p>The {@code *.shadow.jsonl.zst}→merge rule (§5.6) requires the rotation runtime and is handled
 * by the WsConnectionManager, not here.
 */
public final class SegmentRecovery {

  private SegmentRecovery() {}

  /** Counts of remedial actions taken. */
  public record Result(int tmpDeleted, int sidecarsWritten) {}

  public static Result recover(Path segmentsRoot) throws IOException {
    if (!Files.isDirectory(segmentsRoot)) {
      return new Result(0, 0);
    }
    int tmpDeleted = 0;
    int sidecarsWritten = 0;
    try (Stream<Path> walk = Files.walk(segmentsRoot)) {
      for (Path p : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
        String name = p.getFileName().toString();
        if (name.endsWith(".tmp")) {
          Files.deleteIfExists(p);
          tmpDeleted++;
        } else if (name.endsWith(".jsonl.zst")) {
          if (ensureSidecar(p)) {
            sidecarsWritten++;
          }
        }
      }
    }
    return new Result(tmpDeleted, sidecarsWritten);
  }

  /** Writes/corrects the sidecar for {@code data}; returns true if it had to write. */
  private static boolean ensureSidecar(Path data) throws IOException {
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    String actual = Sha256Sidecar.sha256Hex(data);
    if (Files.exists(sidecar)) {
      try {
        if (Sha256Sidecar.readHash(sidecar).equals(actual)) {
          return false; // already correct
        }
      } catch (IOException malformed) {
        // fall through to rewrite
      }
    }
    Sha256Sidecar.computeAndWrite(data, sidecar);
    return true;
  }
}
