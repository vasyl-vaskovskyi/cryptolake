package com.cryptopanner.collector;

import com.cryptopanner.common.DurableSegment;
import com.cryptopanner.common.OverlapMerger;
import com.cryptopanner.common.Sha256Sidecar;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Startup recovery over the {@code segments/} tree (design doc §3.2 recovery rules, §5.6). Applied
 * before the Collector resumes writing so a crash mid-write or mid-rotation can never leave
 * inconsistent state:
 *
 * <ul>
 *   <li>Any {@code *.tmp} file → delete (an atomic rename never completed).
 *   <li>A leftover {@code *.shadow.jsonl.zst} (rotation crash, §5.6 step 3): merged with its
 *       primary via {@link OverlapMerger} when both exist, or promoted to the primary name when
 *       orphaned.
 *   <li>A {@code *.jsonl.zst} whose {@code .sha256} sidecar is missing or mismatched → recompute it
 *       atomically from the file's current bytes.
 * </ul>
 *
 * <p>After recovery the steady-state invariant holds: at most one {@code .jsonl.zst} per (symbol,
 * stream, minute), no {@code .shadow} files, every segment with a valid sidecar.
 */
public final class SegmentRecovery {

  private static final String SHADOW_SUFFIX = ".shadow.jsonl.zst";

  private SegmentRecovery() {}

  /** Counts of remedial actions taken. */
  public record Result(
      int tmpDeleted, int sidecarsWritten, int shadowsMerged, int shadowsPromoted) {}

  /** Recovery without shadow handling (no merger available). */
  public static Result recover(Path segmentsRoot) throws IOException {
    return recover(segmentsRoot, null);
  }

  /**
   * Full recovery. When {@code mapper} is non-null, leftover {@code .shadow} segments are merged or
   * promoted (§5.6 step 3); otherwise they are left in place.
   */
  public static Result recover(Path segmentsRoot, ObjectMapper mapper) throws IOException {
    if (!Files.isDirectory(segmentsRoot)) {
      return new Result(0, 0, 0, 0);
    }
    int tmpDeleted = sweepTmp(segmentsRoot);
    int shadowsMerged = 0;
    int shadowsPromoted = 0;
    if (mapper != null) {
      int[] shadowCounts = handleShadows(segmentsRoot, mapper);
      shadowsMerged = shadowCounts[0];
      shadowsPromoted = shadowCounts[1];
    }
    int sidecarsWritten = validateSidecars(segmentsRoot);
    return new Result(tmpDeleted, sidecarsWritten, shadowsMerged, shadowsPromoted);
  }

  private static int sweepTmp(Path root) throws IOException {
    int deleted = 0;
    for (Path p : regularFiles(root)) {
      if (p.getFileName().toString().endsWith(".tmp")) {
        Files.deleteIfExists(p);
        deleted++;
      }
    }
    return deleted;
  }

  private static int[] handleShadows(Path root, ObjectMapper mapper) throws IOException {
    int merged = 0;
    int promoted = 0;
    OverlapMerger merger = new OverlapMerger(mapper);
    for (Path shadow : regularFiles(root)) {
      String name = shadow.getFileName().toString();
      if (!name.endsWith(SHADOW_SUFFIX)) {
        continue;
      }
      Path primary =
          shadow.resolveSibling(
              name.substring(0, name.length() - SHADOW_SUFFIX.length()) + ".jsonl.zst");
      Path shadowSidecar = shadow.resolveSibling(name + ".sha256");
      if (Files.exists(primary)) {
        String stream = streamFromPath(root, shadow);
        OverlapMerger.Merged m =
            merger.merge(
                stream, DurableSegment.readLines(primary), DurableSegment.readLines(shadow));
        DurableSegment.writeLines(primary, m.lines());
        Files.deleteIfExists(shadow);
        Files.deleteIfExists(shadowSidecar);
        merged++;
      } else {
        // Orphan: shadow data is all we have. Promote it to the primary name + fresh sidecar.
        Files.move(shadow, primary, StandardCopyOption.ATOMIC_MOVE);
        Sha256Sidecar.computeAndWrite(
            primary, primary.resolveSibling(primary.getFileName() + ".sha256"));
        Files.deleteIfExists(shadowSidecar);
        promoted++;
      }
    }
    return new int[] {merged, promoted};
  }

  private static int validateSidecars(Path root) throws IOException {
    int written = 0;
    for (Path p : regularFiles(root)) {
      String name = p.getFileName().toString();
      if (name.endsWith(".jsonl.zst") && !name.endsWith(SHADOW_SUFFIX) && ensureSidecar(p)) {
        written++;
      }
    }
    return written;
  }

  /** Derives the stream from {@code segments/<symbol>/<stream>/<date>/file}. */
  private static String streamFromPath(Path root, Path file) {
    Path rel = root.relativize(file);
    return rel.getNameCount() >= 2 ? rel.getName(1).toString() : "";
  }

  private static boolean ensureSidecar(Path data) throws IOException {
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    String actual = Sha256Sidecar.sha256Hex(data);
    if (Files.exists(sidecar)) {
      try {
        if (Sha256Sidecar.readHash(sidecar).equals(actual)) {
          return false;
        }
      } catch (IOException malformed) {
        // fall through to rewrite
      }
    }
    Sha256Sidecar.computeAndWrite(data, sidecar);
    return true;
  }

  private static List<Path> regularFiles(Path root) throws IOException {
    try (Stream<Path> walk = Files.walk(root)) {
      List<Path> out = new ArrayList<>();
      walk.filter(Files::isRegularFile).forEach(out::add);
      return out;
    }
  }
}
