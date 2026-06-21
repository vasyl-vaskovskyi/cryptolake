package com.cryptopanner.common;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Writes a minute-segment {@code .jsonl.zst} plus its {@code .sha256} sidecar durably (design doc
 * §3.2 output contract): compress, write to {@code .tmp}, fsync, atomic rename, then the sidecar.
 * Used for {@link OverlapMerger} merge output and any other place that materializes a sealed
 * segment outside the Collector's own {@code MinuteSegmentWriter}. Overwrites an existing target
 * atomically.
 */
public final class DurableSegment {

  private DurableSegment() {}

  /** Writes the given already-terminated lines (each should include its trailing LF). */
  public static void writeLines(Path target, List<String> lines) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    for (String l : lines) {
      buf.write(l.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    writeBytes(target, buf.toByteArray());
  }

  /** Compresses {@code uncompressed} with zstd and writes target + sidecar durably. */
  public static void writeBytes(Path target, byte[] uncompressed) throws IOException {
    Path parent = target.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    byte[] compressed = Zstd.compress(uncompressed, 3);
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    try (FileChannel ch =
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(ByteBuffer.wrap(compressed));
      ch.force(true);
    }
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
    Sha256Sidecar.computeAndWrite(target, target.resolveSibling(target.getFileName() + ".sha256"));
  }
}
