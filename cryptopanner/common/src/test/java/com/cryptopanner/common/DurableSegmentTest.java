package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableSegmentTest {

  private static String decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return new String(out, StandardCharsets.UTF_8);
  }

  @Test
  void writesZstdSegmentWithMatchingSidecarAndNoTmpLeftover(@TempDir Path dir) throws Exception {
    Path target = dir.resolve("sub/minute-14-23.jsonl.zst");
    DurableSegment.writeLines(target, List.of("a\n", "b\n"));

    assertTrue(Files.exists(target));
    assertEquals("a\nb\n", decompress(Files.readAllBytes(target)));

    Path sidecar = target.resolveSibling(target.getFileName() + ".sha256");
    assertTrue(Files.exists(sidecar));
    assertEquals(Sha256Sidecar.sha256Hex(target), Sha256Sidecar.readHash(sidecar));

    assertFalse(Files.exists(target.resolveSibling(target.getFileName() + ".tmp")), "no tmp left");
  }

  @Test
  void overwritesExistingTargetAtomically(@TempDir Path dir) throws Exception {
    Path target = dir.resolve("minute-14-23.jsonl.zst");
    DurableSegment.writeLines(target, List.of("old\n"));
    DurableSegment.writeLines(target, List.of("new1\n", "new2\n"));
    assertEquals("new1\nnew2\n", decompress(Files.readAllBytes(target)));
  }
}
