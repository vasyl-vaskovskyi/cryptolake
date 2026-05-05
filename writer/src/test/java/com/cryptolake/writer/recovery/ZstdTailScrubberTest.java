package com.cryptolake.writer.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.rotate.FilePaths;
import com.cryptolake.writer.rotate.Sha256Sidecar;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ZstdTailScrubber}.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md.
 */
class ZstdTailScrubberTest {

  /** Helper: write a sequence of valid zstd frames into the file plus a fresh sidecar. */
  private static Path writeHealthyArchive(Path dir, String name, int frames) throws IOException {
    Files.createDirectories(dir);
    Path file = dir.resolve(name);
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);
    for (int i = 0; i < frames; i++) {
      byte[] line = ("{\"i\":" + i + "}\n").getBytes();
      byte[] frame = compressor.compressFrame(List.of(line));
      Files.write(file, frame, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
    Sha256Sidecar.write(file, FilePaths.sidecarPath(file));
    return file;
  }

  /** Helper: append raw bytes (e.g., to simulate a torn-tail). */
  private static void appendRaw(Path file, byte[] bytes) throws IOException {
    Files.write(file, bytes, StandardOpenOption.APPEND);
  }

  /** Test 1: a clean file with valid frames + sidecar is left untouched. */
  @Test
  void scrub_leavesHealthyFileUnchanged(@TempDir Path tmp) throws IOException {
    Path file = writeHealthyArchive(tmp, "hour-14.jsonl.zst", 3);
    Path sidecar = FilePaths.sidecarPath(file);
    long sizeBefore = Files.size(file);
    byte[] sidecarBefore = Files.readAllBytes(sidecar);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isZero();
    assertThat(Files.size(file)).isEqualTo(sizeBefore);
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(sidecarBefore);
  }

  /** Test 2: a torn tail is truncated and the sidecar is recomputed to match. */
  @Test
  void scrub_truncatesTornTail_andRecomputesSidecar(@TempDir Path tmp) throws IOException {
    Path file = writeHealthyArchive(tmp, "hour-14.jsonl.zst", 2);
    Path sidecar = FilePaths.sidecarPath(file);
    long sizeAfterTwoFrames = Files.size(file);
    byte[] sidecarAfterTwoFrames = Files.readAllBytes(sidecar);

    // Append a partial third frame (zstd magic prefix + truncated content).
    byte[] zstdMagic = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD};
    byte[] tornBytes = new byte[zstdMagic.length + 64];
    System.arraycopy(zstdMagic, 0, tornBytes, 0, zstdMagic.length);
    // Remaining 64 bytes are zeros — not a valid frame body.
    appendRaw(file, tornBytes);
    assertThat(Files.size(file)).isGreaterThan(sizeAfterTwoFrames);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isEqualTo(1);
    assertThat(Files.size(file)).isEqualTo(sizeAfterTwoFrames);
    // Sidecar matches the post-truncation file (which is identical to the 2-frame state).
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(sidecarAfterTwoFrames);
  }

  /** Test 3: an entirely-corrupt file (no valid zstd frames) is truncated to size 0. */
  @Test
  void scrub_truncatesEntirelyCorruptFile_toZero(@TempDir Path tmp) throws IOException {
    Files.createDirectories(tmp);
    Path file = tmp.resolve("hour-14.jsonl.zst");
    byte[] garbage = new byte[200];
    for (int i = 0; i < garbage.length; i++) garbage[i] = (byte) (i * 7);
    Files.write(file, garbage);
    Sha256Sidecar.write(file, FilePaths.sidecarPath(file));
    Path sidecar = FilePaths.sidecarPath(file);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isEqualTo(1);
    assertThat(Files.size(file)).isZero();
    // Sidecar reflects the empty file.
    Path freshSidecar = tmp.resolve("hour-14.jsonl.zst.fresh.sha256");
    Sha256Sidecar.write(file, freshSidecar);
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(Files.readAllBytes(freshSidecar));
  }

  /** Test 4: non-{@code .jsonl.zst} files are ignored. */
  @Test
  void scrub_ignoresNonZstdFiles(@TempDir Path tmp) throws IOException {
    Files.createDirectories(tmp);
    Path txt = tmp.resolve("readme.txt");
    Path tmpFile = tmp.resolve("partial.tmp");
    Files.write(txt, "hello".getBytes());
    Files.write(tmpFile, new byte[] {0x00, 0x01, 0x02});
    long txtSize = Files.size(txt);
    long tmpSize = Files.size(tmpFile);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isZero();
    assertThat(Files.size(txt)).isEqualTo(txtSize);
    assertThat(Files.size(tmpFile)).isEqualTo(tmpSize);
  }

  /** Test 5: returned count equals the number of files actually truncated. */
  @Test
  void scrub_returnsHealedCount(@TempDir Path tmp) throws IOException {
    // 3 healthy files
    writeHealthyArchive(tmp.resolve("a"), "hour-1.jsonl.zst", 1);
    writeHealthyArchive(tmp.resolve("b"), "hour-2.jsonl.zst", 1);
    writeHealthyArchive(tmp.resolve("c"), "hour-3.jsonl.zst", 1);

    // 2 files with torn tails
    Path tornD = writeHealthyArchive(tmp.resolve("d"), "hour-4.jsonl.zst", 1);
    appendRaw(tornD, new byte[] {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, 0x00, 0x00, 0x00});
    Path tornE = writeHealthyArchive(tmp.resolve("e"), "hour-5.jsonl.zst", 1);
    appendRaw(tornE, new byte[] {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, 0x00, 0x00, 0x00});

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isEqualTo(2);
  }
}
