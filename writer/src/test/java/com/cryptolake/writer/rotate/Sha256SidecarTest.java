package com.cryptolake.writer.rotate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link Sha256Sidecar}.
 *
 * <p>Ports: Python's {@code test_sha256_sidecar.py} — sidecar format (Tier 5 I5, I6).
 */
class Sha256SidecarTest {

  @TempDir Path tmp;

  // ports: Tier 5 I5 — two-space separator matching sha256sum(1)
  @Test
  void write_twoSpaceSeparatorBeforeFilename() throws IOException {
    Path dataPath = tmp.resolve("hour-14.jsonl.zst");
    Files.writeString(dataPath, "some compressed data");
    Path sidecarPath = tmp.resolve("hour-14.jsonl.zst.sha256");

    Sha256Sidecar.write(dataPath, sidecarPath);

    String content = Files.readString(sidecarPath);
    // Format: "<hex>  <filename>\n"
    assertThat(content).contains("  hour-14.jsonl.zst");
    assertThat(content).endsWith("\n");
  }

  // ports: Tier 5 I6 — sha256sum -c compatible: just filename, not full path
  @Test
  void write_containsOnlyFilenameNotFullPath() throws IOException {
    Path dir = tmp.resolve("some/deep/dir");
    Files.createDirectories(dir);
    Path dataPath = dir.resolve("hour-0.jsonl.zst");
    Files.writeString(dataPath, "data");
    Path sidecarPath = dir.resolve("hour-0.jsonl.zst.sha256");

    Sha256Sidecar.write(dataPath, sidecarPath);

    String content = Files.readString(sidecarPath);
    // Must contain only the filename (not the full path) after the double-space
    assertThat(content).contains("  hour-0.jsonl.zst");
    assertThat(content).doesNotContain("/some/deep/dir/hour-0.jsonl.zst");
  }

  // ports: Tier 5 I5 — hex is 64 lowercase hex chars
  @Test
  void write_hexPart_64LowercaseHexChars() throws IOException {
    Path dataPath = tmp.resolve("hour-5.jsonl.zst");
    Files.writeString(dataPath, "test content");
    Path sidecarPath = tmp.resolve("hour-5.jsonl.zst.sha256");

    Sha256Sidecar.write(dataPath, sidecarPath);

    String content = Files.readString(sidecarPath);
    String hex = content.split("  ")[0];
    assertThat(hex).hasSize(64).matches("[0-9a-f]+");
  }

  // ports: Tier 5 I5 — deterministic: same content same hash
  @Test
  void write_deterministicHash() throws IOException {
    Path dataPath1 = tmp.resolve("file1.zst");
    Path dataPath2 = tmp.resolve("file2.zst");
    byte[] content = "deterministic".getBytes();
    Files.write(dataPath1, content);
    Files.write(dataPath2, content);

    Path sidecar1 = tmp.resolve("file1.zst.sha256");
    Path sidecar2 = tmp.resolve("file2.zst.sha256");
    Sha256Sidecar.write(dataPath1, sidecar1);
    Sha256Sidecar.write(dataPath2, sidecar2);

    String hex1 = Files.readString(sidecar1).split("  ")[0];
    String hex2 = Files.readString(sidecar2).split("  ")[0];
    assertThat(hex1).isEqualTo(hex2);
  }
}
