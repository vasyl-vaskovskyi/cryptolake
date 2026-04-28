package com.cryptolake.verify.verify;

import com.cryptolake.common.util.Sha256;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Verifies the SHA-256 sidecar against the data file.
 *
 * <p>Ports {@code verify_checksum(data_path, sidecar_path)} from {@code verify.py:25-32}.
 *
 * <p>Thread safety: stateless utility.
 */
public final class ChecksumVerifier {

  private ChecksumVerifier() {}

  /**
   * Compares the SHA-256 of {@code dataPath} against the hex in {@code sidecarPath}.
   *
   * @return empty list on success; singleton list with an error message on failure
   * @throws IOException if reading the sidecar or hashing the data file fails
   */
  public static List<String> verify(Path dataPath, Path sidecarPath) throws IOException {
    if (!Files.exists(sidecarPath)) {
      return List.of("Sidecar not found: " + sidecarPath);
    }
    // Read first whitespace-delimited token (matches Python's .strip().split()[0])
    String sidecarContent = Files.readString(sidecarPath, StandardCharsets.UTF_8);
    String expected = sidecarContent.strip().split("\\s+")[0];
    String actual = Sha256.hexFile(dataPath);
    if (!actual.equals(expected)) {
      return List.of("Checksum mismatch for " + dataPath.getFileName());
    }
    return List.of();
  }
}
