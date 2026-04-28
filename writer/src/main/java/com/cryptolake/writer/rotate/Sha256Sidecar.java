package com.cryptolake.writer.rotate;

import com.cryptolake.common.util.Sha256;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes a SHA-256 sidecar file for a sealed archive.
 *
 * <p>Sidecar format: {@code "<hex> <filename>\n"} (two spaces — matches {@code sha256sum(1)} output
 * — Tier 5 I5, I6). Written directly to the sidecar path (no temp+rename — Tier 5 I6 rationale: the
 * recovery path tolerates missing sidecars).
 *
 * <p>Ports Python's {@code file_rotator.py:write_sha256_sidecar} (design §4.5).
 *
 * <p>Thread safety: stateless utility; safe to call from any thread. Only called from T1.
 */
public final class Sha256Sidecar {

  private Sha256Sidecar() {}

  /**
   * Computes the SHA-256 digest of {@code dataPath} and writes the sidecar file at {@code
   * sidecarPath}.
   *
   * <p>Format: {@code "{hex} {filename}\n"} (two spaces, matching {@code sha256sum(1)} output).
   *
   * @param dataPath sealed archive file to hash
   * @param sidecarPath destination for the sidecar; use {@link FilePaths#sidecarPath(Path)}
   * @throws IOException if hashing or writing fails
   */
  public static void write(Path dataPath, Path sidecarPath) throws IOException {
    String hex = Sha256.hexFile(dataPath); // 8192-byte chunk read (Tier 5 I5)
    // Two spaces between digest and filename — sha256sum(1) convention (Tier 5 I6)
    String content = hex + "  " + dataPath.getFileName() + "\n";
    Files.writeString(sidecarPath, content);
  }
}
