package com.cryptopanner.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * sha256sum-style sidecar format per master spec §10.a.
 *
 * <p>Content: {@code <lowercase 64 hex> + " " + <bare filename> + "\n"}. Compatible with {@code
 * sha256sum -c} from any shell.
 */
public final class Sha256Sidecar {

  private Sha256Sidecar() {}

  /** Compute SHA-256 of {@code data} and write the sidecar atomically (write+rename). */
  public static void computeAndWrite(Path data, Path sidecar) throws IOException {
    String hex = sha256Hex(data);
    String line = hex + "  " + data.getFileName().toString() + "\n";
    Path tmp = sidecar.resolveSibling(sidecar.getFileName() + ".tmp");
    Files.writeString(tmp, line);
    Files.move(tmp, sidecar, StandardCopyOption.ATOMIC_MOVE);
  }

  /** Extract the hex hash from a sidecar file. */
  public static String readHash(Path sidecar) throws IOException {
    String content = Files.readString(sidecar);
    int sp = content.indexOf(' ');
    if (sp != 64) {
      throw new IOException("malformed sidecar: " + sidecar);
    }
    return content.substring(0, sp);
  }

  /** Lowercase 64-hex SHA-256 of the file's bytes (the value stored in the sidecar). */
  public static String sha256Hex(Path data) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
    try (InputStream in = Files.newInputStream(data)) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        md.update(buf, 0, n);
      }
    }
    byte[] digest = md.digest();
    StringBuilder hex = new StringBuilder(64);
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
