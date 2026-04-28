package com.cryptolake.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 utilities.
 *
 * <p>Tier 5 I5: {@code hexFile} uses 8192-byte chunks matching Python's {@code hashlib.sha256()}
 * chunked read. {@code HexFormat.of().formatHex} returns lowercase hex, matching {@code
 * hashlib.hexdigest()}.
 */
public final class Sha256 {

  private Sha256() {}

  /**
   * Returns lowercase hex SHA-256 of {@code text.getBytes(UTF_8)}.
   *
   * <p>Tier 1 §2, Tier 5 E2: called from {@code DataEnvelope.create} on {@code rawText} at capture
   * time.
   */
  public static String hexDigestUtf8(String text) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(text.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  /**
   * Returns lowercase hex SHA-256 of file at {@code p}, reading in 8192-byte chunks (Tier 5 I5).
   */
  public static String hexFile(Path p) throws IOException {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      try (InputStream in = Files.newInputStream(p)) {
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
          md.update(buf, 0, n);
        }
      }
      return HexFormat.of().formatHex(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
