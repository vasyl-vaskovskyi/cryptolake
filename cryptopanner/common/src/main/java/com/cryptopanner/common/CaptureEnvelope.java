package com.cryptopanner.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Builds the per-frame {@code ws_frame} capture envelope (master spec §8.c). The original WebSocket
 * frame is stored verbatim in {@code raw} (a JSON string — never parsed-and-re-serialized), so the
 * exact wire bytes survive; {@code received_at} records when the frame arrived, and {@code
 * raw_sha256} keeps the inner frame provable independent of the envelope.
 *
 * <p>Field order — {@code envelope}, {@code received_at}, {@code raw_sha256}, {@code raw} — matches
 * the spec example and is preserved by {@link ObjectNode}'s insertion order.
 */
public final class CaptureEnvelope {

  private CaptureEnvelope() {}

  /** Returns the single-line JSON envelope (no trailing LF) wrapping {@code rawText}. */
  public static String wsFrame(ObjectMapper mapper, String rawText, java.time.Instant receivedAt) {
    ObjectNode env = mapper.createObjectNode();
    env.put("envelope", "ws_frame");
    env.put("received_at", receivedAt.toString());
    env.put("raw_sha256", sha256Hex(rawText));
    env.put("raw", rawText);
    try {
      return mapper.writeValueAsString(env);
    } catch (JsonProcessingException e) {
      // An ObjectNode of strings cannot fail to serialize; surface defensively.
      throw new IllegalStateException("ws_frame envelope serialization failed", e);
    }
  }

  /** Lowercase hex SHA-256 over the UTF-8 bytes of {@code s}. */
  static String sha256Hex(String s) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
