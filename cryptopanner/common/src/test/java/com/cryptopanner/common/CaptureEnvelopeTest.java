package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class CaptureEnvelopeTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();

  private static String independentSha256Hex(String s) throws Exception {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void wrapsFrameWithMetadataAndVerbatimRaw() throws Exception {
    String rawFrame =
        "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":1750000000182,\"T\":1750000000180,\"t\":145003}}";
    Instant receivedAt = Instant.parse("2026-06-20T14:23:15.182Z");

    String line = CaptureEnvelope.wsFrame(MAPPER, rawFrame, receivedAt);

    JsonNode env = MAPPER.readTree(line);
    assertEquals("ws_frame", env.get("envelope").asText());
    assertEquals("2026-06-20T14:23:15.182Z", env.get("received_at").asText());
    assertEquals(independentSha256Hex(rawFrame), env.get("raw_sha256").asText());
    // raw must round-trip byte-for-byte, including the nested quotes.
    assertEquals(rawFrame, env.get("raw").asText());
  }

  @Test
  void rawPreservesExactBytesWithEmbeddedQuotesAndBackslashes() throws Exception {
    String rawFrame = "{\"a\":\"he said \\\"hi\\\"\",\"b\":\"c:\\\\path\"}";
    Instant receivedAt = Instant.parse("2026-06-20T00:00:00Z");

    String line = CaptureEnvelope.wsFrame(MAPPER, rawFrame, receivedAt);

    JsonNode env = MAPPER.readTree(line);
    assertEquals(rawFrame, env.get("raw").asText());
    assertEquals(independentSha256Hex(rawFrame), env.get("raw_sha256").asText());
  }

  @Test
  void unwrapReturnsInnerFrameForWsEnvelope() throws Exception {
    String raw = "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":7}}";
    String line = CaptureEnvelope.wsFrame(MAPPER, raw, Instant.parse("2026-06-20T00:00:00Z"));
    JsonNode frame = CaptureEnvelope.unwrap(MAPPER, MAPPER.readTree(line));
    assertEquals(7, frame.get("data").get("t").asInt());
  }

  @Test
  void unwrapReturnsNodeUnchangedForBareFrame() throws Exception {
    JsonNode bare = MAPPER.readTree("{\"data\":{\"t\":9}}");
    JsonNode frame = CaptureEnvelope.unwrap(MAPPER, bare);
    assertEquals(9, frame.get("data").get("t").asInt());
  }

  @Test
  void fieldOrderMatchesSpecExample() {
    String line =
        CaptureEnvelope.wsFrame(MAPPER, "{\"x\":1}", Instant.parse("2026-06-20T00:00:00Z"));
    int env = line.indexOf("\"envelope\"");
    int recv = line.indexOf("\"received_at\"");
    int sha = line.indexOf("\"raw_sha256\"");
    int raw = line.indexOf("\"raw\"");
    org.junit.jupiter.api.Assertions.assertTrue(
        env < recv && recv < sha && sha < raw,
        "field order should be envelope, received_at, raw_sha256, raw");
  }
}
