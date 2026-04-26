package com.cryptolake.collector.harness;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Sha256;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Gate 3 parity harness: verifies byte-for-byte identity of {@code raw_text} and {@code
 * raw_sha256} in Java vs Python-produced fixture envelopes.
 *
 * <p>Fixture layout: {@code parity-fixtures/websocket-frames/{stream}/{timestamp}-{seq}-{stream}.raw}
 * with a sibling {@code .json} file (the Python-produced {@link DataEnvelope}).
 *
 * <p>The {@code .raw} files contain the extracted {@code raw_text} value directly — the Python
 * {@code FrameTap} saves the inner data payload, not the full WebSocket frame. Therefore, the
 * harness reads raw bytes as UTF-8 and compares directly against {@code envelope.rawText()}.
 *
 * <p>Gate 3 invariant (Tier 1 §7): exact byte equality of raw_text + matching SHA-256.
 */
public final class RawTextParityHarness {

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: RawTextParityHarness <fixturesDir> [reportPath]");
      System.exit(1);
    }
    Path fixturesDir = Path.of(args[0]);
    Path reportPath = args.length >= 2 ? Path.of(args[1]) : null;

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());

    List<String> failures = new ArrayList<>();
    int total = 0;

    try (Stream<Path> streamDirs = Files.list(fixturesDir)) {
      for (Path streamDir : streamDirs.filter(Files::isDirectory).toList()) {
        try (Stream<Path> files = Files.list(streamDir)) {
          for (Path rawPath : files.filter(p -> p.toString().endsWith(".raw")).toList()) {
            Path jsonPath =
                rawPath.resolveSibling(
                    rawPath.getFileName().toString().replace(".raw", ".json"));
            if (!Files.exists(jsonPath)) {
              System.err.println("WARNING: no JSON sibling for " + rawPath);
              continue;
            }
            total++;
            String failure = checkParity(codec, rawPath, jsonPath);
            if (failure != null) {
              failures.add(failure);
            }
          }
        }
      }
    }

    StringBuilder report = new StringBuilder();
    if (failures.isEmpty()) {
      report.append("OK ").append(total).append(" fixtures\n");
      System.out.println("gate3 OK: " + total + " fixtures all byte-identical");
    } else {
      report.append("FAIL ").append(failures.size()).append("/").append(total)
          .append(" fixtures differ\n");
      for (String f : failures) {
        report.append(f).append("\n");
      }
      System.err.println("gate3 FAIL: " + failures.size() + "/" + total + " fixtures differ");
    }

    if (reportPath != null) {
      Files.createDirectories(reportPath.getParent());
      Files.writeString(reportPath, report.toString());
    }

    System.exit(failures.isEmpty() ? 0 : 1);
  }

  /**
   * Returns {@code null} on success; a failure description string otherwise.
   *
   * <p>The {@code .raw} file IS the raw_text (extracted inner data payload). Compare directly
   * against the Python envelope's {@code raw_text} field.
   */
  static String checkParity(EnvelopeCodec codec, Path rawPath, Path jsonPath) {
    try {
      byte[] rawBytes = Files.readAllBytes(rawPath);
      DataEnvelope expected = codec.readData(Files.readAllBytes(jsonPath));

      // .raw files contain the raw_text value directly (Tier 1 §7)
      String javaRawText = new String(rawBytes, StandardCharsets.UTF_8);

      // Compare raw_text bytes (Tier 1 §7)
      byte[] javaBytes = javaRawText.getBytes(StandardCharsets.UTF_8);
      byte[] expectedBytes = expected.rawText().getBytes(StandardCharsets.UTF_8);
      if (!Arrays.equals(javaBytes, expectedBytes)) {
        String hexDiff = hexDiff(javaBytes, expectedBytes, 64);
        return "FAIL " + rawPath + ":\n  rawText mismatch (first 64 bytes)\n" + hexDiff;
      }

      // Compare raw_sha256 (Tier 1 §2)
      String javaSha = Sha256.hexDigestUtf8(javaRawText);
      if (!javaSha.equals(expected.rawSha256())) {
        return "FAIL " + rawPath + ":\n  sha256 mismatch java=" + javaSha
            + " expected=" + expected.rawSha256();
      }

      return null; // success

    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      return "FAIL " + rawPath + ": exception " + sw;
    }
  }

  private static String hexDiff(byte[] java, byte[] expected, int maxBytes) {
    StringBuilder sb = new StringBuilder();
    sb.append("  java    : ").append(hexDump(java, maxBytes)).append("\n");
    sb.append("  expected: ").append(hexDump(expected, maxBytes));
    return sb.toString();
  }

  private static String hexDump(byte[] bytes, int max) {
    int len = Math.min(bytes.length, max);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append(String.format("%02x", bytes[i] & 0xFF));
      if (i < len - 1) sb.append(' ');
    }
    if (bytes.length > max) sb.append("...");
    return sb.toString();
  }
}
