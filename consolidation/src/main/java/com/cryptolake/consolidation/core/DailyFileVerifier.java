package com.cryptolake.consolidation.core;

import com.cryptolake.common.util.Sha256;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Verifies the integrity of a consolidated daily file.
 *
 * <p>Ports {@code verify_daily_file} from {@code consolidate.py}. Checks SHA-256 and validates
 * monotonic ordering of data records by natural key. Gap envelopes are skipped in ordering checks.
 *
 * <p>Tier 5 I2 — streaming decompress.
 *
 * <p>Thread safety: stateless per call.
 */
public final class DailyFileVerifier {

  private DailyFileVerifier() {}

  /**
   * Verification result.
   *
   * <p>Tier 2 §12 — record.
   */
  public record VerifyResult(boolean success, String errorMessage) {
    public static VerifyResult ok() {
      return new VerifyResult(true, null);
    }

    public static VerifyResult fail(String msg) {
      return new VerifyResult(false, msg);
    }
  }

  /**
   * Verifies the daily file's SHA-256 and record ordering.
   *
   * @param dailyPath path to the {@code .jsonl.zst} daily file
   * @param expectedCount expected total record count
   * @param sidecarPath path to the SHA-256 sidecar
   * @param stream stream type (for ordering key selection)
   * @param mapper shared {@link ObjectMapper}
   * @return {@link VerifyResult}
   * @throws IOException on I/O failure
   */
  public static VerifyResult verify(
      Path dailyPath, long expectedCount, Path sidecarPath, String stream, ObjectMapper mapper)
      throws IOException {

    // 1. SHA-256 check
    if (Files.exists(sidecarPath)) {
      String sidecarContent = Files.readString(sidecarPath, StandardCharsets.UTF_8);
      String expected = sidecarContent.strip().split("\\s+")[0];
      String actual = Sha256.hexFile(dailyPath);
      if (!actual.equals(expected)) {
        return VerifyResult.fail("daily file SHA-256 mismatch");
      }
    }

    // 2. Streaming walk for count and ordering
    long count = 0L;
    long lastKey = Long.MIN_VALUE;

    try (var in = Files.newInputStream(dailyPath);
        var zstdIn = new ZstdInputStream(in);
        var reader = new BufferedReader(new InputStreamReader(zstdIn, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        JsonNode record;
        try {
          record = mapper.readTree(line);
        } catch (IOException e) {
          return VerifyResult.fail("parse error: " + e.getMessage());
        }
        count++;

        // Skip gaps in ordering check
        if ("gap".equals(record.path("type").asText())) {
          continue;
        }

        long key = DataSortKey.of(record, stream, mapper);
        if (key < lastKey) {
          return VerifyResult.fail(
              "ordering violation at record " + count + ": key " + key + " < previous " + lastKey);
        }
        lastKey = key;
      }
    }

    if (expectedCount > 0 && count != expectedCount) {
      return VerifyResult.fail(
          "record count mismatch: expected " + expectedCount + ", found " + count);
    }

    return VerifyResult.ok();
  }
}
