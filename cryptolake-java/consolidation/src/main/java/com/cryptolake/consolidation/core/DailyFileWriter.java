package com.cryptolake.consolidation.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Writes all envelopes for a day into a single zstd-compressed JSONL file.
 *
 * <p>Ports {@code write_daily_file} from {@code consolidate.py}. Daily files are SEALED in one shot
 * — {@link ZstdOutputStream} is acceptable (design §2.2). Explicit {@code fc.force(true)} before
 * close for durability (Tier 5 I1, I3; Q14 preferred path).
 *
 * <p>Tier 5 B2: compact JSON, newline after each record. Tier 5 I1, I3.
 *
 * <p>Thread safety: each call opens its own channel; no shared mutable state.
 */
public final class DailyFileWriter {

  private static final int ZSTD_LEVEL = 3;

  private DailyFileWriter() {}

  /**
   * Write stats returned from {@link #write}.
   *
   * <p>Tier 2 §12 — record.
   */
  public record WriteStats(long totalRecords, long dataRecords, long gapRecords) {}

  /**
   * Writes all envelope batches to the daily file.
   *
   * @param outputPath target {@code .jsonl.zst} file path
   * @param batches list of envelope batches (one per hour or merged merge result)
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return write statistics
   * @throws IOException on I/O or serialization failure
   */
  public static WriteStats write(Path outputPath, List<List<JsonNode>> batches, ObjectMapper mapper)
      throws IOException {
    Files.createDirectories(outputPath.getParent());

    long total = 0L, dataCount = 0L, gapCount = 0L;

    // Tier 5 I1, I3: FileChannel + ZstdOutputStream; explicit force(true) before close
    try (var fc =
            FileChannel.open(
                outputPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        var zstdOut = new ZstdOutputStream(Channels.newOutputStream(fc), ZSTD_LEVEL);
        var bufOut = new BufferedOutputStream(zstdOut)) {

      for (List<JsonNode> batch : batches) {
        for (JsonNode record : batch) {
          byte[] bytes = mapper.writeValueAsBytes(record); // compact (Tier 5 B2)
          bufOut.write(bytes);
          bufOut.write(0x0A); // newline (Tier 5 B2)
          total++;
          if ("gap".equals(record.path("type").asText())) {
            gapCount++;
          } else {
            dataCount++;
          }
        }
      }

      // Tier 5 I3: flush all buffers, then fsync
      bufOut.flush();
      zstdOut.flush();
      fc.force(true); // explicit fsync before close (Q14 preferred path)
    }

    return new WriteStats(total, dataCount, gapCount);
  }
}
