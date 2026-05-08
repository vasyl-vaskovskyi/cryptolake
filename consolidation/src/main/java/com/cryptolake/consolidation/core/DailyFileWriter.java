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
 * Streams envelopes hour-by-hour into a zstd-compressed daily JSONL file.
 *
 * <p>Streaming keeps peak memory bounded to one hour's worth of parsed envelopes (spec §"Streaming
 * Writer"). The previous "build all 24 hour batches in memory, then write" shape multiplied peak
 * memory by 24 — unsafe on the 8 GB VPS for depth on busy days.
 *
 * <p>Tier 5 I1, I3: explicit {@code force(true)} before close (Q14 preferred path).
 *
 * <p>Thread safety: not safe for concurrent use; one instance per consolidation run.
 */
public final class DailyFileWriter implements AutoCloseable {

  private static final int ZSTD_LEVEL = 3;

  /** Cumulative write stats returned from {@link #stats()} after close. */
  public record WriteStats(long totalRecords, long dataRecords, long gapRecords) {}

  /** Per-hour counts returned from {@link #appendHour}. */
  public record HourCounts(long dataRecords, long gapRecords) {}

  private final FileChannel channel;
  private final ZstdOutputStream zstdOut;
  private final BufferedOutputStream bufOut;

  private long total;
  private long dataCount;
  private long gapCount;
  private boolean closed;

  private DailyFileWriter(FileChannel channel, ZstdOutputStream zstdOut, BufferedOutputStream buf) {
    this.channel = channel;
    this.zstdOut = zstdOut;
    this.bufOut = buf;
  }

  /** Opens a new daily file at {@code outputPath}, truncating any existing file. */
  public static DailyFileWriter open(Path outputPath) throws IOException {
    Files.createDirectories(outputPath.getParent());
    FileChannel fc =
        FileChannel.open(
            outputPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
    ZstdOutputStream zstd = new ZstdOutputStream(Channels.newOutputStream(fc), ZSTD_LEVEL);
    return new DailyFileWriter(fc, zstd, new BufferedOutputStream(zstd));
  }

  /**
   * Appends a single hour's envelopes to the daily file and returns the per-hour data/gap counts.
   * The caller may release {@code envelopes} immediately after this returns.
   */
  public HourCounts appendHour(List<JsonNode> envelopes, ObjectMapper mapper) throws IOException {
    long hourData = 0L;
    long hourGap = 0L;
    for (JsonNode record : envelopes) {
      byte[] bytes = mapper.writeValueAsBytes(record); // compact (Tier 5 B2)
      bufOut.write(bytes);
      bufOut.write(0x0A); // newline (Tier 5 B2)
      total++;
      if ("gap".equals(record.path("type").asText())) {
        gapCount++;
        hourGap++;
      } else {
        dataCount++;
        hourData++;
      }
    }
    return new HourCounts(hourData, hourGap);
  }

  /** Cumulative stats since open. Call after {@link #close()} for the final value. */
  public WriteStats stats() {
    return new WriteStats(total, dataCount, gapCount);
  }

  /** Closes the streams and fsyncs the file to disk. */
  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    bufOut.flush();
    zstdOut.flush();
    channel.force(true); // Tier 5 I3: explicit fsync before close
    bufOut.close(); // closes zstdOut and underlying channel-output-stream chain
    channel.close();
  }
}
