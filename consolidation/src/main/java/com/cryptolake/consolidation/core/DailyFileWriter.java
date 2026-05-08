package com.cryptolake.consolidation.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
 * <p>Close ordering: write zstd frame epilogue first (by closing the buffered + zstd chain), then
 * {@code fsync}, then close the channel. Calling {@code fsync} before the epilogue would persist an
 * incomplete zstd frame; the chain is wrapped in a non-closing OutputStream so closing the zstd
 * stream does not also close the FileChannel — we want to fsync between epilogue-write and
 * channel-close.
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
  private final BufferedOutputStream bufOut;

  private long total;
  private long dataCount;
  private long gapCount;
  private boolean closed;

  private DailyFileWriter(FileChannel channel, BufferedOutputStream buf) {
    this.channel = channel;
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
    // Wrap so the close cascade (bufOut → zstd → here) does NOT close the FileChannel —
    // we need the channel open after the zstd epilogue is written so we can fsync it.
    OutputStream nonClosing =
        new FilterOutputStream(Channels.newOutputStream(fc)) {
          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len); // FilterOutputStream's default loops one byte at a time
          }

          @Override
          public void close() throws IOException {
            flush();
          }
        };
    ZstdOutputStream zstd = new ZstdOutputStream(nonClosing, ZSTD_LEVEL);
    return new DailyFileWriter(fc, new BufferedOutputStream(zstd));
  }

  /**
   * Appends a single hour's envelopes to the daily file and returns the per-hour data/gap counts.
   * The caller may release {@code envelopes} immediately after this returns.
   */
  public HourCounts appendHour(List<JsonNode> envelopes, ObjectMapper mapper) throws IOException {
    long hourData = 0L;
    long hourGap = 0L;
    for (JsonNode record : envelopes) {
      byte[] bytes = mapper.writeValueAsBytes(record);
      bufOut.write(bytes);
      bufOut.write(0x0A); // newline
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

  /** Closes the streams (writing the zstd frame epilogue), fsyncs, then closes the channel. */
  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    // Cascade: bufOut.close() → zstd.close() (writes frame epilogue) → nonClosing.close() (no-op).
    // FileChannel is intentionally NOT closed by this chain.
    bufOut.close();
    channel.force(true); // fsync after every byte (including the zstd epilogue) is on the channel
    channel.close();
  }
}
