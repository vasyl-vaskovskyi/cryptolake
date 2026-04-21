package com.cryptolake.writer.rotate;

import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.buffer.FileTarget;
import com.cryptolake.writer.buffer.FlushResult;
import com.cryptolake.writer.consumer.LateArrivalSequencer;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.metrics.WriterMetrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seals an active {@code hour-H.jsonl.zst} file: flush → write → sha256 sidecar.
 *
 * <p>Ports Python's {@code WriterConsumer._rotate_file} (design §2.4). File rotation happens on
 * hour boundary — driven by {@link
 * com.cryptolake.writer.consumer.HourRotationScheduler}.
 *
 * <p>Thread safety: consume-loop thread only (T1).
 */
public final class FileRotator {

  private static final Logger log = LoggerFactory.getLogger(FileRotator.class);

  private final DurableAppender appender;
  private final ZstdFrameCompressor compressor;
  private final LateArrivalSequencer lateSeq;
  private final BufferManager buffers;
  private final WriterMetrics metrics;
  private final String baseDir;

  public FileRotator(
      DurableAppender appender,
      ZstdFrameCompressor compressor,
      LateArrivalSequencer lateSeq,
      BufferManager buffers,
      WriterMetrics metrics,
      String baseDir) {
    this.appender = appender;
    this.compressor = compressor;
    this.lateSeq = lateSeq;
    this.buffers = buffers;
    this.metrics = metrics;
    this.baseDir = baseDir;
  }

  /**
   * Seals the file for the given target: flushes remaining buffered data, writes compressed bytes,
   * computes SHA-256 sidecar, increments metrics.
   *
   * <p>Returns {@link SealResult} with paths and final byte size.
   *
   * @param target the file target to seal
   * @throws UncheckedIOException if write or sidecar fails
   */
  public SealResult seal(FileTarget target) {
    // Determine if this is a late-arrival (file already exists from a previous hour that should
    // have been rotated but wasn't — e.g., restart scenario).
    Path hourPath = FilePaths.buildFilePath(
        baseDir, target.exchange(), target.symbol(), target.stream(), target.date(), target.hour(), null);
    Path dataPath;
    if (java.nio.file.Files.exists(hourPath)) {
      // Late arrival: already sealed or being sealed; generate late sequence name (Tier 5 M15)
      int seq = lateSeq.nextSeq(hourPath);
      dataPath = FilePaths.buildFilePath(
          baseDir, target.exchange(), target.symbol(), target.stream(), target.date(), target.hour(), seq);
    } else {
      dataPath = hourPath;
      lateSeq.markSealed(hourPath);
    }

    // Flush remaining buffer for this target
    List<FlushResult> results = buffers.flushKey(
        new com.cryptolake.writer.StreamKey(target.exchange(), target.symbol(), target.stream()));

    // Write all results to disk
    for (FlushResult r : results) {
      if (r.count() == 0) continue;
      byte[] compressed = compressor.compressFrame(r.lines());
      try {
        appender.appendAndFsync(dataPath, compressed);
      } catch (IOException e) {
        throw new UncheckedIOException("Seal write failed for " + dataPath, e);
      }
    }

    // Write sidecar
    Path sidecarPath = FilePaths.sidecarPath(dataPath);
    try {
      Sha256Sidecar.write(dataPath, sidecarPath);
    } catch (IOException e) {
      // Sidecar failure is logged but does not abort the seal; will be retried on next rotation
      log.warn("sidecar_write_failed", "path", sidecarPath, "error", e.getMessage());
      throw new UncheckedIOException("Sidecar write failed for " + sidecarPath, e);
    }

    // Metrics (Tier 5 H2)
    long byteSize;
    try {
      byteSize = java.nio.file.Files.size(dataPath);
    } catch (IOException e) {
      byteSize = 0L;
    }
    metrics.filesRotated(target.exchange(), target.symbol(), target.stream()).increment();

    log.info("file_rotated",
        "exchange", target.exchange(),
        "symbol", target.symbol(),
        "stream", target.stream(),
        "date", target.date(),
        "hour", target.hour(),
        "path", dataPath.toString(),
        "byte_size", byteSize);

    return new SealResult(dataPath, sidecarPath, byteSize);
  }

  /**
   * Returns the late-file path for a sealed hour path (used by recovery coordinator).
   *
   * @param hourPath the base hour path (e.g. {@code .../hour-14.jsonl.zst})
   */
  public Path lateFilePath(Path hourPath) {
    int seq = lateSeq.nextSeq(hourPath);
    String stem = hourPath.getFileName().toString().split("\\.")[0]; // "hour-14"
    return hourPath.resolveSibling(stem + ".late-" + seq + ".jsonl.zst"); // Tier 5 M15
  }

  /** Builds the UTC date string for the current moment (Tier 5 F3, M11). */
  public static String currentUtcDate() {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(ZonedDateTime.now(ZoneOffset.UTC));
  }

  /** Returns the current UTC hour (Tier 5 F3, M11). */
  public static int currentUtcHour() {
    return ZonedDateTime.now(ZoneOffset.UTC).getHour();
  }
}
