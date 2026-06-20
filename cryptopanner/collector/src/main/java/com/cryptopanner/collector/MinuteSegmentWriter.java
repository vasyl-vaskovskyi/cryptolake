package com.cryptopanner.collector;

import com.cryptopanner.common.Paths;
import com.cryptopanner.common.Sha256Sidecar;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Writes capture-envelope lines into per-minute zstd files, bucketed by the caller-supplied
 * <em>server event time</em> (master spec §8.c) rather than local receive time.
 *
 * <p>Late frames are never discarded: a frame whose event-minute is already sealed (a straggler) is
 * appended to the current open minute and counted in {@link #lateFrames()} — its own event
 * timestamp keeps the lateness recoverable downstream (master spec §8.e). Skeleton-only: one zstd
 * frame per minute file (re-compresses at rotation); no frame-buffer / seal-grace windows yet.
 */
public final class MinuteSegmentWriter implements AutoCloseable {

  private static final DateTimeFormatter MINUTE_KEY =
      DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);

  private final Path baseSegments;
  private final String symbol;
  private final String stream;

  private String currentMinuteKey;
  private ByteArrayOutputStream buffer;
  private long lateFrames;

  public MinuteSegmentWriter(Path baseSegments, String symbol, String stream) {
    this.baseSegments = baseSegments;
    this.symbol = symbol;
    this.stream = stream;
  }

  /**
   * Accept one capture-envelope line (caller must include trailing LF), placed by its {@code
   * bucketInstant} (server event time, or receive time when the event time is unavailable).
   */
  public synchronized void accept(byte[] line, Instant bucketInstant) throws IOException {
    String minuteKey = MINUTE_KEY.format(bucketInstant);
    if (currentMinuteKey == null) {
      currentMinuteKey = minuteKey;
      buffer = new ByteArrayOutputStream();
    } else if (minuteKey.compareTo(currentMinuteKey) > 0) {
      // Later minute: seal the open one and start the new minute.
      sealCurrent();
      currentMinuteKey = minuteKey;
      buffer = new ByteArrayOutputStream();
    } else if (minuteKey.compareTo(currentMinuteKey) < 0) {
      // Straggler: its minute is already sealed. Keep it in the current open minute, count it.
      lateFrames++;
    }
    // (equal minute: append to the open buffer)
    buffer.write(line);
  }

  /** Count of frames whose event-minute was already sealed on arrival (kept, never discarded). */
  public synchronized long lateFrames() {
    return lateFrames;
  }

  @Override
  public synchronized void close() throws IOException {
    sealCurrent();
  }

  private void sealCurrent() throws IOException {
    if (currentMinuteKey == null || buffer.size() == 0) {
      currentMinuteKey = null;
      buffer = null;
      return;
    }
    Instant minuteInstant = minuteInstantFromKey(currentMinuteKey);
    Path dataPath = Paths.minuteSegment(baseSegments, symbol, stream, minuteInstant);
    Files.createDirectories(dataPath.getParent());

    byte[] raw = buffer.toByteArray();
    byte[] compressed = Zstd.compress(raw, 3);
    Path tmp = dataPath.resolveSibling(dataPath.getFileName() + ".tmp");
    try (FileChannel ch =
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(ByteBuffer.wrap(compressed));
      ch.force(true);
    }
    Files.move(tmp, dataPath, StandardCopyOption.ATOMIC_MOVE);

    Path sidecar = dataPath.resolveSibling(dataPath.getFileName() + ".sha256");
    Sha256Sidecar.computeAndWrite(dataPath, sidecar);

    currentMinuteKey = null;
    buffer = null;
  }

  private static Instant minuteInstantFromKey(String key) {
    int y = Integer.parseInt(key.substring(0, 4));
    int mo = Integer.parseInt(key.substring(4, 6));
    int d = Integer.parseInt(key.substring(6, 8));
    int h = Integer.parseInt(key.substring(8, 10));
    int mi = Integer.parseInt(key.substring(10, 12));
    return LocalDateTime.of(y, mo, d, h, mi).toInstant(ZoneOffset.UTC);
  }
}
