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
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Writes raw frames into per-minute zstd files. Skeleton-only:
 *
 * <ul>
 *   <li>Buckets by <em>local receive time</em>, not server-event-time (post-skeleton work).
 *   <li>No frame-buffer / seal-grace windows.
 *   <li>One zstd frame per minute file (re-compresses at rotation).
 * </ul>
 */
public final class MinuteSegmentWriter implements AutoCloseable {

  private static final DateTimeFormatter MINUTE_KEY =
      DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);

  private final Path baseSegments;
  private final String symbol;
  private final String stream;
  private final Clock clock;

  private String currentMinuteKey;
  private ByteArrayOutputStream buffer;

  public MinuteSegmentWriter(Path baseSegments, String symbol, String stream, Clock clock) {
    this.baseSegments = baseSegments;
    this.symbol = symbol;
    this.stream = stream;
    this.clock = clock;
  }

  /** Accept one raw frame (caller must include trailing LF). */
  public synchronized void accept(byte[] frame) throws IOException {
    Instant now = clock.instant();
    String minuteKey = MINUTE_KEY.format(now);
    if (currentMinuteKey != null && !minuteKey.equals(currentMinuteKey)) {
      sealCurrent();
    }
    if (currentMinuteKey == null) {
      currentMinuteKey = minuteKey;
      buffer = new ByteArrayOutputStream();
    }
    buffer.write(frame);
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
