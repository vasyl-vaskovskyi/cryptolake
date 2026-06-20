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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes capture-envelope lines into per-minute zstd files, bucketed by the caller-supplied
 * <em>server event time</em> (master spec §8.c).
 *
 * <p><b>Seal-grace window (§8.e).</b> Several minutes stay open at once. A minute is sealed only
 * once the caller-driven clock has passed its close instant plus {@code sealGrace} (via {@link
 * #sealElapsed(Instant)}); the latest minute is always kept open so it can absorb near-boundary
 * stragglers. A frame whose minute is still open lands in its <em>correct</em> file and is not
 * counted late. Only a frame whose minute has already been sealed is kept in the current open
 * minute and counted in {@link #lateFrames()} — never discarded. Sealing is never triggered by
 * {@link #accept} itself; production drives {@link #sealElapsed} on a ticker, and {@link #close}
 * seals the remainder.
 */
public final class MinuteSegmentWriter implements AutoCloseable {

  private static final DateTimeFormatter MINUTE_KEY =
      DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);

  private final Path baseSegments;
  private final String symbol;
  private final String stream;
  private final Duration sealGrace;

  // Open minute buffers keyed by minute key (sorted), so the latest minute is always lastKey().
  private final TreeMap<String, ByteArrayOutputStream> open = new TreeMap<>();
  private String lastSealedKey;
  private long lateFrames;

  public MinuteSegmentWriter(Path baseSegments, String symbol, String stream, Duration sealGrace) {
    this.baseSegments = baseSegments;
    this.symbol = symbol;
    this.stream = stream;
    this.sealGrace = sealGrace;
  }

  /**
   * Accept one capture-envelope line (caller must include trailing LF), placed by its {@code
   * bucketInstant} (server event time, or receive time when the event time is unavailable).
   */
  public synchronized void accept(byte[] line, Instant bucketInstant) throws IOException {
    String key = MINUTE_KEY.format(bucketInstant);
    ByteArrayOutputStream buf = open.get(key);
    if (buf == null) {
      if (lastSealedKey == null || key.compareTo(lastSealedKey) > 0) {
        // Minute not yet sealed (current, future, or an out-of-order minute still within grace).
        buf = new ByteArrayOutputStream();
        open.put(key, buf);
      } else {
        // Its minute is already sealed: keep the straggler in the latest open minute, count it.
        lateFrames++;
        buf = open.lastEntry().getValue();
      }
    }
    buf.write(line);
  }

  /**
   * Seals every open minute whose close instant plus {@code sealGrace} is at or before {@code now},
   * except the latest open minute (kept as a straggler target). Idempotent; safe to call on a
   * timer.
   */
  public synchronized void sealElapsed(Instant now) throws IOException {
    if (open.size() <= 1) {
      return;
    }
    String maxKey = open.lastKey();
    Iterator<Map.Entry<String, ByteArrayOutputStream>> it = open.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, ByteArrayOutputStream> e = it.next();
      if (e.getKey().equals(maxKey)) {
        continue;
      }
      Instant deadline = minuteInstantFromKey(e.getKey()).plusSeconds(60).plus(sealGrace);
      if (!now.isBefore(deadline)) {
        seal(e.getKey(), e.getValue());
        it.remove();
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (Map.Entry<String, ByteArrayOutputStream> e : open.entrySet()) {
      seal(e.getKey(), e.getValue());
    }
    open.clear();
  }

  /** Count of frames whose event-minute was already sealed on arrival (kept, never discarded). */
  public synchronized long lateFrames() {
    return lateFrames;
  }

  private void seal(String key, ByteArrayOutputStream buffer) throws IOException {
    advanceLastSealed(key);
    if (buffer.size() == 0) {
      return;
    }
    Instant minuteInstant = minuteInstantFromKey(key);
    Path dataPath = Paths.minuteSegment(baseSegments, symbol, stream, minuteInstant);
    Files.createDirectories(dataPath.getParent());

    byte[] compressed = Zstd.compress(buffer.toByteArray(), 3);
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
  }

  private void advanceLastSealed(String key) {
    if (lastSealedKey == null || key.compareTo(lastSealedKey) > 0) {
      lastSealedKey = key;
    }
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
