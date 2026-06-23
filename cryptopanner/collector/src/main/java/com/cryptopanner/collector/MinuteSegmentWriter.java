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
 * once {@code sealGrace} of monotonic time has elapsed since its wall-end (via {@link
 * #sealElapsed(Instant, long)}); the latest minute is always kept open so it can absorb
 * near-boundary stragglers. A frame whose minute is still open lands in its <em>correct</em> file
 * and is not counted late. Only a frame whose minute has already been sealed is kept in the current
 * open minute and counted in {@link #lateFrames()} — never discarded. Sealing is never triggered by
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
  // When true, sealed minutes carry the `.shadow` infix (design doc §5.3): an overlap-minute
  // segment written by a rotation/deploy shadow connection, kept separate from the primary until
  // the cutover merge. Flipped to false by {@link #promote()} once the shadow becomes the primary.
  private boolean shadow;
  // Optional structured logger for §11.e events (minute_sealed, late_frame_after_seal). Null in
  // tests and tooling that don't observe events.
  private com.cryptopanner.common.StructuredLog log;

  // Open minute buckets keyed by minute key (sorted), so the latest minute is always lastKey().
  private final TreeMap<String, Bucket> open = new TreeMap<>();
  private String lastSealedKey;
  private long lateFrames;
  private boolean closed;

  /** A minute's in-memory content plus the monotonic instant its wall-end was first observed. */
  private static final class Bucket {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    long endObservedNanos = Long.MIN_VALUE;
  }

  public MinuteSegmentWriter(Path baseSegments, String symbol, String stream, Duration sealGrace) {
    this(baseSegments, symbol, stream, sealGrace, false);
  }

  public MinuteSegmentWriter(
      Path baseSegments, String symbol, String stream, Duration sealGrace, boolean shadow) {
    this.baseSegments = baseSegments;
    this.symbol = symbol;
    this.stream = stream;
    this.sealGrace = sealGrace;
    this.shadow = shadow;
  }

  /**
   * Accept one capture-envelope line (caller must include trailing LF), placed by its {@code
   * bucketInstant} (server event time, or receive time when the event time is unavailable).
   */
  public synchronized void accept(byte[] line, Instant bucketInstant) throws IOException {
    if (closed) {
      return; // racing frame after shutdown seal — unavoidably late, drop it
    }
    String key = MINUTE_KEY.format(bucketInstant);
    Bucket bucket = open.get(key);
    if (bucket == null) {
      if (lastSealedKey == null || key.compareTo(lastSealedKey) > 0) {
        // Minute not yet sealed (current, future, or an out-of-order minute still within grace).
        bucket = new Bucket();
        open.put(key, bucket);
      } else {
        // Its minute is already sealed: keep the straggler in the latest open minute, count it.
        lateFrames++;
        if (log != null) {
          log.warn(
              "late_frame_after_seal",
              Map.of(
                  "symbol",
                  symbol,
                  "stream",
                  stream,
                  "server_event_time",
                  bucketInstant.toString()));
        }
        bucket = open.lastEntry().getValue();
      }
    }
    bucket.buffer.write(line);
  }

  /**
   * Seals open minutes (except the latest, kept as a straggler target) whose wall-end has passed
   * and for which at least {@code sealGrace} of <em>monotonic</em> time has elapsed since that end
   * was first observed. {@code wallNow} only detects that a minute has ended; the grace delay
   * itself is measured against {@code monoNanos} ({@link System#nanoTime}) so an NTP wall-clock
   * step during the window cannot seal a minute early (master spec §8.e). Idempotent; safe to call
   * on a timer.
   */
  public synchronized void sealElapsed(Instant wallNow, long monoNanos) throws IOException {
    if (open.size() <= 1) {
      return;
    }
    long graceNanos = sealGrace.toNanos();
    String maxKey = open.lastKey();
    Iterator<Map.Entry<String, Bucket>> it = open.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Bucket> e = it.next();
      if (e.getKey().equals(maxKey)) {
        continue;
      }
      Instant minuteEnd = minuteInstantFromKey(e.getKey()).plusSeconds(60);
      if (wallNow.isBefore(minuteEnd)) {
        continue; // minute has not ended yet
      }
      Bucket b = e.getValue();
      if (b.endObservedNanos == Long.MIN_VALUE) {
        b.endObservedNanos = monoNanos; // anchor the grace timer in monotonic time
      }
      if (monoNanos - b.endObservedNanos >= graceNanos) {
        seal(e.getKey(), b.buffer);
        it.remove();
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
    for (Map.Entry<String, Bucket> e : open.entrySet()) {
      seal(e.getKey(), e.getValue().buffer);
    }
    open.clear();
  }

  /**
   * Promotes this writer from shadow to primary naming (design doc §5.2 step 5): minutes sealed
   * after this call write primary-named {@code .jsonl.zst} files. Already-sealed {@code .shadow}
   * segments are untouched (the cutover merges them). No-op on a non-shadow writer.
   */
  public synchronized void promote() {
    shadow = false;
  }

  /**
   * Attaches a structured logger for {@code minute_sealed} / {@code late_frame_after_seal} (§11.e).
   */
  public MinuteSegmentWriter withLog(com.cryptopanner.common.StructuredLog log) {
    this.log = log;
    return this;
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
    Path dataPath = Paths.minuteSegment(baseSegments, symbol, stream, minuteInstant, shadow);
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

    if (log != null) {
      log.info(
          "minute_sealed",
          Map.of(
              "symbol", symbol,
              "stream", stream,
              "minute",
                  DateTimeFormatter.ofPattern("HH-mm")
                      .withZone(ZoneOffset.UTC)
                      .format(minuteInstant),
              "shadow", shadow));
    }
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
