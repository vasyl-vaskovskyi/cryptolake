package com.cryptolake.writer.buffer;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.rotate.FilePaths;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes envelopes by {@link FileTarget}, maintains per-target in-memory buffers, and triggers
 * flush at the configured threshold or on demand.
 *
 * <p>Ports Python's {@code buffer_manager.py:BufferManager} (design §2.3; design §4.3).
 *
 * <p>Thread safety: consume-loop thread only (T1). No synchronization, no {@code ConcurrentHashMap}
 * (Tier 5 A5). All state is owned by T1.
 *
 * <p>Serialization: uses {@link EnvelopeCodec#toJsonBytes(Object)} + {@link
 * EnvelopeCodec#appendNewline(byte[])} for compact JSON lines without re-indent (Tier 5 B2).
 *
 * <p>Coordinates: Python mutates envelope dict in place; Java records are immutable. We use {@link
 * EnvelopeCodec#withBrokerCoordinates(DataEnvelope, BrokerCoordinates)} wrapper so coordinates
 * appear as last three fields (design §6.1 Option A).
 */
public final class BufferManager {

  private static final Logger log = LoggerFactory.getLogger(BufferManager.class);

  private final String baseDir;
  private final int flushMessages;
  private final int flushIntervalSeconds;
  private final EnvelopeCodec codec;

  /** Per-target buffer of JSON line bytes. */
  private final Map<FileTarget, List<byte[]>> buffers = new HashMap<>();

  /** Per-target high-water Kafka offset. */
  private final Map<FileTarget, Long> highWaterOffsets = new HashMap<>();

  /** Per-target partition (last observed). */
  private final Map<FileTarget, Integer> partitions = new HashMap<>();

  /** Per-target checkpoint meta (built from last envelope). */
  private final Map<FileTarget, CheckpointMeta> checkpointMetas = new HashMap<>();

  /** Per-target source (last observed — "primary" or "backup"). */
  private final Map<FileTarget, String> sources = new HashMap<>();

  /** Monotonic flush interval tracking (Tier 5 F4). */
  private long lastFlushNanos = System.nanoTime();

  public BufferManager(
      String baseDir, int flushMessages, int flushIntervalSeconds, EnvelopeCodec codec) {
    this.baseDir = baseDir;
    this.flushMessages = flushMessages;
    this.flushIntervalSeconds = flushIntervalSeconds;
    this.codec = codec;
  }

  // ── Routing ──────────────────────────────────────────────────────────────────────────────────

  /**
   * Derives a {@link FileTarget} from a {@link DataEnvelope} using UTC date and hour (Tier 5 F3,
   * M14). Symbol is lowercased (Tier 5 M1).
   */
  public FileTarget route(DataEnvelope env) {
    long ns = env.receivedAt();
    // Tier 5 E5: Instant.ofEpochSecond with modulo nanos for full precision
    Instant inst = Instant.ofEpochSecond(ns / 1_000_000_000L, ns % 1_000_000_000L);
    ZonedDateTime zdt = inst.atZone(ZoneOffset.UTC);
    int hour = zdt.getHour();
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(zdt); // Tier 5 F3 — UTC only
    return new FileTarget(
        env.exchange(),
        env.symbol().toLowerCase(Locale.ROOT), // Tier 5 M1
        env.stream(),
        date,
        hour);
  }

  // ── Add ──────────────────────────────────────────────────────────────────────────────────────

  /**
   * Adds a {@link DataEnvelope} to the buffer. If adding causes the buffer to reach the flush
   * threshold, flushes and returns the results.
   *
   * @param env the data envelope
   * @param coords broker coordinates to append (Tier 5 M8, M9)
   * @param source "primary" or "backup"
   * @return flush results if threshold reached, otherwise empty
   */
  public Optional<List<FlushResult>> add(
      DataEnvelope env, BrokerCoordinates coords, String source) {
    FileTarget target = route(env);
    byte[] line =
        codec.appendNewline(
            codec.toJsonBytes(EnvelopeCodec.withBrokerCoordinates(env, coords))); // Tier 5 B2
    buffers.computeIfAbsent(target, k -> new ArrayList<>()).add(line);

    // Update high-water and checkpoint
    if (coords.offset() >= 0) { // skip synthetic (Tier 5 M9)
      long current = highWaterOffsets.getOrDefault(target, -1L);
      if (coords.offset() > current) {
        highWaterOffsets.put(target, coords.offset());
      }
    }
    partitions.put(target, coords.partition());
    sources.put(target, source);

    // Build checkpoint meta from this envelope
    StreamKey sk = new StreamKey(env.exchange(), env.symbol(), env.stream());
    checkpointMetas.put(
        target,
        new CheckpointMeta(env.receivedAt(), env.collectorSessionId(), env.sessionSeq(), sk));

    // Auto-flush at threshold
    List<byte[]> buf = buffers.get(target);
    if (buf != null && buf.size() >= flushMessages) {
      List<FlushResult> results = new ArrayList<>();
      results.add(flushBuffer(target));
      return Optional.of(results);
    }
    return Optional.empty();
  }

  /**
   * Adds a {@link GapEnvelope} to the buffer (gap records have synthetic offset = -1L; Tier 5 M9).
   *
   * @param env the gap envelope
   * @param coords broker coordinates (typically offset = -1L for synthetic — Tier 5 M9)
   * @param source "primary" or "backup"
   * @return flush results if threshold reached, otherwise empty
   */
  public Optional<List<FlushResult>> add(GapEnvelope env, BrokerCoordinates coords, String source) {
    // Route gap envelopes by their exchange/symbol/stream and receivedAt timestamp
    long ns = env.receivedAt();
    Instant inst = Instant.ofEpochSecond(ns / 1_000_000_000L, ns % 1_000_000_000L);
    ZonedDateTime zdt = inst.atZone(ZoneOffset.UTC);
    int hour = zdt.getHour();
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(zdt);
    FileTarget target =
        new FileTarget(
            env.exchange(), env.symbol().toLowerCase(Locale.ROOT), env.stream(), date, hour);

    byte[] line =
        codec.appendNewline(codec.toJsonBytes(EnvelopeCodec.withBrokerCoordinates(env, coords)));
    buffers.computeIfAbsent(target, k -> new ArrayList<>()).add(line);
    sources.put(target, source);
    partitions.put(target, coords.partition());

    // Gap envelopes do not update high-water or checkpoint meta
    // (they carry offset = -1L, a sentinel — Tier 5 M9)

    List<byte[]> buf = buffers.get(target);
    if (buf != null && buf.size() >= flushMessages) {
      List<FlushResult> results = new ArrayList<>();
      results.add(flushBuffer(target));
      return Optional.of(results);
    }
    return Optional.empty();
  }

  // ── Flush ─────────────────────────────────────────────────────────────────────────────────────

  /**
   * Flushes the buffer for the given {@link StreamKey} (all hours for that stream).
   *
   * <p>Ports {@code BufferManager.flush_key(key)}.
   */
  public List<FlushResult> flushKey(StreamKey key) {
    List<FlushResult> results = new ArrayList<>();
    List<FileTarget> toFlush = new ArrayList<>();
    for (FileTarget t : buffers.keySet()) {
      if (t.exchange().equals(key.exchange())
          && t.symbol().equals(key.symbol())
          && t.stream().equals(key.stream())) {
        toFlush.add(t);
      }
    }
    for (FileTarget t : toFlush) {
      FlushResult r = flushBuffer(t);
      if (r.count() > 0) results.add(r);
    }
    return results;
  }

  /**
   * Flushes all buffers and returns results for all targets.
   *
   * <p>Ports {@code BufferManager.flush_all()}.
   */
  public List<FlushResult> flushAll() {
    List<FlushResult> results = new ArrayList<>();
    for (FileTarget t : new ArrayList<>(buffers.keySet())) {
      FlushResult r = flushBuffer(t);
      if (r.count() > 0) results.add(r);
    }
    lastFlushNanos = System.nanoTime(); // reset flush interval timer (Tier 5 F4)
    return results;
  }

  /** Returns the flush interval in seconds (for use by the consume loop timer check). */
  public int flushIntervalSeconds() {
    return flushIntervalSeconds;
  }

  /**
   * Returns {@code true} if the flush interval has elapsed since the last flush (Tier 5 F4 — uses
   * {@code System.nanoTime()}, not wall clock).
   */
  public boolean shouldFlushByInterval() {
    long elapsedNs = System.nanoTime() - lastFlushNanos;
    long thresholdNs = (long) flushIntervalSeconds * 1_000_000_000L;
    return elapsedNs >= thresholdNs;
  }

  // ── Private ──────────────────────────────────────────────────────────────────────────────────

  private FlushResult flushBuffer(FileTarget target) {
    List<byte[]> lines = buffers.remove(target);
    if (lines == null || lines.isEmpty()) {
      // Return empty result
      Path filePath = buildPath(target, null);
      return new FlushResult(target, filePath, List.of(), -1L, 0, 0, null, false);
    }

    long highWater = highWaterOffsets.getOrDefault(target, -1L);
    int partition = partitions.getOrDefault(target, 0);
    CheckpointMeta cpMeta = checkpointMetas.get(target);
    String source = sources.getOrDefault(target, "primary");
    boolean hasBackupSource = "backup".equals(source); // Q6: last envelope decides (design §11)

    // Clean up tracking state for this target
    highWaterOffsets.remove(target);
    partitions.remove(target);
    checkpointMetas.remove(target);
    sources.remove(target);

    Path filePath = buildPath(target, null);
    return new FlushResult(
        target, filePath, lines, highWater, partition, lines.size(), cpMeta, hasBackupSource);
  }

  private Path buildPath(FileTarget t, Integer lateSeq) {
    return FilePaths.buildFilePath(
        baseDir, t.exchange(), t.symbol(), t.stream(), t.date(), t.hour(), lateSeq);
  }
}
