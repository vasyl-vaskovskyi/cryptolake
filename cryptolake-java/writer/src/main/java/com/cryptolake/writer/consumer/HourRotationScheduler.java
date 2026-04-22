package com.cryptolake.writer.consumer;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.buffer.FileTarget;
import com.cryptolake.writer.buffer.FlushResult;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.rotate.FileRotator;
import com.cryptolake.writer.rotate.SealResult;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches per-stream UTC (date, hour) state. Triggers file rotation and commit when an envelope's
 * UTC date/hour differs from the previous one.
 *
 * <p>Ports Python's {@code WriterConsumer._handle_rotation_and_buffer} hour-rotation logic (design
 * §2.2; design §4.2). UTC ONLY (Tier 5 F3, M11).
 *
 * <p>Thread safety: consume-loop thread only (T1). No synchronization (Tier 5 A5).
 */
public final class HourRotationScheduler {

  private static final Logger log = LoggerFactory.getLogger(HourRotationScheduler.class);

  private final FileRotator rotator;
  private final BufferManager buffers;
  private final OffsetCommitCoordinator committer;
  private final WriterMetrics metrics;

  /** Per-stream last-seen (date, hour) — used to detect UTC hour boundaries. */
  private final Map<StreamKey, DateHour> lastSeen = new HashMap<>();

  /** Tracks sealed hours per stream for today's gauge. */
  private final Map<StreamKey, List<DateHour>> sealedHours = new HashMap<>();

  private record DateHour(String date, int hour) {}

  public HourRotationScheduler(
      FileRotator rotator,
      BufferManager buffers,
      OffsetCommitCoordinator committer,
      WriterMetrics metrics) {
    this.rotator = rotator;
    this.buffers = buffers;
    this.committer = committer;
    this.metrics = metrics;
  }

  /**
   * Observes an envelope. If its UTC date/hour differs from the previously recorded state for this
   * stream, triggers rotation of the old hour.
   *
   * <p>Ports Python's hour-rotation check in {@code consume_loop}.
   *
   * @param env the incoming data envelope
   */
  public void onEnvelope(DataEnvelope env) {
    StreamKey key = new StreamKey(env.exchange(), env.symbol(), env.stream());

    // Extract UTC date and hour from envelope timestamp (Tier 5 F3, E5)
    long ns = env.receivedAt();
    Instant inst = Instant.ofEpochSecond(ns / 1_000_000_000L, ns % 1_000_000_000L);
    ZonedDateTime zdt = inst.atZone(ZoneOffset.UTC); // UTC only (Tier 5 M11)
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(zdt);
    int hour = zdt.getHour();
    DateHour current = new DateHour(date, hour);

    DateHour prev = lastSeen.put(key, current);
    if (prev == null || (prev.date().equals(date) && prev.hour() == hour)) {
      return; // Same hour — no rotation needed
    }

    // Hour (or date) boundary crossed — rotate the old hour
    log.info(
        "hour_rotation_triggered",
        "exchange",
        env.exchange(),
        "symbol",
        env.symbol(),
        "stream",
        env.stream(),
        "prev_date",
        prev.date(),
        "prev_hour",
        prev.hour(),
        "new_date",
        date,
        "new_hour",
        hour);

    FileTarget oldTarget =
        new FileTarget(env.exchange(), env.symbol(), env.stream(), prev.date(), prev.hour());
    try {
      SealResult seal = rotator.seal(oldTarget);

      // Track sealed hours for gauge (Tier 5 M11)
      List<DateHour> history = sealedHours.computeIfAbsent(key, k -> new ArrayList<>());
      history.add(prev);

      // Update sealed gauges
      String currentDate = date;
      long todayCount = history.stream().filter(dh -> dh.date().equals(currentDate)).count();
      long prevDayCount = history.stream().filter(dh -> !dh.date().equals(currentDate)).count();
      metrics.setHoursSealedToday(env.exchange(), env.symbol(), env.stream(), (int) todayCount);
      metrics.setHoursSealedPreviousDay(
          env.exchange(), env.symbol(), env.stream(), (int) prevDayCount);

      // Commit the sealed hour
      List<FlushResult> flushResults =
          buffers.flushKey(key); // should be empty (already flushed by rotator)
      committer.commitSealedHour(flushResults, List.of(seal.dataPath()));

    } catch (Exception e) {
      log.error(
          "hour_rotation_failed",
          "exchange",
          env.exchange(),
          "symbol",
          env.symbol(),
          "stream",
          env.stream(),
          "error",
          e.getMessage());
      // Continue consume loop — rotation failure is not fatal; will retry on next envelope
    }
  }

  /**
   * Rotates all streams EXCEPT the current hour (called during shutdown — Tier 5 §3.4).
   *
   * <p>Ports Python's {@code _rotate_hour} call at shutdown.
   *
   * @param currentDate current UTC date
   * @param currentHour current UTC hour
   */
  public void rotateAllOnShutdown(String currentDate, int currentHour) {
    for (Map.Entry<StreamKey, DateHour> entry : lastSeen.entrySet()) {
      StreamKey key = entry.getKey();
      DateHour dh = entry.getValue();
      // Skip current hour — not yet complete (Python behavior)
      if (dh.date().equals(currentDate) && dh.hour() == currentHour) {
        continue;
      }
      FileTarget target =
          new FileTarget(key.exchange(), key.symbol(), key.stream(), dh.date(), dh.hour());
      try {
        rotator.seal(target);
        log.info(
            "shutdown_hour_rotated",
            "exchange",
            key.exchange(),
            "symbol",
            key.symbol(),
            "stream",
            key.stream(),
            "date",
            dh.date(),
            "hour",
            dh.hour());
      } catch (Exception e) {
        log.warn(
            "shutdown_rotation_failed",
            "exchange",
            key.exchange(),
            "symbol",
            key.symbol(),
            "error",
            e.getMessage());
      }
    }
  }
}
