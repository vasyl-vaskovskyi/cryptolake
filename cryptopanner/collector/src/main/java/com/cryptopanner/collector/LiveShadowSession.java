package com.cryptopanner.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Production {@link ShadowSession}: opens a real second WS connection set (one client per routed
 * socket, distinct {@code +shadow} User-Agent), routes its frames into {@code .shadow} minute
 * segments, and exposes the overlap minutes for the {@link WsConnectionManager} to verify and cut
 * over (design doc §5.2). The primary-side signals (drop detection) and the primary-close action
 * are injected so this class owns only the shadow side; the full two-connection overlap is
 * exercised by the soak (§14.e).
 */
public final class LiveShadowSession implements ShadowSession {

  /** One routed shadow socket: endpoint URL + the combined streams to subscribe. */
  public record SocketSpec(URI endpoint, List<String> streams) {}

  /** One tracked capture target so the shadow writers mirror the primary keys exactly. */
  public record WriterSpec(String symbol, String stream) {}

  /**
   * Hand-off callback fired at the end of {@link #promote()} (design doc §5.2 step 5): the shadow
   * has become the new primary. The composition root swaps its half-open watchdog / connection-age
   * / next-rotation references to {@code newPrimaryClients} and retires {@code session} on the next
   * rotation, so sequential rotations never read the stale old-primary state.
   */
  @FunctionalInterface
  public interface PromotionSink {
    void promoted(ShadowSession session, List<BinanceWsClient> newPrimaryClients);
  }

  private final PromotionSink onPromoted;

  private static final String SHADOW_SUFFIX = ".shadow.jsonl.zst";

  private final Path segments;
  private final Map<String, MinuteSegmentWriter> shadowWriters;
  private final FrameRouter router;
  private final List<BinanceWsClient> shadowClients = new ArrayList<>();
  private final ScheduledExecutorService ticker;
  private final Duration probeWindow;
  private final BooleanSupplier primaryDropped;
  private final BooleanSupplier bothDropped;
  private final Runnable closePrimary;
  private final Set<String> seenMinutes = new java.util.HashSet<>();
  private volatile boolean closed;

  public LiveShadowSession(
      List<SocketSpec> sockets,
      Collection<WriterSpec> writers,
      Path segments,
      Duration sealGrace,
      ObjectMapper mapper,
      String shadowUserAgent,
      Duration probeWindow,
      BooleanSupplier primaryDropped,
      BooleanSupplier bothDropped,
      Runnable closePrimary,
      PromotionSink onPromoted)
      throws Exception {
    this.segments = segments;
    this.probeWindow = probeWindow;
    this.primaryDropped = primaryDropped;
    this.bothDropped = bothDropped;
    this.closePrimary = closePrimary;
    this.onPromoted = onPromoted;

    this.shadowWriters = new HashMap<>();
    for (WriterSpec w : writers) {
      shadowWriters.put(
          w.symbol() + "@" + w.stream(),
          new MinuteSegmentWriter(segments, w.symbol(), w.stream(), sealGrace, /* shadow= */ true));
    }
    this.router = new FrameRouter(mapper, shadowWriters);

    for (SocketSpec spec : sockets) {
      BinanceWsClient client =
          new BinanceWsClient(spec.endpoint(), spec.streams(), router::handle, shadowUserAgent);
      client.start();
      shadowClients.add(client);
    }

    this.ticker =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "shadow-seal-ticker");
              t.setDaemon(true);
              return t;
            });
    ticker.scheduleAtFixedRate(this::sealTick, 1, 1, TimeUnit.SECONDS);
  }

  private void sealTick() {
    Instant now = Instant.now();
    long mono = System.nanoTime();
    for (MinuteSegmentWriter w : shadowWriters.values()) {
      try {
        w.sealElapsed(now, mono);
      } catch (Exception e) {
        System.err.println("[shadow] seal failed: " + e.getMessage());
      }
    }
  }

  @Override
  public boolean probeLiveness() {
    long deadline = System.nanoTime() + probeWindow.toNanos();
    while (System.nanoTime() < deadline) {
      if (router.framesWritten() > 0) {
        return true;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return router.framesWritten() > 0;
  }

  @Override
  public Optional<OverlapMinute> awaitSealedMinute() {
    while (!closed && !bothDropped.getAsBoolean()) {
      Optional<OverlapMinute> minute = scanSealedShadowMinute(segments, seenMinutes);
      if (minute.isPresent()) {
        return minute;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean primaryDropped() {
    return primaryDropped.getAsBoolean();
  }

  @Override
  public boolean bothDropped() {
    return bothDropped.getAsBoolean();
  }

  @Override
  public void promote() {
    closePrimary.run(); // retire the old primary first (make-before-break is already past)
    for (MinuteSegmentWriter w : shadowWriters.values()) {
      w.promote();
    }
    onPromoted.promoted(this, shadowClients); // hand this session off as the new primary
  }

  @Override
  public void close() {
    closed = true;
    ticker.shutdownNow();
    for (BinanceWsClient c : shadowClients) {
      c.stop();
    }
    for (MinuteSegmentWriter w : shadowWriters.values()) {
      try {
        w.close();
      } catch (Exception e) {
        System.err.println("[shadow] writer close failed: " + e.getMessage());
      }
    }
  }

  /**
   * Finds the oldest sealed {@code .shadow} overlap minute not already in {@code seen}, pairing
   * each shadow segment with its primary sibling. Records the returned minute in {@code seen} so a
   * later call advances to the next minute. Pure filesystem logic — unit-testable without a live
   * socket.
   */
  static Optional<OverlapMinute> scanSealedShadowMinute(Path segments, Set<String> seen) {
    if (!Files.isDirectory(segments)) {
      return Optional.empty();
    }
    // minuteKey ("<date> <HH-MM>", chronological when sorted) → its shadow file pairs.
    TreeMap<String, List<SegmentPair>> byMinute = new TreeMap<>();
    Map<String, int[]> minuteMeta = new HashMap<>(); // minuteKey → {minuteOfHour}
    Map<String, Instant> minuteStart = new HashMap<>();
    try (Stream<Path> walk = Files.walk(segments)) {
      for (Path shadow : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
        String name = shadow.getFileName().toString();
        if (!name.endsWith(SHADOW_SUFFIX)) {
          continue;
        }
        Path rel = segments.relativize(shadow);
        if (rel.getNameCount() < 4) {
          continue;
        }
        String symbol = rel.getName(0).toString();
        String stream = rel.getName(1).toString();
        String date = rel.getName(2).toString();
        String hhmm = name.substring("minute-".length(), name.length() - SHADOW_SUFFIX.length());
        String minuteKey = date + " " + hhmm;
        if (seen.contains(minuteKey)) {
          continue;
        }
        Path primary = shadow.resolveSibling(hhmm(name) + ".jsonl.zst");
        byMinute
            .computeIfAbsent(minuteKey, k -> new ArrayList<>())
            .add(new SegmentPair(symbol, stream, primary, shadow));
        int mm = Integer.parseInt(hhmm.substring(hhmm.indexOf('-') + 1));
        minuteMeta.put(minuteKey, new int[] {mm});
        minuteStart.put(minuteKey, Instant.parse(date + "T" + hhmm.replace('-', ':') + ":00Z"));
      }
    } catch (Exception e) {
      return Optional.empty();
    }
    if (byMinute.isEmpty()) {
      return Optional.empty();
    }
    String oldest = byMinute.firstKey();
    seen.add(oldest);
    return Optional.of(
        new OverlapMinute(
            minuteMeta.get(oldest)[0], minuteStart.get(oldest), byMinute.get(oldest)));
  }

  /** "minute-14-23.shadow.jsonl.zst" → "minute-14-23" (the primary file's stem). */
  private static String hhmm(String shadowName) {
    return shadowName.substring(0, shadowName.length() - SHADOW_SUFFIX.length());
  }
}
