package com.cryptolake.collector.snapshot;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically fetches depth snapshots via REST and produces them as {@code depth_snapshot}
 * envelopes.
 *
 * <p>Ports {@code SnapshotScheduler} from {@code src/collector/snapshot.py}. Each symbol runs on
 * its own virtual thread with a staggered initial delay ({@code interval / symbolCount * i}).
 *
 * <p>Important: periodic snapshots do NOT call {@code DepthStreamHandler.setSyncPoint} — that is
 * only done by {@link DepthSnapshotResync} (matches Python comment in {@code snapshot.py:156-159}).
 *
 * <p>Uses {@link CountDownLatch#await(long, TimeUnit)} for interruptible waits (Tier 5 A3).
 */
public final class SnapshotScheduler {

  private static final StructuredLogger log = StructuredLogger.of(SnapshotScheduler.class);

  private final String exchange;
  private final CollectorSession session;
  private final SnapshotFetcher fetcher;
  private final KafkaProducerBridge producer;
  private final GapEmitter gapEmitter;
  private final ClockSupplier clock;
  private final List<String> symbols;
  private final int defaultIntervalSec;
  private final Map<String, Integer> intervalOverrides;

  private final CountDownLatch stopLatch = new CountDownLatch(1);
  private final AtomicBoolean running = new AtomicBoolean(false);

  public SnapshotScheduler(
      String exchange,
      CollectorSession session,
      SnapshotFetcher fetcher,
      KafkaProducerBridge producer,
      GapEmitter gapEmitter,
      ClockSupplier clock,
      List<String> symbols,
      String defaultInterval,
      Map<String, String> intervalOverrides) {
    this.exchange = exchange;
    this.session = session;
    this.fetcher = fetcher;
    this.producer = producer;
    this.gapEmitter = gapEmitter;
    this.clock = clock;
    this.symbols = symbols;
    this.defaultIntervalSec = parseIntervalSeconds(defaultInterval);
    this.intervalOverrides = new java.util.HashMap<>();
    if (intervalOverrides != null) {
      for (var e : intervalOverrides.entrySet()) {
        this.intervalOverrides.put(e.getKey(), parseIntervalSeconds(e.getValue()));
      }
    }
  }

  /** Parses an interval string ({@code "5m"}, {@code "1m"}, {@code "30s"}) to seconds. */
  public static int parseIntervalSeconds(String interval) {
    if (interval == null || interval.isEmpty()) return 300;
    if (interval.endsWith("m")) {
      return Integer.parseInt(interval.substring(0, interval.length() - 1)) * 60;
    }
    if (interval.endsWith("s")) {
      return Integer.parseInt(interval.substring(0, interval.length() - 1));
    }
    throw new IllegalArgumentException("Invalid interval: " + interval);
  }

  /** Starts per-symbol snapshot loops on virtual threads. */
  public void start() {
    running.set(true);
    int n = symbols.size();
    for (int i = 0; i < n; i++) {
      String symbol = symbols.get(i);
      int intervalSec = intervalOverrides.getOrDefault(symbol, defaultIntervalSec);
      long delaySec = n > 0 ? (long) intervalSec * i / n : 0;
      final String sym = symbol;
      final int intSec = intervalSec;
      final long delSec = delaySec;
      Thread.ofVirtual().name("snapshot-" + symbol).start(() -> pollLoop(sym, intSec, delSec));
    }
  }

  /** Stops all snapshot loops. */
  public void stop() {
    running.set(false);
    stopLatch.countDown();
  }

  private void pollLoop(String symbol, int intervalSec, long initialDelaySec) {
    log.info("snapshot_scheduler_started", "symbol", symbol, "interval_sec", intervalSec);

    // Initial staggered delay (Tier 5 A3)
    if (initialDelaySec > 0) {
      try {
        if (stopLatch.await(initialDelaySec, TimeUnit.SECONDS)) return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    while (running.get()) {
      takeSnapshot(symbol);
      try {
        if (stopLatch.await(intervalSec, TimeUnit.SECONDS)) break; // stop signal
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    log.info("snapshot_scheduler_stopped", "symbol", symbol);
  }

  private void takeSnapshot(String symbol) {
    var rawText = fetcher.fetch(symbol);
    if (rawText.isEmpty()) {
      log.warn("snapshot_poll_miss", "symbol", symbol);
      gapEmitter.emit(
          symbol,
          "depth_snapshot",
          -1L,
          "snapshot_poll_miss",
          "REST snapshot fetch failed after 3 retries");
      return;
    }
    // Produce depth_snapshot envelope
    DataEnvelope env =
        DataEnvelope.create(
            exchange,
            symbol,
            "depth_snapshot",
            rawText.get(),
            0L, // depth_snapshot has no exchange_ts
            session.sessionId(),
            -1L,
            clock);
    producer.produce(env);
    log.info("snapshot_produced", "symbol", symbol);
  }

  /** Returns the interval for the given symbol in seconds. */
  public int getInterval(String symbol) {
    return intervalOverrides.getOrDefault(symbol, defaultIntervalSec);
  }
}
