package com.cryptolake.collector.connection;

import com.cryptolake.collector.capture.RawFrameCapture;
import com.cryptolake.common.logging.StructuredLogger;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Forces a WebSocket reconnect when an established subscription goes silent.
 *
 * <p>Complements {@link FirstFrameWatchdog}: that watcher only verifies the FIRST frame
 * post-connect. After it returns healthy, a half-open subscription (Binance fstream silently drops
 * bookticker/depth on some reconnects — see CLAUDE.md) goes undetected until the next ping_failed
 * cycle ~60s later, leaving the writer with no coverage on that stream during the silence window.
 *
 * <p>This watcher runs for the LIFE of the WS connection, polling per-stream silence against
 * per-stream thresholds. If any non-exempt {@code (symbol, stream)} tuple hasn't delivered a frame
 * within its threshold, the WS is closed to trigger a fresh connect+subscribe.
 *
 * <p>Per-stream thresholds reflect Binance USD-M Futures wire cadence:
 *
 * <ul>
 *   <li>{@code bookticker} — every bookTicker change, ~10/s on btcusdt → 30s threshold
 *   <li>{@code depth} — {@code @depth@100ms}, every 100ms → 30s threshold
 *   <li>{@code trades} — {@code aggTrade}, irregular but high-volume on majors → 60s threshold
 *   <li>{@code funding_rate} — {@code @markPrice@1s}, every 1s → 30s threshold
 *   <li>{@code liquidations} — {@code forceOrder}, may produce zero frames → exempt
 * </ul>
 *
 * <p>Thread safety: stateless except for the silence-poll loop; one instance per connection.
 */
public final class OngoingLivenessWatchdog {

  private static final StructuredLogger log = StructuredLogger.of(OngoingLivenessWatchdog.class);

  /** Per-stream silence threshold past which we force a reconnect. */
  private static final Map<String, Duration> SILENCE_THRESHOLDS =
      Map.of(
          "bookticker", Duration.ofSeconds(30),
          "depth", Duration.ofSeconds(30),
          "trades", Duration.ofSeconds(60),
          "funding_rate", Duration.ofSeconds(30));

  /** Streams exempt from ongoing-silence checks (may legitimately produce zero frames). */
  private static final Set<String> EXEMPT_STREAMS = Set.of("liquidations");

  private static final Duration CHECK_INTERVAL = Duration.ofSeconds(5);

  private final RawFrameCapture capture;
  private final CountDownLatch stopLatch;

  public OngoingLivenessWatchdog(RawFrameCapture capture, CountDownLatch stopLatch) {
    this.capture = capture;
    this.stopLatch = stopLatch;
  }

  /**
   * Watches expected tuples for ongoing liveness; closes the WS on extended silence.
   *
   * <p>Blocks on the calling virtual thread until either silence is detected and the WS is closed,
   * or {@code stopLatch} fires (shutdown / disconnect).
   *
   * @param ws active WebSocket; closed when extended silence is detected on any tuple
   * @param expected expected tuples in {@code "symbol\0stream"} format (built by the supervisor)
   */
  public void watch(WebSocket ws, Set<String> expected) {
    while (true) {
      try {
        if (stopLatch.await(CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)) {
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      long nowNs = capture.getClock().nowNs();
      List<String> stale = new ArrayList<>();
      for (String key : expected) {
        int sep = key.indexOf('\0');
        if (sep < 0) continue;
        String stream = key.substring(sep + 1);
        if (EXEMPT_STREAMS.contains(stream)) continue;
        Duration threshold = SILENCE_THRESHOLDS.get(stream);
        if (threshold == null) continue;

        Long lastNs = capture.lastReceivedAt.get(key);
        if (lastNs == null) {
          // First-frame watchdog handles this case (no frame yet); skip.
          continue;
        }
        long silentNs = nowNs - lastNs;
        if (silentNs > threshold.toNanos()) {
          stale.add(key.replace('\0', '/') + " (silent " + (silentNs / 1_000_000) + "ms)");
        }
      }

      if (!stale.isEmpty()) {
        log.warn(
            "ongoing_liveness_silence_detected",
            "stale",
            stale.toString(),
            "action",
            "closing WS to force reconnect");
        try {
          ws.sendClose(WebSocket.NORMAL_CLOSURE, "ongoing_liveness_silence").join();
        } catch (Exception ignored) {
          ws.abort();
        }
        return;
      }
    }
  }
}
