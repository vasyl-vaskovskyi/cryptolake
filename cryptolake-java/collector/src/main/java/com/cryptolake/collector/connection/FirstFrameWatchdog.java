package com.cryptolake.collector.connection;

import com.cryptolake.collector.capture.RawFrameCapture;
import com.cryptolake.common.logging.StructuredLogger;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Forces a WebSocket reconnect when Binance silently fails to wire some subscriptions.
 *
 * <p>Per-connect, watches a set of expected {@code (symbol, stream)} tuples. Polls
 * {@link RawFrameCapture#lastReceivedAt} every 2s; if all expected pairs have received at least one
 * frame before the deadline, returns healthy. If the deadline passes with any pair still silent,
 * logs the missing list and closes the socket.
 *
 * <p>Exempt streams: {@code liquidations} — Binance documents zero frames if no liquidations occur
 * during the window. Hardcoded, not configurable.
 *
 * <p>Thread safety: stateless except for the deadline check; one instance per connection loop
 * iteration.
 */
public final class FirstFrameWatchdog {

  private static final StructuredLogger log = StructuredLogger.of(FirstFrameWatchdog.class);

  /** Streams exempt from the first-frame check (may legitimately produce zero frames). */
  private static final Set<String> EXEMPT_STREAMS = Set.of("liquidations");

  private static final Duration CHECK_INTERVAL = Duration.ofSeconds(2);

  private final RawFrameCapture capture;
  private final CountDownLatch stopLatch;

  public FirstFrameWatchdog(RawFrameCapture capture, CountDownLatch stopLatch) {
    this.capture = capture;
    this.stopLatch = stopLatch;
  }

  /**
   * Watches the expected tuples. Blocks on the calling virtual thread until either all pairs have
   * delivered a frame or the deadline passes.
   *
   * @param ws the active WebSocket; closed if the deadline passes without all frames arriving
   * @param expected set of expected tuples in the format {@code "symbol\0stream"}
   * @param deadline maximum duration to wait from call time
   */
  public void watch(WebSocket ws, Set<String> expected, Duration deadline) {
    long deadlineNs = System.nanoTime() + deadline.toNanos();

    while (true) {
      // Check if all expected (non-exempt) pairs have received data
      List<String> missing = new ArrayList<>();
      for (String key : expected) {
        int sep = key.indexOf('\0');
        if (sep < 0) continue;
        String stream = key.substring(sep + 1);
        if (EXEMPT_STREAMS.contains(stream)) continue;
        if (!capture.lastReceivedAt.containsKey(key)) {
          missing.add(key.replace('\0', '/'));
        }
      }

      if (missing.isEmpty()) {
        log.info("first_frame_watchdog_healthy", "checked", expected.size());
        return;
      }

      if (System.nanoTime() > deadlineNs) {
        log.warn("first_frame_deadline_missed", "missing", missing.toString());
        // Close the socket to trigger reconnect
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "first_frame_deadline").join();
        return;
      }

      // Sleep for check interval or stop signal (Tier 5 A3)
      try {
        if (stopLatch.await(CHECK_INTERVAL.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
          return; // stop requested
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
