package com.cryptopanner.collector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * WebSocket client for Binance combined-streams. Connects, sends SUBSCRIBE, waits for ACK, then
 * delivers frames to the consumer. If the connection is closed or errors while {@link #stop()} has
 * NOT been called, the client reconnects with exponential backoff + ±25 % jitter per master spec
 * §8.b: 1 s, 2 s, 4 s, 8 s, 16 s, 32 s, 60 s (cap). The attempt counter resets after each
 * successful SUBSCRIBE ACK so a single mid-day disconnect doesn't burn the long-end backoff.
 */
public final class BinanceWsClient {

  // Backoff schedule: 1 s, 2 s, 4 s, 8 s, 16 s, 32 s, then 60 s forever.
  static final long BACKOFF_CAP_MS = 60_000L;
  static final long BACKOFF_BASE_MS = 1_000L;

  private final URI endpoint;
  private final List<String> streams;
  // Delivers (raw frame text, receive instant). Receive time is captured in the read loop so a
  // downstream backtest knows when each record was actually in hand (master spec §8.c).
  private final BiConsumer<String, Instant> onFrame;
  private final AtomicInteger nextId = new AtomicInteger(1);
  // Reused across reconnects — JDK HttpClient owns its own executor and is meant to be shared.
  private final HttpClient http = HttpClient.newHttpClient();
  // Written from caller (start), WS reader thread (onText ACK), and the reconnect executor;
  // AtomicInteger keeps the field JMM-safe under those three accessors.
  private final AtomicInteger attempt = new AtomicInteger(0);

  private volatile WebSocket ws;
  private volatile boolean stopped = false;
  // Monotonic timestamp of the last inbound text message (ACK or data). Feeds the half-open
  // watchdog: a healthy socket on /public or /market produces sub-second traffic, so a long idle
  // gap means the connection is silently dead (the fstream half-open bug, §8.a).
  private volatile long lastActivityNanos = System.nanoTime();
  // Count of binary WS frames received. Binance fstream is text-only, so any binary frame is
  // anomalous (§12 / §13): we count it for metrics and log a WARN rather than silently dropping it.
  private final AtomicLong binaryFramesUnexpected = new AtomicLong();
  // Monotonic instant the current connection's SUBSCRIBE was ACKed (Long.MIN_VALUE = not yet
  // connected). Reset on every successful (re)subscribe — feeds the rotation scheduler (§5.1) and
  // /status current_connection_age_s (§11.c).
  private volatile long connectionAckedNanos = Long.MIN_VALUE;

  private final ScheduledExecutorService reconnectExec =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "ws-reconnect");
            t.setDaemon(true);
            return t;
          });

  public BinanceWsClient(URI endpoint, List<String> streams, BiConsumer<String, Instant> onFrame) {
    this.endpoint = endpoint;
    this.streams = streams;
    this.onFrame = onFrame;
  }

  /**
   * Establishes the WebSocket connection, issues SUBSCRIBE, and blocks until the ACK is received.
   * Returns immediately after the ACK; subsequent frames are delivered to the consumer
   * asynchronously. The client automatically reconnects if the connection drops.
   */
  public void start() throws Exception {
    attempt.set(0);
    connectAndSubscribe();
  }

  /** Nanos since the last inbound message — the half-open watchdog's liveness signal. */
  public long idleNanos() {
    return System.nanoTime() - lastActivityNanos;
  }

  /** Count of unexpected binary WS frames received (Binance fstream should be text-only). */
  public long binaryFramesUnexpected() {
    return binaryFramesUnexpected.get();
  }

  /** Age of the current connection since its SUBSCRIBE ACK, or empty if not yet subscribed. */
  public java.util.Optional<java.time.Duration> currentConnectionAge() {
    long acked = connectionAckedNanos;
    if (acked == Long.MIN_VALUE) {
      return java.util.Optional.empty();
    }
    return java.util.Optional.of(java.time.Duration.ofNanos(System.nanoTime() - acked));
  }

  /**
   * Forces recovery from a (suspected half-open) connection: aborts the current socket and
   * schedules an immediate reconnect + re-subscribe. {@link WebSocket#abort()} does not notify the
   * listener, so the reconnect is driven here directly. No-op once {@link #stop()} has been called.
   */
  public void forceReconnect() {
    if (stopped) {
      return;
    }
    WebSocket w = ws;
    if (w != null) {
      w.abort();
    }
    lastActivityNanos = System.nanoTime(); // avoid an immediate re-trigger before the new socket
    scheduleReconnect();
  }

  /** Stops reconnect scheduling and closes the current WebSocket connection. Idempotent. */
  public void stop() {
    stopped = true;
    reconnectExec.shutdownNow();
    WebSocket w = ws;
    if (w != null) {
      w.sendClose(WebSocket.NORMAL_CLOSURE, "bye").orTimeout(2, TimeUnit.SECONDS);
    }
  }

  // ── internals ────────────────────────────────────────────────────────────────

  private void connectAndSubscribe() throws Exception {
    connectionAckedNanos = Long.MIN_VALUE; // age is undefined until this connection's ACK
    int id = nextId.getAndIncrement();
    CompletableFuture<Void> ackSeen = new CompletableFuture<>();

    WebSocket.Listener listener =
        new WebSocket.Listener() {
          private final StringBuilder buf = new StringBuilder();

          @Override
          public void onOpen(WebSocket webSocket) {
            ws = webSocket;
            String sub =
                "{\"method\":\"SUBSCRIBE\",\"params\":[\""
                    + String.join("\",\"", streams)
                    + "\"],\"id\":"
                    + id
                    + "}";
            webSocket.sendText(sub, true);
            webSocket.request(1);
          }

          @Override
          public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buf.append(data);
            if (last) {
              lastActivityNanos = System.nanoTime();
              String full = buf.toString();
              buf.setLength(0);
              if (!ackSeen.isDone() && full.contains("\"result\"")) {
                attempt.set(0); // reset backoff after successful handshake
                connectionAckedNanos = System.nanoTime(); // connection age starts at ACK
                ackSeen.complete(null);
              } else if (ackSeen.isDone()) {
                onFrame.accept(full, Instant.now());
              }
            }
            webSocket.request(1);
            return null;
          }

          @Override
          public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            // Binance fstream is text-only. A binary frame is anomalous: count it, WARN, and keep
            // the read loop flowing (request the next message) so one stray frame can't stall the
            // socket. The payload is not captured — there is no defined schema for it.
            lastActivityNanos = System.nanoTime();
            if (last) {
              long n = binaryFramesUnexpected.incrementAndGet();
              System.err.println(
                  "[ws] WARN ws_binary_frame_unexpected endpoint=" + endpoint + " count=" + n);
            }
            webSocket.request(1);
            return null;
          }

          @Override
          public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            // Complete the ack future exceptionally so start() doesn't hang if ACK never arrived.
            ackSeen.completeExceptionally(
                new Exception("ws closed before ACK: " + statusCode + " " + reason));
            scheduleReconnect();
            return null;
          }

          @Override
          public void onError(WebSocket webSocket, Throwable error) {
            ackSeen.completeExceptionally(error);
            scheduleReconnect();
          }
        };

    ws =
        http.newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .buildAsync(endpoint, listener)
            .get();

    // Block until ACK (or timeout/close). On initial start() this is the caller's sync point.
    try {
      ackSeen.orTimeout(10, TimeUnit.SECONDS).join();
    } catch (Exception e) {
      // If stopped while waiting, don't propagate.
      if (stopped) return;
      throw e;
    }
  }

  private void scheduleReconnect() {
    if (stopped) return;
    long delayMs = computeBackoffMillis(attempt.getAndIncrement());
    try {
      reconnectExec.schedule(
          () -> {
            if (stopped) return;
            try {
              connectAndSubscribe();
            } catch (Exception ignored) {
              // onClose / onError on the new connection will re-trigger scheduleReconnect.
            }
          },
          delayMs,
          TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.RejectedExecutionException ignored) {
      // Executor was shut down by stop() — no reconnect needed.
    }
  }

  /**
   * Returns the backoff delay in milliseconds for the given attempt index. Base: {@code min(60000,
   * 1000 * 2^attempt)}. Jitter: uniform ±25 % of the base.
   *
   * <p>Package-private for test access.
   */
  static long computeBackoffMillis(int attempt) {
    // Clamp shift to avoid overflow: 2^6 = 64 s > cap, so anything >= 6 is already capped.
    long base = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS << Math.min(attempt, 6));
    // Uniform jitter in [-0.25, +0.25] of base.
    double jitterFactor = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.5;
    return Math.max(1L, (long) (base * jitterFactor));
  }
}
