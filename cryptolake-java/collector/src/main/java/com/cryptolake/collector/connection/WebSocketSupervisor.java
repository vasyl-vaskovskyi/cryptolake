package com.cryptolake.collector.connection;

import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.adapter.StreamKey;
import com.cryptolake.collector.capture.RawFrameCapture;
import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.collector.snapshot.DepthSnapshotResync;
import com.cryptolake.common.logging.StructuredLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Top-level WebSocket connection coordinator for the collector.
 *
 * <p>Owns one WebSocket ({@code "ws"} socket, all 5 subscribable streams). Manages the connect →
 * SUBSCRIBE+ack → FirstFrameWatchdog → receive loop with exponential reconnect backoff.
 *
 * <p>On the first data frame after a successful connect+ack, triggers
 * {@link DepthSnapshotResync#start(String)} for each depth-enabled symbol.
 *
 * <p>Thread safety: {@link ConcurrentHashMap} for cross-thread state; {@code volatile} flags.
 */
public final class WebSocketSupervisor {

  private static final StructuredLogger log = StructuredLogger.of(WebSocketSupervisor.class);

  private static final String SOCKET_NAME = "ws";
  private static final Duration WATCHDOG_DEADLINE = Duration.ofSeconds(30);

  private final HttpClient httpClient;
  private final BinanceAdapter adapter;
  private final RawFrameCapture capture;
  private final DepthSnapshotResync depthResync;
  private final List<String> symbols;
  private final List<String> enabledStreams;
  private final CollectorMetrics metrics;
  private final ObjectMapper mapper;
  private final CountDownLatch globalStop;
  private final ExecutorService virtualExec;
  private final String exchange;
  private final ReconnectPolicy reconnectPolicy = new ReconnectPolicy();

  /**
   * Cross-thread readable map: {@code "ws"} → connected. Shared with
   * {@code StreamHeartbeatEmitter}.
   */
  public final ConcurrentHashMap<String, Boolean> wsConnected = new ConcurrentHashMap<>();

  private volatile WebSocket activeWs;
  private volatile boolean stopped = false;

  public WebSocketSupervisor(
      HttpClient httpClient,
      BinanceAdapter adapter,
      RawFrameCapture capture,
      DepthSnapshotResync depthResync,
      List<String> symbols,
      List<String> enabledStreams,
      CollectorMetrics metrics,
      ObjectMapper mapper,
      CountDownLatch globalStop,
      ExecutorService virtualExec,
      String exchange) {
    this.httpClient = httpClient;
    this.adapter = adapter;
    this.capture = capture;
    this.depthResync = depthResync;
    this.symbols = symbols;
    this.enabledStreams = enabledStreams;
    this.metrics = metrics;
    this.mapper = mapper;
    this.globalStop = globalStop;
    this.virtualExec = virtualExec;
    this.exchange = exchange;
  }

  /** Starts the connection supervisor loop on a virtual thread. */
  public void start() {
    Thread.ofVirtual().name("ws-supervisor").start(this::supervisorLoop);
  }

  /** Stops the supervisor and closes the active WebSocket. */
  public void stop() {
    stopped = true;
    WebSocket ws = activeWs;
    if (ws != null) {
      try {
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").join();
      } catch (Exception ignored) {
        // best-effort (Tier 5 G1)
      }
    }
  }

  public boolean isConnected() {
    return Boolean.TRUE.equals(wsConnected.get(SOCKET_NAME));
  }

  public boolean hasWsStreams() {
    return enabledStreams.stream().anyMatch(s -> StreamKey.WS_STREAMS.contains(s));
  }

  /**
   * Triggers a depth snapshot resync for the given symbol on a new virtual thread. Called by
   * {@code DepthStreamHandler} when a pu-chain break is detected.
   */
  public void triggerDepthResync(String symbol) {
    virtualExec.submit(
        () -> {
          try {
            depthResync.start(symbol);
          } catch (Exception e) {
            log.error("depth_resync_failed", e, "symbol", symbol, "error", e.getMessage());
          }
        });
  }

  private void supervisorLoop() {
    log.info("ws_supervisor_started");
    while (!stopped) {
      try {
        connectionLoop();
      } catch (Exception e) {
        if (stopped) break;
        log.warn("ws_connection_failed", "error", e.getMessage());
      }

      if (stopped) break;

      long backoffMs = reconnectPolicy.nextBackoffMillis();
      log.info("ws_reconnect_backoff", "backoff_ms", backoffMs);
      try {
        if (globalStop.await(backoffMs, TimeUnit.MILLISECONDS)) break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    log.info("ws_supervisor_stopped");
  }

  private void connectionLoop() throws Exception {
    Map<String, String> urls = adapter.getWsUrls(symbols, enabledStreams);
    if (urls.isEmpty()) {
      log.info("ws_no_subscriptions");
      try {
        globalStop.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return;
    }
    String url = urls.get(SOCKET_NAME);
    if (url == null) return;

    SubscriptionHandshake.QueueAckListener ackListener =
        new SubscriptionHandshake.QueueAckListener(mapper);

    CountDownLatch disconnectLatch = new CountDownLatch(1);
    AtomicBoolean firstFrameReceived = new AtomicBoolean(false);
    AtomicLong subscribeAckAtNsRef = new AtomicLong(0L);

    WebSocketListenerImpl listener =
        new WebSocketListenerImpl(
            SOCKET_NAME,
            (socketName, rawFrame) -> {
              if (firstFrameReceived.compareAndSet(false, true)) {
                for (String symbol : symbols) {
                  if (enabledStreams.contains("depth")) {
                    triggerDepthResync(symbol);
                  }
                }
              }
              capture.onFrame(socketName, rawFrame);
            },
            socketName -> {
              wsConnected.put(SOCKET_NAME, false);
              metrics.setWsConnectionsActive(exchange, 0);
              metrics.wsReconnects(exchange).increment();
              capture.onDisconnect(socketName, symbols, subscribeAckAtNsRef.get());
              disconnectLatch.countDown();
            },
            ackListener);

    log.info("ws_connecting", "url", url);
    WebSocket ws =
        httpClient
            .newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .buildAsync(URI.create(url), listener)
            .get(30, TimeUnit.SECONDS);
    activeWs = ws;

    List<String> subscriptions = adapter.getSubscriptions(symbols, enabledStreams);
    try {
      new SubscriptionHandshake(mapper).subscribe(ws, subscriptions, ackListener);
      subscribeAckAtNsRef.set(capture.getClock().nowNs());
    } catch (SubscriptionHandshake.ConnectionException e) {
      log.warn("subscribe_ack_failed", "error", e.getMessage());
      ws.sendClose(WebSocket.NORMAL_CLOSURE, "subscribe_failed").join();
      return;
    }

    wsConnected.put(SOCKET_NAME, true);
    metrics.setWsConnectionsActive(exchange, 1);
    reconnectPolicy.reset();
    log.info("ws_connected", "socket", SOCKET_NAME, "url", url);

    // Start FirstFrameWatchdog
    Set<String> expectedTuples = buildExpectedTuples();
    CountDownLatch watchdogStop = new CountDownLatch(1);
    FirstFrameWatchdog watchdog = new FirstFrameWatchdog(capture, watchdogStop);
    Thread.ofVirtual()
        .name("ws-watchdog")
        .start(() -> watchdog.watch(ws, expectedTuples, WATCHDOG_DEADLINE));

    // Start ping loop
    Thread.ofVirtual().name("ws-ping").start(() -> pingLoop(ws, disconnectLatch));

    // Wait for disconnect
    try {
      disconnectLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      watchdogStop.countDown();
    }
  }

  private Set<String> buildExpectedTuples() {
    Set<String> expected = new HashSet<>();
    for (String symbol : symbols) {
      for (String stream : enabledStreams) {
        if (!StreamKey.REST_ONLY_STREAMS.contains(stream) && StreamKey.WS_STREAMS.contains(stream)) {
          expected.add(RawFrameCapture.tupleKey(symbol, stream));
        }
      }
    }
    return expected;
  }

  private void pingLoop(WebSocket ws, CountDownLatch disconnectLatch) {
    while (true) {
      try {
        if (disconnectLatch.await(30, TimeUnit.SECONDS)) break;
        ws.sendPing(ByteBuffer.allocate(0)).join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.warn("ws_ping_failed", "error", e.getMessage());
        break;
      }
    }
  }
}
