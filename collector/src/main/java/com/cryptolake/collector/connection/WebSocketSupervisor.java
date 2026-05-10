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

/**
 * Top-level WebSocket connection coordinator for the collector.
 *
 * <p>Owns up to two parallel WebSocket connections — one per Binance routed endpoint:
 *
 * <ul>
 *   <li>{@link StreamKey#SOCKET_PUBLIC} → {@code wss://.../public/stream} (depth, bookTicker)
 *   <li>{@link StreamKey#SOCKET_MARKET} → {@code wss://.../market/stream} (aggTrade, markPrice,
 *       forceOrder broadcast)
 * </ul>
 *
 * <p>Each socket runs its own independent connect → SUBSCRIBE+ack → watchdog → receive loop with
 * its own exponential reconnect backoff. The two loops share the rest of the collector wiring
 * ({@link RawFrameCapture}, {@link BinanceAdapter}, {@link DepthSnapshotResync}). Either socket can
 * go down and reconnect without affecting the other.
 *
 * <p>On the first data frame on the public socket, triggers {@link
 * DepthSnapshotResync#start(String)} for each depth-enabled symbol (depth lives on /public).
 *
 * <p>Thread safety: per-socket state lives in concurrent maps and per-loop locals; {@code volatile}
 * flags for shared booleans.
 */
public final class WebSocketSupervisor {

  private static final StructuredLogger log = StructuredLogger.of(WebSocketSupervisor.class);

  private static final Duration WATCHDOG_DEADLINE = Duration.ofSeconds(30);

  // Ping interval injected here so unit tests can use a sub-second interval without sleeping 30s.
  // Production code uses the constant PING_INTERVAL_SECONDS via the package-private constructor.
  private final long pingIntervalSeconds;

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

  /** Per-socket reconnect backoff state. */
  private final ConcurrentHashMap<String, ReconnectPolicy> reconnectPolicies =
      new ConcurrentHashMap<>();

  /**
   * Cross-thread readable map: {@code socketName} → connected. Shared with {@code
   * StreamHeartbeatEmitter}.
   */
  public final ConcurrentHashMap<String, Boolean> wsConnected = new ConcurrentHashMap<>();

  /** Per-socket active WebSocket reference; populated/cleared by the socket's loop. */
  private final ConcurrentHashMap<String, WebSocket> activeWebSockets = new ConcurrentHashMap<>();

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
    this(
        httpClient,
        adapter,
        capture,
        depthResync,
        symbols,
        enabledStreams,
        metrics,
        mapper,
        globalStop,
        virtualExec,
        exchange,
        20L); // production default: 20-second ping interval (faster than the observed
    // ~60s upstream/NAT idle close so we detect dead sockets within one ping cycle
    // instead of waiting for the upstream cleanup; was 30s).
  }

  /** Package-private constructor for tests — allows injecting a custom ping interval. */
  WebSocketSupervisor(
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
      String exchange,
      long pingIntervalSeconds) {
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
    this.pingIntervalSeconds = pingIntervalSeconds;
  }

  /** Starts the supervisor — one virtual-thread loop per routed socket. */
  public void start() {
    Map<String, String> urls =
        adapter == null ? Map.of() : adapter.getWsUrls(symbols, enabledStreams);
    if (urls.isEmpty()) {
      log.info("ws_no_subscriptions");
      return;
    }
    for (String socketName : urls.keySet()) {
      Thread.ofVirtual()
          .name("ws-supervisor-" + socketName)
          .start(() -> supervisorLoop(socketName));
    }
  }

  /** Stops the supervisor and closes all active WebSockets. */
  public void stop() {
    stopped = true;
    for (Map.Entry<String, WebSocket> e : activeWebSockets.entrySet()) {
      WebSocket ws = e.getValue();
      if (ws == null) continue;
      try {
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").join();
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }

  /** True if at least one socket is currently connected. */
  public boolean isConnected() {
    for (Boolean v : wsConnected.values()) {
      if (Boolean.TRUE.equals(v)) return true;
    }
    return false;
  }

  /** True if a specific socket is currently connected. */
  public boolean isConnected(String socketName) {
    return Boolean.TRUE.equals(wsConnected.get(socketName));
  }

  public boolean hasWsStreams() {
    return enabledStreams.stream().anyMatch(StreamKey.WS_STREAMS::contains);
  }

  /**
   * Triggers a depth snapshot resync for the given symbol on a new virtual thread. Called by {@code
   * DepthStreamHandler} when a pu-chain break is detected.
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

  private void supervisorLoop(String socketName) {
    log.info("ws_supervisor_started", "socket", socketName);
    ReconnectPolicy policy =
        reconnectPolicies.computeIfAbsent(socketName, k -> new ReconnectPolicy());
    while (!stopped) {
      try {
        connectionLoop(socketName);
      } catch (Exception e) {
        if (stopped) break;
        log.warn("ws_connection_failed", "socket", socketName, "error", e.getMessage());
      }

      if (stopped) break;

      long backoffMs = policy.nextBackoffMillis();
      log.info("ws_reconnect_backoff", "socket", socketName, "backoff_ms", backoffMs);
      try {
        if (globalStop.await(backoffMs, TimeUnit.MILLISECONDS)) break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    log.info("ws_supervisor_stopped", "socket", socketName);
  }

  private void connectionLoop(String socketName) throws Exception {
    // Reset stateful handlers (e.g. depth detector) at the START of every public-socket connection
    // iteration. Public is the socket that carries depth; the market socket carries no stateful
    // streams, so resetting on its reconnect would only burn cycles. The reset is the abort()-path
    // safety net (ws.abort() in pingLoop bypasses Listener.onClose, leaving the depth detector
    // with stale lastU from the previous connection).
    if (StreamKey.SOCKET_PUBLIC.equals(socketName)) {
      capture.resetStatefulHandlers(symbols);
    }
    Map<String, String> urls = adapter.getWsUrls(symbols, enabledStreams);
    String url = urls.get(socketName);
    if (url == null) {
      // This socket has no subscriptions — block on shutdown.
      try {
        globalStop.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return;
    }

    SubscriptionHandshake.QueueAckListener ackListener =
        new SubscriptionHandshake.QueueAckListener(mapper);

    CountDownLatch disconnectLatch = new CountDownLatch(1);
    AtomicBoolean firstFrameReceived = new AtomicBoolean(false);
    AtomicLong subscribeAckAtNsRef = new AtomicLong(0L);
    final boolean isPublicSocket = StreamKey.SOCKET_PUBLIC.equals(socketName);
    final boolean depthEnabled = enabledStreams.contains("depth");

    WebSocketListenerImpl listener =
        new WebSocketListenerImpl(
            socketName,
            (sn, rawFrame) -> {
              // Depth resync trigger lives on the public socket (depth is /public-only).
              if (isPublicSocket && depthEnabled && firstFrameReceived.compareAndSet(false, true)) {
                for (String symbol : symbols) {
                  triggerDepthResync(symbol);
                }
              }
              capture.onFrame(sn, rawFrame);
            },
            sn -> {
              wsConnected.put(socketName, false);
              setActiveCount();
              metrics.wsReconnects(exchange).increment();
              capture.onDisconnect(sn, symbols, subscribeAckAtNsRef.get());
              disconnectLatch.countDown();
            },
            ackListener);

    log.info("ws_connecting", "socket", socketName, "url", url);
    WebSocket ws =
        httpClient
            .newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .buildAsync(URI.create(url), listener)
            .get(30, TimeUnit.SECONDS);
    activeWebSockets.put(socketName, ws);

    List<String> subscriptions =
        adapter.getSubscriptionsForSocket(socketName, symbols, enabledStreams);
    try {
      new SubscriptionHandshake(mapper).subscribe(ws, subscriptions, ackListener);
      subscribeAckAtNsRef.set(capture.getClock().nowNs());
    } catch (SubscriptionHandshake.ConnectionException e) {
      log.warn("subscribe_ack_failed", "socket", socketName, "error", e.getMessage());
      ws.sendClose(WebSocket.NORMAL_CLOSURE, "subscribe_failed").join();
      return;
    }

    boolean wasDisconnected = !wsConnected.getOrDefault(socketName, false);
    wsConnected.put(socketName, true);
    setActiveCount();
    reconnectPolicies.get(socketName).reset();
    log.info("ws_connected", "socket", socketName, "url", url);
    if (wasDisconnected) {
      log.info(
          "LIFECYCLE COLLECTOR_UPSTREAM_WS_CONNECTED: WebSocket to the exchange is open"
              + " — this collector is receiving market data again. socket={} url={}",
          socketName,
          url);
    }

    // Start FirstFrameWatchdog (one-shot: ensures every subscription on this socket gets >=1 frame
    // post-connect)
    Set<String> expectedTuples = buildExpectedTuples(socketName);
    CountDownLatch watchdogStop = new CountDownLatch(1);
    FirstFrameWatchdog watchdog = new FirstFrameWatchdog(capture, watchdogStop);
    Thread.ofVirtual()
        .name("ws-watchdog-" + socketName)
        .start(() -> watchdog.watch(ws, expectedTuples, WATCHDOG_DEADLINE));

    // Start OngoingLivenessWatchdog — runs for the life of the connection, forces reconnect if
    // any non-exempt subscription goes silent past its per-stream threshold (catches Binance
    // fstream half-open subscriptions that pass first-frame but stop delivering soon after).
    CountDownLatch ongoingStop = new CountDownLatch(1);
    OngoingLivenessWatchdog ongoingWatchdog = new OngoingLivenessWatchdog(capture, ongoingStop);
    Thread.ofVirtual()
        .name("ws-ongoing-watchdog-" + socketName)
        .start(() -> ongoingWatchdog.watch(ws, expectedTuples));

    // Start ping loop
    Thread.ofVirtual().name("ws-ping-" + socketName).start(() -> pingLoop(ws, disconnectLatch));

    // Wait for disconnect
    try {
      disconnectLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      watchdogStop.countDown();
      ongoingStop.countDown();
      activeWebSockets.remove(socketName);
    }
  }

  private Set<String> buildExpectedTuples(String socketName) {
    Set<String> socketStreams =
        StreamKey.SOCKET_PUBLIC.equals(socketName)
            ? StreamKey.PUBLIC_WS_STREAMS
            : StreamKey.MARKET_WS_STREAMS;
    Set<String> expected = new HashSet<>();
    for (String symbol : symbols) {
      for (String stream : enabledStreams) {
        if (StreamKey.REST_ONLY_STREAMS.contains(stream)) continue;
        if (!socketStreams.contains(stream)) continue;
        expected.add(RawFrameCapture.tupleKey(symbol, stream));
      }
    }
    return expected;
  }

  private void setActiveCount() {
    int active = 0;
    for (Boolean v : wsConnected.values()) if (Boolean.TRUE.equals(v)) active++;
    metrics.setWsConnectionsActive(exchange, active);
  }

  // package-private for testing
  void pingLoop(WebSocket ws, CountDownLatch disconnectLatch) {
    while (true) {
      try {
        if (disconnectLatch.await(pingIntervalSeconds, TimeUnit.SECONDS)) break;
        ws.sendPing(ByteBuffer.allocate(0)).join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.warn("ws_ping_failed", "error", e.getMessage());
        // Abort the dead WebSocket so the listener's onError/onClose fires and the
        // main loop exits its disconnectLatch.await(); then release the latch ourselves
        // in case the listener never fires (half-open connection).
        ws.abort();
        disconnectLatch.countDown();
        break;
      }
    }
  }
}
