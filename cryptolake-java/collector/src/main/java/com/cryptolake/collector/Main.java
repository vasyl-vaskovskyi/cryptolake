package com.cryptolake.collector;

import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.adapter.StreamKey;
import com.cryptolake.collector.backup.BackupChainReader;
import com.cryptolake.collector.capture.ExchangeLatencyRecorder;
import com.cryptolake.collector.capture.RawFrameCapture;
import com.cryptolake.collector.capture.SessionSeqAllocator;
import com.cryptolake.collector.connection.BackpressureGate;
import com.cryptolake.collector.connection.WebSocketSupervisor;
import com.cryptolake.collector.gap.DisconnectGapCoalescer;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.lifecycle.ComponentRuntimeState;
import com.cryptolake.collector.lifecycle.LifecycleStateManager;
import com.cryptolake.collector.lifecycle.ProcessHeartbeatScheduler;
import com.cryptolake.collector.liveness.StreamHeartbeatEmitter;
import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.collector.snapshot.DepthSnapshotResync;
import com.cryptolake.collector.snapshot.SnapshotFetcher;
import com.cryptolake.collector.snapshot.SnapshotScheduler;
import com.cryptolake.collector.streams.DepthStreamHandler;
import com.cryptolake.collector.streams.OpenInterestPoller;
import com.cryptolake.collector.streams.SimpleStreamHandler;
import com.cryptolake.collector.streams.StreamHandler;
import com.cryptolake.common.config.AppConfig;
import com.cryptolake.common.config.BinanceExchangeConfig;
import com.cryptolake.common.config.YamlConfigLoader;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.health.HealthServer;
import com.cryptolake.common.identity.SystemIdentity;
import com.cryptolake.common.logging.LogInit;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.Clocks;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Collector service entry point.
 *
 * <p>Wires all components, installs SIGTERM hook, blocks on a {@link CountDownLatch} until
 * shutdown. Shutdown order (design §3.3):
 *
 * <ol>
 *   <li>{@code StreamHeartbeatEmitter.stop()}
 *   <li>{@code WebSocketSupervisor.stop()}
 *   <li>{@code SnapshotScheduler.stop()}
 *   <li>{@code OpenInterestPoller.stop()}
 *   <li>{@code KafkaProducerBridge.flush(10s)}
 *   <li>{@code LifecycleStateManager.markCleanShutdown} + {@code close()}
 *   <li>{@code HealthServer.stop()}
 *   <li>{@code mainExec.close()}
 * </ol>
 *
 * <p>Thread safety: wiring runs on the main platform thread; no shared mutation after start.
 */
public final class Main {

  private static final StructuredLogger log = StructuredLogger.of(Main.class);

  public static void main(String[] args) throws Exception {
    LogInit.setLevel("INFO");

    // ── Config loading ────────────────────────────────────────────────────
    String configPath = args.length > 0 ? args[0] : "/etc/cryptolake/config.yml";
    AppConfig config = YamlConfigLoader.load(Path.of(configPath));

    BinanceExchangeConfig binance = config.exchanges().binance();
    if (binance == null || !binance.enabled()) {
      log.warn("binance_disabled");
      return;
    }

    List<String> symbols = binance.symbols();
    List<String> enabledStreams = binance.getEnabledStreams();
    String exchange = "binance";
    String topicPrefix = ""; // primary (no backup prefix)
    String collectorId = binance.collectorId();

    // ── Core singletons ───────────────────────────────────────────────────
    var clock = Clocks.systemNanoClock();
    var codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    var session = CollectorSession.create(collectorId, clock);

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = new CollectorMetrics(registry);

    // MDC: set session ID for all log lines (Tier 5 H3)
    org.slf4j.MDC.put("collector_session_id", session.sessionId());

    log.info(
        "collector_starting",
        "session_id", session.sessionId(),
        "symbols", symbols.toString(),
        "streams", enabledStreams.toString());

    // ── Kafka producer ────────────────────────────────────────────────────
    var producerBridge =
        new KafkaProducerBridge(
            config.redpanda().brokers(),
            exchange,
            topicPrefix,
            config.redpanda().producer(),
            codec,
            metrics);

    // ── Gap emitter ───────────────────────────────────────────────────────
    var gapEmitter = new GapEmitter(exchange, session, producerBridge, metrics, clock);
    producerBridge.setGapEmitter(gapEmitter);

    // ── Backpressure gate ─────────────────────────────────────────────────
    var backpressureGate = new BackpressureGate();
    producerBridge.setOverflowListener((ex, sym, st) -> backpressureGate.onDrop());

    // ── Adapters + capture chain ──────────────────────────────────────────
    var adapter =
        new BinanceAdapter(binance.wsBase(), binance.restBase(), EnvelopeCodec.newMapper());

    var seqAllocator = new SessionSeqAllocator();
    var disconnectGapCoalescer = new DisconnectGapCoalescer();
    var latencyRecorder = new ExchangeLatencyRecorder(exchange, metrics);

    // Build stream handlers
    Map<String, StreamHandler> handlers = new HashMap<>();
    for (String stream : enabledStreams) {
      switch (stream) {
        case "depth" -> {
          // Depth handler wired after resync (see below — chicken-and-egg resolution)
          // Added after WebSocketSupervisor is created
        }
        case "depth_snapshot", "open_interest" -> {
          // REST-only: no WS handler needed; produced by SnapshotScheduler/OpenInterestPoller
        }
        default -> {
          handlers.put(
              stream,
              new SimpleStreamHandler(
                  exchange, session, producerBridge, stream, gapEmitter, clock));
        }
      }
    }

    // ── HTTP client (one per service — Tier 2 §14, Tier 5 D3) ────────────
    HttpClient httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    // ── Snapshot fetcher + resync ─────────────────────────────────────────
    var snapshotFetcher = new SnapshotFetcher(httpClient, adapter, exchange, metrics);
    var backupChainReader = new BackupChainReader(codec);

    CountDownLatch globalStop = new CountDownLatch(1);
    ExecutorService virtualExec = Executors.newVirtualThreadPerTaskExecutor();

    // DepthSnapshotResync and DepthStreamHandler have a mutual dependency:
    // - DepthStreamHandler's onPuChainBreak calls depthResync.start(symbol)
    // - DepthSnapshotResync calls depthHandler.setSyncPoint(symbol, lastU)
    // Resolution: create depthResync first with null handler, then set handler via setter.
    var depthResync =
        new DepthSnapshotResync(
            exchange,
            session,
            snapshotFetcher,
            backupChainReader,
            null, // set below via setDepthHandler()
            producerBridge,
            gapEmitter,
            clock,
            config.redpanda().brokers(),
            topicPrefix,
            globalStop);

    if (enabledStreams.contains("depth")) {
      DepthStreamHandler depthHandler =
          new DepthStreamHandler(
              exchange,
              session,
              adapter,
              producerBridge,
              gapEmitter,
              clock,
              symbol -> virtualExec.submit(() -> depthResync.start(symbol)));
      handlers.put("depth", depthHandler);
      depthResync.setDepthHandler(depthHandler); // wire back (Tier 5 A5 — no lock needed here)
    }

    var capture =
        new RawFrameCapture(
            exchange,
            adapter,
            handlers,
            seqAllocator,
            backpressureGate,
            disconnectGapCoalescer,
            latencyRecorder,
            gapEmitter,
            clock,
            enabledStreams);

    // ── WebSocket supervisor ──────────────────────────────────────────────
    var supervisor =
        new WebSocketSupervisor(
            httpClient,
            adapter,
            capture,
            depthResync,
            symbols,
            enabledStreams,
            metrics,
            EnvelopeCodec.newMapper(),
            globalStop,
            virtualExec,
            exchange);

    // ── Snapshot scheduler ────────────────────────────────────────────────
    SnapshotScheduler snapshotScheduler = null;
    if (enabledStreams.contains("depth_snapshot")) {
      snapshotScheduler =
          new SnapshotScheduler(
              exchange, session, snapshotFetcher, producerBridge, gapEmitter, clock,
              symbols, binance.depth().snapshotInterval(),
              binance.depth().snapshotOverrides());
    }

    // ── OI poller ─────────────────────────────────────────────────────────
    OpenInterestPoller oiPoller = null;
    if (enabledStreams.contains("open_interest")) {
      long oiIntervalSec = SnapshotScheduler.parseIntervalSeconds(
          binance.openInterest().pollInterval());
      oiPoller = new OpenInterestPoller(
          exchange, session, adapter, httpClient, producerBridge, gapEmitter, clock, metrics,
          symbols, oiIntervalSec);
    }

    // ── Stream heartbeat emitter ──────────────────────────────────────────
    var heartbeatEmitter =
        new StreamHeartbeatEmitter(
            producerBridge,
            codec,
            exchange,
            topicPrefix,
            symbols,
            enabledStreams,
            session.sessionId(),
            capture,
            seqAllocator,
            supervisor.wsConnected,
            clock,
            metrics,
            Duration.ofSeconds(5));

    // ── Lifecycle ─────────────────────────────────────────────────────────
    var lifecycleManager = new LifecycleStateManager();
    lifecycleManager.connect(config.database());
    String bootId = SystemIdentity.getHostBootId();
    var componentState =
        new ComponentRuntimeState(
            "collector",
            session.sessionId(),
            bootId,
            Instant.now().toString(),
            Instant.now().toString());
    lifecycleManager.registerStart(componentState);
    var heartbeatScheduler = new ProcessHeartbeatScheduler(lifecycleManager, componentState);

    // ── Health server ─────────────────────────────────────────────────────
    int healthPort =
        config.monitoring() != null && config.monitoring().prometheusPort() > 0
            ? config.monitoring().prometheusPort()
            : 8080;
    var healthServer =
        new HealthServer(
            healthPort,
            () -> java.util.Map.of("ws_connected", supervisor.isConnected()),
            () -> registry.scrape().getBytes(java.nio.charset.StandardCharsets.UTF_8));

    // ── SIGTERM hook ──────────────────────────────────────────────────────
    final SnapshotScheduler snapshotSchedulerFinal = snapshotScheduler;
    final OpenInterestPoller oiPollerFinal = oiPoller;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("sigterm_received");
                  globalStop.countDown();
                }));

    // ── Start all services ────────────────────────────────────────────────
    heartbeatScheduler.start();
    heartbeatEmitter.start();
    supervisor.start();
    if (snapshotSchedulerFinal != null) snapshotSchedulerFinal.start();
    if (oiPollerFinal != null) oiPollerFinal.start();
    healthServer.start();

    log.info("collector_started", "session_id", session.sessionId());

    // ── Block until SIGTERM ───────────────────────────────────────────────
    try {
      globalStop.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // ── Orderly shutdown ──────────────────────────────────────────────────
    shutdownAll(
        heartbeatEmitter,
        supervisor,
        snapshotSchedulerFinal,
        oiPollerFinal,
        heartbeatScheduler,
        producerBridge,
        lifecycleManager,
        componentState,
        healthServer,
        virtualExec);

    log.info("collector_stopped");
  }

  private static void shutdownAll(
      StreamHeartbeatEmitter heartbeatEmitter,
      WebSocketSupervisor supervisor,
      SnapshotScheduler snapshotScheduler,
      OpenInterestPoller oiPoller,
      ProcessHeartbeatScheduler processHeartbeat,
      KafkaProducerBridge producer,
      LifecycleStateManager lifecycle,
      ComponentRuntimeState state,
      HealthServer healthServer,
      ExecutorService virtualExec) {

    log.info("shutdown_started");

    try { heartbeatEmitter.stop(); } catch (Exception ignored) {}
    try { supervisor.stop(); } catch (Exception ignored) {}
    if (snapshotScheduler != null) { try { snapshotScheduler.stop(); } catch (Exception ignored) {} }
    if (oiPoller != null) { try { oiPoller.stop(); } catch (Exception ignored) {} }
    try { processHeartbeat.stop(); } catch (Exception ignored) {}
    try { producer.flush(Duration.ofSeconds(10)); } catch (Exception ignored) {}
    try { producer.close(); } catch (Exception ignored) {}
    try {
      lifecycle.markCleanShutdown(state.component(), state.instanceId(), false, null);
    } catch (Exception ignored) {}
    try { lifecycle.close(); } catch (Exception ignored) {}
    try { healthServer.stop(); } catch (Exception ignored) {}
    virtualExec.close();

    log.info("shutdown_complete");
  }
}
