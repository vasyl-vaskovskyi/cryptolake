package com.cryptopanner.collector;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.HealthServer;
import com.cryptopanner.common.RestPoller;
import com.cryptopanner.common.RotationTrigger;
import com.cryptopanner.common.StreamRouting;
import com.cryptopanner.common.StructuredLog;
import com.cryptopanner.common.config.NodeConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class Main {

  // On-disk stream for depth snapshots (periodic baseline poll + on-demand resync, §7.b.1).
  private static final String DEPTH_SNAPSHOT_STREAM = "depthSnapshot";

  // Distinct User-Agent for the rotation shadow connection (§5.2 step 1).
  private static final String SHADOW_USER_AGENT = "cryptopanner/0.1.0+shadow";

  /**
   * Production runs until SIGTERM (no dev overlay → maxRuntime 0); a positive value smoke-tests.
   */
  static boolean runsUntilSignal(long maxRuntimeSeconds) {
    return maxRuntimeSeconds <= 0;
  }

  /**
   * This collector's deploy slot from {@code CRYPTOPANNER_SLOT} (systemd {@code @a}/{@code @b}).
   */
  static String resolveSlot(String envSlot) {
    return envSlot == null || envSlot.isBlank() ? "a" : envSlot.trim();
  }

  /** Per-slot heartbeat path (§11.b) — the exact path the Node Agent's StatusBuilder reads. */
  static Path heartbeatFile(String slot) {
    return Path.of("/tmp/cryptopanner-collector@" + slot + ".heartbeat");
  }

  /** Per-slot rotation-status path (§11.c) — published here, read by the Node Agent for /status. */
  static Path rotationStatusFile(String slot) {
    return Path.of("/tmp/cryptopanner-collector@" + slot + ".rotation.json");
  }

  /**
   * The shadow connection's captured {@code (symbol, stream)} set, derived from config (§15.f): the
   * subscriptions plus one {@code forceOrder} writer per symbol when the broadcast is configured.
   * The rotation opener rebuilds this from a fresh config load each time, so an operator symbol-set
   * edit applies on the next rotation.
   */
  static List<LiveShadowSession.WriterSpec> shadowSpecsFrom(
      List<NodeConfig.Subscription> subscriptions, List<String> broadcasts, List<String> symbols) {
    List<LiveShadowSession.WriterSpec> specs = new ArrayList<>();
    for (NodeConfig.Subscription s : subscriptions) {
      specs.add(new LiveShadowSession.WriterSpec(s.symbol(), s.stream()));
    }
    if (broadcasts.contains(FrameRouter.FORCE_ORDER_BROADCAST)) {
      for (String symbol : symbols) {
        specs.add(new LiveShadowSession.WriterSpec(symbol, FrameRouter.FORCE_ORDER_STREAM));
      }
    }
    return specs;
  }

  /** The shadow's routed sockets, derived from config (§15.f) — rebuilt on each rotation open. */
  static List<LiveShadowSession.SocketSpec> shadowSocketsFrom(NodeConfig cfg) {
    Map<StreamRouting.Socket, List<String>> bySocket = new EnumMap<>(StreamRouting.Socket.class);
    for (NodeConfig.Subscription s : cfg.subscriptions()) {
      bySocket
          .computeIfAbsent(StreamRouting.forStreamType(s.stream()), k -> new ArrayList<>())
          .add(s.symbol() + "@" + s.stream());
    }
    for (String b : cfg.broadcasts()) {
      bySocket.computeIfAbsent(StreamRouting.forBroadcast(b), k -> new ArrayList<>()).add(b);
    }
    List<LiveShadowSession.SocketSpec> sockets = new ArrayList<>();
    List<String> pub = bySocket.get(StreamRouting.Socket.PUBLIC);
    if (pub != null) {
      sockets.add(new LiveShadowSession.SocketSpec(URI.create(cfg.wsPublicEndpointUrl()), pub));
    }
    List<String> mkt = bySocket.get(StreamRouting.Socket.MARKET);
    if (mkt != null) {
      sockets.add(new LiveShadowSession.SocketSpec(URI.create(cfg.wsMarketEndpointUrl()), mkt));
    }
    return sockets;
  }

  /**
   * OpenMetrics text for {@code /metrics} (§11.c), including the spec-named rotation + age series.
   */
  static String metricsText(
      long framesWritten,
      long unparseable,
      long late,
      long depthResyncs,
      long binaryUnexpected,
      long rotationEvents,
      long connectionAgeSeconds) {
    return "# TYPE cryptopanner_frames_written_total counter\n"
        + "cryptopanner_frames_written_total "
        + framesWritten
        + "\n# TYPE cryptopanner_unparseable_frames_total counter\n"
        + "cryptopanner_unparseable_frames_total "
        + unparseable
        + "\n# TYPE cryptopanner_late_frames_total counter\n"
        + "cryptopanner_late_frames_total "
        + late
        + "\n# TYPE cryptopanner_depth_resyncs_total counter\n"
        + "cryptopanner_depth_resyncs_total "
        + depthResyncs
        + "\n# TYPE cryptopanner_ws_binary_frame_unexpected_total counter\n"
        + "cryptopanner_ws_binary_frame_unexpected_total "
        + binaryUnexpected
        + "\n# TYPE cryptopanner_rotation_events_total counter\n"
        + "cryptopanner_rotation_events_total "
        + rotationEvents
        + "\n# TYPE cryptopanner_current_connection_age_seconds gauge\n"
        + "cryptopanner_current_connection_age_seconds "
        + connectionAgeSeconds
        + "\n";
  }

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    NodeConfig cfg = NodeConfig.load(configPath);
    ObjectMapper mapper = EnvelopeCodec.newMapper();

    // Startup crash-recovery over segments/ (design doc §3.2/§5.6): drop incomplete .tmp writes,
    // merge/promote any leftover .shadow overlap segments, and repair sidecars before we resume
    // writing. A completed shadow overlap records a RECOVERED_AT_STARTUP rotation event (§10.d).
    Path recoveryDeployDir =
        cfg.paths().deploy() != null
            ? cfg.paths().deploy()
            : cfg.paths().segments().resolveSibling("deploy");
    SegmentRecovery.Result recovery =
        SegmentRecovery.recover(
            cfg.paths().segments(),
            mapper,
            recoveryDeployDir.resolve("rotations.jsonl"),
            "recovery-" + java.util.UUID.randomUUID(),
            Instant.now());
    if (recovery.tmpDeleted() > 0
        || recovery.sidecarsWritten() > 0
        || recovery.shadowsMerged() > 0
        || recovery.shadowsPromoted() > 0) {
      System.out.println(
          "[collector] recovery: deleted "
              + recovery.tmpDeleted()
              + " .tmp, rewrote "
              + recovery.sidecarsWritten()
              + " sidecar(s), merged "
              + recovery.shadowsMerged()
              + " shadow(s), promoted "
              + recovery.shadowsPromoted()
              + " orphan(s)");
    }

    // Seal-grace window before a closed minute is finalized (master spec §8.e; config-overridable).
    Duration sealGrace = Duration.ofSeconds(cfg.sealGraceSeconds());

    // Structured JSON-Lines log for §11.e events (ws/minute/rotation lifecycle), per slot (§11.a).
    String slot = resolveSlot(System.getenv("CRYPTOPANNER_SLOT"));
    Path logsDir =
        cfg.paths().logs() != null
            ? cfg.paths().logs()
            : cfg.paths().segments().resolveSibling("logs");
    StructuredLog collectorLog =
        new StructuredLog(
            logsDir.resolve("cryptopanner-collector@" + slot + ".jsonl"),
            "cryptopanner-collector",
            slot);

    // Per-(symbol,stream) writers. Keyed by "<symbol>@<stream>" for direct lookup from
    // wrapper.stream
    // and from broadcast-fanout writers (e.g. "btcusdt@forceOrder").
    Map<String, MinuteSegmentWriter> writers = new HashMap<>();
    for (NodeConfig.Subscription s : cfg.subscriptions()) {
      String key = s.symbol() + "@" + s.stream();
      writers.put(
          key,
          new MinuteSegmentWriter(cfg.paths().segments(), s.symbol(), s.stream(), sealGrace)
              .withLog(collectorLog));
    }
    // Broadcast fan-out writers: one (symbol, forceOrder) writer per configured symbol.
    if (cfg.broadcasts().contains(FrameRouter.FORCE_ORDER_BROADCAST)) {
      for (String symbol : cfg.symbols()) {
        String key = symbol + "@" + FrameRouter.FORCE_ORDER_STREAM;
        writers.computeIfAbsent(
            key,
            k ->
                new MinuteSegmentWriter(
                        cfg.paths().segments(), symbol, FrameRouter.FORCE_ORDER_STREAM, sealGrace)
                    .withLog(collectorLog));
      }
    }
    // REST-poll fan-out writers: per-symbol polls → one per configured symbol; non-per-symbol
    // polls (e.g. exchangeInfo) → a single writer under the reserved global symbol.
    for (NodeConfig.RestPoll p : cfg.restPolls()) {
      for (String sym : restSymbolScope(cfg, p)) {
        writers.computeIfAbsent(
            sym + "@" + p.stream(),
            k ->
                new MinuteSegmentWriter(cfg.paths().segments(), sym, p.stream(), sealGrace)
                    .withLog(collectorLog));
      }
    }

    // Partition subscriptions + broadcasts by routed socket (§8.a).
    Map<StreamRouting.Socket, List<String>> bySocket = new EnumMap<>(StreamRouting.Socket.class);
    for (NodeConfig.Subscription s : cfg.subscriptions()) {
      StreamRouting.Socket sock = StreamRouting.forStreamType(s.stream());
      bySocket.computeIfAbsent(sock, k -> new ArrayList<>()).add(s.symbol() + "@" + s.stream());
    }
    for (String b : cfg.broadcasts()) {
      StreamRouting.Socket sock = StreamRouting.forBroadcast(b);
      bySocket.computeIfAbsent(sock, k -> new ArrayList<>()).add(b);
    }

    // Depth pu-chain resync (§7.b.1): on a chain break, re-fire that symbol's depthSnapshot poller
    // to fetch a fresh /fapi/v1/depth snapshot. The trigger runs off the WS thread on a virtual
    // thread. depthSnapshotPollers is captured by reference and populated in the poller loop below
    // (before any break can fire at runtime).
    Map<String, RestPoller> depthSnapshotPollers = new HashMap<>();
    boolean depthResyncEnabled =
        cfg.restPolls().stream()
            .anyMatch(p -> p.perSymbol() && DEPTH_SNAPSHOT_STREAM.equals(p.stream()));
    ExecutorService resyncExec =
        depthResyncEnabled ? Executors.newVirtualThreadPerTaskExecutor() : null;
    DepthResync depthResync =
        depthResyncEnabled
            ? new DepthResync(
                sym -> {
                  RestPoller rp = depthSnapshotPollers.get(sym);
                  if (rp != null) {
                    rp.pollOnce();
                  }
                },
                resyncExec)
            : null;

    // Shared frame router — parses, routes, wraps in a ws_frame envelope, and buckets by server
    // event time. MinuteSegmentWriter.accept() is synchronized, safe for concurrent sockets.
    FrameRouter router = new FrameRouter(mapper, writers, depthResync);
    BiConsumer<String, Instant> onFrame = router::handle;

    // Open one BinanceWsClient per non-empty socket group.
    List<BinanceWsClient> clients = new ArrayList<>();

    List<String> publicSubs = bySocket.get(StreamRouting.Socket.PUBLIC);
    if (publicSubs != null) {
      BinanceWsClient c =
          new BinanceWsClient(URI.create(cfg.wsPublicEndpointUrl()), publicSubs, onFrame)
              .withLog(collectorLog);
      c.start();
      clients.add(c);
      System.out.println("[collector] /public connected with " + publicSubs);
    }

    List<String> marketSubs = bySocket.get(StreamRouting.Socket.MARKET);
    if (marketSubs != null) {
      BinanceWsClient c =
          new BinanceWsClient(URI.create(cfg.wsMarketEndpointUrl()), marketSubs, onFrame)
              .withLog(collectorLog);
      c.start();
      clients.add(c);
      System.out.println("[collector] /market connected with " + marketSubs);
    }

    // REST pollers — one per (rest_poll, configured symbol). Cadences pinned per spec §7.b in
    // production; the skeleton config can override for fast smoke tests.
    PollerScheduler scheduler = new PollerScheduler();
    if (!cfg.restPolls().isEmpty()) {
      if (cfg.restBaseUrl() == null) {
        throw new IllegalStateException("rest_polls configured but rest_base_url is missing");
      }
      HttpClient httpClient = RestPoller.newHttpClient();
      URI baseUrl = URI.create(cfg.restBaseUrl());
      for (NodeConfig.RestPoll p : cfg.restPolls()) {
        for (String sym : restSymbolScope(cfg, p)) {
          String writerKey = sym + "@" + p.stream();
          MinuteSegmentWriter writer = writers.get(writerKey);
          // Merge the poll's static params with the per-symbol query param (omitted for globals).
          Map<String, String> params = new LinkedHashMap<>(p.params());
          if (p.perSymbol()) {
            params.put("symbol", sym);
          }
          Consumer<byte[]> sink =
              b -> {
                try {
                  writer.accept(b, Instant.now());
                } catch (Exception e) {
                  System.err.println(
                      "[collector] rest sink write failed for "
                          + writerKey
                          + ": "
                          + e.getMessage());
                }
              };
          RestPoller poller =
              new RestPoller(httpClient, mapper, baseUrl, p.endpoint(), params, sink);
          scheduler.add(poller, p.cadenceSeconds(), writerKey);
          if (p.perSymbol() && DEPTH_SNAPSHOT_STREAM.equals(p.stream())) {
            depthSnapshotPollers.put(sym, poller); // on-demand resync re-fires this poller
          }
        }
      }
      scheduler.start();
    }

    // 1s ticker: (a) seals minutes past close+grace (§8.e); (b) half-open watchdog — a /public or
    // /market socket that goes silent past the idle window is forced to reconnect (§8.a). A healthy
    // socket emits sub-second traffic (depth@100ms, ticker, markPrice@1s), so a long idle gap means
    // the connection is silently dead.
    long halfOpenIdleNanos = TimeUnit.SECONDS.toNanos(30);

    // Daily WS-rotation decision inputs (§5.1). The cutover execution (open shadow, verify, merge)
    // is driven by WsConnectionManager/RotationExecutor under the soak; here we evaluate the
    // decision once per minute against the oldest live connection's age so it is observable and the
    // EMERGENCY/SCHEDULED path fires ahead of Binance's 24h cliff.
    final String rotNodeId = cfg.nodeId();
    final Duration connMaxAge =
        (cfg.collector() != null && cfg.collector().connectionMaxAge() != null)
            ? cfg.collector().connectionMaxAgeDuration()
            : Duration.ofHours(23);
    final com.cryptopanner.common.config.ConfigParse.HourWindow rotWindow =
        (cfg.collector() != null && cfg.collector().rotationWindow() != null)
            ? cfg.collector().rotationWindowParsed()
            : com.cryptopanner.common.config.ConfigParse.hourWindow("HH:10-HH:50");

    // ── WS-rotation conductor (§5.2). The shadow opener mirrors the primary's routed sockets and
    // captured (symbol,stream) set; the primary-drop signals and primary-close are derived from the
    // live primary clients. rotate() blocks for minutes, so it runs on its own single-thread
    // executor and the manager's in-progress guard rejects overlapping attempts. The live two-
    // connection overlap itself is soak-validated (§14.e).
    List<LiveShadowSession.SocketSpec> shadowSockets = new ArrayList<>();
    if (publicSubs != null) {
      shadowSockets.add(
          new LiveShadowSession.SocketSpec(URI.create(cfg.wsPublicEndpointUrl()), publicSubs));
    }
    if (marketSubs != null) {
      shadowSockets.add(
          new LiveShadowSession.SocketSpec(URI.create(cfg.wsMarketEndpointUrl()), marketSubs));
    }
    List<LiveShadowSession.WriterSpec> shadowSpecs = new ArrayList<>();
    for (NodeConfig.Subscription s : cfg.subscriptions()) {
      shadowSpecs.add(new LiveShadowSession.WriterSpec(s.symbol(), s.stream()));
    }
    if (cfg.broadcasts().contains(FrameRouter.FORCE_ORDER_BROADCAST)) {
      for (String symbol : cfg.symbols()) {
        shadowSpecs.add(new LiveShadowSession.WriterSpec(symbol, FrameRouter.FORCE_ORDER_STREAM));
      }
    }
    // The currently-running primary. Starts as the Main-built clients; after each rotation's
    // promote it becomes the just-promoted LiveShadowSession (which self-seals via its own ticker).
    // The watchdog, connection-age, drop signals and next rotation's primary-close all read these
    // refs so sequential rotations never act on the retired old-primary state (§5.2 step 5).
    java.util.concurrent.atomic.AtomicReference<List<BinanceWsClient>> activeClients =
        new java.util.concurrent.atomic.AtomicReference<>(clients);
    java.util.concurrent.atomic.AtomicReference<ShadowSession> activePrimarySession =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.function.BooleanSupplier primaryDropped =
        () -> activeClients.get().stream().anyMatch(c -> c.currentConnectionAge().isEmpty());
    java.util.function.BooleanSupplier bothDropped =
        () -> {
          List<BinanceWsClient> a = activeClients.get();
          return !a.isEmpty() && a.stream().allMatch(c -> c.currentConnectionAge().isEmpty());
        };
    // Retire whatever is primary now: a previously-promoted shadow session is fully closed; the
    // original Main-built primary just has its clients stopped (its writers seal via the ticker).
    Runnable retireActivePrimary =
        () -> {
          ShadowSession prev = activePrimarySession.get();
          if (prev != null) {
            prev.close();
          } else {
            clients.forEach(BinanceWsClient::stop);
          }
        };
    LiveShadowSession.PromotionSink onPromoted =
        (session, newClients) -> {
          activePrimarySession.set(session);
          activeClients.set(newClients);
        };
    java.util.function.Supplier<ShadowSession> shadowOpener =
        () -> {
          try {
            // §15.f: re-read config at open time so an operator symbol-set edit is picked up by the
            // shadow's SUBSCRIBE; fall back to the startup set if the reload fails.
            List<LiveShadowSession.SocketSpec> sockets = shadowSockets;
            List<LiveShadowSession.WriterSpec> specs = shadowSpecs;
            try {
              NodeConfig fresh = NodeConfig.load(configPath);
              sockets = shadowSocketsFrom(fresh);
              specs = shadowSpecsFrom(fresh.subscriptions(), fresh.broadcasts(), fresh.symbols());
            } catch (Exception reload) {
              System.err.println(
                  "[collector] shadow config reload failed, using startup set: "
                      + reload.getMessage());
            }
            return new LiveShadowSession(
                    sockets,
                    specs,
                    cfg.paths().segments(),
                    sealGrace,
                    mapper,
                    SHADOW_USER_AGENT,
                    Duration.ofSeconds(30),
                    primaryDropped,
                    bothDropped,
                    retireActivePrimary,
                    onPromoted)
                .withLog(collectorLog);
          } catch (Exception e) {
            System.err.println("[collector] shadow open failed: " + e.getMessage());
            return null;
          }
        };
    Path fsHeavyLock =
        cfg.paths().fsHeavyLock() != null
            ? cfg.paths().fsHeavyLock()
            : cfg.paths().segments().resolveSibling(".fs-heavy.lock");
    Path deployDir =
        cfg.paths().deploy() != null
            ? cfg.paths().deploy()
            : cfg.paths().segments().resolveSibling("deploy");
    Path rotationsLog = deployDir.resolve("rotations.jsonl");
    Path rotationTriggerFile = deployDir.resolve("rotation-trigger");
    java.util.function.Supplier<java.util.Optional<Duration>> connectionAge =
        () ->
            activeClients.get().stream()
                .map(BinanceWsClient::currentConnectionAge)
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .max(java.util.Comparator.naturalOrder());
    WsConnectionManager rotationManager =
        new WsConnectionManager(
                shadowOpener,
                mapper,
                fsHeavyLock,
                rotationsLog,
                connectionAge,
                () -> "rot-" + java.util.UUID.randomUUID(),
                Instant::now,
                WsConnectionManager.Config.defaults("collector:" + cfg.nodeId()))
            .withLog(collectorLog);
    java.util.concurrent.atomic.AtomicLong rotationEventsTotal =
        new java.util.concurrent.atomic.AtomicLong();
    ExecutorService rotationExec =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "collector-rotation");
              t.setDaemon(true);
              return t;
            });

    // Per-slot heartbeat + rotation-status the Node Agent reads (§11.b / §11.c).
    Path heartbeatFile = heartbeatFile(slot);
    Path rotationStatusFile = rotationStatusFile(slot);

    ScheduledExecutorService ticker =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "collector-ticker");
              t.setDaemon(true);
              return t;
            });
    ticker.scheduleAtFixedRate(
        () -> {
          Instant now = Instant.now();
          long monoNanos = System.nanoTime();
          try {
            com.cryptopanner.common.Heartbeat.touch(heartbeatFile); // §11.b liveness, every tick
          } catch (IOException e) {
            System.err.println("[collector] heartbeat touch failed: " + e.getMessage());
          }
          try {
            // Publish rotation state + connection age for the Node Agent's /status (§11.c).
            long ageS = connectionAge.get().map(Duration::toSeconds).orElse(0L);
            String ph = rotationManager.currentPhase();
            boolean idle = "IDLE".equals(ph);
            com.cryptopanner.common.RotationStatus.write(
                rotationStatusFile,
                mapper,
                new com.cryptopanner.common.RotationStatus(
                    ph,
                    ageS,
                    idle ? null : rotationManager.activeRotationId(),
                    idle ? null : ageS));
          } catch (IOException e) {
            System.err.println("[collector] rotation-status write failed: " + e.getMessage());
          }
          for (MinuteSegmentWriter w : writers.values()) {
            try {
              w.sealElapsed(now, monoNanos);
            } catch (IOException e) {
              System.err.println("[collector] seal failed: " + e.getMessage());
            }
          }
          for (BinanceWsClient c : activeClients.get()) {
            if (c.idleNanos() > halfOpenIdleNanos) {
              long idleS = c.idleNanos() / 1_000_000_000L;
              collectorLog.warn(
                  "ws_ping_missed", Map.of("idle_seconds", idleS)); // §11.e half-open watchdog
              System.err.println(
                  "[collector] half-open suspected (idle " + idleS + "s); forcing reconnect");
              c.forceReconnect();
            }
          }
          // Once per minute: (a) honor any operator-forced rotation trigger from the Node Agent
          // (§5.4); (b) evaluate the scheduled/emergency rotation decision against the oldest live
          // connection (§5.1). Both dispatch the (blocking) cutover onto the rotation executor; the
          // manager's in-progress guard makes a second dispatch a no-op.
          if (now.atOffset(java.time.ZoneOffset.UTC).getSecond() == 0) {
            try {
              RotationTrigger.consume(rotationTriggerFile)
                  .ifPresent(
                      reason ->
                          rotationExec.submit(
                              () -> safeRotate(rotationManager, reason, rotationEventsTotal)));
            } catch (IOException e) {
              System.err.println("[collector] rotation-trigger read failed: " + e.getMessage());
            }
            java.util.Optional<Duration> oldest = connectionAge.get();
            if (oldest.isPresent()) {
              int minuteOfHour = now.atOffset(java.time.ZoneOffset.UTC).getMinute();
              RotationScheduler.Decision d =
                  RotationScheduler.decide(
                      rotNodeId, oldest.get(), minuteOfHour, connMaxAge, rotWindow);
              if (d != RotationScheduler.Decision.NONE) {
                System.out.println(
                    "[collector] WS rotation "
                        + d
                        + " due (connection age "
                        + oldest.get().toHours()
                        + "h); dispatching cutover");
                rotationExec.submit(
                    () -> safeRotate(rotationManager, d.name(), rotationEventsTotal));
              }
            }
          }
        },
        1,
        1,
        TimeUnit.SECONDS);

    // Liveness + metrics endpoint (§13), if a port is configured.
    HealthServer health = null;
    if (cfg.healthPort() > 0) {
      health =
          new HealthServer(
              cfg.healthPort(),
              () -> {
                long late = 0;
                for (MinuteSegmentWriter w : writers.values()) {
                  late += w.lateFrames();
                }
                long resyncs = depthResync != null ? depthResync.resyncs() : 0;
                long binaryUnexpected = 0;
                for (BinanceWsClient c : activeClients.get()) {
                  binaryUnexpected += c.binaryFramesUnexpected();
                }
                long ageSeconds = connectionAge.get().map(Duration::toSeconds).orElse(0L);
                return metricsText(
                    router.framesWritten(),
                    router.unparseableFrames(),
                    late,
                    resyncs,
                    binaryUnexpected,
                    rotationEventsTotal.get(),
                    ageSeconds);
              });
      System.out.println("[collector] health endpoint on :" + cfg.healthPort());
    }
    final HealthServer healthRef = health;

    System.out.println(
        "[collector] started; running for "
            + cfg.collectorMaxRuntimeS()
            + "s; sockets="
            + clients.size());

    // Single idempotent close path used by both the timed-exit and the SIGTERM hook so buffered
    // minute segments are never lost on signal — durability invariant (master spec §3.b).
    AtomicBoolean closed = new AtomicBoolean(false);
    Runnable closeAll =
        () -> {
          if (!closed.compareAndSet(false, true)) return;
          System.out.println(
              "[collector] stopping after "
                  + router.framesWritten()
                  + " frames ("
                  + router.unparseableFrames()
                  + " unparseable)");
          scheduler.close();
          ticker.shutdownNow();
          rotationExec.shutdownNow();
          if (resyncExec != null) {
            resyncExec.shutdownNow();
          }
          if (healthRef != null) {
            healthRef.close();
          }
          // Tear down whichever shadow session is the current primary (if a rotation promoted one),
          // plus the original Main-built primary clients + writers.
          ShadowSession activeSession = activePrimarySession.get();
          if (activeSession != null) {
            activeSession.close();
          }
          for (BinanceWsClient c : clients) {
            c.stop();
          }
          for (MinuteSegmentWriter w : writers.values()) {
            try {
              w.close();
            } catch (IOException e) {
              System.err.println("[collector] writer close failed: " + e.getMessage());
            }
          }
          System.out.println("[collector] done");
        };

    Runtime.getRuntime().addShutdownHook(new Thread(closeAll, "collector-shutdown"));

    if (runsUntilSignal(cfg.collectorMaxRuntimeS())) {
      System.out.println("[collector] running until SIGTERM");
      new java.util.concurrent.CountDownLatch(1).await(); // released by JVM exit; hook seals
    } else {
      Thread.sleep(cfg.collectorMaxRuntimeS() * 1000L);
      closeAll.run();
    }
  }

  /**
   * Runs one rotation attempt off the ticker thread, counts completed rotations, logs the outcome.
   */
  private static void safeRotate(
      WsConnectionManager manager, String reason, java.util.concurrent.atomic.AtomicLong events) {
    try {
      WsConnectionManager.RotateOutcome out = manager.rotate(reason);
      if (out.status() == WsConnectionManager.Status.COMPLETED) {
        events.incrementAndGet(); // §11.c cryptopanner_rotation_events_total
      }
      System.out.println(
          "[collector] rotation "
              + reason
              + " → "
              + out.status()
              + (out.verifyResult() != null ? " (" + out.verifyResult() + ")" : ""));
    } catch (Exception e) {
      System.err.println("[collector] rotation " + reason + " failed: " + e.getMessage());
    }
  }

  /**
   * Per-symbol polls fan out across configured symbols; non-per-symbol polls run once as global.
   */
  private static List<String> restSymbolScope(NodeConfig cfg, NodeConfig.RestPoll p) {
    return p.perSymbol() ? new ArrayList<>(cfg.symbols()) : List.of(NodeConfig.GLOBAL_SYMBOL);
  }
}
