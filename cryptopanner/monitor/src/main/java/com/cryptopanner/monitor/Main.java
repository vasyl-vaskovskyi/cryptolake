package com.cryptopanner.monitor;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.config.ConfigParse;
import com.cryptopanner.common.config.MonitorConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Composition root for the cross-node Monitor (master spec §11–§13). Wires the scrape → evaluate →
 * restart → dispatch pipeline ({@link MonitorCore}), the §11.d dashboard server, and the §13.c
 * dead-man's switch, then schedules a cycle every {@code scrape_interval_s}.
 *
 * <p>Wiring order: config → shared mapper + HTTP client → channels → evaluator/dispatcher/
 * orchestrator → core → server → scheduler. {@link #start} returns an {@link App} handle so the
 * whole stack is constructed and torn down in tests without touching {@code main}'s SIGTERM wait.
 */
public final class Main {

  /** Default WS connection-age cliff (§13.a): {@code connection_max_age (23h) + 30m}. */
  private static final Duration WS_CLIFF = Duration.ofHours(23).plusMinutes(30);

  private Main() {}

  /** Running Monitor: the dashboard server + the scrape scheduler. Close to stop both. */
  public record App(MonitorServer server, ScheduledExecutorService scheduler)
      implements AutoCloseable {
    @Override
    public void close() {
      scheduler.shutdownNow();
      server.close();
    }
  }

  public static App start(MonitorConfig cfg) throws IOException {
    ObjectMapper mapper = EnvelopeCodec.newMapper();
    HttpClient http = NodeScraper.newHttpClient();

    List<AlertChannel> channels =
        List.of(
            new WebhookAlertChannel("telegram", telegramUrl(cfg), http, mapper),
            new WebhookAlertChannel("whatsapp", whatsappUrl(cfg), http, mapper));

    NodeScraper scraper = new NodeScraper(http, mapper);
    AlertEvaluator evaluator = new AlertEvaluator(cfg.alert(), WS_CLIFF);
    AlertDispatcher dispatcher =
        new AlertDispatcher(
            cfg.alert().dedupTtlDuration(),
            cfg.alert().correlation().thresholdNodes(),
            cfg.alert().correlation().windowDuration());
    RestartOrchestrator orchestrator =
        new RestartOrchestrator(
            cfg.restart().backoff(),
            cfg.restart().circuitBreaker().failureCount(),
            cfg.restart().circuitBreaker().windowDuration(),
            WS_CLIFF,
            new AgentRestartClient(http, Instant::now));
    DeadMansSwitch deadMan =
        new DeadMansSwitch(
            http,
            cfg.alerting() == null ? "" : cfg.alerting().healthchecksUrl(),
            ConfigParse.duration(cfg.deadMan().healthchecksPushInterval()),
            LocalTime.parse(cfg.deadMan().selfTestTimeUtc()),
            channels);

    AtomicReference<List<NodeView>> view = new AtomicReference<>(List.of());
    MonitorCore core =
        new MonitorCore(
            cfg.nodes(),
            scraper,
            new NodeStateTracker(),
            evaluator,
            orchestrator,
            dispatcher,
            channels,
            deadMan,
            view);

    MonitorServer server =
        new MonitorServer(
            dashboardPort(cfg), mapper, view::get, cfg.dashboard().refreshIntervalS());

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(
        () -> {
          try {
            core.runCycle(Instant.now());
          } catch (RuntimeException e) {
            // A cycle must never kill the scheduler; the next tick retries.
          }
        },
        0,
        Math.max(1, cfg.scrapeIntervalS()),
        TimeUnit.SECONDS);

    return new App(server, scheduler);
  }

  public static void main(String[] args) throws Exception {
    Path configPath =
        Path.of(System.getProperty("config", "/etc/cryptopanner-monitor/monitor.yaml"));
    App app = start(MonitorConfig.load(configPath));
    Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    Thread.currentThread().join();
  }

  private static String telegramUrl(MonitorConfig cfg) {
    return cfg.alerting() == null || cfg.alerting().telegram() == null
        ? ""
        : cfg.alerting().telegram().webhookUrl();
  }

  private static String whatsappUrl(MonitorConfig cfg) {
    return cfg.alerting() == null || cfg.alerting().whatsapp() == null
        ? ""
        : cfg.alerting().whatsapp().webhookUrl();
  }

  private static int dashboardPort(MonitorConfig cfg) {
    String addr = cfg.dashboard().listenAddress();
    int colon = addr.lastIndexOf(':');
    return colon < 0 ? 9200 : Integer.parseInt(addr.substring(colon + 1).strip());
  }
}
