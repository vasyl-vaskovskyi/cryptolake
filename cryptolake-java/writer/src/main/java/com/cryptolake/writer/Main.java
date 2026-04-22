package com.cryptolake.writer;

import com.cryptolake.common.config.AppConfig;
import com.cryptolake.common.config.CryptoLakeConfigException;
import com.cryptolake.common.config.WriterConfig;
import com.cryptolake.common.config.YamlConfigLoader;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.health.HealthServer;
import com.cryptolake.common.health.MetricsSource;
import com.cryptolake.common.identity.SystemIdentity;
import com.cryptolake.common.logging.LogInit;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.consumer.DepthRecoveryGapFilter;
import com.cryptolake.writer.consumer.HourRotationScheduler;
import com.cryptolake.writer.consumer.KafkaConsumerLoop;
import com.cryptolake.writer.consumer.LateArrivalSequencer;
import com.cryptolake.writer.consumer.OffsetCommitCoordinator;
import com.cryptolake.writer.consumer.RecordHandler;
import com.cryptolake.writer.consumer.RecoveryCoordinator;
import com.cryptolake.writer.consumer.RecoveryResult;
import com.cryptolake.writer.consumer.SessionChangeDetector;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.failover.HostLifecycleEvidence;
import com.cryptolake.writer.failover.HostLifecycleReader;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.health.WriterReadyCheck;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.recovery.LastEnvelopeReader;
import com.cryptolake.writer.recovery.SealedFileIndex;
import com.cryptolake.writer.rotate.FileRotator;
import com.cryptolake.writer.state.ComponentRuntimeState;
import com.cryptolake.writer.state.CryptoLakeStateException;
import com.cryptolake.writer.state.StateManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer service entry point. Wires all components and starts the consume loop.
 *
 * <p>Wiring order (design §2.1): {@code LogInit.apply()} → {@code YamlConfigLoader.load()} → {@code
 * EnvelopeCodec.newMapper()} → {@code StateManager.connect()} → {@code HostLifecycleReader.load()}
 * → {@code WriterMetrics} → {@code ZstdFrameCompressor} → {@code BufferManager} → {@code
 * CoverageFilter} → {@code FailoverController} → {@code FileRotator} → {@code
 * OffsetCommitCoordinator} → {@code RecoveryCoordinator} → {@code SessionChangeDetector} → {@code
 * DepthRecoveryGapFilter} → {@code GapEmitter} → {@code RecordHandler} → {@code KafkaConsumerLoop}
 * → {@code HealthServer} → SIGTERM hook → start.
 *
 * <p>Thread model: one virtual-thread executor for T1 (consume loop); health server runs on its own
 * JDK httpserver executor; SIGTERM hook on a platform thread (JVM requirement). No other threads
 * (design §3.1).
 *
 * <p>Thread safety: Main wires in a single thread; no synchronization needed here.
 */
public final class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  /** Backup consumer prefix (design §2.7; Tier 5 M12). */
  private static final String BACKUP_PREFIX = "backup.";

  private Main() {}

  /**
   * Entry point.
   *
   * @param args optional config file path (defaults to {@code CRYPTOLAKE_CONFIG} env var)
   */
  public static void main(String[] args) {
    // Tier 2 §15: programmatic log-level override (logback.xml is declarative).
    // Common's LogInit exposes setLevel(String); honour LOG_LEVEL env when present.
    String logLevel = System.getenv("LOG_LEVEL");
    if (logLevel != null && !logLevel.isEmpty()) {
      LogInit.setLevel(logLevel);
    }

    // ── Config path ──────────────────────────────────────────────────────────────────────────
    String configPath =
        args.length > 0
            ? args[0]
            : System.getenv().getOrDefault("CRYPTOLAKE_CONFIG", "config.yaml");

    AppConfig config;
    try {
      config = YamlConfigLoader.load(Path.of(configPath));
    } catch (CryptoLakeConfigException e) {
      System.err.println("Configuration error: " + e.getMessage());
      System.exit(1);
      return;
    }

    WriterConfig writerConfig = config.writer();
    String baseDir = writerConfig.baseDir();

    // ── Identity ─────────────────────────────────────────────────────────────────────────────
    String currentBootId = SystemIdentity.getHostBootId();
    String instanceId = "writer-" + UUID.randomUUID().toString().substring(0, 8);
    String startedAt = Instant.now().toString();

    // ── Single ObjectMapper (Tier 2 §14; Tier 5 B6) ─────────────────────────────────────────
    var mapper = EnvelopeCodec.newMapper();
    var codec = new EnvelopeCodec(mapper);

    // ── Metrics ─────────────────────────────────────────────────────────────────────────────
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    MetricsSource metricsSource =
        () -> registry.scrape().getBytes(java.nio.charset.StandardCharsets.UTF_8);

    // ── PostgreSQL / HikariCP ────────────────────────────────────────────────────────────────
    HikariConfig hikariCfg = new HikariConfig();
    hikariCfg.setJdbcUrl(buildJdbcUrl(config));
    hikariCfg.setMaximumPoolSize(2); // Q4 preferred: min=1, max=2 (design §11)
    hikariCfg.setMinimumIdle(1);
    hikariCfg.setConnectionTimeout(30_000);
    HikariDataSource ds = new HikariDataSource(hikariCfg);

    StateManager stateManager = new StateManager(ds, Clocks.systemNanoClock());
    try {
      stateManager.connect();
    } catch (CryptoLakeStateException e) {
      System.err.println("StateManager.connect failed: " + e.getMessage());
      System.exit(2);
      return;
    }

    // Register component runtime state
    ComponentRuntimeState runtimeState =
        new ComponentRuntimeState(
            "writer", instanceId, currentBootId, startedAt, startedAt, null, false, null);
    stateManager.upsertComponentRuntime(runtimeState);

    // ── Host lifecycle evidence ──────────────────────────────────────────────────────────────
    String ledgerPathEnv = System.getenv("LIFECYCLE_LEDGER_PATH");
    Path ledgerPath =
        ledgerPathEnv != null ? Path.of(ledgerPathEnv) : HostLifecycleReader.DEFAULT_LEDGER_PATH;
    HostLifecycleEvidence hostEvidence =
        HostLifecycleReader.load(ledgerPath, null, startedAt, mapper);

    // ── IO components ────────────────────────────────────────────────────────────────────────
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(writerConfig.compressionLevel());
    DurableAppender appender = new DurableAppender();
    LateArrivalSequencer lateSeq = new LateArrivalSequencer();
    SealedFileIndex sealedIndex = new SealedFileIndex(Path.of(baseDir));

    // ── Buffer + codec ───────────────────────────────────────────────────────────────────────
    BufferManager buffers =
        new BufferManager(
            baseDir, writerConfig.flushMessages(), writerConfig.flushIntervalSeconds(), codec);

    // ── Coverage filter ──────────────────────────────────────────────────────────────────────
    CoverageFilter coverage =
        new CoverageFilter(
            writerConfig.gapFilter().gracePeriodSeconds(),
            writerConfig.gapFilter().snapshotMissGraceSeconds(),
            metrics,
            Clocks.systemNanoClock());

    // ── Build enabled topics (Tier 1 §3 — disabled streams excluded) ─────────────────────────
    List<String> enabledTopics = buildEnabledTopics(config);
    if (enabledTopics.isEmpty()) {
      System.err.println("No enabled topics — check configuration");
      System.exit(1);
      return;
    }

    // ── Kafka consumer ────────────────────────────────────────────────────────────────────────
    Properties consumerProps = buildConsumerProps(config);
    KafkaConsumer<byte[], byte[]> primaryConsumer =
        new KafkaConsumer<>(
            consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());

    // ── Failover controller ──────────────────────────────────────────────────────────────────
    FailoverController failover =
        new FailoverController(
            () ->
                new KafkaConsumer<>(
                    consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
            enabledTopics,
            BACKUP_PREFIX,
            Duration.ofSeconds(5),
            coverage,
            metrics,
            Clocks.systemNanoClock());

    // ── File rotation ─────────────────────────────────────────────────────────────────────────
    LastEnvelopeReader envelopeReader = new LastEnvelopeReader(codec);

    // GapEmitter depends on CoverageFilter (create before other consumers of it)
    // Temporary placeholder — will be wired after full construction
    // We use a forward reference pattern: create GapEmitter after all its deps are ready
    // CoverageFilter is already created above
    GapEmitter gapEmitter; // declared here, constructed below

    // ── OffsetCommitCoordinator ───────────────────────────────────────────────────────────────
    OffsetCommitCoordinator committer =
        new OffsetCommitCoordinator(
            primaryConsumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock());

    // ── GapEmitter (now we have coverage and buffers) ─────────────────────────────────────────
    gapEmitter = new GapEmitter(buffers, metrics, null, coverage);

    // ── FileRotator ───────────────────────────────────────────────────────────────────────────
    FileRotator rotator = new FileRotator(appender, compressor, lateSeq, buffers, metrics, baseDir);

    // ── HourRotationScheduler ────────────────────────────────────────────────────────────────
    HourRotationScheduler hourScheduler =
        new HourRotationScheduler(rotator, buffers, committer, metrics);

    // ── RecoveryCoordinator ───────────────────────────────────────────────────────────────────
    RecoveryCoordinator recovery =
        new RecoveryCoordinator(
            stateManager,
            sealedIndex,
            envelopeReader,
            hostEvidence,
            null, // RestartGapClassifier is a static utility
            gapEmitter,
            metrics,
            Clocks.systemNanoClock(),
            currentBootId,
            instanceId);

    RecoveryResult recoveryResult = recovery.runOnStartup();
    committer.seedDurableCheckpoints(recoveryResult.checkpoints());

    // ── SessionChangeDetector ─────────────────────────────────────────────────────────────────
    SessionChangeDetector sessionDetector =
        new SessionChangeDetector(gapEmitter, coverage, metrics, Clocks.systemNanoClock());

    // ── DepthRecoveryGapFilter ────────────────────────────────────────────────────────────────
    DepthRecoveryGapFilter depthFilter =
        new DepthRecoveryGapFilter(gapEmitter, Clocks.systemNanoClock());

    // ── RecordHandler ─────────────────────────────────────────────────────────────────────────
    RecordHandler recordHandler =
        new RecordHandler(
            codec,
            sessionDetector,
            depthFilter,
            coverage,
            failover,
            recovery,
            buffers,
            gapEmitter,
            metrics,
            BACKUP_PREFIX);

    // ── KafkaConsumerLoop ─────────────────────────────────────────────────────────────────────
    KafkaConsumerLoop consumerLoop =
        new KafkaConsumerLoop(
            primaryConsumer,
            enabledTopics,
            recordHandler,
            failover,
            committer,
            recovery,
            hourScheduler,
            buffers,
            coverage,
            gapEmitter,
            metrics);

    // ── Health server ─────────────────────────────────────────────────────────────────────────
    WriterReadyCheck readyCheck = new WriterReadyCheck(consumerLoop, Path.of(baseDir));
    // Common's MonitoringConfig exposes prometheusPort() only; reuse that for the combined
    // health+metrics HTTP server (design §2.1 — single HTTP endpoint for /ready and /metrics).
    int healthPort = config.monitoring() != null ? config.monitoring().prometheusPort() : 8080;
    HealthServer healthServer = new HealthServer(healthPort, readyCheck, metricsSource);
    healthServer.start();

    // ── Disk usage gauge — periodic update via nanoTime counter in loop ───────────────────────
    // NOTE: Disk usage is tracked in OffsetCommitCoordinator after flushAndCommit; for simplicity
    // the gauge is initialized here and updated on first flush.

    // ── Shutdown latch ────────────────────────────────────────────────────────────────────────
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    // ── SIGTERM hook (T3 — platform thread per JVM requirement) ─────────────────────────────
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("shutdown_hook_triggered");
                  consumerLoop.requestShutdown();
                  try {
                    shutdownLatch.await(
                        java.util.concurrent.TimeUnit.SECONDS.toMillis(35),
                        java.util.concurrent.TimeUnit.MILLISECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  try {
                    healthServer.stop();
                  } catch (Exception ignored) {
                    // best-effort shutdown; never block main shutdown path
                  }
                  try {
                    stateManager.markComponentCleanShutdown("writer", instanceId);
                    stateManager.close();
                  } catch (Exception ignored) {
                    // best-effort shutdown; never block main shutdown path
                  }
                }));

    // ── Start consume loop on virtual thread (Tier 5 A2; design §3.1 T1) ────────────────────
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var future =
          executor.submit(
              () -> {
                try {
                  consumerLoop.run();
                } finally {
                  shutdownLatch.countDown();
                }
              });

      log.info(
          "writer_started",
          "instance_id",
          instanceId,
          "boot_id",
          currentBootId,
          "base_dir",
          baseDir,
          "topics",
          enabledTopics.size());

      // Block until the consume loop terminates
      try {
        shutdownLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  // ── Private helpers ──────────────────────────────────────────────────────────────────────────

  private static List<String> buildEnabledTopics(AppConfig config) {
    List<String> topics = new ArrayList<>();
    if (config.exchanges() == null || config.exchanges().binance() == null) {
      return topics;
    }
    var binance = config.exchanges().binance();
    if (!binance.enabled()) return topics;

    // Tier 1 §3: never subscribe to streams with enabled=false. BinanceExchangeConfig's
    // getEnabledStreams() already filters by the StreamsConfig boolean flags.
    List<String> streams =
        binance.writerStreamsOverride() != null
            ? binance.writerStreamsOverride()
            : binance.getEnabledStreams();

    for (String sym : binance.symbols()) {
      for (String stream : streams) {
        // Topic format: exchange.stream (partitioned by symbol key — Tier 5 M13)
        topics.add("binance." + stream);
      }
    }

    // Deduplicate
    return topics.stream().distinct().toList();
  }

  private static Properties buildConsumerProps(AppConfig config) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", config.redpanda().brokers()));
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "cryptolake-writer");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // manual commit (Tier 1 §4)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return p;
  }

  /**
   * Returns the JDBC URL for the PostgreSQL state store.
   *
   * <p>Common's {@link com.cryptolake.common.config.DatabaseConfig} exposes only {@code url()}
   * (single-field parity with Python's {@code DatabaseConfig}). The URL itself carries user /
   * password via query parameters per the PostgreSQL JDBC driver convention ({@code
   * jdbc:postgresql://host/db?user=X&password=Y}), so HikariCP receives everything it needs without
   * the writer parsing components out of the URL.
   */
  private static String buildJdbcUrl(AppConfig config) {
    return config.database().url();
  }
}
