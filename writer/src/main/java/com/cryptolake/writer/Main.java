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
import com.cryptolake.writer.consumer.BackupTailConsumer;
import com.cryptolake.writer.consumer.DepthRecoveryGapFilter;
import com.cryptolake.writer.consumer.HourRotationScheduler;
import com.cryptolake.writer.consumer.KafkaConsumerLoop;
import com.cryptolake.writer.consumer.LateArrivalSequencer;
import com.cryptolake.writer.consumer.OffsetCommitCoordinator;
import com.cryptolake.writer.consumer.RecordHandler;
import com.cryptolake.writer.consumer.RecoveryCoordinator;
import com.cryptolake.writer.consumer.RecoveryResult;
import com.cryptolake.writer.consumer.SessionChangeDetector;
import com.cryptolake.writer.durability.DiskFullHoldController;
import com.cryptolake.writer.durability.PgOutageHoldController;
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
import com.cryptolake.writer.recovery.ZstdTailScrubber;
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
 * CoverageFilter} → {@code FailoverController} → {@code FileRotator} → {@code GapEmitter} → {@code
 * DiskFullHoldController} → {@code PgOutageHoldController} → {@code OffsetCommitCoordinator} →
 * {@code RecoveryCoordinator} → {@code SessionChangeDetector} → {@code DepthRecoveryGapFilter} →
 * {@code RecordHandler} → {@code KafkaConsumerLoop} → {@code HealthServer} → SIGTERM hook →
 * controllers.start() → consume loop start.
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
            baseDir,
            writerConfig.flushMessages(),
            writerConfig.flushIntervalSeconds(),
            codec,
            metrics);

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

    // ── Backup tail Kafka consumer (plan 2026-05-03 — continuous dual-source tailing) ─────────
    // Distinct group.id so it does not share the primary's offsets; auto.offset.reset=latest
    // (the tail tracks live liveness — there is no value in re-reading historical backup
    // records); enable.auto.commit=false (this consumer never commits offsets).
    //
    // Both consumers above are constructed BEFORE the SIGTERM hook is installed below; if any
    // subsequent stage of wiring throws (recovery, health server, etc.), the consumers leak —
    // their internal threads keep running. We track them in `startupConsumers` and close both
    // best-effort in a single catch on the wiring path (Issue #4).
    KafkaConsumer<byte[], byte[]> backupTailKafka;
    Properties backupTailProps =
        buildBackupTailConsumerProps(config, "writer-backup-tail-" + UUID.randomUUID());
    try {
      backupTailKafka =
          new KafkaConsumer<>(
              backupTailProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    } catch (RuntimeException e) {
      // Backup-tail constructor failed; primary consumer is already alive — close it before
      // propagating so its background threads do not leak.
      try {
        primaryConsumer.close(Duration.ofSeconds(5));
      } catch (Exception suppressed) {
        // best-effort cleanup on failed startup
      }
      throw e;
    }
    List<String> backupTailTopics = enabledTopics.stream().map(t -> BACKUP_PREFIX + t).toList();
    BackupTailConsumer backupTail = new BackupTailConsumer(backupTailKafka, backupTailTopics);

    // From here through executor.submit(consumerLoop), if any wiring stage throws we must close
    // BOTH consumers — primary AND backup-tail — before propagating; their internal threads
    // would otherwise leak. (Issue #4: the leak was pre-existing for the primary; this commit
    // adds the backup tail to the same lifecycle.)
    try {
      // ── Failover controller ──────────────────────────────────────────────────────────────────
      // After plan 2026-05-03 (Task 4): state-only. Backup topic is tailed continuously by
      // BackupTailConsumer (constructed above); FailoverController no longer owns a backup
      // consumer.
      FailoverController failover =
          new FailoverController(
              BACKUP_PREFIX,
              Duration.ofSeconds(5),
              Duration.ofSeconds(10), // recoveryStabilityWindow — bug B hysteresis
              metrics,
              Clocks.systemNanoClock());

      // ── File rotation ─────────────────────────────────────────────────────────────────────────
      LastEnvelopeReader envelopeReader = new LastEnvelopeReader(codec);

      // GapEmitter depends on CoverageFilter (create before other consumers of it)
      // Temporary placeholder — will be wired after full construction
      // We use a forward reference pattern: create GapEmitter after all its deps are ready
      // CoverageFilter is already created above
      GapEmitter gapEmitter; // declared here, constructed below

      // ── GapEmitter (now we have coverage and buffers) ─────────────────────────────────────────
      gapEmitter = new GapEmitter(buffers, metrics, null, coverage);

      // ── (symbol, stream) lists for hold controllers ───────────────────────────────────────────
      // Build the same list shape both controllers need. Sourced from the writer's enabled
      // symbols × streams (config). One pair per (symbol, stream) the writer is responsible for.
      var binance = config.exchanges().binance();
      java.util.List<String> enabledStreamsForHolds =
          binance.writerStreamsOverride() != null
              ? binance.writerStreamsOverride()
              : binance.getEnabledStreams();
      java.util.List<DiskFullHoldController.SymbolStream> diskSymbolStreams =
          new java.util.ArrayList<>();
      java.util.List<PgOutageHoldController.SymbolStream> pgSymbolStreams =
          new java.util.ArrayList<>();
      for (String sym : binance.symbols()) {
        for (String stream : enabledStreamsForHolds) {
          diskSymbolStreams.add(new DiskFullHoldController.SymbolStream(sym, stream));
          pgSymbolStreams.add(new PgOutageHoldController.SymbolStream(sym, stream));
        }
      }

      // ── DiskFullHoldController ────────────────────────────────────────────────────────────────
      // Probe: usable bytes on the baseDir filesystem > 50 MiB. Conservative: stay in hold on
      // probe failure so we don't exit prematurely on a flaky FS.
      final long minFreeBytesForRecovery = 50L * 1024 * 1024;
      java.util.function.BooleanSupplier diskProbe =
          () -> {
            try {
              return java.nio.file.Files.getFileStore(java.nio.file.Path.of(baseDir))
                      .getUsableSpace()
                  > minFreeBytesForRecovery;
            } catch (java.io.IOException probeErr) {
              return false;
            }
          };
      DiskFullHoldController diskHold =
          DiskFullHoldController.of(
              Clocks.systemNanoClock(), diskProbe, gapEmitter, "binance", diskSymbolStreams);

      // ── PgOutageHoldController ────────────────────────────────────────────────────────────────
      // Probe: cheap PG reachability check via Connection.isValid(5). Lighter-weight than the
      // earlier draft which used saveStatesAndCheckpoints (that path retries 3× with backoff
      // inside StateManager.retry, generating 3 WARN log lines per probe failure). The
      // ping method swallows SQLException internally and returns false on any unhealthy state.
      java.util.function.BooleanSupplier pgProbe = stateManager::ping;
      PgOutageHoldController pgHold =
          PgOutageHoldController.of(
              Clocks.systemNanoClock(), pgProbe, gapEmitter, "binance", pgSymbolStreams);

      // ── OffsetCommitCoordinator ───────────────────────────────────────────────────────────────
      OffsetCommitCoordinator committer =
          new OffsetCommitCoordinator(
              primaryConsumer,
              appender,
              compressor,
              stateManager,
              sealedIndex,
              metrics,
              Clocks.systemNanoClock(),
              diskHold,
              pgHold);

      // ── FileRotator ───────────────────────────────────────────────────────────────────────────
      FileRotator rotator =
          new FileRotator(appender, compressor, lateSeq, buffers, metrics, baseDir);

      // ── Startup sidecar repair (Bug B / Tier 1 sidecar-per-archive) ──────────────────────────
      // If the writer crashed before writing sidecars, any *.jsonl.zst without a .sha256 sibling
      // will be repaired here — BEFORE the consume loop reads any messages. This covers the
      // "writer crashed before sidecar was written" path that writeMissingSidecars at shutdown
      // cannot reach (design §3.4; Tier 1 sidecar invariant).
      rotator.writeMissingSidecars();
      log.info("startup_sidecar_repair_complete");

      // ── Startup zstd-tail scrub ──────────────────────────────────────────────────────────────
      // If a previous unexpected exit (SIGKILL, OOM, segfault, host crash) left a torn zstd
      // frame at the tail of an archive file, ZstdTailScrubber finds the last valid frame
      // boundary and truncates beyond it (recomputing the sidecar). This complements
      // DurableAppender.appendAndFsync's IOException-truncate path — which only runs when the
      // JVM lives long enough to handle the exception. Kernel-level kills bypass that path;
      // this scrubber heals the leftover damage on the next startup.
      int scrubHealed;
      try {
        scrubHealed = ZstdTailScrubber.scrub(java.nio.file.Path.of(baseDir));
      } catch (java.io.IOException scrubErr) {
        throw new java.io.UncheckedIOException("startup_tail_scrub_failed", scrubErr);
      }
      log.info("startup_tail_scrub_complete", "healed_files", scrubHealed);

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
              backupTail,
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
                    // Stop hold-controller retry loops AFTER the consume loop has drained,
                    // to avoid racing the probes against a closing PG / FS.
                    try {
                      diskHold.stop();
                    } catch (Exception ignored) {
                      // best-effort shutdown
                    }
                    try {
                      pgHold.stop();
                    } catch (Exception ignored) {
                      // best-effort shutdown
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

      // ── Start hold-controller retry loops ────────────────────────────────────────────────────
      // Each starts a virtual-thread retry loop that probes the underlying resource every 30s
      // and flips its hold state off via onRecovery / recordPgSuccess once recovered.
      diskHold.start();
      pgHold.start();

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
    } catch (RuntimeException | Error startupErr) {
      // Issue #4: any failure during wiring (recovery, health server bind, etc.) leaks both
      // consumer threads. Close both best-effort, suppressing exceptions, then rethrow so the
      // process exits with the original error.
      try {
        primaryConsumer.close(Duration.ofSeconds(5));
      } catch (Exception suppressed) {
        // best-effort
      }
      try {
        backupTailKafka.close(Duration.ofSeconds(5));
      } catch (Exception suppressed) {
        // best-effort
      }
      throw startupErr;
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
   * Builds Kafka consumer properties for the continuous backup-topic tail (plan 2026-05-03).
   *
   * <p>Delegates to {@link #buildConsumerProps(AppConfig)} for the shared keys (one source of truth
   * for {@code bootstrap.servers}, {@code max.poll.records}, etc.) and overrides only the keys that
   * differ:
   *
   * <ul>
   *   <li>Distinct, unique {@code group.id} per writer instance — the tail must not share the
   *       primary writer's consumer group state; we never commit offsets here.
   *   <li>{@code auto.offset.reset=latest} — the tail tracks live liveness only; there is no value
   *       in re-reading historical backup records on first connect.
   *   <li>{@code enable.auto.commit=false} — already set by {@code buildConsumerProps}, restated
   *       here for clarity since this consumer never commits offsets.
   * </ul>
   */
  private static Properties buildBackupTailConsumerProps(AppConfig config, String groupId) {
    Properties p = buildConsumerProps(config);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
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
