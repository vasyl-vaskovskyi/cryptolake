package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.verify.gaps.BackfillCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Keystone audit command: runs the full file + state gap diff and gates backfill on the result.
 *
 * <p>Executes all three file sources ({@link FileGapSource}, {@link MissingHourGapSource}, {@link
 * SequenceIdGapSource}) and all five state sources ({@link PgComponentRuntimeGapSource}, {@link
 * PgMaintenanceIntentGapSource}, {@link LedgerGapSource}, {@link KafkaOutageGapSource}, {@link
 * ManifestGapSource}), diffs them via {@link GapRecordDiff}, then:
 *
 * <ul>
 *   <li>Clean diff: prints a success message and (unless {@code --dry-run}) invokes the backfill
 *       orchestrator for each date × symbol × stream in scope.
 *   <li>Divergent diff: writes a divergence report to {@code archiveOutputDir}, fires a critical
 *       alert via {@link AlertmanagerNotifier}, prints to stderr, and returns exit code 2.
 * </ul>
 *
 * <p>Alert failures are logged but do NOT change the exit code.
 */
@Command(name = "backfill", description = "Diff file vs state gaps, then backfill if clean.")
public final class AuditBackfillCommand implements Callable<Integer> {

  private static final StructuredLogger LOG = StructuredLogger.of(AuditBackfillCommand.class);
  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;

  // ---- period flags ----

  @Option(names = "--day", description = "Day to audit (YYYY-MM-DD)")
  private String day;

  @Option(names = "--week", description = "ISO week to audit (YYYY-Www, e.g. 2026-W20)")
  private String week;

  @Option(names = "--hour", description = "Single hour to audit (YYYY-MM-DDTHH)")
  private String hour;

  @Option(names = "--since", description = "Start of custom range (ISO-8601 instant)")
  private String since;

  @Option(names = "--until", description = "End of custom range (ISO-8601 instant)")
  private String until;

  // ---- scope options ----

  @Option(
      names = "--exchange",
      defaultValue = "binance",
      description = "Exchange name (default: ${DEFAULT-VALUE})")
  private String exchange;

  @Option(
      names = "--symbol",
      split = ",",
      description = "Comma-separated symbols to include (default: all)")
  private List<String> symbols;

  @Option(
      names = "--stream",
      split = ",",
      description = "Comma-separated streams to include (default: all)")
  private List<String> streams;

  @Option(
      names = "--base-dir",
      defaultValue = "/data/archive",
      description = "Archive base directory (default: ${DEFAULT-VALUE})")
  private String baseDir;

  @Option(
      names = "--data-dir",
      defaultValue = "/data",
      description =
          "Host data directory for LedgerGapSource and KafkaOutageGapSource"
              + " (default: ${DEFAULT-VALUE})")
  private String dataDir;

  // ---- DB options ----

  @Option(
      names = "--db-url",
      description = "JDBC URL for Postgres (default: env PG_URL; empty = skip PG sources)")
  private String dbUrl;

  @Option(
      names = "--db-user",
      description = "Postgres user (default: env PG_USER, fallback 'cryptolake')")
  private String dbUser;

  @Option(
      names = "--db-password",
      description = "Postgres password (default: env PG_PASSWORD, fallback empty)")
  private String dbPassword;

  // ---- backfill-specific options ----

  @Option(
      names = "--archive-output-dir",
      description =
          "Directory for divergence-<ts>.json reports"
              + " (default: ${baseDir}/.cryptolake/audit/)")
  private String archiveOutputDir;

  @Option(
      names = "--alertmanager-url",
      description =
          "Alertmanager webhook URL (overrides env CRYPTOLAKE_ALERTMANAGER_WEBHOOK"
              + " and default 127.0.0.1:9093)")
  private String alertmanagerUrl;

  @Option(
      names = "--dry-run",
      description = "Diff and report, but skip backfill invocation even when diff is clean")
  private boolean dryRun;

  /** Hidden option for test determinism: overrides "now" used by {@link MissingHourGapSource}. */
  @Option(names = "--now-override-iso", hidden = true)
  private String nowOverrideIso;

  private final ObjectMapper mapper;

  public AuditBackfillCommand() {
    this(EnvelopeCodec.newMapper());
  }

  public AuditBackfillCommand(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Integer call() {
    // --- resolve period ---
    PeriodSelector period;
    try {
      if (day != null) {
        period = PeriodSelector.parse("--day", day, null, null);
      } else if (week != null) {
        period = PeriodSelector.parse("--week", week, null, null);
      } else if (hour != null) {
        period = PeriodSelector.parse("--hour", hour, null, null);
      } else if (since != null && until != null) {
        period = PeriodSelector.parse(null, null, since, until);
      } else {
        System.err.println("must specify one of --day / --week / --hour, or --since with --until");
        return 2;
      }
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid period: " + e.getMessage());
      return 2;
    }

    // --- build scope ---
    AuditScope scope =
        new AuditScope(
            period.startMs(),
            period.endMs(),
            List.of(exchange),
            nullOrEmpty(symbols) ? null : symbols,
            nullOrEmpty(streams) ? null : streams,
            baseDir);

    // --- build now supplier ---
    Supplier<Instant> nowSupplier =
        nowOverrideIso != null ? () -> Instant.parse(nowOverrideIso) : Instant::now;

    LOG.info(
        "audit_backfill_start",
        "startMs",
        scope.startMs(),
        "endMs",
        scope.endMs(),
        "exchange",
        exchange,
        "dryRun",
        dryRun);

    // --- run file sources ---
    List<GapRecord> fileRecords = new ArrayList<>();
    fileRecords.addAll(new FileGapSource(mapper).read(scope));
    fileRecords.addAll(new MissingHourGapSource(nowSupplier).read(scope));
    fileRecords.addAll(new SequenceIdGapSource(mapper).read(scope));

    // --- run state sources ---
    String resolvedUrl = resolveDbUrl();
    String resolvedUser = resolveDbUser();
    String resolvedPassword = resolveDbPassword();
    JdbcConfig jdbcConfig = new JdbcConfig(resolvedUrl, resolvedUser, resolvedPassword);

    List<GapRecord> stateRecords = new ArrayList<>();
    stateRecords.addAll(new PgComponentRuntimeGapSource(jdbcConfig).read(scope));
    stateRecords.addAll(new PgMaintenanceIntentGapSource(jdbcConfig).read(scope));
    stateRecords.addAll(new LedgerGapSource(Path.of(dataDir), mapper).read(scope));
    stateRecords.addAll(
        new KafkaOutageGapSource(Path.of(dataDir), mapper, nowSupplier).read(scope));
    stateRecords.addAll(new ManifestGapSource(Path.of(baseDir), mapper).read(scope));

    // --- diff ---
    GapRecordDiff.DiffResult diff = GapRecordDiff.diff(fileRecords, stateRecords);

    LOG.info(
        "audit_backfill_diff",
        "matched",
        diff.matched().size(),
        "onlyInFiles",
        diff.onlyInFiles().size(),
        "onlyInState",
        diff.onlyInState().size());

    if (diff.isClean()) {
      return handleCleanDiff(diff, period, scope);
    } else {
      return handleDivergentDiff(diff, scope);
    }
  }

  // -------------------------------------------------------------------------
  // Clean path
  // -------------------------------------------------------------------------

  private int handleCleanDiff(
      GapRecordDiff.DiffResult diff, PeriodSelector period, AuditScope scope) {
    System.out.println(
        "Audit clean: " + diff.matched().size() + " matched records, proceeding with backfill");

    LOG.info("audit_backfill_clean", "matched", diff.matched().size(), "dryRun", dryRun);

    if (dryRun) {
      LOG.info("audit_backfill_dry_run_skip", "reason", "dry-run flag set");
      return 0;
    }

    // Non-dry-run: invoke gaps backfill for each date × symbol × stream in scope.
    return invokeBackfill(period, scope);
  }

  /**
   * Enumerates all dates in the scope and invokes {@code gaps backfill} for each date × symbol ×
   * stream combination via a recursive {@link CommandLine} invocation.
   */
  private int invokeBackfill(PeriodSelector period, AuditScope scope) {
    List<String> dates = datesInScope(period);
    List<String> effectiveSymbols = nullOrEmpty(scope.symbols()) ? List.of() : scope.symbols();
    List<String> effectiveStreams = nullOrEmpty(scope.streams()) ? List.of() : scope.streams();

    // When no symbol/stream filter: we can't enumerate all symbols — just run without them
    // filtered. The BackfillCommand's ArchiveAnalyzer will scan whatever is present.
    // For the scope where symbols/streams ARE specified, iterate each combination.
    int lastExitCode = 0;

    if (effectiveSymbols.isEmpty() || effectiveStreams.isEmpty()) {
      // No fine-grained filter → warn that without symbol+stream the backfill needs explicit args
      LOG.warn(
          "audit_backfill_no_symbol_stream_filter",
          "note",
          "gaps backfill requires --symbol and --stream; skipping non-dry-run invocation");
      System.err.println(
          "Warning: backfill requires --symbol and --stream; specify both to enable automatic"
              + " backfill invocation.");
      return 0;
    }

    for (String date : dates) {
      for (String sym : effectiveSymbols) {
        for (String str : effectiveStreams) {
          LOG.info("audit_backfill_invoke", "date", date, "symbol", sym, "stream", str);
          int exitCode =
              new CommandLine(new BackfillCommand())
                  .execute(
                      "--base-dir=" + baseDir,
                      "--exchange=" + exchange,
                      "--symbol=" + sym,
                      "--stream=" + str,
                      "--date=" + date);
          if (exitCode != 0) {
            LOG.warn(
                "audit_backfill_invoke_failed",
                "date",
                date,
                "symbol",
                sym,
                "stream",
                str,
                "exitCode",
                exitCode);
            lastExitCode = exitCode;
          }
        }
      }
    }
    return lastExitCode;
  }

  // -------------------------------------------------------------------------
  // Divergence path
  // -------------------------------------------------------------------------

  private int handleDivergentDiff(GapRecordDiff.DiffResult diff, AuditScope scope) {
    int onlyFiles = diff.onlyInFiles().size();
    int onlyState = diff.onlyInState().size();
    int matched = diff.matched().size();

    LOG.warn(
        "audit_backfill_divergence",
        "onlyInFiles",
        onlyFiles,
        "onlyInState",
        onlyState,
        "matched",
        matched);

    // --- build divergence report ---
    long epochMs = System.currentTimeMillis();
    ObjectNode report = mapper.createObjectNode();
    report.put("audit_ts", Instant.now().toString());
    ObjectNode scopeNode = report.putObject("scope");
    scopeNode.put("start_ms", scope.startMs());
    scopeNode.put("end_ms", scope.endMs());
    report.put("matched_count", matched);

    ArrayNode onlyInFilesNode = report.putArray("only_in_files");
    for (GapRecord r : diff.onlyInFiles()) {
      onlyInFilesNode.add(mapper.valueToTree(r));
    }
    ArrayNode onlyInStateNode = report.putArray("only_in_state");
    for (GapRecord r : diff.onlyInState()) {
      onlyInStateNode.add(mapper.valueToTree(r));
    }

    // --- write divergence report to disk ---
    Path outputDir = resolveArchiveOutputDir(scope);
    String reportFileName = "divergence-" + scope.startMs() + "-" + epochMs + ".json";
    Path reportPath = outputDir.resolve(reportFileName);

    try {
      Files.createDirectories(outputDir);
      Files.writeString(
          reportPath,
          mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report),
          StandardCharsets.UTF_8);
      LOG.info("audit_backfill_divergence_report_written", "path", reportPath.toString());
    } catch (IOException e) {
      LOG.warn(
          "audit_backfill_divergence_report_write_failed",
          "path",
          reportPath.toString(),
          "error",
          e.getMessage());
      // Non-fatal: still fire alert and return 2
    }

    // --- fire alert ---
    String summary =
        "Audit divergence detected: " + onlyFiles + " file-only, " + onlyState + " state-only";
    String description =
        "onlyInFiles="
            + onlyFiles
            + ", onlyInState="
            + onlyState
            + ", matched="
            + matched
            + "; report="
            + reportPath;

    AlertmanagerNotifier notifier = buildNotifier();
    boolean alertSent = notifier.post("AuditDivergence", "critical", summary, description, null);
    if (!alertSent) {
      LOG.warn("audit_backfill_alert_post_failed", "summary", summary);
      // Do NOT change exit code — we still exit 2 because the diff was divergent
    }

    // --- stderr summary ---
    System.err.println(
        "Audit divergence: "
            + onlyFiles
            + " gap(s) only in files, "
            + onlyState
            + " gap(s) only in state. Report: "
            + reportPath);

    return 2;
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Path resolveArchiveOutputDir(AuditScope scope) {
    if (archiveOutputDir != null && !archiveOutputDir.isBlank()) {
      return Path.of(archiveOutputDir);
    }
    return Path.of(scope.baseDir()).resolve(".cryptolake").resolve("audit");
  }

  private AlertmanagerNotifier buildNotifier() {
    if (alertmanagerUrl != null && !alertmanagerUrl.isBlank()) {
      java.net.http.HttpClient httpClient =
          java.net.http.HttpClient.newBuilder()
              .version(java.net.http.HttpClient.Version.HTTP_1_1)
              .connectTimeout(java.time.Duration.ofSeconds(5))
              .build();
      return new AlertmanagerNotifier(alertmanagerUrl, httpClient, mapper);
    }
    return AlertmanagerNotifier.fromEnv(mapper);
  }

  private static List<String> datesInScope(PeriodSelector period) {
    LocalDate start = Instant.ofEpochMilli(period.startMs()).atZone(ZoneOffset.UTC).toLocalDate();
    LocalDate end = Instant.ofEpochMilli(period.endMs()).atZone(ZoneOffset.UTC).toLocalDate();
    List<String> dates = new ArrayList<>();
    for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
      dates.add(d.format(DATE_FMT));
    }
    return dates;
  }

  // ---- env fallback helpers (mirrors AuditStateCommand) ----

  private String resolveDbUrl() {
    if (dbUrl != null) {
      return dbUrl;
    }
    String env = System.getenv("PG_URL");
    return env != null ? env : "";
  }

  private String resolveDbUser() {
    if (dbUser != null && !dbUser.isBlank()) {
      return dbUser;
    }
    String env = System.getenv("PG_USER");
    return (env != null && !env.isBlank()) ? env : "cryptolake";
  }

  private String resolveDbPassword() {
    if (dbPassword != null) {
      return dbPassword;
    }
    String env = System.getenv("PG_PASSWORD");
    return env != null ? env : "";
  }

  private static boolean nullOrEmpty(List<String> list) {
    return list == null || list.isEmpty();
  }
}
