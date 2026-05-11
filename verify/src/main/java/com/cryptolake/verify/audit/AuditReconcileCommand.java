package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Read-only reconcile command: runs the full file + state gap reconciliation and prints a unified
 * table showing the reconcile status of every record.
 *
 * <p>Executes all three file sources ({@link FileGapSource}, {@link MissingHourGapSource}, {@link
 * SequenceIdGapSource}) and all five state sources ({@link PgComponentRuntimeGapSource}, {@link
 * PgMaintenanceIntentGapSource}, {@link LedgerGapSource}, {@link KafkaOutageGapSource}, {@link
 * ManifestGapSource}), reconciles them via {@link GapRecordReconciler}, then prints the result.
 *
 * <p>This command is intentionally read-only: it does not fire alerts, trigger backfill, or write
 * any files. The gate command that acts on the result is {@code audit backfill}.
 *
 * <p>Exit codes:
 *
 * <ul>
 *   <li>0 — all file gaps are explained (or there are none); orphan state records do not fail.
 *   <li>1 — one or more unexplained file gaps were found.
 * </ul>
 */
@Command(name = "reconcile", description = "Reconcile file vs state gaps (read-only).")
public final class AuditReconcileCommand implements Callable<Integer> {

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

  // ---- output options ----

  @Option(names = "--json", description = "Print output as JSON instead of a human-readable table")
  private boolean json;

  /** Hidden option for test determinism: overrides "now" used by {@link MissingHourGapSource}. */
  @Option(names = "--now-override-iso", hidden = true)
  private String nowOverrideIso;

  private final ObjectMapper mapper;

  public AuditReconcileCommand() {
    this(EnvelopeCodec.newMapper());
  }

  public AuditReconcileCommand(ObjectMapper mapper) {
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

    // --- reconcile ---
    GapRecordReconciler.ReconcileResult result =
        GapRecordReconciler.reconcile(fileRecords, stateRecords);

    // --- print output ---
    String output =
        json
            ? OutputFormatter.toReconcileJson(result, mapper)
            : OutputFormatter.toReconcileHuman(result);
    System.out.println(output);

    return result.isClean() ? 0 : 1;
  }

  // ---- env fallback helpers (mirrors AuditBackfillCommand) ----

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
