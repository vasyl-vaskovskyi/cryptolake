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
 * Audit state sources for gaps: PG component_runtime, PG maintenance_intent, lifecycle ledger,
 * Kafka outage journal, and archive manifests.
 *
 * <p>Accepts a period expressed as one of {@code --day}, {@code --week}, {@code --hour}, or {@code
 * --since}/{@code --until}. Instantiates all five state-based {@link GapSource} implementations,
 * concatenates their results, and prints via {@link OutputFormatter}.
 *
 * <p>DB sources ({@link PgComponentRuntimeGapSource}, {@link PgMaintenanceIntentGapSource})
 * gracefully return empty lists when {@code --db-url} is blank or the database is unreachable, so
 * the command is usable without a live Postgres connection.
 */
@Command(name = "state", description = "Audit state sources for gaps.")
public final class AuditStateCommand implements Callable<Integer> {

  // ---- period flags (mutually exclusive by convention; validated in call()) ----

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
      description = "Archive base directory for ManifestGapSource (default: ${DEFAULT-VALUE})")
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

  // ---- output flag ----

  @Option(names = "--json", description = "Output as JSON array (default: human table)")
  private boolean json;

  /**
   * Hidden option for test determinism: overrides the "now" instant used by {@link
   * KafkaOutageGapSource}. Not intended for production use.
   */
  @Option(names = "--now-override-iso", hidden = true)
  private String nowOverrideIso;

  private final ObjectMapper mapper;

  public AuditStateCommand() {
    this(EnvelopeCodec.newMapper());
  }

  public AuditStateCommand(ObjectMapper mapper) {
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

    // --- resolve DB config from flags + env fallbacks ---
    String resolvedUrl = resolveDbUrl();
    String resolvedUser = resolveDbUser();
    String resolvedPassword = resolveDbPassword();
    JdbcConfig jdbcConfig = new JdbcConfig(resolvedUrl, resolvedUser, resolvedPassword);

    // --- build now supplier ---
    Supplier<Instant> nowSupplier =
        nowOverrideIso != null ? () -> Instant.parse(nowOverrideIso) : Instant::now;

    // --- instantiate all five sources ---
    List<GapSource> sources = new ArrayList<>();
    sources.add(new PgComponentRuntimeGapSource(jdbcConfig));
    sources.add(new PgMaintenanceIntentGapSource(jdbcConfig));
    sources.add(new LedgerGapSource(Path.of(dataDir), mapper));
    sources.add(new KafkaOutageGapSource(Path.of(dataDir), mapper, nowSupplier));
    sources.add(new ManifestGapSource(Path.of(baseDir), mapper));

    // --- collect results ---
    List<GapRecord> records = new ArrayList<>();
    for (GapSource source : sources) {
      records.addAll(source.read(scope));
    }

    // --- format output ---
    if (json) {
      System.out.println(OutputFormatter.toJson(records, mapper));
    } else {
      System.out.print(OutputFormatter.toHuman(records));
    }
    return 0;
  }

  // ── env fallback helpers ──────────────────────────────────────────────────

  private String resolveDbUrl() {
    if (dbUrl != null) {
      // explicit flag (including empty string = intentional skip)
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
