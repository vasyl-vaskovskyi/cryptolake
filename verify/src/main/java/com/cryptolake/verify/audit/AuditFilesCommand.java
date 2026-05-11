package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Audit archive files for gaps: envelope gaps, missing hours, and sequence-ID breaks.
 *
 * <p>Accepts a period expressed as one of {@code --day}, {@code --week}, {@code --hour}, or {@code
 * --since}/{@code --until}. Constructs an {@link AuditScope}, runs all three file-based {@link
 * GapSource} implementations, and prints results via {@link OutputFormatter}.
 *
 * <p>Tier 5 K2 — all output via {@link System#out}. Tier 5 K3 — returns exit code.
 */
@Command(name = "files", description = "Audit archive files for gaps.")
public final class AuditFilesCommand implements Callable<Integer> {

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
      description = "Archive base directory (default: ${DEFAULT-VALUE})")
  private String baseDir;

  // ---- output flag ----

  @Option(names = "--json", description = "Output as JSON array (default: human table)")
  private boolean json;

  /**
   * Hidden option for test determinism: overrides the "now" instant used by {@link
   * MissingHourGapSource}. Not intended for production use.
   */
  @Option(names = "--now-override-iso", hidden = true)
  private String nowOverrideIso;

  private final ObjectMapper mapper;

  public AuditFilesCommand() {
    this(EnvelopeCodec.newMapper());
  }

  public AuditFilesCommand(ObjectMapper mapper) {
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

    // --- run sources ---
    List<GapRecord> records = new ArrayList<>();
    records.addAll(new FileGapSource(mapper).read(scope));
    records.addAll(new MissingHourGapSource(nowSupplier).read(scope));
    records.addAll(new SequenceIdGapSource(mapper).read(scope));

    // --- format output ---
    if (json) {
      System.out.println(OutputFormatter.toJson(records, mapper));
    } else {
      System.out.print(OutputFormatter.toHuman(records));
    }
    return 0;
  }

  private static boolean nullOrEmpty(List<String> list) {
    return list == null || list.isEmpty();
  }
}
