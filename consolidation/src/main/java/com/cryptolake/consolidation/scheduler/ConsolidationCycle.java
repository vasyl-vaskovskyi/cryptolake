package com.cryptolake.consolidation.scheduler;

import com.cryptolake.consolidation.core.ConsolidateDay;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one consolidation cycle: walks all (exchange, symbol, stream, date) tuples and calls {@link
 * ConsolidateDay#run} for each.
 *
 * <p>Ports {@code _run_consolidation_cycle} from {@code consolidation_scheduler.py:75-145}.
 * Sequential (Q12 — no fan-out).
 *
 * <p>Thread safety: stateless per call; isolation by directory ensures no shared mutation across
 * concurrent calls (if ever parallelized).
 */
public final class ConsolidationCycle {

  private static final Logger log = LoggerFactory.getLogger(ConsolidationCycle.class);

  private ConsolidationCycle() {}

  /**
   * Cycle summary.
   *
   * <p>Tier 2 §12 — record.
   */
  public record CycleSummary(
      int symbolsProcessed, long totalRecords, int missingHours, boolean anyFailed) {}

  /**
   * Runs the consolidation cycle.
   *
   * @param baseDir archive base directory
   * @param dateOverride null = yesterday UTC; non-null = explicit override
   * @param metrics consolidation metrics to update (Tier 1 §5 — missing hour metric increment)
   * @param mapper shared {@link ObjectMapper}
   * @return cycle summary
   */
  public static CycleSummary run(
      Path baseDir, String dateOverride, ConsolidationMetrics metrics, ObjectMapper mapper) {

    String date =
        dateOverride != null
            ? dateOverride
            : LocalDate.now(ZoneOffset.UTC).minusDays(1).toString(); // default: yesterday UTC

    log.info("consolidation_cycle_started", "date", date);

    int symbolsProcessed = 0;
    long totalRecords = 0L;
    int totalMissingHours = 0;
    boolean anyFailed = false;

    if (!Files.exists(baseDir)) {
      log.warn("consolidation_base_dir_missing", "base_dir", baseDir.toString());
      return new CycleSummary(0, 0L, 0, false);
    }

    // Walk exchange/symbol/stream directories
    try {
      List<Path> exchangeDirs = new ArrayList<>();
      try (var s = Files.list(baseDir)) {
        s.filter(Files::isDirectory).forEach(exchangeDirs::add);
      }
      exchangeDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

      for (Path exchangeDir : exchangeDirs) {
        String exchange = exchangeDir.getFileName().toString();
        List<Path> symbolDirs = new ArrayList<>();
        try (var s = Files.list(exchangeDir)) {
          s.filter(Files::isDirectory).forEach(symbolDirs::add);
        }
        symbolDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

        for (Path symbolDir : symbolDirs) {
          String symbol = symbolDir.getFileName().toString();
          List<Path> streamDirs = new ArrayList<>();
          try (var s = Files.list(symbolDir)) {
            s.filter(Files::isDirectory).forEach(streamDirs::add);
          }
          streamDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

          for (Path streamDir : streamDirs) {
            String stream = streamDir.getFileName().toString();
            Path dateDir = streamDir.resolve(date);
            if (!Files.exists(dateDir)) {
              continue;
            }

            try {
              ConsolidateDay.ConsolidateResult result =
                  ConsolidateDay.run(baseDir, exchange, symbol, stream, date, mapper);

              symbolsProcessed++;
              totalRecords += result.totalRecords();
              totalMissingHours += result.missingHours();

              if (!result.success()) {
                anyFailed = true;
                metrics.incVerificationFailures();
                log.error(
                    "consolidation_day_failed",
                    "exchange",
                    exchange,
                    "symbol",
                    symbol,
                    "stream",
                    stream,
                    "date",
                    date);
              }

              // Tier 1 §5: missing hour metric + log (log already in MissingHourGapFactory)
              metrics.incMissingHours(result.missingHours());
              metrics.incFilesConsolidated(1.0);

            } catch (IOException e) {
              anyFailed = true;
              metrics.incVerificationFailures();
              log.error(
                  "consolidation_day_exception",
                  "exchange",
                  exchange,
                  "symbol",
                  symbol,
                  "stream",
                  stream,
                  "date",
                  date,
                  "error",
                  e.getMessage());
            }
          }
        }
      }
    } catch (IOException e) {
      log.error("consolidation_cycle_io_error", "error", e.getMessage());
      return new CycleSummary(symbolsProcessed, totalRecords, totalMissingHours, true);
    }

    metrics.incDaysProcessed(1.0);
    log.info(
        "consolidation_cycle_finished",
        "date",
        date,
        "symbols_processed",
        symbolsProcessed,
        "total_records",
        totalRecords,
        "missing_hours",
        totalMissingHours,
        "any_failed",
        anyFailed);

    return new CycleSummary(symbolsProcessed, totalRecords, totalMissingHours, anyFailed);
  }
}
