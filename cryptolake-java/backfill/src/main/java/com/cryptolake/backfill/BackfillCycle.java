package com.cryptolake.backfill;

import com.cryptolake.verify.gaps.ArchiveAnalyzer;
import com.cryptolake.verify.gaps.BackfillOrchestrator;
import com.cryptolake.verify.gaps.BinanceRestClient;
import com.cryptolake.verify.gaps.GapStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one backfill cycle: analyze archive for missing hours → fetch → write.
 *
 * <p>Ports {@code _run_backfill_cycle} from {@code backfill_scheduler.py}. Delegates REST/write
 * work to {@link BackfillOrchestrator} (in {@code :verify}) — mirrors Python's structure where
 * {@code backfill_scheduler.py} imports from {@code gaps.py} (design §1.3, §2.3).
 *
 * <p>Sequential (Q7 — no fan-out). Tier 5 A2.
 *
 * <p>Thread safety: stateless per call.
 */
public final class BackfillCycle {

  private static final Logger log = LoggerFactory.getLogger(BackfillCycle.class);

  private BackfillCycle() {}

  /**
   * Cycle summary.
   *
   * <p>Tier 2 §12 — record.
   */
  public record CycleSummary(int gapsFound, int recordsWritten) {}

  /**
   * Runs the backfill cycle.
   *
   * @param baseDir archive base directory
   * @param restClient shared REST client (Tier 5 D3)
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return cycle summary
   */
  public static CycleSummary run(Path baseDir, BinanceRestClient restClient, ObjectMapper mapper) {
    int totalGapsFound = 0;
    int totalRecordsWritten = 0;

    if (!Files.exists(baseDir)) {
      return new CycleSummary(0, 0);
    }

    BackfillOrchestrator orchestrator = new BackfillOrchestrator(restClient, mapper);

    // Walk exchange/symbol/stream directories (same pattern as ConsolidationCycle)
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

        for (Path symbolDir : symbolDirs) {
          String symbol = symbolDir.getFileName().toString();
          List<Path> streamDirs = new ArrayList<>();
          try (var s = Files.list(symbolDir)) {
            s.filter(Files::isDirectory).forEach(streamDirs::add);
          }

          for (Path streamDir : streamDirs) {
            String stream = streamDir.getFileName().toString();
            if (!GapStreams.BACKFILLABLE.contains(stream)) {
              continue; // Tier 1 §6 — non-backfillable streams are skipped
            }

            // Walk date directories
            List<Path> dateDirs = new ArrayList<>();
            try (var s = Files.list(streamDir)) {
              s.filter(Files::isDirectory).forEach(dateDirs::add);
            }

            for (Path dateDir : dateDirs) {
              String date = dateDir.getFileName().toString();
              try {
                Map<String, List<Integer>> gaps =
                    ArchiveAnalyzer.findGaps(baseDir, exchange, symbol, stream, date);
                List<Integer> missing = gaps.getOrDefault(stream, List.of());
                if (missing.isEmpty()) {
                  continue;
                }

                totalGapsFound += missing.size();
                BackfillOrchestrator.BackfillSummary summary =
                    orchestrator.fetchAndWrite(baseDir, exchange, symbol, stream, date, missing);
                totalRecordsWritten += summary.recordsWritten();

              } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                  Thread.currentThread().interrupt(); // Tier 5 A4
                  return new CycleSummary(totalGapsFound, totalRecordsWritten);
                }
                log.warn(
                    "backfill_date_error",
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
      }
    } catch (IOException e) {
      log.error("backfill_cycle_io_error", "error", e.getMessage());
    }

    return new CycleSummary(totalGapsFound, totalRecordsWritten);
  }
}
