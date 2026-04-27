package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * REST-driven gap recovery command.
 *
 * <p>Ports {@code @cli.command() def backfill(...)} from {@code gaps.py}. Analyzes missing hours,
 * filters to backfillable streams, and fetches+writes backfill archive files.
 *
 * <p>Tier 1 §6 honored: REST replay is preferred; non-backfillable streams reported unrecoverable.
 * Tier 5 D3 — single shared {@link HttpClient}. Tier 5 K2, K3.
 */
@Command(name = "backfill", description = "Backfill missing hours from REST API.")
public final class BackfillCommand implements Callable<Integer> {

  @Option(
      names = "--base-dir",
      defaultValue = "/data/archive",
      description = "Archive base directory")
  private String baseDir;

  @Option(names = "--exchange", required = true, description = "Exchange name")
  private String exchange;

  @Option(names = "--symbol", required = true, description = "Symbol name")
  private String symbol;

  @Option(names = "--stream", required = true, description = "Stream type")
  private String stream;

  @Option(names = "--date", required = true, description = "Date (YYYY-MM-DD)")
  private String date;

  @Option(names = "--dry-run", description = "Show plan without writing files")
  private boolean dryRun;

  @Option(names = "--json", description = "Output as JSON")
  private boolean json;

  private final ObjectMapper mapper;
  private final HttpClient httpClient;

  public BackfillCommand() {
    this(com.cryptolake.common.envelope.EnvelopeCodec.newMapper(), HttpClient.newHttpClient());
  }

  public BackfillCommand(ObjectMapper mapper, HttpClient httpClient) {
    this.mapper = mapper;
    this.httpClient = httpClient;
  }

  @Override
  public Integer call() throws IOException, InterruptedException {
    Path base = Path.of(baseDir);
    Map<String, List<Integer>> gaps =
        ArchiveAnalyzer.findGaps(base, exchange, symbol, stream, date);
    List<Integer> missing = gaps.getOrDefault(stream, List.of());

    // Check if stream is backfillable
    if (!GapStreams.BACKFILLABLE.contains(stream)) {
      System.out.println("Stream '" + stream + "' is not backfillable. Unrecoverable via REST.");
      return 0;
    }

    if (missing.isEmpty()) {
      System.out.println("No missing hours to backfill.");
      return 0;
    }

    if (dryRun) {
      System.out.println(
          "Dry-run: would backfill "
              + missing.size()
              + " hours for "
              + exchange
              + "/"
              + symbol
              + "/"
              + stream
              + "/"
              + date
              + ":");
      for (int h : missing) {
        System.out.println("  hour " + h);
      }
      return 0;
    }

    // Execute backfill
    BinanceRestClient restClient = new BinanceRestClient(httpClient, mapper);
    BackfillOrchestrator orchestrator = new BackfillOrchestrator(restClient, mapper);
    BackfillOrchestrator.BackfillSummary summary =
        orchestrator.fetchAndWrite(base, exchange, symbol, stream, date, missing);

    if (json) {
      System.out.println(
          mapper.writeValueAsString(
              Map.of(
                  "hours_attempted", summary.hoursAttempted(),
                  "hours_written", summary.hoursWritten(),
                  "records_written", summary.recordsWritten())));
    } else {
      System.out.println(
          "Backfill complete: "
              + summary.hoursWritten()
              + "/"
              + summary.hoursAttempted()
              + " hours written, "
              + summary.recordsWritten()
              + " records total.");
    }
    return 0;
  }
}
