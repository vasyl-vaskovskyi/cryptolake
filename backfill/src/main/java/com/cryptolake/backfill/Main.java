package com.cryptolake.backfill;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.verify.gaps.ArchiveAnalyzer;
import com.cryptolake.verify.gaps.BackfillOrchestrator;
import com.cryptolake.verify.gaps.BinanceRestClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Entry point for the {@code cryptolake-backfill} service.
 *
 * <p>Default: starts the 6-hourly scheduler. Subcommand {@code run-once} for ad-hoc backfill.
 *
 * <p>Tier 5 K1 — picocli. Tier 2 §14 — single shared {@link ObjectMapper} and {@link HttpClient}.
 */
@Command(
    name = "cryptolake-backfill",
    description = "CryptoLake backfill scheduler.",
    subcommands = {Main.RunOnceCommand.class, Main.SchedulerSubcommand.class},
    mixinStandardHelpOptions = true)
public final class Main {

  static final ObjectMapper MAPPER = EnvelopeCodec.newMapper(); // Tier 2 §14; Tier 5 B6

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  /** Ad-hoc single-run backfill command. */
  @Command(name = "run-once", description = "Run one backfill cycle immediately.")
  static final class RunOnceCommand implements Callable<Integer> {

    @Option(names = "--base-dir", defaultValue = "/data/archive")
    private String baseDir;

    @Option(names = "--exchange", required = true)
    private String exchange;

    @Option(names = "--symbol", required = true)
    private String symbol;

    @Option(names = "--stream", required = true)
    private String stream;

    @Option(names = "--date", required = true)
    private String date;

    @Override
    public Integer call() throws IOException, InterruptedException {
      HttpClient httpClient = HttpClient.newHttpClient();
      BinanceRestClient restClient = new BinanceRestClient(httpClient, MAPPER);
      BackfillOrchestrator orchestrator = new BackfillOrchestrator(restClient, MAPPER);
      Map<String, List<Integer>> gaps =
          ArchiveAnalyzer.findGaps(Path.of(baseDir), exchange, symbol, stream, date);
      List<Integer> missing = gaps.getOrDefault(stream, List.of());
      BackfillOrchestrator.BackfillSummary summary =
          orchestrator.fetchAndWrite(Path.of(baseDir), exchange, symbol, stream, date, missing);
      System.out.println(
          "Backfill complete: "
              + summary.hoursWritten()
              + "/"
              + summary.hoursAttempted()
              + " hours written, "
              + summary.recordsWritten()
              + " records.");
      return 0;
    }
  }

  /** Long-running scheduler (default). */
  @Command(name = "scheduler", description = "Start the 6-hourly backfill scheduler (default).")
  static final class SchedulerSubcommand implements Callable<Integer> {

    @Option(names = "--base-dir", defaultValue = "/data/archive")
    private String baseDir;

    @Option(names = "--interval-hours", defaultValue = "6")
    private int intervalHours;

    @Override
    public Integer call() throws InterruptedException {
      BackfillScheduler scheduler =
          new BackfillScheduler(Path.of(baseDir), (long) intervalHours * 3600L, MAPPER);

      CountDownLatch shutdownLatch = new CountDownLatch(1);
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    scheduler.stop();
                    shutdownLatch.countDown();
                  }));

      Thread.ofVirtual().start(scheduler::run);
      shutdownLatch.await();
      return 0;
    }
  }
}
