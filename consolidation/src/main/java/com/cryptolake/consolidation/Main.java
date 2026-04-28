package com.cryptolake.consolidation;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.consolidation.core.ConsolidateDay;
import com.cryptolake.consolidation.scheduler.ConsolidationScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Entry point for the {@code cryptolake-consolidation} service.
 *
 * <p>Default: starts the scheduler (matches Docker entrypoint). Subcommand {@code consolidate-day}
 * for ad-hoc consolidation.
 *
 * <p>Tier 5 K1 — picocli {@code @Command}. Tier 2 §14 — single shared {@link ObjectMapper}.
 */
@Command(
    name = "cryptolake-consolidation",
    description = "CryptoLake daily consolidation service.",
    subcommands = {Main.ConsolidateDayCommand.class, Main.SchedulerCommand.class},
    mixinStandardHelpOptions = true)
public final class Main {

  // Single shared ObjectMapper (Tier 2 §14; Tier 5 B6)
  static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  /** Ad-hoc single-day consolidation command. */
  @Command(name = "consolidate-day", description = "Run consolidation for a specific day.")
  static final class ConsolidateDayCommand implements Callable<Integer> {

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

    @Override
    public Integer call() throws IOException {
      ConsolidateDay.ConsolidateResult result =
          ConsolidateDay.run(Path.of(baseDir), exchange, symbol, stream, date, MAPPER);
      System.out.println(
          "Consolidation "
              + (result.success() ? "OK" : "FAILED")
              + ": "
              + result.totalRecords()
              + " records ("
              + result.dataRecords()
              + " data, "
              + result.gapRecords()
              + " gaps, "
              + result.missingHours()
              + " missing hours)");
      return result.success() ? 0 : 1;
    }
  }

  /** Long-running scheduler command (default). */
  @Command(name = "scheduler", description = "Start the daily consolidation scheduler (default).")
  static final class SchedulerCommand implements Callable<Integer> {

    @Option(
        names = "--base-dir",
        defaultValue = "/data/archive",
        description = "Archive base directory")
    private String baseDir;

    @Option(
        names = "--start-hour-utc",
        defaultValue = "2",
        description = "UTC hour to run consolidation (default: 2 → 02:30 UTC)")
    private int startHourUtc;

    @Override
    public Integer call() throws InterruptedException {
      Path base = Path.of(baseDir);
      ConsolidationScheduler scheduler = new ConsolidationScheduler(base, startHourUtc, MAPPER);

      // SIGTERM hook — preferred path (Q5; Tier 5 A3)
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
