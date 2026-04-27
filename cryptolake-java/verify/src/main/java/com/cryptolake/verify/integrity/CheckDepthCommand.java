package com.cryptolake.verify.integrity;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Checks depth pu-chain continuity.
 *
 * <p>Ports {@code @cli.command() def check_depth(...)} from {@code integrity.py}.
 */
@Command(name = "check-depth", description = "Check depth pu-chain continuity.")
public final class CheckDepthCommand implements Callable<Integer> {

  @Option(
      names = "--base-dir",
      defaultValue = "/data/archive",
      description = "Archive base directory")
  private String baseDir;

  @Option(names = "--exchange", required = true, description = "Exchange name")
  private String exchange;

  @Option(names = "--symbol", required = true, description = "Symbol name")
  private String symbol;

  @Option(names = "--date", required = true, description = "Date (YYYY-MM-DD)")
  private String date;

  @Option(names = "--json", description = "Output as JSON")
  private boolean json;

  private final ObjectMapper mapper;

  public CheckDepthCommand() {
    this(com.cryptolake.common.envelope.EnvelopeCodec.newMapper());
  }

  public CheckDepthCommand(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Integer call() throws IOException {
    Path dateDir =
        Path.of(baseDir).resolve(exchange).resolve(symbol).resolve("depth").resolve(date);
    List<Path> files = com.cryptolake.verify.gaps.ArchiveAnalyzer.listArchiveFiles(dateDir);
    IntegrityCheckResult result = ContinuityChecker.run("depth", files, mapper);
    printResult(result);
    return result.breaks().isEmpty() ? 0 : 1;
  }

  private void printResult(IntegrityCheckResult result) throws IOException {
    if (json) {
      System.out.println(mapper.writeValueAsString(result));
    } else {
      System.out.println("Records checked: " + result.recordCount());
      if (result.breaks().isEmpty()) {
        System.out.println("No continuity breaks found.");
      } else {
        System.out.println("Breaks (" + result.breaks().size() + "):");
        for (var b : result.breaks()) {
          System.out.println(
              "  field="
                  + b.field()
                  + " expected="
                  + b.expected()
                  + " actual="
                  + b.actual()
                  + " missing="
                  + b.missing()
                  + " at_received="
                  + b.atReceived());
        }
      }
    }
  }
}
