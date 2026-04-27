package com.cryptolake.verify.integrity;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Checks bookticker update ID backwards-jump detection.
 *
 * <p>Ports {@code @cli.command() def check_bookticker(...)} from {@code integrity.py}.
 */
@Command(name = "check-bookticker", description = "Check bookticker update ID for backwards jumps.")
public final class CheckBooktickerCommand implements Callable<Integer> {

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

  public CheckBooktickerCommand() {
    this(com.cryptolake.common.envelope.EnvelopeCodec.newMapper());
  }

  public CheckBooktickerCommand(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Integer call() throws IOException {
    Path dateDir =
        Path.of(baseDir).resolve(exchange).resolve(symbol).resolve("bookticker").resolve(date);
    List<Path> files = com.cryptolake.verify.gaps.ArchiveAnalyzer.listArchiveFiles(dateDir);
    IntegrityCheckResult result = ContinuityChecker.run("bookticker", files, mapper);
    printResult(result);
    return result.breaks().isEmpty() ? 0 : 1;
  }

  private void printResult(IntegrityCheckResult result) throws IOException {
    if (json) {
      System.out.println(mapper.writeValueAsString(result));
    } else {
      System.out.println("Records checked: " + result.recordCount());
      if (result.breaks().isEmpty()) {
        System.out.println("No backwards jumps found.");
      } else {
        System.out.println("Backwards jumps (" + result.breaks().size() + "):");
        for (var b : result.breaks()) {
          System.out.println(
              "  field="
                  + b.field()
                  + " expected="
                  + b.expected()
                  + " actual="
                  + b.actual()
                  + " at_received="
                  + b.atReceived());
        }
      }
    }
  }
}
