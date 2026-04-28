package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Read-only gap report command.
 *
 * <p>Ports {@code @cli.command() def analyze(...)} from {@code gaps.py}. Analyzes the archive for
 * missing hours without writing any files.
 *
 * <p>Tier 5 K2 — all output via {@link System#out}. Tier 5 K3 — returns exit code.
 */
@Command(name = "analyze", description = "Analyze archive for missing hours (read-only).")
public final class AnalyzeCommand implements Callable<Integer> {

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

  @Option(names = "--json", description = "Output as JSON")
  private boolean json;

  private final ObjectMapper mapper;

  public AnalyzeCommand() {
    this(com.cryptolake.common.envelope.EnvelopeCodec.newMapper());
  }

  public AnalyzeCommand(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Integer call() throws IOException {
    Path base = Path.of(baseDir);
    Map<String, List<Integer>> gaps =
        ArchiveAnalyzer.findGaps(base, exchange, symbol, stream, date);

    if (json) {
      System.out.println(mapper.writeValueAsString(gaps));
    } else {
      List<Integer> missing = gaps.getOrDefault(stream, List.of());
      if (missing.isEmpty()) {
        System.out.println(
            "No missing hours for " + exchange + "/" + symbol + "/" + stream + "/" + date);
      } else {
        System.out.println(
            "Missing hours for " + exchange + "/" + symbol + "/" + stream + "/" + date + ":");
        for (int h : missing) {
          boolean backfillable = GapStreams.BACKFILLABLE.contains(stream);
          System.out.println(
              "  hour " + h + " (" + (backfillable ? "backfillable" : "NOT backfillable") + ")");
        }
      }
    }
    return 0;
  }
}
