package com.cryptolake.verify.cli;

import com.cryptolake.verify.verify.ManifestGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generates {@code manifest.json} per stream/date directory.
 *
 * <p>Ports {@code @cli.command() def manifest(...)} from {@code verify.py:379-398}.
 *
 * <p>Tier 5 K3 — returns exit code; Tier 5 K2 — output via {@link System#out}.
 */
@Command(name = "manifest", description = "Generate manifest.json for a date directory.")
public final class ManifestCommand implements java.util.concurrent.Callable<Integer> {

  @Option(names = "--date", required = true, description = "Date (YYYY-MM-DD)")
  private String date;

  @Option(
      names = "--base-dir",
      defaultValue = "/data/archive",
      description = "Archive base directory")
  private String baseDir;

  @Option(names = "--exchange", defaultValue = "binance", description = "Exchange name")
  private String exchange;

  private final ObjectMapper mapper;

  public ManifestCommand() {
    this(com.cryptolake.common.envelope.EnvelopeCodec.newMapper());
  }

  public ManifestCommand(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Integer call() throws IOException {
    Path base = Path.of(baseDir);
    ObjectNode manifest = ManifestGenerator.generate(base, exchange, date, mapper);

    // Write per-stream-dir manifest.json files
    Path exchangeDir = base.resolve(exchange);
    if (Files.exists(exchangeDir)) {
      List<Path> symbolDirs = new ArrayList<>();
      try (var stream = Files.list(exchangeDir)) {
        stream.filter(Files::isDirectory).forEach(symbolDirs::add);
      }
      symbolDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

      for (Path symbolDir : symbolDirs) {
        List<Path> streamDirs = new ArrayList<>();
        try (var stream = Files.list(symbolDir)) {
          stream.filter(Files::isDirectory).forEach(streamDirs::add);
        }
        for (Path streamDir : streamDirs) {
          Path dateDir = streamDir.resolve(date);
          if (Files.exists(dateDir)) {
            Path manifestPath = dateDir.resolve("manifest.json");
            // Python: json.dumps(m, indent=2) + "\n"
            String content =
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest) + "\n";
            Files.writeString(manifestPath, content, StandardCharsets.UTF_8);
            System.out.println("Written: " + manifestPath);
          }
        }
      }
    }

    // Print the merged manifest as pretty JSON (matches Python's click.echo(json.dumps(m,
    // indent=2)))
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest));
    return 0;
  }
}
