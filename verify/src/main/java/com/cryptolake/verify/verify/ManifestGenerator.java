package com.cryptolake.verify.verify;

import com.cryptolake.verify.archive.DecompressAndParse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * Generates a JSON manifest summarizing archive contents for a date.
 *
 * <p>Ports {@code generate_manifest(base_dir, exchange, date)} from {@code verify.py:215-271}. Uses
 * insertion-ordered {@link ObjectNode} to match Python's dict insertion order (Tier 5 B1).
 *
 * <p>Thread safety: stateless utility.
 */
public final class ManifestGenerator {

  private ManifestGenerator() {}

  /**
   * Generates the manifest for {@code exchange}/{@code date} in {@code baseDir}.
   *
   * @param baseDir archive base directory
   * @param exchange exchange name (e.g. "binance")
   * @param date date string (YYYY-MM-DD)
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return {@link ObjectNode} with insertion-ordered keys matching Python's output
   * @throws IOException on file I/O errors
   */
  public static ObjectNode generate(Path baseDir, String exchange, String date, ObjectMapper mapper)
      throws IOException {
    // Python builds: {"date": date, "exchange": exchange, "symbols": {...}}
    ObjectNode manifest = mapper.createObjectNode();
    manifest.put("date", date);
    manifest.put("exchange", exchange);
    ObjectNode symbols = mapper.createObjectNode();
    manifest.set("symbols", symbols);

    Path exchangeDir = baseDir.resolve(exchange);
    if (!Files.exists(exchangeDir)) {
      return manifest;
    }

    // Iterate symbol directories sorted (matches Python's sorted(exchange_dir.iterdir()))
    List<Path> symbolDirs = new ArrayList<>();
    try (var stream = Files.list(exchangeDir)) {
      stream.filter(Files::isDirectory).forEach(symbolDirs::add);
    }
    symbolDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

    for (Path symbolDir : symbolDirs) {
      String sym = symbolDir.getFileName().toString();
      ObjectNode symNode = mapper.createObjectNode();
      ObjectNode streams = mapper.createObjectNode();
      symNode.set("streams", streams);
      symbols.set(sym, symNode);

      // Iterate stream directories sorted
      List<Path> streamDirs = new ArrayList<>();
      try (var stream = Files.list(symbolDir)) {
        stream.filter(Files::isDirectory).forEach(streamDirs::add);
      }
      streamDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

      for (Path streamDir : streamDirs) {
        Path dateDir = streamDir.resolve(date);
        if (!Files.exists(dateDir)) {
          continue;
        }

        // Collect all matching archive files (base + backfill + late)
        List<Path> allFiles = new ArrayList<>();
        try (var stream = Files.list(dateDir)) {
          stream
              .filter(
                  f -> {
                    String n = f.getFileName().toString();
                    return n.startsWith("hour-") && n.endsWith(".jsonl.zst");
                  })
              .forEach(allFiles::add);
        }
        if (allFiles.isEmpty()) {
          continue;
        }
        allFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));

        TreeSet<Integer> hours = new TreeSet<>();
        int recordCount = 0;
        ArrayNode gapsList = mapper.createArrayNode();

        for (Path f : allFiles) {
          // Extract hour from filename (e.g. "hour-16.jsonl.zst" → 16)
          String name = f.getFileName().toString();
          String hourPart = name.split("\\.")[0].replace("hour-", "");
          try {
            hours.add(Integer.parseInt(hourPart));
          } catch (NumberFormatException ignored) {
            // best-effort shutdown; never block main shutdown path (Tier 5 G1)
          }

          try {
            List<JsonNode> envs = DecompressAndParse.parse(f, mapper);
            recordCount += envs.size();
            for (JsonNode env : envs) {
              if ("gap".equals(env.path("type").asText())) {
                ObjectNode gapEntry = mapper.createObjectNode();
                gapEntry.set("symbol", env.path("symbol"));
                gapEntry.set("reason", env.path("reason"));
                gapEntry.set("gap_start_ts", env.path("gap_start_ts"));
                gapEntry.set("gap_end_ts", env.path("gap_end_ts"));
                setNullableNode(gapEntry, "component", env.path("component"), mapper);
                setNullableNode(gapEntry, "cause", env.path("cause"), mapper);
                setNullableNode(gapEntry, "planned", env.path("planned"), mapper);
                setNullableNode(gapEntry, "maintenance_id", env.path("maintenance_id"), mapper);
                gapsList.add(gapEntry);
              }
            }
          } catch (Exception ignored) {
            // Skip unreadable/corrupt files; they surface via checksum errors
          }
        }

        // Only add the stream entry if hours were found (matches Python's `if hours:`)
        if (!hours.isEmpty()) {
          ObjectNode streamEntry = mapper.createObjectNode();
          ArrayNode hoursNode = mapper.createArrayNode();
          for (int h : hours) {
            hoursNode.add(h);
          }
          streamEntry.set("hours", hoursNode);
          streamEntry.put("record_count", recordCount);
          streamEntry.set("gaps", gapsList);
          streams.set(streamDir.getFileName().toString(), streamEntry);
        }
      }
    }
    return manifest;
  }

  /**
   * Sets a field on {@code target} as null if the source node is missing or null, else sets the
   * source node's value.
   */
  private static void setNullableNode(
      ObjectNode target, String field, JsonNode sourceNode, ObjectMapper mapper) {
    if (sourceNode.isMissingNode() || sourceNode.isNull()) {
      target.putNull(field);
    } else {
      target.set(field, sourceNode);
    }
  }
}
