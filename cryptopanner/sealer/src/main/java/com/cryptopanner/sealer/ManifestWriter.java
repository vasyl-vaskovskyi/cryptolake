package com.cryptopanner.sealer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Skeleton manifest writer. Full §10.d fields are post-skeleton work. */
public final class ManifestWriter {

  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

  private ManifestWriter() {}

  public static void write(
      Path target,
      String node,
      String symbol,
      String stream,
      Instant hourStart,
      HourMerger.Result merge,
      Instant sealedAt)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    ObjectNode root = mapper.createObjectNode();
    root.put("manifest_schema_version", 1);
    root.put("node", node);
    root.put("symbol", symbol);
    root.put("stream", stream);
    root.put("date", DATE.format(hourStart));
    root.put("hour", hourStart.atOffset(ZoneOffset.UTC).getHour());
    root.put("sealed_at", sealedAt.toString());
    ArrayNode minutes = root.putArray("minutes_present");
    for (int m : merge.minutesPresent()) {
      minutes.add(m);
    }
    root.putArray("minutes_missing"); // empty for skeleton (no gap tracking)
    root.put("file_sha256", merge.sha256Hex());
    root.put("file_size_bytes", merge.fileSizeBytes());
    root.put("record_count", merge.recordCount());

    // Pretty-printed, LF line endings (master spec §10.d).
    String json = mapper.writeValueAsString(root);
    Files.writeString(target, json);
  }
}
