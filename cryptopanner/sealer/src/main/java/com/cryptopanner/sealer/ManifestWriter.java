package com.cryptopanner.sealer;

import com.cryptopanner.common.EnvelopeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Skeleton manifest writer. Full §10.d fields are post-skeleton work. */
public final class ManifestWriter {

  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
  // Pretty-printed; sealer only writes manifests at hour boundaries, so per-class instance is fine.
  private static final ObjectMapper MAPPER = EnvelopeCodec.newPrettyMapper();

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
    write(target, node, symbol, stream, hourStart, merge, sealedAt, List.of(), List.of());
  }

  /**
   * Writes the manifest, additionally attaching {@code deploy_events[]} and {@code
   * connection_rotation_events[]} (design doc §6) when the corresponding hour contained them. Both
   * arrays are <em>absent</em> (not empty) when their list is empty.
   */
  public static void write(
      Path target,
      String node,
      String symbol,
      String stream,
      Instant hourStart,
      HourMerger.Result merge,
      Instant sealedAt,
      List<JsonNode> deployEvents,
      List<JsonNode> rotationEvents)
      throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
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
    root.putArray("minutes_missing"); // skeleton: minute-segment gap tracking is post-skeleton.
    root.put("file_sha256", merge.sha256Hex());
    root.put("file_size_bytes", merge.fileSizeBytes());
    root.put("record_count", merge.recordCount());

    // Sequence-ID fields for ID-bearing streams only (master spec §10.d "non-ID streams omit
    // sequence_id_range, sequence_gaps, backfill_attempts entirely").
    SequenceAnalyzer.Analysis seq = merge.sequence();
    if (seq != null && seq.firstId() >= 0) {
      ObjectNode range = root.putObject("sequence_id_range");
      range.put("first", seq.firstId());
      range.put("last", seq.lastId());
      ArrayNode gaps = root.putArray("sequence_gaps");
      for (int i = 0; i < seq.gaps().size(); i++) {
        SequenceAnalyzer.Gap g = seq.gaps().get(i);
        ObjectNode gn = gaps.addObject();
        gn.put("from", g.from());
        gn.put("to", g.to());
        gn.put("count", g.count());
        RestBackfiller.Outcome outcome =
            i < merge.gapOutcomes().size()
                ? merge.gapOutcomes().get(i)
                : RestBackfiller.Outcome.NOT_ATTEMPTED;
        gn.put("backfill_outcome", outcome.name());
      }
      ArrayNode attempts = root.putArray("backfill_attempts");
      for (BackfillAttempt a : merge.backfillAttempts()) {
        ObjectNode an = attempts.addObject();
        an.put("endpoint", a.endpoint());
        an.put("from_id", a.fromId());
        an.put("to_id", a.toId());
        an.put("attempts", a.attempts());
        an.put("http_status", a.httpStatus());
        an.put("records_inserted", a.recordsInserted());
        an.put("outcome", a.outcome().name());
        if (a.error() != null) an.put("error", a.error());
      }
    }

    // Deploy / rotation events that fell in this hour (design doc §6) — absent when none.
    if (!deployEvents.isEmpty()) {
      ArrayNode arr = root.putArray("deploy_events");
      deployEvents.forEach(arr::add);
    }
    if (!rotationEvents.isEmpty()) {
      ArrayNode arr = root.putArray("connection_rotation_events");
      rotationEvents.forEach(arr::add);
    }

    // Pretty-printed, LF line endings (master spec §10.d).
    String json = MAPPER.writeValueAsString(root);
    Files.writeString(target, json);
  }
}
