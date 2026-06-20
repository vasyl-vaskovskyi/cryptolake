package com.cryptopanner.sealer;

import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.SequenceId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Walks the concatenated bytes of one hour's records, parses the sequence ID from each line, and
 * reports the first/last IDs seen plus any gaps. Duplicates ({@code id == prev}) are treated as
 * no-op (Binance can re-emit during reconnect); only {@code id > prev + 1} counts as a gap (master
 * spec §10.b for ID-bearing gap-fillable streams).
 *
 * <p><b>Known limitations</b> (master spec §7.d / §10.b — planned, not in scope yet):
 *
 * <ul>
 *   <li><b>Cross-hour gaps are invisible.</b> A gap that spans an hour boundary — last ID of hour N
 *       → first ID of hour N+1 — is structurally undetectable here because the analyzer carries no
 *       cross-hour state. Closing this needs a "last ID seen" cursor persisted between sealer runs
 *       (file in {@code /data/cryptopanner} or PG row).
 *   <li><b>Out-of-order IDs ({@code id < prevId}) are silently accepted</b> and update {@code
 *       lastId} to the smaller value. Binance is not expected to emit out-of-order IDs on a fresh
 *       stream; if it does, this analyzer would lie. Add a strict-mode flag once we observe the
 *       real-world rate.
 * </ul>
 */
public final class SequenceAnalyzer {

  private SequenceAnalyzer() {}

  /** One gap window. {@code from} and {@code to} are inclusive bounds of the missing range. */
  public record Gap(long from, long to, long count) {}

  /**
   * Result of analyzing one hour. {@code gaps} may be empty; {@code firstId == lastId} on 1-record
   * hours.
   */
  public record Analysis(long firstId, long lastId, List<Gap> gaps) {}

  /**
   * Returns {@code null} if {@code stream} is not ID-bearing (caller should omit the manifest
   * sequence fields entirely). Throws on malformed JSON or missing ID field — those are bugs the
   * caller wants to know about, not data the Sealer should silently elide.
   */
  public static Analysis analyze(byte[] mergedBytes, String stream, ObjectMapper mapper)
      throws IOException {
    String idField = SequenceId.idField(stream);
    if (idField == null) return null;
    if (mergedBytes.length == 0) {
      return new Analysis(-1, -1, List.of());
    }

    List<Gap> gaps = new ArrayList<>();
    long firstId = -1;
    long lastId = -1;
    long prevId = -1;
    boolean haveAny = false;

    int start = 0;
    for (int i = 0; i < mergedBytes.length; i++) {
      if (mergedBytes[i] != '\n') continue;
      int len = i - start;
      if (len > 0) {
        String line = new String(mergedBytes, start, len, StandardCharsets.UTF_8);
        JsonNode root = mapper.readTree(line);
        JsonNode idNode;
        if ("backfill_record".equals(root.path("envelope").asText())) {
          // Spliced REST record: ID lives at record.<restIdField> (trades use "id", not "t").
          String restField = SequenceId.restIdField(stream);
          JsonNode record = root.get("record");
          idNode = record == null ? null : record.get(restField);
          if (idNode == null || !idNode.canConvertToLong()) {
            throw new IOException(
                "backfill record missing record." + restField + " (long): " + previewLine(line));
          }
        } else {
          JsonNode data = CaptureEnvelope.unwrap(mapper, root).get("data");
          if (data == null) {
            throw new IOException("record missing 'data' field: " + previewLine(line));
          }
          idNode = data.get(idField);
          if (idNode == null || !idNode.canConvertToLong()) {
            throw new IOException(
                "record missing data." + idField + " (long): " + previewLine(line));
          }
        }
        long id = idNode.asLong();
        if (!haveAny) {
          firstId = id;
          haveAny = true;
        } else if (id > prevId + 1) {
          gaps.add(new Gap(prevId + 1, id - 1, id - prevId - 1));
        }
        prevId = id;
        lastId = id;
      }
      start = i + 1;
    }

    if (!haveAny) {
      return new Analysis(-1, -1, List.of());
    }
    return new Analysis(firstId, lastId, gaps);
  }

  private static String previewLine(String line) {
    return line.length() > 120 ? line.substring(0, 120) + "..." : line;
  }
}
