package com.cryptopanner.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

/**
 * Appends WS-rotation event records to {@code deploy/rotations.jsonl} (design doc §5.2 step 5, §6).
 * The Sealer reads this at the hour boundary (via {@code HourEventReader}) to populate a manifest's
 * {@code connection_rotation_events[]}. Each append is a single fsynced JSON line so a crash can
 * leave whole lines only.
 */
public final class RotationsLog {

  private RotationsLog() {}

  /**
   * A rotation outcome record (subset of the §6 schema; diff details added by the caller later).
   */
  public record RotationEvent(
      String rotationId,
      String reason,
      double oldConnectionAgeHours,
      Instant promotedAt,
      List<Integer> minutesMerged,
      String verifyResult) {}

  public static void append(Path rotationsFile, ObjectMapper mapper, RotationEvent e)
      throws IOException {
    Path parent = rotationsFile.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    ObjectNode node = mapper.createObjectNode();
    node.put("rotation_id", e.rotationId());
    node.put("reason", e.reason());
    node.put("old_connection_age_hours", e.oldConnectionAgeHours());
    node.put("promoted_at", e.promotedAt().toString());
    ArrayNode mins = node.putArray("minutes_merged");
    for (int m : e.minutesMerged()) {
      mins.add(m);
    }
    node.put("verify_result", e.verifyResult());

    byte[] line;
    try {
      line = (mapper.writeValueAsString(node) + "\n").getBytes(StandardCharsets.UTF_8);
    } catch (Exception ex) {
      throw new IOException("rotation event serialization failed", ex);
    }
    try (FileChannel ch =
        FileChannel.open(
            rotationsFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND)) {
      ch.write(java.nio.ByteBuffer.wrap(line));
      ch.force(true);
    }
  }
}
