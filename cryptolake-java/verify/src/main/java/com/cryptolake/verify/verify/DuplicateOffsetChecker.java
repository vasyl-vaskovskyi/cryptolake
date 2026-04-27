package com.cryptolake.verify.verify;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Detects duplicate (topic, partition, offset) broker coordinate tuples.
 *
 * <p>Ports {@code check_duplicate_offsets(envelopes)} from {@code verify.py:53-69}. Records with
 * {@code _offset < 0} are skipped (Tier 5 M9 sentinel for synthetic/gap records).
 *
 * <p>Thread safety: stateless utility.
 */
public final class DuplicateOffsetChecker {

  private DuplicateOffsetChecker() {}

  /**
   * Scans {@code envelopes} for duplicate broker coordinates.
   *
   * @return list of error strings (empty = no duplicates)
   */
  public static List<String> check(List<JsonNode> envelopes) {
    Set<String> seen = new HashSet<>();
    List<String> errors = new ArrayList<>();

    for (JsonNode env : envelopes) {
      JsonNode offsetNode = env.path("_offset");
      boolean isPresent = !offsetNode.isMissingNode() && !offsetNode.isNull();

      if (isPresent) {
        long offset = offsetNode.asLong(0L);
        if (offset < 0L) {
          continue; // Tier 5 M9 — synthetic/gap sentinel, skip
        }
      }

      // Extract broker coordinates
      JsonNode topicNode = env.path("_topic");
      JsonNode partitionNode = env.path("_partition");

      // Build the dedup key (internal; Python uses a tuple)
      String internalKey =
          topicNode.asText("null")
              + "|"
              + partitionNode.asText("null")
              + "|"
              + (isPresent ? offsetNode.asLong(0L) : "null");

      if (seen.contains(internalKey)) {
        // Format matches Python's f"Duplicate broker record: {key}" where key is a tuple repr
        // Python tuple: (topic, partition, offset) — single quotes around strings, None for nulls
        String tupleRepr = buildTupleRepr(topicNode, partitionNode, offsetNode, isPresent);
        errors.add("Duplicate broker record: " + tupleRepr);
      }
      seen.add(internalKey);
    }
    return errors;
  }

  /**
   * Formats a Python-style tuple repr for a broker coordinate triple.
   *
   * <p>Matches Python's {@code str((topic, partition, offset))} format (§6.7 line 9): single-quoted
   * strings, {@code None} for null/missing values.
   */
  private static String buildTupleRepr(
      JsonNode topicNode, JsonNode partitionNode, JsonNode offsetNode, boolean offsetPresent) {
    // Topic: None if missing/null, else 'value'
    String topic;
    if (topicNode.isMissingNode() || topicNode.isNull()) {
      topic = "None";
    } else {
      topic = "'" + topicNode.asText() + "'";
    }

    // Partition: None if missing/null, else integer
    String partition;
    if (partitionNode.isMissingNode() || partitionNode.isNull()) {
      partition = "None";
    } else {
      partition = String.valueOf(partitionNode.asInt(0));
    }

    // Offset: None if not present, else integer
    String offset;
    if (!offsetPresent) {
      offset = "None";
    } else {
      offset = String.valueOf(offsetNode.asLong(0L));
    }

    return "(" + topic + ", " + partition + ", " + offset + ")";
  }
}
