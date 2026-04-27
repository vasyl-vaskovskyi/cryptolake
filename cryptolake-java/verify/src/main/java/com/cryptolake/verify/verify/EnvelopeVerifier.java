package com.cryptolake.verify.verify;

import com.cryptolake.common.util.Sha256;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Validates required envelope fields and re-hashes {@code raw_sha256} for data envelopes.
 *
 * <p>Ports {@code verify_envelopes(envelopes)} from {@code verify.py:35-50}.
 *
 * <p>Tier 1 §2 honored: re-hash via {@link Sha256#hexDigestUtf8} and compare. Tier 3 §20 honored:
 * output strings are byte-identical to Python.
 *
 * <p>Thread safety: stateless utility.
 */
public final class EnvelopeVerifier {

  // Matches Python's DATA_ENVELOPE_FIELDS in src/common/envelope.py:28-32
  public static final Set<String> DATA_REQUIRED_FIELDS =
      Set.of(
          "v",
          "type",
          "exchange",
          "symbol",
          "stream",
          "received_at",
          "exchange_ts",
          "collector_session_id",
          "session_seq",
          "raw_text",
          "raw_sha256",
          "_topic",
          "_partition",
          "_offset");

  // Matches Python's GAP_ENVELOPE_FIELDS | BROKER_COORD_FIELDS
  public static final Set<String> GAP_REQUIRED_FIELDS =
      Set.of(
          "v",
          "type",
          "exchange",
          "symbol",
          "stream",
          "received_at",
          "collector_session_id",
          "session_seq",
          "gap_start_ts",
          "gap_end_ts",
          "reason",
          "detail",
          "_topic",
          "_partition",
          "_offset");

  private EnvelopeVerifier() {}

  /**
   * Validates each envelope in the list.
   *
   * @param envelopes parsed envelopes from a single archive file
   * @return list of error strings (empty = no errors)
   */
  public static List<String> verify(List<JsonNode> envelopes) {
    List<String> errors = new ArrayList<>();
    for (int i = 0; i < envelopes.size(); i++) {
      JsonNode env = envelopes.get(i);
      String type = env.path("type").asText("");
      Set<String> required = "gap".equals(type) ? GAP_REQUIRED_FIELDS : DATA_REQUIRED_FIELDS;

      // Collect present field names
      Set<String> presentFields = new java.util.HashSet<>();
      for (Iterator<String> it = env.fieldNames(); it.hasNext(); ) {
        presentFields.add(it.next());
      }

      // Find missing fields
      Set<String> missing = new TreeSet<>(); // sorted for Python sorted() match
      for (String f : required) {
        if (!presentFields.contains(f)) {
          missing.add(f);
        }
      }

      if (!missing.isEmpty()) {
        // Format matches Python's f"Line {i}: missing fields: {sorted(missing)}"
        // Python sorted(missing) returns a list with single-quoted strings in repr
        String missingRepr = pythonListRepr(missing);
        errors.add("Line " + i + ": missing fields: " + missingRepr);
        continue; // Python does `continue` after missing fields
      }

      // For data envelopes: re-hash raw_text and compare
      if ("data".equals(type)) {
        String rawText = env.path("raw_text").asText("");
        String actual = Sha256.hexDigestUtf8(rawText);
        String stored = env.path("raw_sha256").asText("");
        if (!actual.equals(stored)) {
          long offset = env.path("_offset").asLong(0L);
          errors.add("Line " + i + ": raw_sha256 mismatch at offset " + offset);
        }
      }
    }
    return errors;
  }

  /**
   * Formats a sorted set of field names as Python's repr of a sorted list.
   *
   * <p>Example: {@code "['_offset', '_partition', '_topic']"} — matches Python's {@code
   * str(sorted(missing))} output exactly (§6.7 line template 7).
   */
  static String pythonListRepr(Set<String> sorted) {
    return "[" + sorted.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")) + "]";
  }
}
