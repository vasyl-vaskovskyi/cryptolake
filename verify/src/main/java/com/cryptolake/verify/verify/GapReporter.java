package com.cryptolake.verify.verify;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Filters gap envelopes from a list of parsed envelopes.
 *
 * <p>Ports {@code report_gaps(envelopes)} from {@code verify.py:72-74}.
 *
 * <p>Thread safety: stateless utility.
 */
public final class GapReporter {

  private GapReporter() {}

  /**
   * Returns all envelopes whose {@code type} field equals {@code "gap"}.
   *
   * @param envelopes all envelopes from one or more archive files
   * @return list of gap envelopes (preserving order)
   */
  public static List<JsonNode> collect(List<JsonNode> envelopes) {
    return envelopes.stream()
        .filter(e -> "gap".equals(e.path("type").asText()))
        .collect(Collectors.toList());
  }
}
