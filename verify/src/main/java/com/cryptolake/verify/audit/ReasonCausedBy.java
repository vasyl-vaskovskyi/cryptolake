package com.cryptolake.verify.audit;

import java.util.Map;
import java.util.Set;

/**
 * Maps each STATE-side gap reason to the set of FILE-side gap reasons it can explain ("caused by").
 *
 * <p>A single state event (e.g. one PG {@code component_runtime_state} row for a planned restart)
 * is the root cause of N file-side gap envelopes — one per active stream. This mapping bridges that
 * 1-to-N relationship so {@link GapRecordReconciler} can match them.
 */
public final class ReasonCausedBy {

  /** State reason → set of file reasons it explains. */
  public static final Map<String, Set<String>> MAPPING =
      Map.of(
          "collector_restart",
          Set.of(
              "collector_restart",
              "restart_gap",
              "ws_disconnect",
              "pu_chain_break",
              "session_seq_skip",
              "recovery_depth_anchor"),
          "restart_gap",
          Set.of(
              "restart_gap",
              "ws_disconnect",
              "pu_chain_break",
              "session_seq_skip",
              "recovery_depth_anchor"),
          "kafka_producer_outage",
          Set.of(
              "kafka_producer_outage",
              "kafka_delivery_failed",
              "kafka_offset_reset",
              "write_error"),
          "missing_hour",
          Set.of("missing_hour"));

  /**
   * Returns {@code true} iff {@code stateReason} can explain {@code fileReason}.
   *
   * @param stateReason the reason carried by the state-side {@link GapRecord}
   * @param fileReason the reason carried by the file-side {@link GapRecord}
   */
  public static boolean explains(String stateReason, String fileReason) {
    Set<String> set = MAPPING.get(stateReason);
    return set != null && set.contains(fileReason);
  }

  private ReasonCausedBy() {}
}
