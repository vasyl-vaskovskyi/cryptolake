package com.cryptolake.verify.audit;

import java.util.Set;

/**
 * Classifies gap reasons into two disjoint buckets.
 *
 * <p>RUNTIME_ONLY: ephemeral, not persisted to PG component_runtime. These are excluded from the
 * file↔state diff, so they never fire a divergence alert.
 *
 * <p>PERSISTENT_CLASS: written to PG by the writer on every occurrence. These are included in the
 * exact-tuple diff; a mismatch is a real signal.
 *
 * <p>Invariant: {@code RUNTIME_ONLY ∪ PERSISTENT_CLASS == GapReasons.VALID} and the two sets are
 * disjoint. RuntimeOnlyReasonsTest enforces this at build time.
 */
public final class RuntimeOnlyReasons {

  public static final Set<String> RUNTIME_ONLY =
      Set.of(
          "ws_disconnect",
          "pu_chain_break",
          "session_seq_skip",
          "buffer_overflow",
          "snapshot_poll_miss",
          "recovery_depth_anchor",
          "write_error",
          "deserialization_error",
          "handler_error",
          "checkpoint_lost",
          "kafka_offset_reset",
          "kafka_delivery_failed",
          "kafka_consumer_outage",
          "pg_outage_hold",
          "disk_full_hold",
          "cross_source_pu_chain_break",
          "both_collectors_silent");

  public static final Set<String> PERSISTENT_CLASS =
      Set.of("collector_restart", "restart_gap", "kafka_producer_outage", "missing_hour");

  private RuntimeOnlyReasons() {}
}
