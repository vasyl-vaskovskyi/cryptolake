package com.cryptolake.common.envelope;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public enum GapReason {

  // ---- Persistent class — must reconcile between file and state ----
  COLLECTOR_RESTART("collector_restart", Classification.PERSISTENT),
  RESTART_GAP("restart_gap", Classification.PERSISTENT),
  KAFKA_PRODUCER_OUTAGE("kafka_producer_outage", Classification.PERSISTENT),
  MISSING_HOUR("missing_hour", Classification.PERSISTENT),

  // ---- Runtime-only — file-side only, no state counterpart by design ----
  WS_DISCONNECT("ws_disconnect", Classification.RUNTIME_ONLY),
  PU_CHAIN_BREAK("pu_chain_break", Classification.RUNTIME_ONLY),
  SESSION_SEQ_SKIP("session_seq_skip", Classification.RUNTIME_ONLY),
  RECOVERY_DEPTH_ANCHOR("recovery_depth_anchor", Classification.RUNTIME_ONLY),
  BUFFER_OVERFLOW("buffer_overflow", Classification.RUNTIME_ONLY),
  SNAPSHOT_POLL_MISS("snapshot_poll_miss", Classification.RUNTIME_ONLY),
  WRITE_ERROR("write_error", Classification.RUNTIME_ONLY),
  DESERIALIZATION_ERROR("deserialization_error", Classification.RUNTIME_ONLY),
  HANDLER_ERROR("handler_error", Classification.RUNTIME_ONLY),
  CHECKPOINT_LOST("checkpoint_lost", Classification.RUNTIME_ONLY),
  KAFKA_OFFSET_RESET("kafka_offset_reset", Classification.RUNTIME_ONLY),
  KAFKA_DELIVERY_FAILED("kafka_delivery_failed", Classification.RUNTIME_ONLY),
  KAFKA_CONSUMER_OUTAGE("kafka_consumer_outage", Classification.RUNTIME_ONLY),
  PG_OUTAGE_HOLD("pg_outage_hold", Classification.RUNTIME_ONLY),
  DISK_FULL_HOLD("disk_full_hold", Classification.RUNTIME_ONLY),
  CROSS_SOURCE_PU_CHAIN_BREAK("cross_source_pu_chain_break", Classification.RUNTIME_ONLY),
  BOTH_COLLECTORS_SILENT("both_collectors_silent", Classification.RUNTIME_ONLY);

  private static final Map<String, GapReason> BY_WIRE;
  private static final Map<GapReason, Set<GapReason>> CAUSED_BY;

  static {
    Map<String, GapReason> byWire = new HashMap<>();
    for (GapReason r : values()) {
      byWire.put(r.wire, r);
    }
    BY_WIRE = Map.copyOf(byWire);

    EnumMap<GapReason, Set<GapReason>> causedBy = new EnumMap<>(GapReason.class);
    causedBy.put(
        COLLECTOR_RESTART,
        Collections.unmodifiableSet(
            EnumSet.of(
                COLLECTOR_RESTART,
                RESTART_GAP,
                WS_DISCONNECT,
                PU_CHAIN_BREAK,
                SESSION_SEQ_SKIP,
                RECOVERY_DEPTH_ANCHOR)));
    causedBy.put(
        RESTART_GAP,
        Collections.unmodifiableSet(
            EnumSet.of(
                RESTART_GAP,
                WS_DISCONNECT,
                PU_CHAIN_BREAK,
                SESSION_SEQ_SKIP,
                RECOVERY_DEPTH_ANCHOR)));
    causedBy.put(
        KAFKA_PRODUCER_OUTAGE,
        Collections.unmodifiableSet(
            EnumSet.of(
                KAFKA_PRODUCER_OUTAGE, KAFKA_DELIVERY_FAILED, KAFKA_OFFSET_RESET, WRITE_ERROR)));
    causedBy.put(MISSING_HOUR, Collections.unmodifiableSet(EnumSet.of(MISSING_HOUR)));
    CAUSED_BY = Collections.unmodifiableMap(causedBy);
  }

  public enum Classification {
    PERSISTENT,
    RUNTIME_ONLY
  }

  private final String wire;
  private final Classification classification;

  GapReason(String wire, Classification classification) {
    this.wire = wire;
    this.classification = classification;
  }

  @JsonValue
  public String wire() {
    return wire;
  }

  public Classification classification() {
    return classification;
  }

  public boolean isPersistent() {
    return classification == Classification.PERSISTENT;
  }

  public boolean isRuntimeOnly() {
    return classification == Classification.RUNTIME_ONLY;
  }

  /**
   * True iff this (persistent) reason can cause the given file-side reason. Runtime-only reasons
   * cause nothing (return false).
   */
  public boolean explains(GapReason fileReason) {
    java.util.Objects.requireNonNull(fileReason, "fileReason");
    Set<GapReason> set = CAUSED_BY.get(this);
    return set != null && set.contains(fileReason);
  }

  @JsonCreator
  public static GapReason fromWire(String wire) {
    GapReason r = BY_WIRE.get(wire);
    if (r == null) {
      throw new IllegalArgumentException("Unknown gap reason: '" + wire + "'");
    }
    return r;
  }
}
