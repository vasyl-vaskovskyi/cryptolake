package com.cryptolake.collector.liveness;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Liveness status for per-(symbol, stream) heartbeat envelopes.
 *
 * <p>Matches Python's {@code VALID_HEARTBEAT_STATUS} from {@code src/collector/heartbeat.py}.
 * Serializes to lowercase strings (Tier 5 B1).
 */
public enum HeartbeatStatus {
  ALIVE,
  SUBSCRIBED_SILENT,
  DISCONNECTED;

  @JsonValue
  public String toJsonValue() {
    return name().toLowerCase(java.util.Locale.ROOT);
  }
}
