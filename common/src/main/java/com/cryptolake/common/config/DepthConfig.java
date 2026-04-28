package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Depth stream configuration.
 *
 * <p>Ports Python's {@code DepthConfig}. Immutable record (Tier 2 §12).
 */
public record DepthConfig(
    @JsonProperty("update_speed") String updateSpeed,
    @JsonProperty("snapshot_interval") String snapshotInterval,
    @JsonProperty("snapshot_overrides") Map<String, String> snapshotOverrides) {

  public DepthConfig {
    if (updateSpeed == null) updateSpeed = "100ms";
    if (snapshotInterval == null) snapshotInterval = "5m";
    snapshotOverrides = (snapshotOverrides == null) ? Map.of() : Map.copyOf(snapshotOverrides);
  }

  /** Default constructor with all defaults applied. */
  public DepthConfig() {
    this(null, null, null);
  }
}
