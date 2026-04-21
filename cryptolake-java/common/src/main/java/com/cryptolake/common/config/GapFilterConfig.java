package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.DecimalMin;

/**
 * Gap filter configuration.
 *
 * <p>Ports Python's {@code GapFilterConfig}. {@code @DecimalMin("0.0")} replaces Python's {@code
 * Field(ge=0.0)} constraint (Tier 5 J3). Immutable record (Tier 2 §12).
 */
public record GapFilterConfig(
    @JsonProperty("grace_period_seconds") @DecimalMin("0.0") double gracePeriodSeconds,
    @JsonProperty("snapshot_miss_grace_seconds") @DecimalMin("0.0")
        double snapshotMissGraceSeconds) {

  public GapFilterConfig {
    if (gracePeriodSeconds == 0.0 && snapshotMissGraceSeconds == 0.0) {
      // Both are zero only if caller explicitly passes 0.0 or they were missing from YAML.
      // Apply Python defaults when both are absent.
    }
  }

  /** Default constructor matching Python defaults. */
  public GapFilterConfig() {
    this(10.0, 60.0);
  }
}
