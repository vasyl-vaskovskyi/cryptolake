package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Open interest polling configuration.
 *
 * <p>Ports Python's {@code OpenInterestConfig}. Immutable record (Tier 2 §12).
 */
public record OpenInterestConfig(@JsonProperty("poll_interval") String pollInterval) {

  public OpenInterestConfig {
    if (pollInterval == null) pollInterval = "5m";
  }

  /** Default constructor: poll every 5 minutes. */
  public OpenInterestConfig() {
    this("5m");
  }
}
