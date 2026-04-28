package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Collector service configuration.
 *
 * <p>Ports Python's {@code CollectorConfig}. {@code tapOutputDir} is a nullable path string; when
 * non-null, incoming envelopes are persisted verbatim for parity-fixture capture. Immutable record
 * (Tier 2 §12).
 */
public record CollectorConfig(@JsonProperty("tap_output_dir") String tapOutputDir) {

  /** Default constructor: tap disabled. */
  public CollectorConfig() {
    this(null);
  }
}
