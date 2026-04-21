package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stream-enablement flags per exchange.
 *
 * <p>Ports Python's {@code StreamsConfig}. All streams default to {@code true} in the compact
 * constructor when the record is missing fields (Jackson passes {@code false} for missing boolean
 * primitives, so we use boxed {@code Boolean} and default to {@code true} in the compact ctor).
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record StreamsConfig(
    @JsonProperty("trades") boolean trades,
    @JsonProperty("depth") boolean depth,
    @JsonProperty("bookticker") boolean bookticker,
    @JsonProperty("funding_rate") boolean fundingRate,
    @JsonProperty("liquidations") boolean liquidations,
    @JsonProperty("open_interest") boolean openInterest) {

  /** Default: all streams enabled. */
  public StreamsConfig() {
    this(true, true, true, true, true, true);
  }
}
