package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Kafka producer configuration.
 *
 * <p>Ports Python's {@code ProducerConfig}. Compact constructor applies defaults and enforces
 * immutability of {@code bufferCaps} (Tier 5 J2; Tier 2 §12).
 */
public record ProducerConfig(
    @JsonProperty("max_buffer") int maxBuffer,
    @JsonProperty("buffer_caps") Map<String, Integer> bufferCaps,
    @JsonProperty("default_stream_cap") int defaultStreamCap) {

  public ProducerConfig {
    if (maxBuffer == 0) maxBuffer = 100_000;
    if (defaultStreamCap == 0) defaultStreamCap = 10_000;
    // Tier 5 J2: default factory when null
    bufferCaps =
        (bufferCaps == null) ? Map.of("depth", 80_000, "trades", 10_000) : Map.copyOf(bufferCaps);
  }

  /** Default constructor with all defaults applied. */
  public ProducerConfig() {
    this(100_000, null, 10_000);
  }
}
