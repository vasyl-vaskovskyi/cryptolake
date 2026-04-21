package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

/**
 * Redpanda (Kafka-compatible) broker configuration.
 *
 * <p>Ports Python's {@code RedpandaConfig}. {@code @Min(12)} replaces Python's {@code
 * field_validator("retention_hours")} (Tier 5 J3). Compact constructor applies defaults and
 * immutability.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record RedpandaConfig(
    @JsonProperty("brokers") @NotNull @Size(min = 1) List<String> brokers,
    @JsonProperty("retention_hours") @Min(12) int retentionHours,
    @JsonProperty("producer") @Valid ProducerConfig producer) {

  public RedpandaConfig {
    if (retentionHours == 0) retentionHours = 48;
    if (producer == null) producer = new ProducerConfig(100_000, null, 10_000);
    if (brokers != null) brokers = List.copyOf(brokers);
  }
}
