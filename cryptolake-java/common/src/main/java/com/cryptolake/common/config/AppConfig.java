package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * Root configuration record (ports Python's {@code CryptoLakeConfig}).
 *
 * <p>Compact constructor supplies defaults for optional sub-configs. Immutable record (Tier 2 §12).
 */
public record AppConfig(
    @JsonProperty("database") @NotNull @Valid DatabaseConfig database,
    @JsonProperty("exchanges") @NotNull @Valid ExchangesConfig exchanges,
    @JsonProperty("redpanda") @NotNull @Valid RedpandaConfig redpanda,
    @JsonProperty("writer") @Valid WriterConfig writer,
    @JsonProperty("monitoring") @Valid MonitoringConfig monitoring,
    @JsonProperty("collector") @Valid CollectorConfig collector) {

  public AppConfig {
    if (writer == null) writer = new WriterConfig();
    if (monitoring == null) monitoring = new MonitoringConfig();
    if (collector == null) collector = new CollectorConfig();
  }
}
