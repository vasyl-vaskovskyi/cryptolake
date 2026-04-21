package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Monitoring/Prometheus configuration.
 *
 * <p>Ports Python's {@code MonitoringConfig}. Immutable record (Tier 2 §12).
 */
public record MonitoringConfig(
    @JsonProperty("prometheus_port") int prometheusPort,
    @JsonProperty("webhook_url") String webhookUrl) {

  public MonitoringConfig {
    if (webhookUrl == null) webhookUrl = "";
    if (prometheusPort == 0) prometheusPort = 8000;
  }

  /** Default constructor: port 8000, no webhook. */
  public MonitoringConfig() {
    this(8000, "");
  }
}
