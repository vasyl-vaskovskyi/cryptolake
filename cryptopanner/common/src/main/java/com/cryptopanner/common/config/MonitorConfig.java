package com.cryptopanner.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Full §15.c monitor configuration (the cross-node Monitor reads {@code monitor.yaml}). Mirrors the
 * spec's nested groups: the scraped {@code nodes}, the dashboard, the restart policy + circuit
 * breaker (§13.b), alert thresholds + dedup/correlation (§13.a, §13.e), the alerting channels, and
 * the dead-man's switch (§13.c). Duration scalars are exposed parsed via {@link ConfigParse}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MonitorConfig(
    List<Node> nodes,
    int scrapeIntervalS,
    Dashboard dashboard,
    Restart restart,
    Alert alert,
    Alerting alerting,
    DeadMan deadMan) {

  public MonitorConfig {
    if (nodes == null) nodes = List.of();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Node(String id, String endpoint, Path tokenFile) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Dashboard(String listenAddress, int refreshIntervalS) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Restart(List<String> backoff, CircuitBreaker circuitBreaker) {
    public Restart {
      if (backoff == null) backoff = List.of();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record CircuitBreaker(int failureCount, String window) {
    public Duration windowDuration() {
      return ConfigParse.duration(window);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Alert(
      String dedupTtl, Correlation correlation, Warning warning, Critical critical) {
    public Duration dedupTtlDuration() {
      return ConfigParse.duration(dedupTtl);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Correlation(int thresholdNodes, String window) {
    public Duration windowDuration() {
      return ConfigParse.duration(window);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Warning(
      String degradedPersisting,
      String uploadBacklogAge,
      String deployStuck,
      int restFailedPollRatePct,
      String restFailedPollWindow,
      int diskDataPct,
      int clockSkewS) {
    public Duration degradedPersistingDuration() {
      return ConfigParse.duration(degradedPersisting);
    }

    public Duration uploadBacklogAgeDuration() {
      return ConfigParse.duration(uploadBacklogAge);
    }

    public Duration deployStuckDuration() {
      return ConfigParse.duration(deployStuck);
    }

    public Duration restFailedPollWindowDuration() {
      return ConfigParse.duration(restFailedPollWindow);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Critical(
      String extendedWsDisconnect,
      int restRateLimitPersistenceHours,
      int diskDataPct,
      int clockSkewS) {
    public Duration extendedWsDisconnectDuration() {
      return ConfigParse.duration(extendedWsDisconnect);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Alerting(Telegram telegram, Whatsapp whatsapp, String healthchecksUrl) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Telegram(String webhookUrl) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Whatsapp(String webhookUrl) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record DeadMan(String healthchecksPushInterval, String selfTestTimeUtc) {}

  public static MonitorConfig load(Path yaml) throws IOException {
    return load(yaml, System.getenv());
  }

  static MonitorConfig load(Path yaml, Map<String, String> env) throws IOException {
    return ConfigLoader.load(yaml, MonitorConfig.class, env);
  }
}
