package com.cryptopanner.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Skeleton-only YAML config. Replaced by the full §15 schema once we add hot-swap, rotation,
 * Monitor, Agent, etc.
 */
public record SkeletonConfig(
    String nodeId,
    List<Subscription> subscriptions,
    List<String> broadcasts,
    String wsPublicEndpointUrl,
    String wsMarketEndpointUrl,
    Paths paths,
    int collectorMaxRuntimeS,
    Storage storage) {

  public SkeletonConfig {
    if (broadcasts == null) broadcasts = List.of();
  }

  public record Subscription(String symbol, String stream) {}

  public record Paths(Path segments, Path sealed) {}

  public record Storage(
      String endpoint,
      String bucket,
      String accessKey,
      String secretKey,
      String region,
      boolean pathStyleAccess) {}

  /** Distinct configured symbols (insertion-ordered). Used to fan out all-symbol broadcasts. */
  public Set<String> symbols() {
    Set<String> out = new LinkedHashSet<>();
    for (Subscription s : subscriptions) out.add(s.symbol());
    return out;
  }

  /**
   * Returns every {@code (symbol, stream)} triple that should appear on disk and in S3 — explicit
   * subscriptions plus broadcasts fanned out across configured symbols (e.g. {@code
   * !forceOrder@arr} yields one {@code (symbol, "forceOrder")} per symbol). Sealer, Uploader, and
   * Verify iterate this rather than {@link #subscriptions()} so broadcast-fanout files aren't
   * silently skipped.
   */
  public List<Subscription> effectiveSubscriptions() {
    List<Subscription> out = new ArrayList<>(subscriptions);
    for (String b : broadcasts) {
      String streamName = broadcastStreamName(b);
      for (String symbol : symbols()) {
        out.add(new Subscription(symbol, streamName));
      }
    }
    return out;
  }

  /** Maps a wire-protocol broadcast (e.g. {@code !forceOrder@arr}) to the on-disk stream name. */
  private static String broadcastStreamName(String broadcast) {
    if ("!forceOrder@arr".equals(broadcast)) return "forceOrder";
    throw new IllegalArgumentException("Unknown broadcast: " + broadcast);
  }

  public static SkeletonConfig load(Path yaml) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return mapper.readValue(yaml.toFile(), SkeletonConfig.class);
  }
}
