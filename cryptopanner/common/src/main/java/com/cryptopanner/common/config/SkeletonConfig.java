package com.cryptopanner.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Skeleton-only YAML config. Replaced by the full §15 schema once we add hot-swap, rotation,
 * Monitor, Agent, etc.
 */
public record SkeletonConfig(
    String nodeId,
    List<Subscription> subscriptions,
    String wsEndpointUrl,
    Paths paths,
    int collectorMaxRuntimeS,
    Storage storage) {

  public record Subscription(String symbol, String stream) {}

  public record Paths(Path segments, Path sealed) {}

  public record Storage(
      String endpoint,
      String bucket,
      String accessKey,
      String secretKey,
      String region,
      boolean pathStyleAccess) {}

  public static SkeletonConfig load(Path yaml) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return mapper.readValue(yaml.toFile(), SkeletonConfig.class);
  }
}
