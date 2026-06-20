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
    String restBaseUrl,
    String restApiKey,
    List<RestPoll> restPolls,
    String wsPublicEndpointUrl,
    String wsMarketEndpointUrl,
    Paths paths,
    int collectorMaxRuntimeS,
    int sealGraceSeconds,
    Storage storage) {

  public SkeletonConfig {
    if (broadcasts == null) broadcasts = List.of();
    if (restPolls == null) restPolls = List.of();
    if (sealGraceSeconds <= 0) sealGraceSeconds = 10; // master spec §8.e default
  }

  public record Subscription(String symbol, String stream) {}

  /**
   * One REST endpoint to poll. {@code perSymbol=true} fans out across configured symbols; {@code
   * false} polls a single global instance (e.g. {@code /fapi/v1/exchangeInfo}).
   */
  public record RestPoll(String stream, String endpoint, int cadenceSeconds, boolean perSymbol) {}

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
   * subscriptions, broadcasts fanned across configured symbols (e.g. {@code !forceOrder@arr} → one
   * {@code (symbol, "forceOrder")} per symbol), and per-symbol REST polls (e.g. {@code
   * openInterest} → one per symbol). Sealer, Uploader, and Verify iterate this rather than {@link
   * #subscriptions()} so broadcast- and REST-fanout files aren't silently skipped.
   */
  public List<Subscription> effectiveSubscriptions() {
    List<Subscription> out = new ArrayList<>(subscriptions);
    for (String b : broadcasts) {
      String streamName = broadcastStreamName(b);
      for (String symbol : symbols()) {
        out.add(new Subscription(symbol, streamName));
      }
    }
    for (RestPoll p : restPolls) {
      if (p.perSymbol()) {
        for (String symbol : symbols()) {
          out.add(new Subscription(symbol, p.stream()));
        }
      }
      // Non-per-symbol REST polls (exchangeInfo) need a global path scheme — TODO.
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
