package com.cryptopanner.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Full §15.b node configuration. This is the on-disk source of truth (nested {@code collector:},
 * {@code sealer:}, {@code deploy:}, {@code agent:} groups exactly as the master spec lays them
 * out); the {@code dev:} block is an explicitly non-spec overlay carrying knobs that only the
 * standalone dev runner needs ({@code health_port}, {@code collector_max_runtime_s}) — in
 * production the Node Agent owns the HTTP surface (§11.c) and systemd owns process lifetime.
 *
 * <p>For ergonomics the loaded config also exposes the <em>derived</em> shapes the collector,
 * sealer, uploader, and verify already consume — {@link #subscriptions()} (symbols × per-symbol
 * streams), {@link #broadcasts()}, {@link #restPolls()} (built from the named {@code
 * collector.rest} endpoints), and {@link #effectiveSubscriptions()} — so wiring reads the same API
 * regardless of the nested YAML shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NodeConfig(
    String nodeId,
    List<String> symbols,
    Streams streams,
    Paths paths,
    Logging logging,
    Storage storage,
    Collector collector,
    Sealer sealer,
    Uploader uploader,
    Deploy deploy,
    Agent agent,
    Dev dev) {

  /**
   * Reserved pseudo-symbol for non-per-symbol REST polls (e.g. {@code exchangeInfo}), which have no
   * natural symbol. Underscore-prefixed so it never collides with a real lowercase Binance symbol.
   */
  public static final String GLOBAL_SYMBOL = "_global";

  public NodeConfig {
    if (symbols == null) symbols = List.of();
  }

  // ── §15.b nested groups ──────────────────────────────────────────────────────

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Streams(List<String> perSymbol, List<String> allSymbol) {
    public Streams {
      if (perSymbol == null) perSymbol = List.of();
      if (allSymbol == null) allSymbol = List.of();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Paths(
      Path segments, Path sealed, Path staging, Path deploy, Path logs, Path fsHeavyLock) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Logging(String level, Rotation rotation) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Rotation(
      String maxFileSize,
      String rotationPeriod,
      int compressedHistoryWeeks,
      int deleteAfterDays,
      Path criticalEventsSidecar) {}

  /**
   * §15.b {@code storage:}. The spec lists {@code endpoint}/{@code bucket}/{@code
   * credentials_file}; the inline {@code access_key}/{@code secret_key}/{@code region}/{@code
   * path_style_access} fields are retained for the dev/MinIO path and for the S3 client knobs §15
   * leaves implicit.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Storage(
      String endpoint,
      String bucket,
      Path credentialsFile,
      String accessKey,
      String secretKey,
      String region,
      boolean pathStyleAccess) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Collector(
      String wsPublicEndpointUrl,
      String wsMarketEndpointUrl,
      String restBaseUrl,
      String restApiKey,
      int restConnectTimeoutS,
      int restRequestTimeoutS,
      int unplannedReconnectBackoffMaxS,
      int unplannedReconnectJitterPct,
      int subscribeAckTimeoutS,
      String sealGraceWindow,
      String connectionMaxAge,
      String rotationWindow,
      Rest rest) {

    public Duration sealGraceWindowDuration() {
      return ConfigParse.duration(sealGraceWindow);
    }

    public Duration connectionMaxAgeDuration() {
      return ConfigParse.duration(connectionMaxAge);
    }

    public ConfigParse.HourWindow rotationWindowParsed() {
      return ConfigParse.hourWindow(rotationWindow);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Rest(Depth depth, OpenInterest openInterest, ExchangeInfo exchangeInfo) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Depth(String urlTemplate, String baselinePollInterval, boolean onDemandResync) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record OpenInterest(String urlTemplate, String pollInterval) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ExchangeInfo(String urlTemplate, String pollTimeUtc) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Sealer(String hourGraceWindow, Backfill backfill) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Backfill(int attemptsPerGap, List<String> backoff, boolean crossMergeRetry) {
    public Backfill {
      if (backoff == null) backoff = List.of();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Uploader(int retryBackoffMaxS) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Deploy(
      String forbiddenWindow,
      String recommendedWindow,
      int supersededRetentionDays,
      int versionsKept) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Agent(
      String listenAddress,
      Path tokenFile,
      boolean testMode,
      boolean metricsEnabled,
      List<Path> diskMounts,
      Heartbeat heartbeat) {
    public Agent {
      if (diskMounts == null) diskMounts = List.of();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Heartbeat(int degradedThresholdS, int stuckThresholdS) {}

  /** Non-§15 dev overlay: knobs the standalone collector runner needs, absent in production. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Dev(int healthPort, int collectorMaxRuntimeS) {}

  // ── derived shapes consumed by the apps ──────────────────────────────────────

  public record Subscription(String symbol, String stream) {}

  /** One REST endpoint to poll, derived from a named {@code collector.rest} entry. */
  public record RestPoll(
      String stream,
      String endpoint,
      int cadenceSeconds,
      boolean perSymbol,
      Map<String, String> params) {
    public RestPoll {
      if (params == null) params = Map.of();
    }
  }

  /** Explicit per-symbol WS subscriptions: symbols × {@code streams.per_symbol}. */
  public List<Subscription> subscriptions() {
    List<Subscription> out = new ArrayList<>();
    if (streams == null) return out;
    for (String symbol : symbols) {
      for (String stream : streams.perSymbol()) {
        out.add(new Subscription(symbol, stream));
      }
    }
    return out;
  }

  /** All-symbol broadcast stream names (e.g. {@code !forceOrder@arr}). */
  public List<String> broadcasts() {
    return streams == null ? List.of() : streams.allSymbol();
  }

  /** REST pollers built from the named {@code collector.rest} endpoints. */
  public List<RestPoll> restPolls() {
    List<RestPoll> out = new ArrayList<>();
    if (collector == null || collector.rest() == null) return out;
    Rest r = collector.rest();
    if (r.depth() != null) {
      out.add(
          restPoll(
              "depthSnapshot",
              r.depth().urlTemplate(),
              (int) ConfigParse.duration(r.depth().baselinePollInterval()).toSeconds()));
    }
    if (r.openInterest() != null) {
      out.add(
          restPoll(
              "openInterest",
              r.openInterest().urlTemplate(),
              (int) ConfigParse.duration(r.openInterest().pollInterval()).toSeconds()));
    }
    if (r.exchangeInfo() != null) {
      // exchangeInfo is a daily poll (poll_time_utc); model its cadence as one day.
      out.add(restPoll("exchangeInfo", r.exchangeInfo().urlTemplate(), 86_400));
    }
    return out;
  }

  /**
   * Parses a §15 {@code url_template} into a {@link RestPoll}: the path is the endpoint; a {@code
   * symbol={symbol}} query placeholder marks the poll per-symbol (and is dropped, not stored);
   * every other query pair becomes a static param (e.g. {@code limit=1000}).
   */
  private static RestPoll restPoll(String stream, String urlTemplate, int cadenceSeconds) {
    String path = urlTemplate;
    boolean perSymbol = false;
    Map<String, String> params = new LinkedHashMap<>();
    int q = urlTemplate.indexOf('?');
    if (q >= 0) {
      path = urlTemplate.substring(0, q);
      for (String pair : urlTemplate.substring(q + 1).split("&")) {
        if (pair.isEmpty()) continue;
        int eq = pair.indexOf('=');
        String k = eq >= 0 ? pair.substring(0, eq) : pair;
        String v = eq >= 0 ? pair.substring(eq + 1) : "";
        if ("{symbol}".equals(v)) {
          perSymbol = true; // placeholder → per-symbol fan-out, supplied at request time
        } else {
          params.put(k, v);
        }
      }
    }
    return new RestPoll(stream, path, cadenceSeconds, perSymbol, params);
  }

  /**
   * Every {@code (symbol, stream)} that should appear on disk and in S3 — explicit subscriptions,
   * broadcasts fanned across symbols, per-symbol REST polls fanned across symbols, and
   * non-per-symbol polls under {@link #GLOBAL_SYMBOL}.
   */
  public List<Subscription> effectiveSubscriptions() {
    List<Subscription> out = new ArrayList<>(subscriptions());
    for (String b : broadcasts()) {
      String streamName = broadcastStreamName(b);
      for (String symbol : symbols) {
        out.add(new Subscription(symbol, streamName));
      }
    }
    for (RestPoll p : restPolls()) {
      if (p.perSymbol()) {
        for (String symbol : symbols) {
          out.add(new Subscription(symbol, p.stream()));
        }
      } else {
        out.add(new Subscription(GLOBAL_SYMBOL, p.stream()));
      }
    }
    return out;
  }

  private static String broadcastStreamName(String broadcast) {
    if ("!forceOrder@arr".equals(broadcast)) return "forceOrder";
    throw new IllegalArgumentException("Unknown broadcast: " + broadcast);
  }

  // ── compat scalar accessors used by the apps ─────────────────────────────────

  public String wsPublicEndpointUrl() {
    return collector == null ? null : collector.wsPublicEndpointUrl();
  }

  public String wsMarketEndpointUrl() {
    return collector == null ? null : collector.wsMarketEndpointUrl();
  }

  public String restBaseUrl() {
    return collector == null ? null : collector.restBaseUrl();
  }

  public String restApiKey() {
    return collector == null ? null : collector.restApiKey();
  }

  public int sealGraceSeconds() {
    if (collector == null || collector.sealGraceWindow() == null) return 10; // §8.e default
    return (int) collector.sealGraceWindowDuration().toSeconds();
  }

  public int healthPort() {
    return dev == null ? 0 : dev.healthPort();
  }

  public int collectorMaxRuntimeS() {
    return dev == null ? 0 : dev.collectorMaxRuntimeS();
  }

  /**
   * Fail-fast structural validation (§15.a): required keys present, scalar formats parse, and no
   * contradictions. Each failure throws {@link IllegalArgumentException} naming the offending key.
   * Called by {@link #load}, so a malformed config never reaches the running components.
   */
  public void validate() {
    if (nodeId == null || nodeId.isBlank()) {
      throw new IllegalArgumentException("config: node_id is required");
    }
    if (symbols.isEmpty()) {
      throw new IllegalArgumentException("config: symbols must not be empty");
    }
    if (paths == null || paths.segments() == null) {
      throw new IllegalArgumentException("config: paths.segments is required");
    }
    if (paths.sealed() == null) {
      throw new IllegalArgumentException("config: paths.sealed is required");
    }
    if (storage == null || storage.endpoint() == null) {
      throw new IllegalArgumentException("config: storage.endpoint is required");
    }
    if (storage.bucket() == null) {
      throw new IllegalArgumentException("config: storage.bucket is required");
    }
    // Scalar formats parse (when present), re-throwing tagged with the owning key. These collector
    // timing keys are optional — only validate the ones the config actually sets.
    if (collector != null) {
      if (collector.sealGraceWindow() != null) {
        parseKeyed(
            "collector.seal_grace_window", collector.sealGraceWindow(), ConfigParse::duration);
      }
      if (collector.connectionMaxAge() != null) {
        parseKeyed(
            "collector.connection_max_age", collector.connectionMaxAge(), ConfigParse::duration);
      }
      if (collector.rotationWindow() != null) {
        parseKeyed(
            "collector.rotation_window", collector.rotationWindow(), ConfigParse::hourWindow);
      }
    }
    if (sealer != null && sealer.hourGraceWindow() != null) {
      parseKeyed("sealer.hour_grace_window", sealer.hourGraceWindow(), ConfigParse::duration);
    }
    // Contradiction (§15.a): a recommended deploy window must not overlap the forbidden window.
    if (deploy != null && deploy.forbiddenWindow() != null && deploy.recommendedWindow() != null) {
      ConfigParse.HourWindow forbidden =
          parseKeyed("deploy.forbidden_window", deploy.forbiddenWindow(), ConfigParse::hourWindow);
      ConfigParse.HourWindow recommended =
          parseKeyed(
              "deploy.recommended_window", deploy.recommendedWindow(), ConfigParse::hourWindow);
      if (recommended.overlaps(forbidden)) {
        throw new IllegalArgumentException(
            "config: deploy.recommended_window overlaps deploy.forbidden_window — a deploy cannot be"
                + " recommended during the forbidden window");
      }
    }
  }

  /**
   * Applies {@code parser} to {@code value}, re-throwing parse failures tagged with {@code key}.
   */
  private static <T> T parseKeyed(
      String key, String value, java.util.function.Function<String, T> parser) {
    try {
      return parser.apply(value);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("config: " + key + ": " + e.getMessage(), e);
    }
  }

  public static NodeConfig load(Path yaml) throws IOException {
    return load(yaml, System.getenv());
  }

  /**
   * Loads the config, then applies {@code CRYPTOPANNER_<KEY_PATH>} environment overrides (§15.d) —
   * any scalar key in the YAML can be overridden by an env var named with the uppercased dotted
   * path and dots/separators as underscores (e.g. {@code storage.secret_key} ← {@code
   * CRYPTOPANNER_STORAGE_SECRET_KEY}). Keeps secrets out of the config file. The {@code env} map is
   * injected for testability; the public {@link #load(Path)} passes {@link System#getenv()}.
   */
  static NodeConfig load(Path yaml, Map<String, String> env) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    com.fasterxml.jackson.databind.JsonNode tree = mapper.readTree(yaml.toFile());
    if (tree instanceof com.fasterxml.jackson.databind.node.ObjectNode root) {
      applyEnvOverrides(root, new ArrayList<>(), env);
    }
    NodeConfig cfg = mapper.treeToValue(tree, NodeConfig.class);
    cfg.validate();
    return cfg;
  }

  /**
   * Walks the YAML tree and replaces any scalar leaf whose derived {@code CRYPTOPANNER_<PATH>} name
   * is present in {@code env}. Object nodes recurse; array/container nodes are not overridable.
   */
  private static void applyEnvOverrides(
      com.fasterxml.jackson.databind.node.ObjectNode node,
      List<String> path,
      Map<String, String> env) {
    for (String field : new ArrayList<>(iterable(node.fieldNames()))) {
      com.fasterxml.jackson.databind.JsonNode child = node.get(field);
      List<String> childPath = new ArrayList<>(path);
      childPath.add(field);
      if (child instanceof com.fasterxml.jackson.databind.node.ObjectNode obj) {
        applyEnvOverrides(obj, childPath, env);
      } else if (child != null && child.isValueNode()) {
        String envName =
            "CRYPTOPANNER_" + String.join("_", childPath).toUpperCase(java.util.Locale.ROOT);
        String override = env.get(envName);
        if (override != null) {
          node.put(field, override);
        }
      }
    }
  }

  private static List<String> iterable(java.util.Iterator<String> it) {
    List<String> out = new ArrayList<>();
    it.forEachRemaining(out::add);
    return out;
  }
}
