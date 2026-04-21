package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Binance exchange configuration.
 *
 * <p>Compact constructor:
 *
 * <ol>
 *   <li>Lowercases {@code symbols} with {@link Locale#ROOT} (Tier 5 M1).
 *   <li>If {@code writerStreamsOverride} contains {@code "depth"} but not {@code "depth_snapshot"},
 *       appends {@code "depth_snapshot"} (design §2.2).
 *   <li>Applies defaults for null fields.
 *   <li>Makes lists immutable via {@link List#copyOf}.
 * </ol>
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record BinanceExchangeConfig(
    @JsonProperty("enabled") boolean enabled,
    @JsonProperty("market") String market,
    @JsonProperty("ws_base") String wsBase,
    @JsonProperty("rest_base") String restBase,
    @JsonProperty("symbols") @NotNull @Size(min = 1) List<String> symbols,
    @JsonProperty("streams") @Valid StreamsConfig streams,
    @JsonProperty("writer_streams_override") List<String> writerStreamsOverride,
    @JsonProperty("depth") @Valid DepthConfig depth,
    @JsonProperty("open_interest") @Valid OpenInterestConfig openInterest,
    @JsonProperty("collector_id") String collectorId) {

  public BinanceExchangeConfig {
    // Apply defaults for null optional fields
    if (market == null) market = "usdm_futures";
    if (wsBase == null) wsBase = "wss://fstream.binance.com";
    if (restBase == null) restBase = "https://fapi.binance.com";
    if (collectorId == null) collectorId = "binance-collector-01";
    if (streams == null) streams = new StreamsConfig();
    if (depth == null) depth = new DepthConfig();
    if (openInterest == null) openInterest = new OpenInterestConfig();

    // 1. Lowercase symbols with Locale.ROOT (Tier 5 M1)
    if (symbols != null) {
      List<String> lower = new ArrayList<>(symbols.size());
      for (String s : symbols) {
        lower.add(s.toLowerCase(Locale.ROOT));
      }
      symbols = List.copyOf(lower);
    }

    // 2. Auto-include depth_snapshot in writerStreamsOverride when depth is present
    if (writerStreamsOverride != null
        && writerStreamsOverride.contains("depth")
        && !writerStreamsOverride.contains("depth_snapshot")) {
      List<String> extended = new ArrayList<>(writerStreamsOverride);
      extended.add("depth_snapshot");
      writerStreamsOverride = List.copyOf(extended);
    } else if (writerStreamsOverride != null) {
      writerStreamsOverride = List.copyOf(writerStreamsOverride);
    }
    // null writerStreamsOverride stays null (no override)
  }

  /**
   * Returns the list of enabled streams, matching Python's {@code get_enabled_streams()}.
   *
   * <p>When {@code depth} is enabled, both {@code "depth"} and {@code "depth_snapshot"} are
   * included.
   */
  public List<String> getEnabledStreams() {
    List<String> enabled = new ArrayList<>();
    if (streams.trades()) enabled.add("trades");
    if (streams.depth()) {
      enabled.add("depth");
      enabled.add("depth_snapshot");
    }
    if (streams.bookticker()) enabled.add("bookticker");
    if (streams.fundingRate()) enabled.add("funding_rate");
    if (streams.liquidations()) enabled.add("liquidations");
    if (streams.openInterest()) enabled.add("open_interest");
    return List.copyOf(enabled);
  }
}
