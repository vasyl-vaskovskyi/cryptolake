package com.cryptolake.common.validation;

import com.cryptolake.common.envelope.DataEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the Binance depth diff {@code pu}-chain across both the primary and backup sources.
 *
 * <p>Ruler #4 from spec §4: for each {@code depth} data envelope (regardless of source), parses
 * {@code U}, {@code u}, {@code pu} from {@code raw_text}. If {@code pu != last_u} AND {@code last_u
 * != null}, a cross-source pu-chain break is detected.
 *
 * <p>This is the new ruler that catches frames lost in the gap between primary's last frame and
 * backup's first frame on the merged depth stream.
 *
 * <p>Per spec §5.7 #38 / Task A3.6. Also integrated into the verify CLI (Task A4.1).
 *
 * <p>State: per-{@code (exchange, symbol)} {@code last_u} value. Thread safety: NOT thread-safe —
 * callers must ensure single-thread access (consume-loop T1 for writer; sequential iteration for
 * verify).
 */
public final class CrossSourcePuChainValidator {

  private static final Logger log = LoggerFactory.getLogger(CrossSourcePuChainValidator.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Callback interface for gap emission (avoids coupling to writer or verify internals). */
  @FunctionalInterface
  public interface GapCallback {
    /**
     * Called when a cross-source pu-chain break is detected.
     *
     * @param exchange exchange name
     * @param symbol symbol
     * @param detail human-readable break description
     * @param gapStartTs nanosecond timestamp of the previous envelope's {@code received_at}
     * @param gapEndTs nanosecond timestamp of the current envelope's {@code received_at}
     */
    void onBreak(String exchange, String symbol, String detail, long gapStartTs, long gapEndTs);
  }

  /** Per-{@code (exchange, symbol)} state: tracks the last seen {@code u} value. */
  private static final class SymbolState {
    long lastU = -1L; // -1 = not yet seen
    long lastReceivedAt = 0L;
  }

  private final Map<String, SymbolState> stateMap = new HashMap<>();
  private final GapCallback callback;

  /**
   * Constructs a {@code CrossSourcePuChainValidator}.
   *
   * @param callback invoked when a cross-source pu-chain break is detected; must not be null
   */
  public CrossSourcePuChainValidator(GapCallback callback) {
    this.callback = callback;
  }

  /**
   * Processes one {@code depth} data envelope from any source.
   *
   * <p>Parses {@code U}, {@code u}, {@code pu} from {@code raw_text}. If {@code pu != last_u} and
   * {@code last_u} has been established, fires the {@link GapCallback}.
   *
   * <p>No-op if {@code rawText} is null, if the JSON cannot be parsed, or if the required fields
   * are missing.
   *
   * @param env the depth data envelope; ignored if stream is not {@code "depth"}
   * @return {@code true} if the chain is intact (or state was just initialized); {@code false} if a
   *     break was detected
   */
  public boolean handle(DataEnvelope env) {
    if (env == null || !"depth".equals(env.stream())) {
      return true; // not a depth envelope — ignore
    }
    String rawText = env.rawText();
    if (rawText == null || rawText.isBlank()) {
      return true;
    }

    // Parse U, u, pu from raw_text
    DepthIds ids = parseDepthIds(rawText);
    if (ids == null) {
      return true; // couldn't parse — skip
    }

    String key = env.exchange() + "|" + env.symbol();
    SymbolState state = stateMap.computeIfAbsent(key, k -> new SymbolState());

    boolean intact = true;
    if (state.lastU >= 0 && ids.pu() != state.lastU) {
      // Break detected
      String detail =
          "merged stream pu="
              + ids.pu()
              + " expected="
              + state.lastU
              + " U="
              + ids.U()
              + " u="
              + ids.u()
              + " symbol="
              + env.symbol();
      log.info(
          "cross_source_pu_chain_break",
          "symbol",
          env.symbol(),
          "expected_pu",
          state.lastU,
          "actual_pu",
          ids.pu());
      callback.onBreak(
          env.exchange(), env.symbol(), detail, state.lastReceivedAt, env.receivedAt());
      intact = false;
    }

    // Update state
    state.lastU = ids.u();
    state.lastReceivedAt = env.receivedAt();
    return intact;
  }

  /**
   * Resets all per-symbol state. Useful after a depth snapshot resync (the pu-chain restarts from
   * the snapshot's {@code lastUpdateId}).
   */
  public void resetAll() {
    stateMap.clear();
  }

  /**
   * Resets state for a specific symbol. Called when that symbol's depth stream resyncs.
   *
   * @param exchange exchange name
   * @param symbol symbol to reset
   */
  public void reset(String exchange, String symbol) {
    stateMap.remove(exchange + "|" + symbol);
  }

  /** Returns an unmodifiable view of the current per-symbol last-u values (for testing). */
  public Map<String, Long> lastUPerSymbol() {
    Map<String, Long> result = new HashMap<>();
    for (Map.Entry<String, SymbolState> entry : stateMap.entrySet()) {
      result.put(entry.getKey(), entry.getValue().lastU);
    }
    return Collections.unmodifiableMap(result);
  }

  // ── private helpers ───────────────────────────────────────────────────────

  /** Parsed depth IDs from a Binance depth diff message. */
  private record DepthIds(long U, long u, long pu) {}

  /**
   * Parses {@code U}, {@code u}, {@code pu} from the Binance depth diff JSON payload.
   *
   * <p>Returns null if any field is missing or the JSON is invalid.
   */
  private static DepthIds parseDepthIds(String rawText) {
    try {
      JsonNode node = MAPPER.readTree(rawText);
      JsonNode uNode = node.path("u");
      JsonNode bigUNode = node.path("U");
      JsonNode puNode = node.path("pu");
      if (uNode.isMissingNode() || bigUNode.isMissingNode() || puNode.isMissingNode()) {
        return null;
      }
      return new DepthIds(bigUNode.asLong(), uNode.asLong(), puNode.asLong());
    } catch (Exception e) {
      return null;
    }
  }
}
