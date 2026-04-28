package com.cryptolake.collector.gap;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coalesces disconnect gaps so each {@code (symbol, stream)} pair emits exactly ONE {@code
 * ws_disconnect} gap per reconnect cycle.
 *
 * <p>Ports the {@code _disconnect_gap_emitted} set behavior from {@code
 * src/collector/connection.py:283-322}. The flag is set on the first disconnect gap for a pair and
 * cleared as soon as live data arrives ({@link #onData}).
 *
 * <p>Thread safety: backed by a {@link ConcurrentHashMap} key-set — lock-free reads and writes
 * (Tier 2 §9). Called from the WebSocket listener thread and the capture path.
 */
public final class DisconnectGapCoalescer {

  /** Key tuple stored in the set: {@code "symbol\0stream"}. */
  private final Set<String> emitted = ConcurrentHashMap.newKeySet();

  /**
   * Marks a {@code (symbol, stream)} pair as having had a disconnect gap emitted.
   *
   * @return {@code true} if this is the first mark (caller should emit the gap); {@code false} if
   *     already marked (caller should suppress)
   */
  public boolean tryMark(String symbol, String stream) {
    return emitted.add(key(symbol, stream));
  }

  /**
   * Clears the emitted flag when live data arrives for a {@code (symbol, stream)} pair, so the next
   * disconnect cycle will emit a fresh gap.
   */
  public void onData(String symbol, String stream) {
    emitted.remove(key(symbol, stream));
  }

  /** Clears all flags — use on full session reset. */
  public void clearAll() {
    emitted.clear();
  }

  private static String key(String symbol, String stream) {
    return symbol + '\0' + stream;
  }
}
