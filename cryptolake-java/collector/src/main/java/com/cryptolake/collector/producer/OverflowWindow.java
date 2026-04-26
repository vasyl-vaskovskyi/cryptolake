package com.cryptolake.collector.producer;

/**
 * Tracks an overflow window for a {@code (symbol, stream)} pair.
 *
 * <p>Immutable record — mutations are done by replacing the whole record in the
 * {@link java.util.concurrent.ConcurrentHashMap} via {@code compute(...)}.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record OverflowWindow(long startTsNs, int dropped) {

  /** Creates a new overflow window starting now with one initial drop. */
  public static OverflowWindow startNew(long nowNs) {
    return new OverflowWindow(nowNs, 1);
  }

  /** Returns a new record with the drop count incremented by one. */
  public OverflowWindow withIncrementedDrop() {
    return new OverflowWindow(startTsNs, dropped + 1);
  }
}
