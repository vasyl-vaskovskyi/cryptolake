package com.cryptolake.collector.producer;

/**
 * Callback invoked when the producer records an overflow for a {@code (exchange, symbol, stream)}
 * tuple.
 *
 * <p>Implemented by {@code BackpressureGate::onDrop} which increments the consecutive-drops
 * counter (design §2.3).
 *
 * <p>Implementations must be thread-safe — called from the kafka-clients IO thread.
 */
@FunctionalInterface
public interface OverflowListener {
  void onOverflow(String exchange, String symbol, String stream);
}
