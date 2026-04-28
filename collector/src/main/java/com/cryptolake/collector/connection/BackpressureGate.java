package com.cryptolake.collector.connection;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Backpressure gate: tracks consecutive producer overflow drops and signals when the capture
 * pipeline should pause.
 *
 * <p>Called from the kafka-clients IO thread (via delivery callback) and from the WebSocket
 * listener thread. Both paths are safe because {@link AtomicInteger} provides lock-free thread
 * safety (design §3.2).
 *
 * <p>Thread safety: lock-free atomic counter (Tier 2 §9).
 */
public final class BackpressureGate {

  /** Number of consecutive drops before we pause the capture pipeline. */
  private static final int DEFAULT_THRESHOLD = 10;

  private final int threshold;
  private final AtomicInteger consecutiveDrops = new AtomicInteger(0);

  public BackpressureGate() {
    this(DEFAULT_THRESHOLD);
  }

  public BackpressureGate(int threshold) {
    this.threshold = threshold;
  }

  /**
   * Returns {@code true} when consecutive drop count has reached the threshold. The capture
   * pipeline should pause briefly to allow the producer buffer to drain.
   */
  public boolean shouldPause() {
    return consecutiveDrops.get() >= threshold;
  }

  /** Increments the consecutive drop counter. Called by {@code OverflowListener::onOverflow}. */
  public void onDrop() {
    consecutiveDrops.incrementAndGet();
  }

  /** Resets the counter to zero. Called when a produce succeeds after a drop period. */
  public void onRecovery() {
    consecutiveDrops.set(0);
  }
}
