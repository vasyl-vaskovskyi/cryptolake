package com.cryptopanner.collector;

/**
 * Tracks the {@code pu}-chain continuity of one symbol's {@code depth} diff stream (master spec
 * §7.a.2 / §8). Each Binance depth event's {@code pu} (previous final update id) must equal the
 * prior event's {@code u} (final update id); a mismatch means updates were missed and the local
 * order book can no longer be rebuilt from diffs — a fresh snapshot is required. The first frame is
 * always treated as a break because there is no anchor snapshot yet.
 *
 * <p>Detection only; the snapshot fetch + write is {@link DepthResync}'s job.
 */
public final class DepthChainTracker {

  private long lastU;
  private boolean started;

  /** Returns true if frame {@code (u, pu)} breaks the chain (needs a snapshot). Updates state. */
  public synchronized boolean isBreak(long u, long pu) {
    boolean broke = !started || pu != lastU;
    started = true;
    lastU = u;
    return broke;
  }
}
