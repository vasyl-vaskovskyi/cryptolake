package com.cryptolake.collector.capture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-{@code (symbol, stream)} monotonically increasing sequence number allocator.
 *
 * <p>Ports {@code WebSocketManager._next_seq} from {@code src/collector/connection.py}. Uses {@link
 * AtomicLong} per key inside a {@link ConcurrentHashMap} for lock-free allocation.
 *
 * <p>Thread safety: lock-free via atomics + CHM (design §3.2).
 */
public final class SessionSeqAllocator {

  private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

  /**
   * Returns the next sequence number for the given {@code (symbol, stream)} pair. Sequence starts
   * at {@code 0} and increments by one on each call.
   */
  public long next(String symbol, String stream) {
    String key = symbol + '\0' + stream;
    AtomicLong counter = counters.computeIfAbsent(key, k -> new AtomicLong(0));
    return counter.getAndIncrement();
  }

  /**
   * Returns the current (last allocated) sequence for the pair without incrementing. Returns {@code
   * -1L} if no sequence has been allocated yet.
   */
  public long current(String symbol, String stream) {
    String key = symbol + '\0' + stream;
    AtomicLong counter = counters.get(key);
    return counter == null ? -1L : counter.get() - 1;
  }
}
