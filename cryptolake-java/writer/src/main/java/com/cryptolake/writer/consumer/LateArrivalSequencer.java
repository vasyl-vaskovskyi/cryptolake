package com.cryptolake.writer.consumer;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Maintains the late-arrival sequence number per sealed path.
 *
 * <p>Generates {@code hour-H.late-{n}.jsonl.zst} names (Tier 5 M15; design §2.2). Ports Python's
 * {@code WriterConsumer._late_seq} dict and associated methods.
 *
 * <p>Thread safety: consume-loop thread only (T1). No synchronization (Tier 5 A5).
 */
public final class LateArrivalSequencer {

  /**
   * Map from base hour path (e.g., {@code .../hour-14.jsonl.zst}) to the next late sequence number
   * (starts at 1 after the base file is sealed).
   */
  private final Map<Path, Integer> seq = new HashMap<>();

  /**
   * Returns the next late sequence number for the given base hour path and increments the counter.
   *
   * <p>Ports Python's {@code self._late_seq[path] = seq + 1; return seq}.
   *
   * @param baseHourPath the base hour file path (e.g., {@code .../hour-14.jsonl.zst})
   * @return the next late sequence number (1, 2, 3, …)
   */
  public int nextSeq(Path baseHourPath) {
    int current = seq.getOrDefault(baseHourPath, 0);
    seq.put(baseHourPath, current + 1);
    return current + 1;
  }

  /**
   * Initializes the sequence counter for the given base hour path to 0 (the base file was just
   * sealed, next late arrival will be {@code .late-1}).
   *
   * <p>Ports Python's {@code self._late_seq[path] = 0} on seal.
   *
   * @param hourPath the base hour file path
   */
  public void markSealed(Path hourPath) {
    seq.put(hourPath, 0);
  }
}
