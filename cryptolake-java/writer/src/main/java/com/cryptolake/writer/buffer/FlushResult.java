package com.cryptolake.writer.buffer;

import java.nio.file.Path;
import java.util.List;

/**
 * Result of flushing a single buffer (one file target).
 *
 * <p>Ports Python's {@code FlushResult} (design §6.3). Not serialized to JSON — internal only.
 *
 * <p>{@code highWaterOffset = -1L} sentinel means all records in this batch are synthetic (Tier 5
 * M9). {@code partition} is {@code int} (Tier 5 M8). {@code checkpointMeta} may be {@code null} if
 * all records are synthetic.
 *
 * <p>Immutable record (Tier 2 §12). Compact constructor makes a defensive copy of {@code lines}.
 */
public record FlushResult(
    FileTarget target,
    Path filePath,
    List<byte[]> lines, // each line is JSON bytes + 0x0A
    long highWaterOffset, // max Kafka offset; -1L if all synthetic (Tier 5 M9)
    int partition, // int (Tier 5 M8)
    int count,
    CheckpointMeta checkpointMeta, // null if all synthetic
    boolean hasBackupSource) {

  /** Defensive copy of lines to satisfy record immutability (Tier 2 §12). */
  public FlushResult {
    lines = List.copyOf(lines);
  }
}
