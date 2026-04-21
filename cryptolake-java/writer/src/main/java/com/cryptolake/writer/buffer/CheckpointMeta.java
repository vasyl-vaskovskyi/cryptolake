package com.cryptolake.writer.buffer;

import com.cryptolake.writer.StreamKey;

/**
 * Captures the checkpoint state of the last envelope in a flush batch.
 *
 * <p>Ports Python's {@code CheckpointMeta} (design §6.4). Not serialized to JSON — internal state
 * only.
 *
 * <p>{@code lastSessionSeq = -1L} is the sentinel for writer-injected envelopes (Tier 5 M10).
 * Immutable record (Tier 2 §12).
 */
public record CheckpointMeta(
    long lastReceivedAt,
    String lastCollectorSessionId,
    long lastSessionSeq, // long; -1 sentinel for injected (Tier 5 M10)
    StreamKey streamKey) {}
