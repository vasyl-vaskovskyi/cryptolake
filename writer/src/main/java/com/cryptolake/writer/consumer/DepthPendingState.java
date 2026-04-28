package com.cryptolake.writer.consumer;

/**
 * State for a pending depth-recovery gap: the first diff timestamp, candidate LID, and candidate
 * timestamp.
 *
 * <p>Design §6.11. Immutable record (Tier 2 §12). Not serialized.
 */
record DepthPendingState(long firstDiffTs, long candidateLid, long candidateTs) {}
