package com.cryptolake.writer.consumer;

/**
 * Composite key for depth-recovery state: {@code (exchange, symbol)}.
 *
 * <p>Used by {@link DepthRecoveryGapFilter} to track per-symbol depth-recovery pending state.
 * Record auto-implements {@code equals/hashCode} (Tier 5 M14). Package-private.
 *
 * <p>Immutable record (Tier 2 §12). Not serialized.
 */
record ExSym(String exchange, String symbol) {}
