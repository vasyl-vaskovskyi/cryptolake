package com.cryptolake.collector.adapter;

/**
 * Depth diff update ID triple from a Binance {@code depthUpdate} event.
 *
 * <p>All three values are {@code long} — Binance update IDs regularly exceed {@code 2^31} and must
 * never be narrowed to {@code int} (Tier 5 E1).
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record DepthUpdateIds(long U, long u, long pu) {}
