package com.cryptolake.collector.capture;

/**
 * Result of routing a raw WebSocket frame to its handler.
 *
 * <p>{@code rawText} is a substring of the original frame — byte-for-byte identical to the "data"
 * value as received from Binance (Tier 1 §1, Tier 5 B4). It is never re-serialized.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record FrameRoute(String streamType, String symbol, String rawText) {}
