package com.cryptolake.writer.consumer;

/**
 * Captures the last-seen session ID and timestamp for a stream, used by {@link
 * SessionChangeDetector} to detect collector restarts.
 *
 * <p>Design §6.11. Immutable record (Tier 2 §12). Not serialized.
 */
record SessionMark(String sessionId, long receivedAtNs) {}
