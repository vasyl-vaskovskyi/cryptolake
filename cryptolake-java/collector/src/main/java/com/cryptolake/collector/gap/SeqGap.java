package com.cryptolake.collector.gap;

/**
 * Represents a detected sequence gap in the session seq tracker.
 *
 * <p>Ports {@code SeqGap} from {@code src/collector/gap_detector.py}.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record SeqGap(long expected, long actual) {}
