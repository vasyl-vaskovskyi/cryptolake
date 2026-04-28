package com.cryptolake.writer;

/**
 * Composite key for identifying a unique stream across the writer.
 *
 * <p>Used as a map key across {@code SessionChangeDetector}, {@code RecoveryCoordinator}, {@code
 * FailoverController}, {@code CoverageFilter}, {@code HourRotationScheduler}. Package-private to
 * avoid leaking a utility record into {@code common} (design §6.10).
 *
 * <p>Record auto-implements {@code equals/hashCode} on components, matching Python tuple identity
 * (Tier 5 M14). Immutable (Tier 2 §12).
 */
public record StreamKey(String exchange, String symbol, String stream) {}
