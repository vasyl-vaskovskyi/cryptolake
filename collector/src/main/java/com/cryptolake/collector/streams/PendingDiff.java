package com.cryptolake.collector.streams;

/**
 * A depth diff that has been received but cannot yet be validated (no sync point established).
 *
 * <p>{@code exchangeTs} is boxed to preserve Python's {@code None} possibility — distinguishes
 * "absent" from {@code 0}.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record PendingDiff(String rawText, Long exchangeTs, long sessionSeq) {}
