package com.cryptolake.collector.durability;

/**
 * Immutable record representing a single lifecycle event from {@link LifecycleJournal}.
 *
 * <p>Events: {@code "start"}, {@code "clean_shutdown"}. The {@code "unclean_shutdown"} event is
 * inferred on the next boot by the writer-side classifier (not written by the collector itself).
 *
 * @param tsNs nanoseconds since Unix epoch
 * @param event event type string
 * @param hostBootId OS boot identifier
 * @param collectorSessionId unique session identifier for this collector process invocation
 * @param planned whether shutdown was planned (null for non-shutdown events)
 * @param maintenanceId optional maintenance intent ID (null if none)
 */
public record LifecycleEvent(
    long tsNs,
    String event,
    String hostBootId,
    String collectorSessionId,
    Boolean planned,
    String maintenanceId) {}
