package com.cryptolake.collector.lifecycle;

/**
 * Component runtime state record for lifecycle management.
 *
 * <p>Carries only the fields Python's {@code Collector._register_lifecycle_start} and
 * {@code _mark_lifecycle_shutdown} actually use (design §6.7). The collector's records deliberately
 * omit writer-specific fields ({@code classifier}, {@code evidence}).
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record ComponentRuntimeState(
    String component,
    String instanceId,
    String hostBootId,
    String startedAt,
    String lastHeartbeatAt) {}
