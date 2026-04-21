package com.cryptolake.writer.state;

/**
 * PG-persisted runtime state for a component instance (collector, writer, etc.).
 *
 * <p>Ports Python's {@code ComponentRuntimeState} (design §6.7). PG table
 * {@code component_runtime_state}, PK {@code (component, instance_id)}.
 *
 * <p>All timestamp fields are ISO-8601 strings for identity with Python's {@code
 * datetime.isoformat()} (Tier 5 F1). {@code cleanShutdownAt} and {@code maintenanceId} are
 * nullable.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record ComponentRuntimeState(
    String component,
    String instanceId,
    String hostBootId,
    String startedAt,
    String lastHeartbeatAt,
    String cleanShutdownAt, // nullable
    boolean plannedShutdown,
    String maintenanceId) {} // nullable
