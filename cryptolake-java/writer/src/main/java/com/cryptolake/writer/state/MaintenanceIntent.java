package com.cryptolake.writer.state;

/**
 * PG-persisted maintenance intent record.
 *
 * <p>Ports Python's {@code MaintenanceIntent} (design §6.8). PG table {@code maintenance_intent},
 * PK {@code maintenance_id}.
 *
 * <p>All timestamp fields are ISO-8601 strings (Tier 5 F1). {@code consumedAt} is nullable.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record MaintenanceIntent(
    String maintenanceId,
    String scope,
    String plannedBy,
    String reason,
    String createdAt,
    String expiresAt,
    String consumedAt) {} // nullable
