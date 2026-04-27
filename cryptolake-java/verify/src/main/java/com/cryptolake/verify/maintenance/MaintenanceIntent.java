package com.cryptolake.verify.maintenance;

/**
 * Immutable record carrying a planned maintenance intent.
 *
 * <p>Ports Python's {@code MaintenanceIntent} namedtuple from {@code writer/state_manager.py}.
 * Field names match the Python fields exactly. Timestamps are ISO-8601 UTC strings (Tier 5 F1).
 *
 * <p>Tier 2 §12 — record (immutable, no setters).
 */
public record MaintenanceIntent(
    String maintenanceId,
    String scope,
    String plannedBy,
    String reason,
    String createdAt,
    String expiresAt) {}
