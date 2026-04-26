package com.cryptolake.collector.lifecycle;

/**
 * Maintenance intent loaded from the database (e.g. planned maintenance window).
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record MaintenanceIntent(String maintenanceId, String reason, String createdAt) {}
