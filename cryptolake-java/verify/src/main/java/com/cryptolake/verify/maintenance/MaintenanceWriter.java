package com.cryptolake.verify.maintenance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * One-shot JDBC writer for maintenance intent records.
 *
 * <p>Ports {@code StateManager.create_maintenance_intent} from {@code writer/state_manager.py}.
 * Uses {@link DriverManager} (not HikariCP) — one-shot CLI, no pooling needed (design §2.1 Q9; Tier
 * 2 §14 applies to long-running services only).
 *
 * <p>Timestamps stored as ISO-8601 strings (Tier 5 F1).
 *
 * <p>Thread safety: each call opens and closes its own connection.
 */
public final class MaintenanceWriter {

  private static final String INSERT_SQL =
      "INSERT INTO maintenance_intents "
          + "(maintenance_id, scope, planned_by, reason, created_at, expires_at) "
          + "VALUES (?, ?, ?, ?, ?, ?)";

  private MaintenanceWriter() {}

  /**
   * Opens a JDBC connection to {@code dbUrl}, executes a single INSERT, and closes.
   *
   * @param dbUrl PostgreSQL JDBC URL (e.g. {@code jdbc:postgresql://host/db?user=u&password=p})
   * @param intent maintenance intent to persist
   * @throws SQLException on connection or execution failure
   */
  public static void write(String dbUrl, MaintenanceIntent intent) throws SQLException {
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
      ps.setString(1, intent.maintenanceId());
      ps.setString(2, intent.scope());
      ps.setString(3, intent.plannedBy());
      ps.setString(4, intent.reason());
      ps.setString(5, intent.createdAt()); // ISO-8601 UTC string (Tier 5 F1)
      ps.setString(6, intent.expiresAt());
      ps.executeUpdate();
    }
  }
}
