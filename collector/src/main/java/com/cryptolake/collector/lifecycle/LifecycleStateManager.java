package com.cryptolake.collector.lifecycle;

import com.cryptolake.common.config.DatabaseConfig;
import com.cryptolake.common.logging.StructuredLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Best-effort lifecycle state manager using JDBC + HikariCP.
 *
 * <p>Ports Python's {@code Collector._init_state_manager}, {@code _register_lifecycle_start},
 * {@code _send_heartbeat}, {@code _mark_lifecycle_shutdown}, and {@code _close_state_manager}.
 *
 * <p>ALL methods catch exceptions, log WARN, and continue (Tier 5 G1, G2 — best-effort; never
 * blocks the main lifecycle). On {@link #connect()} failure, {@link #dataSource} is set to {@code
 * null}; subsequent calls short-circuit silently.
 *
 * <p>Thread safety: {@code volatile HikariDataSource}; method-scoped connections. No shared mutable
 * state beyond the data source reference.
 */
public final class LifecycleStateManager {

  private static final StructuredLogger log = StructuredLogger.of(LifecycleStateManager.class);

  private volatile HikariDataSource dataSource;

  /**
   * Attempts to connect to the database. Sets {@code dataSource=null} on failure — subsequent calls
   * will no-op.
   */
  public void connect(DatabaseConfig config) {
    if (config == null || config.url() == null || config.url().isEmpty()) {
      log.info("lifecycle_db_disabled", "reason", "no_database_config");
      return;
    }
    try {
      HikariConfig hc = new HikariConfig();
      hc.setJdbcUrl(config.url());
      hc.setMaximumPoolSize(2);
      hc.setConnectionTimeout(5_000);
      hc.setInitializationFailTimeout(5_000);
      dataSource = new HikariDataSource(hc);
      log.info("lifecycle_db_connected");
    } catch (Exception e) {
      log.warn("lifecycle_db_connect_failed", "error", e.getMessage());
      dataSource = null;
    }
  }

  /** Upserts the collector's lifecycle start record. */
  public void registerStart(ComponentRuntimeState state) {
    HikariDataSource ds = dataSource;
    if (ds == null) return;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO component_runtime_state "
                    + "(component, instance_id, host_boot_id, started_at, last_heartbeat_at) "
                    + "VALUES (?, ?, ?, ?, ?) "
                    + "ON CONFLICT (component, instance_id) DO UPDATE SET "
                    + "started_at = EXCLUDED.started_at, "
                    + "last_heartbeat_at = EXCLUDED.last_heartbeat_at")) {
      ps.setString(1, state.component());
      ps.setString(2, state.instanceId());
      ps.setString(3, state.hostBootId());
      ps.setString(4, state.startedAt());
      ps.setString(5, state.lastHeartbeatAt());
      ps.executeUpdate();
    } catch (SQLException e) {
      log.warn("lifecycle_register_failed", "error", e.getMessage());
    }
  }

  /** Updates the heartbeat timestamp. */
  public void heartbeat(ComponentRuntimeState state) {
    HikariDataSource ds = dataSource;
    if (ds == null) return;
    String now = Instant.now().toString(); // ISO-8601 UTC (Tier 5 F1)
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE component_runtime_state SET last_heartbeat_at = ? "
                    + "WHERE component = ? AND instance_id = ?")) {
      ps.setString(1, now);
      ps.setString(2, state.component());
      ps.setString(3, state.instanceId());
      ps.executeUpdate();
    } catch (SQLException e) {
      log.warn("lifecycle_heartbeat_failed", "error", e.getMessage());
    }
  }

  /** Marks the component as cleanly shutdown. */
  public void markCleanShutdown(
      String component, String instanceId, boolean planned, String maintenanceId) {
    HikariDataSource ds = dataSource;
    if (ds == null) return;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE component_runtime_state SET clean_shutdown = TRUE, shutdown_at = NOW(), "
                    + "planned = ?, maintenance_id = ? "
                    + "WHERE component = ? AND instance_id = ?")) {
      ps.setBoolean(1, planned);
      ps.setString(2, maintenanceId);
      ps.setString(3, component);
      ps.setString(4, instanceId);
      ps.executeUpdate();
    } catch (SQLException e) {
      log.warn("lifecycle_mark_shutdown_failed", "error", e.getMessage());
    }
  }

  /** Loads an active maintenance intent, if any. */
  public Optional<MaintenanceIntent> loadActiveMaintenanceIntent() {
    HikariDataSource ds = dataSource;
    if (ds == null) return Optional.empty();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT maintenance_id, reason, created_at FROM maintenance_intents "
                    + "WHERE active = TRUE ORDER BY created_at DESC LIMIT 1")) {
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        return Optional.of(
            new MaintenanceIntent(
                rs.getString("maintenance_id"),
                rs.getString("reason"),
                parseTs(rs.getString("created_at"))));
      }
    } catch (SQLException e) {
      log.warn("lifecycle_load_intent_failed", "error", e.getMessage());
    }
    return Optional.empty();
  }

  /** Closes the data source (best-effort). */
  public void close() {
    HikariDataSource ds = dataSource;
    if (ds == null) return;
    try {
      ds.close();
    } catch (Exception ignored) {
      // best-effort (Tier 5 G1)
    }
    dataSource = null;
  }

  /** Parses a PG timestamp string with tz-tolerance (Tier 5 F2). */
  private static String parseTs(String raw) {
    if (raw == null) return Instant.now().toString();
    try {
      return OffsetDateTime.parse(raw).toInstant().toString();
    } catch (Exception e) {
      try {
        return java.time.LocalDateTime.parse(raw).toInstant(ZoneOffset.UTC).toString();
      } catch (Exception ex) {
        return raw;
      }
    }
  }
}
