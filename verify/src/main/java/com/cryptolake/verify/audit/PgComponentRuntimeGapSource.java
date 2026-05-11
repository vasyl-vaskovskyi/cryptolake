package com.cryptolake.verify.audit;

import com.cryptolake.common.logging.StructuredLogger;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads {@link GapRecord}s from the {@code component_runtime_state} PostgreSQL table.
 *
 * <p>For each row whose process lifetime overlaps the {@link AuditScope} window, emits a {@code
 * GapRecord} representing the gap that occurred after the process last heartbeat'd. The records
 * carry {@code source="pg.component_runtime"} and have blank exchange/symbol/stream fields because
 * restart gaps are global (not stream-specific).
 *
 * <p>Reason classification (one record per row that meets the overlap predicate; rows with null
 * {@code last_heartbeat_at} are skipped):
 *
 * <ul>
 *   <li>{@code planned_shutdown = TRUE} → {@code reason="collector_restart"} regardless of {@code
 *       clean_shutdown_at}.
 *   <li>{@code planned_shutdown = FALSE} → {@code reason="restart_gap"}. This covers both the
 *       unclean-exit case ({@code clean_shutdown_at IS NULL}, e.g. crash, SIGKILL, host reboot) and
 *       the graceful-but-unplanned case ({@code clean_shutdown_at IS NOT NULL} without a
 *       maintenance intent, e.g. {@code docker stop} outside the maintenance wrapper). Both are
 *       real periods of data loss from the operator's perspective and need a matching file-side gap
 *       envelope to pass the strict diff in {@link com.cryptolake.verify.audit.GapRecordDiff}.
 * </ul>
 *
 * <p>{@code endMs} is {@code clean_shutdown_at} when present, otherwise {@code NOW()} (a still-
 * running process or one that never wrote a clean-shutdown row).
 *
 * <p>Connection management: uses plain {@link DriverManager#getConnection} per call rather than
 * HikariCP — verify is a one-shot CLI invocation, not a long-running service, so a pool would add
 * complexity without benefit. Each {@link #read} opens, queries, and closes one connection.
 *
 * <p>Graceful degradation: if the JDBC URL is null/empty, or if the DB is unreachable, or if the
 * table doesn't exist (fresh deployment), the source logs a warning and returns an empty list
 * without throwing.
 */
public final class PgComponentRuntimeGapSource implements GapSource {

  private static final StructuredLogger log =
      StructuredLogger.of(PgComponentRuntimeGapSource.class);

  private static final String SOURCE_LABEL = "pg.component_runtime";

  private static final String SQL =
      """
      SELECT component, instance_id, host_boot_id, started_at, last_heartbeat_at,
             clean_shutdown_at, planned_shutdown, maintenance_id
      FROM component_runtime_state
      WHERE started_at <= ?
        AND COALESCE(clean_shutdown_at, NOW()) >= ?
      """;

  private final JdbcConfig config;

  public PgComponentRuntimeGapSource(JdbcConfig config) {
    this.config = config;
  }

  @Override
  public String name() {
    return "PgComponentRuntimeGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    if (config.url() == null || config.url().isBlank()) {
      return List.of();
    }

    List<GapRecord> result = new ArrayList<>();
    try (var conn = DriverManager.getConnection(config.url(), config.user(), config.password());
        var ps = conn.prepareStatement(SQL)) {

      Timestamp scopeEnd = Timestamp.from(Instant.ofEpochMilli(scope.endMs()));
      Timestamp scopeStart = Timestamp.from(Instant.ofEpochMilli(scope.startMs()));
      ps.setTimestamp(1, scopeEnd);
      ps.setTimestamp(2, scopeStart);

      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          Timestamp lastHeartbeat = rs.getTimestamp("last_heartbeat_at");
          if (lastHeartbeat == null) {
            // No meaningful gap window without a heartbeat timestamp — skip.
            continue;
          }

          String component = rs.getString("component");
          String instanceId = rs.getString("instance_id");
          String hostBootId = rs.getString("host_boot_id");
          boolean plannedShutdown = rs.getBoolean("planned_shutdown");
          String maintenanceId = rs.getString("maintenance_id");
          Timestamp cleanShutdown = rs.getTimestamp("clean_shutdown_at");

          long startMs = lastHeartbeat.toInstant().toEpochMilli();
          long endMs =
              cleanShutdown != null
                  ? cleanShutdown.toInstant().toEpochMilli()
                  : Instant.now().toEpochMilli();

          String reason = plannedShutdown ? "collector_restart" : "restart_gap";
          String detail =
              "component="
                  + component
                  + "; instance_id="
                  + instanceId
                  + "; host_boot_id="
                  + (hostBootId != null ? hostBootId : "-")
                  + "; maintenance_id="
                  + (maintenanceId != null ? maintenanceId : "-");

          result.add(new GapRecord(SOURCE_LABEL, "", "", "", startMs, endMs, reason, detail));
        }
      }
    } catch (SQLException e) {
      log.warn(
          "pg_component_runtime_gap_source_read_failed",
          "error",
          e.getMessage(),
          "url",
          config.url());
    }

    return result;
  }
}
