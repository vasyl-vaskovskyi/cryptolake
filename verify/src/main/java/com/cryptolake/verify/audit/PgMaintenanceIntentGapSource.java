package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.GapReason;
import com.cryptolake.common.logging.StructuredLogger;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads {@link GapRecord}s from the {@code maintenance_intent} PostgreSQL table.
 *
 * <p>For each row whose intent window ({@code created_at} → {@code COALESCE(consumed_at,
 * expires_at, NOW())}) overlaps the {@link AuditScope}, emits a {@code GapRecord} with {@code
 * source="pg.maintenance_intent"} and {@code reason="collector_restart"}. Exchange/symbol/stream
 * are blank because maintenance intents are system-wide.
 *
 * <p>Table name note: the authoritative DDL is in {@code
 * writer/src/main/java/com/cryptolake/writer/state/StateManager.java} which creates {@code
 * maintenance_intent} (singular). The {@code LifecycleStateManager} in the collector and {@code
 * MaintenanceWriter} in this module use {@code maintenance_intents} (plural) in their SELECT/INSERT
 * statements — that's a pre-existing inconsistency. This source matches the writer's DDL because
 * that's the table that actually gets created at runtime.
 *
 * <p>Graceful degradation: if the JDBC URL is null/empty, the DB is unreachable, or the table
 * doesn't exist (fresh deployment), logs a warning and returns an empty list without throwing.
 */
public final class PgMaintenanceIntentGapSource implements GapSource {

  private static final StructuredLogger log =
      StructuredLogger.of(PgMaintenanceIntentGapSource.class);

  private static final String SOURCE_LABEL = "pg.maintenance_intent";

  private static final String SQL =
      """
      SELECT maintenance_id, scope, planned_by, reason, created_at, expires_at, consumed_at
      FROM maintenance_intent
      WHERE created_at <= ?
        AND COALESCE(consumed_at, expires_at, NOW()) >= ?
      """;

  private final JdbcConfig config;

  public PgMaintenanceIntentGapSource(JdbcConfig config) {
    this.config = config;
  }

  @Override
  public String name() {
    return "PgMaintenanceIntentGapSource";
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
          String maintenanceId = rs.getString("maintenance_id");
          String intentScope = rs.getString("scope");
          String plannedBy = rs.getString("planned_by");
          String reason = rs.getString("reason");
          Timestamp createdAt = rs.getTimestamp("created_at");
          Timestamp expiresAt = rs.getTimestamp("expires_at");
          Timestamp consumedAt = rs.getTimestamp("consumed_at");

          if (createdAt == null) {
            continue;
          }

          long startMs = createdAt.toInstant().toEpochMilli();
          long endMs;
          if (consumedAt != null) {
            endMs = consumedAt.toInstant().toEpochMilli();
          } else if (expiresAt != null) {
            endMs = expiresAt.toInstant().toEpochMilli();
          } else {
            endMs = Instant.now().toEpochMilli();
          }

          String detail =
              "maintenance_id="
                  + maintenanceId
                  + "; scope="
                  + (intentScope != null ? intentScope : "-")
                  + "; reason="
                  + (reason != null ? reason : "-")
                  + "; planned_by="
                  + (plannedBy != null ? plannedBy : "-");

          result.addAll(fanOut(scope, GapReason.COLLECTOR_RESTART, startMs, endMs, detail));
        }
      }
    } catch (SQLException e) {
      log.warn(
          "pg_maintenance_intent_gap_source_read_failed",
          "error",
          e.getMessage(),
          "url",
          config.url());
    }

    return result;
  }

  // ── private helpers ───────────────────────────────────────────────────────

  /**
   * Fans out a single system-wide maintenance intent into one {@link GapRecord} per {@code (symbol,
   * stream)} tuple in the scope. When the scope has no symbols or streams, falls back to a single
   * record with empty fields (preserving backward-compatible system-wide behaviour).
   *
   * <p>When fan-out actually expands to a non-empty symbol/stream, the detail string is annotated
   * with {@code ; fanout=true} so consumers can tell the record was derived from a system-wide
   * event.
   */
  private List<GapRecord> fanOut(
      AuditScope scope, GapReason reason, long startMs, long endMs, String baseDetail) {
    List<String> symbols =
        (scope.symbols() == null || scope.symbols().isEmpty()) ? List.of("") : scope.symbols();
    List<String> streams =
        (scope.streams() == null || scope.streams().isEmpty()) ? List.of("") : scope.streams();
    String exchange =
        (scope.exchanges() != null && scope.exchanges().size() == 1)
            ? scope.exchanges().get(0)
            : "";

    boolean isFanout =
        symbols.size() > 1
            || streams.size() > 1
            || (symbols.size() == 1 && !symbols.get(0).isEmpty())
            || (streams.size() == 1 && !streams.get(0).isEmpty());
    String detail = isFanout ? baseDetail + "; fanout=true" : baseDetail;

    List<GapRecord> out = new ArrayList<>();
    for (String sym : symbols) {
      for (String str : streams) {
        out.add(new GapRecord(SOURCE_LABEL, exchange, sym, str, startMs, endMs, reason, detail));
      }
    }
    return out;
  }
}
