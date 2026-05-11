package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PgMaintenanceIntentGapSourceIT {

  @Container static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

  @BeforeEach
  void setup() throws Exception {
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var s = c.createStatement()) {
      s.execute("DROP TABLE IF EXISTS maintenance_intent");
      s.execute(
          """
          CREATE TABLE maintenance_intent (
              maintenance_id  TEXT        NOT NULL,
              scope           TEXT,
              planned_by      TEXT,
              reason          TEXT,
              created_at      TIMESTAMPTZ,
              expires_at      TIMESTAMPTZ,
              consumed_at     TIMESTAMPTZ,
              PRIMARY KEY (maintenance_id)
          )
          """);
    }
  }

  // Scope: 2026-05-10T10:00Z to 2026-05-10T12:00Z
  private static final long SCOPE_START_MS = Instant.parse("2026-05-10T10:00:00Z").toEpochMilli();
  private static final long SCOPE_END_MS = Instant.parse("2026-05-10T12:00:00Z").toEpochMilli();

  private AuditScope scope() {
    return new AuditScope(SCOPE_START_MS, SCOPE_END_MS, List.of(), List.of(), List.of(), "/data");
  }

  private JdbcConfig config() {
    return new JdbcConfig(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
  }

  private void insert(
      String maintenanceId,
      String scope,
      String plannedBy,
      String reason,
      Instant createdAt,
      Instant expiresAt,
      Instant consumedAt)
      throws Exception {
    String sql =
        """
        INSERT INTO maintenance_intent
          (maintenance_id, scope, planned_by, reason, created_at, expires_at, consumed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """;
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var ps = c.prepareStatement(sql)) {
      ps.setString(1, maintenanceId);
      ps.setString(2, scope);
      ps.setString(3, plannedBy);
      ps.setString(4, reason);
      ps.setTimestamp(5, createdAt != null ? Timestamp.from(createdAt) : null);
      ps.setTimestamp(6, expiresAt != null ? Timestamp.from(expiresAt) : null);
      ps.setTimestamp(7, consumedAt != null ? Timestamp.from(consumedAt) : null);
      ps.executeUpdate();
    }
  }

  // ── Tests ────────────────────────────────────────────────────────────────────────────────────

  /** Intent within scope with consumed_at set — endMs = consumed_at. */
  @Test
  void intentInScope_withConsumedAt_emitsCollectorRestartGap() throws Exception {
    Instant createdAt = Instant.parse("2026-05-10T10:00:00Z");
    Instant expiresAt = Instant.parse("2026-05-10T11:30:00Z");
    Instant consumedAt = Instant.parse("2026-05-10T10:45:00Z");

    insert("maint-001", "all", "operator", "rolling update", createdAt, expiresAt, consumedAt);

    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(config());
    List<GapRecord> records = source.read(scope());

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("pg.maintenance_intent");
    assertThat(r.exchange()).isEqualTo("");
    assertThat(r.symbol()).isEqualTo("");
    assertThat(r.stream()).isEqualTo("");
    assertThat(r.startMs()).isEqualTo(createdAt.toEpochMilli());
    assertThat(r.endMs()).isEqualTo(consumedAt.toEpochMilli());
    assertThat(r.reason()).isEqualTo("collector_restart");
    assertThat(r.detail()).contains("maintenance_id=maint-001");
    assertThat(r.detail()).contains("scope=all");
    assertThat(r.detail()).contains("reason=rolling update");
    assertThat(r.detail()).contains("planned_by=operator");
  }

  /** Intent with no consumed_at — endMs falls back to expires_at. */
  @Test
  void intentInScope_noConsumedAt_usesExpiresAt() throws Exception {
    Instant createdAt = Instant.parse("2026-05-10T10:05:00Z");
    Instant expiresAt = Instant.parse("2026-05-10T11:00:00Z");

    insert("maint-002", "all", "cron", "scheduled", createdAt, expiresAt, null);

    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(config());
    List<GapRecord> records = source.read(scope());

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.startMs()).isEqualTo(createdAt.toEpochMilli());
    assertThat(r.endMs()).isEqualTo(expiresAt.toEpochMilli());
    assertThat(r.reason()).isEqualTo("collector_restart");
  }

  /** Intent with no consumed_at and no expires_at — endMs is approximately now. */
  @Test
  void intentInScope_noConsumedAtNoExpiresAt_usesNow() throws Exception {
    Instant createdAt = Instant.parse("2026-05-10T10:10:00Z");

    insert("maint-003", null, null, null, createdAt, null, null);

    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(config());
    long beforeMs = System.currentTimeMillis();
    List<GapRecord> records = source.read(scope());
    long afterMs = System.currentTimeMillis();

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.endMs()).isBetween(beforeMs - 1000, afterMs + 1000);
  }

  /** Intent created after scope end — not in scope, no record emitted. */
  @Test
  void intentCreatedAfterScopeEnd_notEmitted() throws Exception {
    Instant createdAt = Instant.parse("2026-05-10T13:00:00Z"); // after scope end
    Instant expiresAt = Instant.parse("2026-05-10T14:00:00Z");

    insert("maint-004", "all", "op", "test", createdAt, expiresAt, null);

    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(config());
    List<GapRecord> records = source.read(scope());

    assertThat(records).isEmpty();
  }

  /** Empty URL → source returns empty list immediately (graceful skip). */
  @Test
  void emptyUrl_returnsEmpty() {
    JdbcConfig cfg = new JdbcConfig("", "user", "pass");
    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(cfg);
    assertThat(source.read(scope())).isEmpty();
  }

  /** Null URL → source returns empty list immediately (graceful skip). */
  @Test
  void nullUrl_returnsEmpty() {
    JdbcConfig cfg = new JdbcConfig(null, "user", "pass");
    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(cfg);
    assertThat(source.read(scope())).isEmpty();
  }

  /** Table doesn't exist → graceful empty (no exception). */
  @Test
  void missingTable_returnsEmpty() throws Exception {
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var s = c.createStatement()) {
      s.execute("DROP TABLE IF EXISTS maintenance_intent");
    }

    PgMaintenanceIntentGapSource source = new PgMaintenanceIntentGapSource(config());
    assertThat(source.read(scope())).isEmpty();
  }

  /** name() returns the expected label. */
  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new PgMaintenanceIntentGapSource(config()).name())
        .isEqualTo("PgMaintenanceIntentGapSource");
  }
}
