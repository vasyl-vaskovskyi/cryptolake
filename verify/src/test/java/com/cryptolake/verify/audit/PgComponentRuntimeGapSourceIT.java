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
class PgComponentRuntimeGapSourceIT {

  @Container static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");

  @BeforeEach
  void setup() throws Exception {
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var s = c.createStatement()) {
      s.execute("DROP TABLE IF EXISTS component_runtime_state");
      s.execute(
          """
          CREATE TABLE component_runtime_state (
              component          TEXT        NOT NULL,
              instance_id        TEXT        NOT NULL,
              host_boot_id       TEXT,
              started_at         TIMESTAMPTZ,
              last_heartbeat_at  TIMESTAMPTZ,
              clean_shutdown_at  TIMESTAMPTZ,
              planned_shutdown   BOOLEAN     NOT NULL DEFAULT FALSE,
              maintenance_id     TEXT,
              PRIMARY KEY (component, instance_id)
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

  /** Insert a row into component_runtime_state. */
  private void insert(
      String component,
      String instanceId,
      String hostBootId,
      Instant startedAt,
      Instant lastHeartbeatAt,
      Instant cleanShutdownAt,
      boolean plannedShutdown,
      String maintenanceId)
      throws Exception {
    String sql =
        """
        INSERT INTO component_runtime_state
          (component, instance_id, host_boot_id, started_at, last_heartbeat_at,
           clean_shutdown_at, planned_shutdown, maintenance_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """;
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var ps = c.prepareStatement(sql)) {
      ps.setString(1, component);
      ps.setString(2, instanceId);
      ps.setString(3, hostBootId);
      ps.setTimestamp(4, startedAt != null ? Timestamp.from(startedAt) : null);
      ps.setTimestamp(5, lastHeartbeatAt != null ? Timestamp.from(lastHeartbeatAt) : null);
      ps.setTimestamp(6, cleanShutdownAt != null ? Timestamp.from(cleanShutdownAt) : null);
      ps.setBoolean(7, plannedShutdown);
      ps.setString(8, maintenanceId);
      ps.executeUpdate();
    }
  }

  // ── Tests ────────────────────────────────────────────────────────────────────────────────────

  /**
   * Row with last_heartbeat_at inside scope, clean shutdown set — should produce one GapRecord with
   * reason "collector_restart" (because planned_shutdown=true).
   */
  @Test
  void rowInScope_plannedShutdown_emitsCollectorRestartGap() throws Exception {
    Instant startedAt = Instant.parse("2026-05-10T09:00:00Z");
    Instant heartbeat = Instant.parse("2026-05-10T10:30:00Z"); // inside scope
    Instant shutdown = Instant.parse("2026-05-10T11:00:00Z"); // inside scope

    insert("collector", "inst-1", "boot-abc", startedAt, heartbeat, shutdown, true, "maint-1");

    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(config());
    List<GapRecord> records = source.read(scope());

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("pg.component_runtime");
    assertThat(r.exchange()).isEqualTo("");
    assertThat(r.symbol()).isEqualTo("");
    assertThat(r.stream()).isEqualTo("");
    assertThat(r.startMs()).isEqualTo(heartbeat.toEpochMilli());
    assertThat(r.endMs()).isEqualTo(shutdown.toEpochMilli());
    assertThat(r.reason()).isEqualTo("collector_restart");
    assertThat(r.detail()).contains("component=collector");
    assertThat(r.detail()).contains("instance_id=inst-1");
    assertThat(r.detail()).contains("host_boot_id=boot-abc");
    assertThat(r.detail()).contains("maintenance_id=maint-1");
  }

  /**
   * Row with last_heartbeat_at inside scope, no clean shutdown, planned_shutdown=false — reason is
   * "restart_gap" and endMs is close to now (within a few seconds).
   */
  @Test
  void rowInScope_unplannedNoShutdown_emitsRestartGap() throws Exception {
    Instant startedAt = Instant.parse("2026-05-10T09:00:00Z");
    Instant heartbeat = Instant.parse("2026-05-10T10:45:00Z");

    insert("collector", "inst-2", "boot-xyz", startedAt, heartbeat, null, false, null);

    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(config());
    long beforeMs = System.currentTimeMillis();
    List<GapRecord> records = source.read(scope());
    long afterMs = System.currentTimeMillis();

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.reason()).isEqualTo("restart_gap");
    assertThat(r.startMs()).isEqualTo(heartbeat.toEpochMilli());
    // endMs should be approximately now (within the window of the test run)
    assertThat(r.endMs()).isBetween(beforeMs - 1000, afterMs + 1000);
    assertThat(r.detail()).contains("maintenance_id=-");
  }

  /**
   * Graceful-but-unplanned: clean_shutdown_at set, planned_shutdown=false. This is the fourth
   * (component_lifecycle × planned) quadrant — e.g. {@code docker stop} outside the
   * cryptolake-maintenance wrapper. Must emit a {@code restart_gap} record so the file-side gap
   * envelope has a matching state record in the diff.
   */
  @Test
  void rowInScope_unplannedCleanShutdown_emitsRestartGap() throws Exception {
    Instant startedAt = Instant.parse("2026-05-10T09:00:00Z");
    Instant heartbeat = Instant.parse("2026-05-10T10:30:00Z");
    Instant cleanShutdown = Instant.parse("2026-05-10T11:00:00Z");

    insert("collector", "inst-4", "boot-z", startedAt, heartbeat, cleanShutdown, false, null);

    List<GapRecord> records = new PgComponentRuntimeGapSource(config()).read(scope());
    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.reason()).isEqualTo("restart_gap");
    assertThat(r.startMs()).isEqualTo(heartbeat.toEpochMilli());
    assertThat(r.endMs()).isEqualTo(cleanShutdown.toEpochMilli());
  }

  /** Row where started_at is AFTER scope end — should not overlap scope, no record emitted. */
  @Test
  void rowStartedAfterScopeEnd_notEmitted() throws Exception {
    Instant startedAt = Instant.parse("2026-05-10T13:00:00Z"); // after scope end
    Instant heartbeat = Instant.parse("2026-05-10T13:30:00Z");

    insert("collector", "inst-3", "boot-def", startedAt, heartbeat, null, false, null);

    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(config());
    List<GapRecord> records = source.read(scope());

    assertThat(records).isEmpty();
  }

  /** Row where last_heartbeat_at is null — should be skipped (no meaningful gap window). */
  @Test
  void rowWithNullHeartbeat_skipped() throws Exception {
    Instant startedAt = Instant.parse("2026-05-10T09:00:00Z");

    insert("collector", "inst-4", "boot-ghi", startedAt, null, null, false, null);

    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(config());
    List<GapRecord> records = source.read(scope());

    assertThat(records).isEmpty();
  }

  /** Empty URL → source returns empty list immediately (graceful skip). */
  @Test
  void emptyUrl_returnsEmpty() {
    JdbcConfig cfg = new JdbcConfig("", "user", "pass");
    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(cfg);
    assertThat(source.read(scope())).isEmpty();
  }

  /** Null URL → source returns empty list immediately (graceful skip). */
  @Test
  void nullUrl_returnsEmpty() {
    JdbcConfig cfg = new JdbcConfig(null, "user", "pass");
    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(cfg);
    assertThat(source.read(scope())).isEmpty();
  }

  /** Table doesn't exist → graceful empty (no exception). */
  @Test
  void missingTable_returnsEmpty() throws Exception {
    // Drop the table to simulate fresh deployment
    try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        var s = c.createStatement()) {
      s.execute("DROP TABLE IF EXISTS component_runtime_state");
    }

    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(config());
    assertThat(source.read(scope())).isEmpty();
  }

  /** name() returns the expected label. */
  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new PgComponentRuntimeGapSource(config()).name())
        .isEqualTo("PgComponentRuntimeGapSource");
  }

  /**
   * Fan-out: one PG row + scope with symbol=btcusdt and streams=[depth, trades] → 2 GapRecords, one
   * per (symbol, stream) tuple, each with populated exchange/symbol/stream and detail containing
   * "fanout=true".
   */
  @Test
  void fanOutWithMultipleStreams_emitsOneRecordPerTuple() throws Exception {
    Instant startedAt = Instant.parse("2026-05-10T09:00:00Z");
    Instant heartbeat = Instant.parse("2026-05-10T10:30:00Z");
    Instant shutdown = Instant.parse("2026-05-10T11:00:00Z");

    insert("collector", "inst-fo", "boot-fo", startedAt, heartbeat, shutdown, true, "maint-fo");

    AuditScope fanOutScope =
        new AuditScope(
            SCOPE_START_MS,
            SCOPE_END_MS,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("depth", "trades"),
            "/data");

    PgComponentRuntimeGapSource source = new PgComponentRuntimeGapSource(config());
    List<GapRecord> records = source.read(fanOutScope);

    assertThat(records).hasSize(2);
    assertThat(records).allMatch(r -> r.exchange().equals("binance"));
    assertThat(records).allMatch(r -> r.symbol().equals("btcusdt"));
    assertThat(records).extracting(GapRecord::stream).containsExactlyInAnyOrder("depth", "trades");
    assertThat(records).allMatch(r -> r.reason().equals("collector_restart"));
    assertThat(records).allMatch(r -> r.detail().contains("fanout=true"));
    assertThat(records).allMatch(r -> r.startMs() == heartbeat.toEpochMilli());
    assertThat(records).allMatch(r -> r.endMs() == shutdown.toEpochMilli());
  }
}
