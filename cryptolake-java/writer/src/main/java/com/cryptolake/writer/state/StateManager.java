package com.cryptolake.writer.state;

import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.StreamKey;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL-backed durable state manager for the writer service.
 *
 * <p>Ports Python's {@code state_manager.py:StateManager}. Uses JDBC + HikariCP (blocking on
 * virtual thread — fine per design §3.2 and Tier 5 A2). Pool size 2 per design §11 Q4 preferred.
 *
 * <p>Every public method acquires its own connection and releases it (connections from Hikari pool
 * — thread-safe, each caller gets its own). No connection shared across threads (Tier 2 §9).
 *
 * <p>All timestamps stored and retrieved as ISO-8601 strings (Tier 5 F1). Parsed via {@link
 * OffsetDateTime#parse} to tolerate both {@code Z} and {@code +00:00} suffixes from PG (Tier 5 F2).
 *
 * <p>Retry policy: {@link #saveStatesAndCheckpoints} retries 3× with exponential backoff (0, 2, 4
 * seconds) — inline, no framework (Tier 5 G3). On final failure: throws {@link
 * CryptoLakeStateException} (Tier 2 §13; design §7.1).
 */
public final class StateManager {

  private static final Logger log = LoggerFactory.getLogger(StateManager.class);

  private final HikariDataSource ds;
  private final ClockSupplier clock;

  public StateManager(HikariDataSource ds, ClockSupplier clock) {
    this.ds = ds;
    this.clock = clock;
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────────────────────────

  /**
   * Creates all required tables if they do not exist. Called once at startup.
   *
   * <p>Ports {@code StateManager.connect()} → {@code _create_tables_if_not_exists()}.
   */
  public void connect() {
    String ddl =
        """
        CREATE TABLE IF NOT EXISTS writer_file_state (
            topic            TEXT        NOT NULL,
            partition        INTEGER     NOT NULL,
            file_path        TEXT        NOT NULL,
            high_water_offset BIGINT     NOT NULL,
            file_byte_size   BIGINT      NOT NULL,
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (topic, partition, file_path)
        );

        CREATE TABLE IF NOT EXISTS stream_checkpoint (
            exchange                   TEXT        NOT NULL,
            symbol                     TEXT        NOT NULL,
            stream                     TEXT        NOT NULL,
            last_received_at           TIMESTAMPTZ,
            last_collector_session_id  TEXT,
            last_gap_reason            TEXT,
            PRIMARY KEY (exchange, symbol, stream)
        );

        CREATE TABLE IF NOT EXISTS component_runtime_state (
            component          TEXT        NOT NULL,
            instance_id        TEXT        NOT NULL,
            host_boot_id       TEXT,
            started_at         TIMESTAMPTZ,
            last_heartbeat_at  TIMESTAMPTZ,
            clean_shutdown_at  TIMESTAMPTZ,
            planned_shutdown   BOOLEAN     NOT NULL DEFAULT FALSE,
            maintenance_id     TEXT,
            PRIMARY KEY (component, instance_id)
        );

        CREATE TABLE IF NOT EXISTS maintenance_intent (
            maintenance_id  TEXT        NOT NULL,
            scope           TEXT,
            planned_by      TEXT,
            reason          TEXT,
            created_at      TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ,
            consumed_at     TIMESTAMPTZ,
            PRIMARY KEY (maintenance_id)
        );
        """;
    try (Connection conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      for (String sql : ddl.split(";")) {
        String trimmed = sql.strip();
        if (!trimmed.isEmpty()) {
          stmt.execute(trimmed);
        }
      }
      log.info("state_manager_tables_ready");
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to create tables", e);
    }
  }

  /** Closes the underlying HikariCP data source. */
  public void close() {
    try {
      ds.close();
    } catch (Exception ignored) {
      // best-effort shutdown; never block main shutdown path
      log.warn("state_manager_close_error", (Object) null);
    }
  }

  // ── Read operations ──────────────────────────────────────────────────────────────────────────

  /**
   * Loads all file states from PG, indexed by {@link TopicPartition}.
   *
   * <p>Ports {@code StateManager.load_all_states()}.
   */
  public Map<TopicPartition, List<FileStateRecord>> loadAllFileStates() {
    String sql =
        "SELECT topic, partition, high_water_offset, file_path, file_byte_size FROM writer_file_state";
    Map<TopicPartition, List<FileStateRecord>> result = new HashMap<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        String topic = rs.getString("topic");
        int partition = rs.getInt("partition");
        long highWater = rs.getLong("high_water_offset");
        String filePath = rs.getString("file_path");
        long fileByteSize = rs.getLong("file_byte_size");
        TopicPartition tp = new TopicPartition(topic, partition);
        result
            .computeIfAbsent(tp, k -> new ArrayList<>())
            .add(new FileStateRecord(topic, partition, highWater, filePath, fileByteSize));
      }
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to load file states", e);
    }
    return result;
  }

  /**
   * Loads all stream checkpoints from PG.
   *
   * <p>Ports {@code StateManager.load_stream_checkpoints()}.
   */
  public Map<StreamKey, StreamCheckpoint> loadStreamCheckpoints() {
    String sql =
        "SELECT exchange, symbol, stream, last_received_at, last_collector_session_id, last_gap_reason FROM stream_checkpoint";
    Map<StreamKey, StreamCheckpoint> result = new HashMap<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        String exchange = rs.getString("exchange");
        String symbol = rs.getString("symbol");
        String stream = rs.getString("stream");
        String lastReceivedAt = toIsoString(rs.getObject("last_received_at", OffsetDateTime.class));
        String lastSessionId = rs.getString("last_collector_session_id");
        String lastGapReason = rs.getString("last_gap_reason");
        StreamKey key = new StreamKey(exchange, symbol, stream);
        result.put(
            key,
            new StreamCheckpoint(
                exchange, symbol, stream, lastReceivedAt, lastSessionId, lastGapReason));
      }
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to load stream checkpoints", e);
    }
    return result;
  }

  /**
   * Loads the latest component runtime states from PG.
   *
   * <p>Ports {@code StateManager.load_latest_component_states()}.
   */
  public Map<String, ComponentRuntimeState> loadLatestComponentStates() {
    String sql =
        """
        SELECT DISTINCT ON (component)
            component, instance_id, host_boot_id, started_at, last_heartbeat_at,
            clean_shutdown_at, planned_shutdown, maintenance_id
        FROM component_runtime_state
        ORDER BY component, started_at DESC NULLS LAST
        """;
    Map<String, ComponentRuntimeState> result = new HashMap<>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        ComponentRuntimeState state = readComponentState(rs);
        result.put(state.component(), state);
      }
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to load component states", e);
    }
    return result;
  }

  /**
   * Loads a specific component instance state from PG.
   *
   * <p>Ports {@code StateManager.load_component_state_by_instance()}.
   */
  public Optional<ComponentRuntimeState> loadComponentStateByInstance(
      String component, String instanceId) {
    String sql =
        """
        SELECT component, instance_id, host_boot_id, started_at, last_heartbeat_at,
               clean_shutdown_at, planned_shutdown, maintenance_id
        FROM component_runtime_state WHERE component = ? AND instance_id = ?
        """;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, component);
      ps.setString(2, instanceId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(readComponentState(rs));
        }
      }
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to load component state", e);
    }
    return Optional.empty();
  }

  /**
   * Loads the active maintenance intent from PG (one with non-null consumed_at = null, not
   * expired).
   *
   * <p>Ports {@code StateManager.load_active_maintenance_intent()}.
   */
  public Optional<MaintenanceIntent> loadActiveMaintenanceIntent() {
    String sql =
        """
        SELECT maintenance_id, scope, planned_by, reason, created_at, expires_at, consumed_at
        FROM maintenance_intent WHERE consumed_at IS NULL
        ORDER BY created_at DESC LIMIT 1
        """;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        String maintenanceId = rs.getString("maintenance_id");
        String scope = rs.getString("scope");
        String plannedBy = rs.getString("planned_by");
        String reason = rs.getString("reason");
        String createdAt = toIsoString(rs.getObject("created_at", OffsetDateTime.class));
        String expiresAt = toIsoString(rs.getObject("expires_at", OffsetDateTime.class));
        String consumedAt = toIsoString(rs.getObject("consumed_at", OffsetDateTime.class));
        return Optional.of(
            new MaintenanceIntent(
                maintenanceId, scope, plannedBy, reason, createdAt, expiresAt, consumedAt));
      }
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to load maintenance intent", e);
    }
    return Optional.empty();
  }

  // ── Write operations ─────────────────────────────────────────────────────────────────────────

  /**
   * Atomically upserts file states and stream checkpoints in a single PG transaction. Retried up to
   * 3× with exponential backoff (0, 2, 4 seconds) — inline, no framework (Tier 5 G3).
   *
   * <p>On final failure: increments caller-side metric + throws {@link CryptoLakeStateException}.
   *
   * <p>Ports {@code StateManager.save_states_and_checkpoints()}.
   */
  public void saveStatesAndCheckpoints(
      List<FileStateRecord> states, List<StreamCheckpoint> checkpoints) {
    retry(
        () -> {
          try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
              upsertFileStates(conn, states);
              upsertStreamCheckpoints(conn, checkpoints);
              conn.commit();
            } catch (Exception e) {
              conn.rollback();
              throw e;
            } finally {
              conn.setAutoCommit(true);
            }
          }
        },
        "save_states_and_checkpoints");
  }

  /**
   * Upserts the component runtime state record (heartbeat update + startup registration).
   *
   * <p>Ports {@code StateManager.upsert_component_runtime()}.
   */
  public void upsertComponentRuntime(ComponentRuntimeState state) {
    String sql =
        """
        INSERT INTO component_runtime_state
            (component, instance_id, host_boot_id, started_at, last_heartbeat_at,
             clean_shutdown_at, planned_shutdown, maintenance_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (component, instance_id) DO UPDATE SET
            host_boot_id      = EXCLUDED.host_boot_id,
            started_at        = EXCLUDED.started_at,
            last_heartbeat_at = EXCLUDED.last_heartbeat_at,
            clean_shutdown_at = EXCLUDED.clean_shutdown_at,
            planned_shutdown  = EXCLUDED.planned_shutdown,
            maintenance_id    = EXCLUDED.maintenance_id
        """;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, state.component());
      ps.setString(2, state.instanceId());
      ps.setString(3, state.hostBootId());
      ps.setObject(4, parseToOffsetDateTime(state.startedAt()));
      ps.setObject(5, parseToOffsetDateTime(state.lastHeartbeatAt()));
      ps.setObject(6, parseToOffsetDateTime(state.cleanShutdownAt()));
      ps.setBoolean(7, state.plannedShutdown());
      ps.setString(8, state.maintenanceId());
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to upsert component runtime state", e);
    }
  }

  /**
   * Marks a component as having performed a clean shutdown.
   *
   * <p>Ports {@code StateManager.mark_component_clean_shutdown()}.
   */
  public void markComponentCleanShutdown(String component, String instanceId) {
    String now = Instant.now().toString();
    String sql =
        """
        UPDATE component_runtime_state SET clean_shutdown_at = ?
        WHERE component = ? AND instance_id = ?
        """;
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setObject(1, parseToOffsetDateTime(now));
      ps.setString(2, component);
      ps.setString(3, instanceId);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new CryptoLakeStateException("Failed to mark clean shutdown", e);
    }
  }

  // ── Private helpers ──────────────────────────────────────────────────────────────────────────

  private void upsertFileStates(Connection conn, List<FileStateRecord> states) throws SQLException {
    String sql =
        """
        INSERT INTO writer_file_state (topic, partition, file_path, high_water_offset, file_byte_size, updated_at)
        VALUES (?, ?, ?, ?, ?, now())
        ON CONFLICT (topic, partition, file_path) DO UPDATE SET
            high_water_offset = GREATEST(EXCLUDED.high_water_offset, writer_file_state.high_water_offset),
            file_byte_size    = EXCLUDED.file_byte_size,
            updated_at        = now()
        """;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (FileStateRecord s : states) {
        ps.setString(1, s.topic());
        ps.setInt(2, s.partition());
        ps.setString(3, s.filePath());
        ps.setLong(4, s.highWaterOffset());
        ps.setLong(5, s.fileByteSize());
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  private void upsertStreamCheckpoints(Connection conn, List<StreamCheckpoint> checkpoints)
      throws SQLException {
    String sql =
        """
        INSERT INTO stream_checkpoint (exchange, symbol, stream, last_received_at, last_collector_session_id, last_gap_reason)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (exchange, symbol, stream) DO UPDATE SET
            last_received_at          = EXCLUDED.last_received_at,
            last_collector_session_id = EXCLUDED.last_collector_session_id,
            last_gap_reason           = EXCLUDED.last_gap_reason
        """;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (StreamCheckpoint cp : checkpoints) {
        ps.setString(1, cp.exchange());
        ps.setString(2, cp.symbol());
        ps.setString(3, cp.stream());
        ps.setObject(4, parseToOffsetDateTime(cp.lastReceivedAt()));
        ps.setString(5, cp.lastCollectorSessionId());
        ps.setString(6, cp.lastGapReason());
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  private ComponentRuntimeState readComponentState(ResultSet rs) throws SQLException {
    String component = rs.getString("component");
    String instanceId = rs.getString("instance_id");
    String hostBootId = rs.getString("host_boot_id");
    String startedAt = toIsoString(rs.getObject("started_at", OffsetDateTime.class));
    String lastHeartbeatAt = toIsoString(rs.getObject("last_heartbeat_at", OffsetDateTime.class));
    String cleanShutdownAt = toIsoString(rs.getObject("clean_shutdown_at", OffsetDateTime.class));
    boolean plannedShutdown = rs.getBoolean("planned_shutdown");
    String maintenanceId = rs.getString("maintenance_id");
    return new ComponentRuntimeState(
        component,
        instanceId,
        hostBootId,
        startedAt,
        lastHeartbeatAt,
        cleanShutdownAt,
        plannedShutdown,
        maintenanceId);
  }

  /**
   * Converts an {@link OffsetDateTime} from PG to ISO-8601 string (Tier 5 F1). Returns {@code null}
   * if the value is {@code null}.
   */
  private static String toIsoString(OffsetDateTime odt) {
    if (odt == null) return null;
    return odt.toInstant().toString(); // "2026-04-18T12:34:56.123Z" (Tier 5 F1)
  }

  /**
   * Parses an ISO-8601 string to {@link OffsetDateTime}, tolerating {@code Z} or {@code +00:00}
   * suffixes (Tier 5 F2). Returns {@code null} for null input.
   */
  private static OffsetDateTime parseToOffsetDateTime(String iso) {
    if (iso == null) return null;
    try {
      return OffsetDateTime.parse(iso); // handles +00:00 and Z
    } catch (DateTimeParseException e1) {
      try {
        // naive timestamp — interpret as UTC (Tier 5 F2)
        return java.time.LocalDateTime.parse(iso).atOffset(ZoneOffset.UTC);
      } catch (DateTimeParseException e2) {
        log.warn("timestamp_parse_failed", "value", iso);
        return null;
      }
    }
  }

  /**
   * Inline retry loop: 3 attempts with exponential backoff (0, 2, 4 seconds) — no framework (Tier 5
   * G3). Rethrows as {@link CryptoLakeStateException} on final failure.
   *
   * <p>Ports {@code StateManager._retry_transaction()}.
   */
  private void retry(CheckedRunnable op, String label) {
    int maxRetries = 3;
    Exception last = null;
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        op.run();
        return;
      } catch (Exception e) {
        last = e;
        log.warn("pg_save_failed", "attempt", attempt + 1, "error", e.getMessage());
        if (attempt < maxRetries - 1) {
          long sleepMs = 1000L * (1 << attempt); // 1s, 2s for attempts 0 and 1
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // Tier 5 A4
            throw new CryptoLakeStateException("Interrupted during retry", ie);
          }
        }
      }
    }
    throw new CryptoLakeStateException(
        "Operation '" + label + "' failed after " + maxRetries + " attempts", last);
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }
}
