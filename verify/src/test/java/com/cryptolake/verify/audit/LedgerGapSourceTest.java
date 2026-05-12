package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapReason;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link LedgerGapSource}.
 *
 * <p>Writes lifecycle.jsonl fixture files into a temp directory that mirrors the {@code
 * ${dataDir}/cryptolake/<collectorId>/lifecycle.jsonl} layout.
 */
class LedgerGapSourceTest {

  @TempDir Path tmpDir;

  private final ObjectMapper mapper = new ObjectMapper();

  // 2026-05-11T09:00:00Z in millis and nanos
  private static final long START_MS = 1778490000_000L;
  private static final long START_NS = START_MS * 1_000_000L;

  // 2026-05-11T10:00:00Z in millis and nanos
  private static final long END_MS = 1778493600_000L;
  private static final long END_NS = END_MS * 1_000_000L;

  /** Writes a lifecycle.jsonl with the two lines under a named collector subdirectory. */
  private void writeJournal(String collectorId, String... lines) throws IOException {
    Path dir = tmpDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    Path file = dir.resolve("lifecycle.jsonl");
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line).append('\n');
    }
    Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
  }

  // -------------------------------------------------------------------------
  // Happy path: planned shutdown emits collector_restart reason
  // -------------------------------------------------------------------------

  @Test
  void plannedShutdown_emitsCollectorRestart() throws IOException {
    String startLine =
        "{\"ts_ns\":"
            + START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-abc\"}";
    String stopLine =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-abc\",\"planned\":true,\"maintenance_id\":\"maint-1\"}";

    writeJournal("binance-collector-primary", startLine, stopLine);

    AuditScope scope =
        new AuditScope(
            START_MS - 1, END_MS + 1, List.of(), List.of(), List.of(), tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("ledger");
    assertThat(r.exchange()).isEqualTo("");
    assertThat(r.symbol()).isEqualTo("");
    assertThat(r.stream()).isEqualTo("");
    assertThat(r.startMs()).isEqualTo(START_MS);
    assertThat(r.endMs()).isEqualTo(END_MS);
    assertThat(r.reason()).isEqualTo(GapReason.COLLECTOR_RESTART);
    assertThat(r.detail())
        .isEqualTo(
            "collector_id=binance-collector-primary; collector_session_id=sess-abc;"
                + " host_boot_id=boot-1; maintenance_id=maint-1");
  }

  // -------------------------------------------------------------------------
  // Unplanned shutdown emits restart_gap reason
  // -------------------------------------------------------------------------

  @Test
  void unplannedShutdown_emitsRestartGap() throws IOException {
    String startLine =
        "{\"ts_ns\":"
            + START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-xyz\"}";
    String stopLine =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-xyz\",\"planned\":false}";

    writeJournal("binance-collector-primary", startLine, stopLine);

    AuditScope scope =
        new AuditScope(
            START_MS - 1, END_MS + 1, List.of(), List.of(), List.of(), tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.reason()).isEqualTo(GapReason.RESTART_GAP);
    assertThat(r.detail()).contains("maintenance_id=-");
  }

  // -------------------------------------------------------------------------
  // Shutdown outside scope window is not emitted
  // -------------------------------------------------------------------------

  /**
   * Session that started before scope.endMs but shut down AFTER scope.endMs — interval overlap
   * means the gap intersects the scope and should still be emitted (matches
   * PgComponentRuntimeGapSource semantics).
   */
  @Test
  void sessionStraddlesScopeEnd_emitted() throws IOException {
    long startNs = (START_MS - 30 * 60_000L) * 1_000_000L; // start 30m before scope
    long shutdownNs = (END_MS + 30 * 60_000L) * 1_000_000L; // shutdown 30m after scope end
    String startLine =
        "{\"ts_ns\":"
            + startNs
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-straddle\"}";
    String stopLine =
        "{\"ts_ns\":"
            + shutdownNs
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-straddle\",\"planned\":false}";

    writeJournal("binance-collector-primary", startLine, stopLine);

    AuditScope scope =
        new AuditScope(START_MS, END_MS, List.of(), List.of(), List.of(), tmpDir.toString());
    List<GapRecord> records = new LedgerGapSource(tmpDir, mapper).read(scope);

    assertThat(records).hasSize(1);
    assertThat(records.get(0).reason()).isEqualTo(GapReason.RESTART_GAP);
    assertThat(records.get(0).startMs()).isEqualTo(startNs / 1_000_000L);
    assertThat(records.get(0).endMs()).isEqualTo(shutdownNs / 1_000_000L);
  }

  @Test
  void shutdownOutsideScope_notEmitted() throws IOException {
    // Scope is AFTER the shutdown — no overlap
    String startLine =
        "{\"ts_ns\":"
            + START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-abc\"}";
    String stopLine =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-abc\",\"planned\":true}";

    writeJournal("binance-collector-primary", startLine, stopLine);

    // scope window starts after the shutdown
    AuditScope scope =
        new AuditScope(
            END_MS + 1, END_MS + 3_600_000, List.of(), List.of(), List.of(), tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Multiple collector directories are each read
  // -------------------------------------------------------------------------

  @Test
  void multipleCollectors_bothEmitRecords() throws IOException {
    long start2Ns = START_NS + 7_200_000_000_000L;
    long end2Ns = END_NS + 7_200_000_000_000L;

    String start1 =
        "{\"ts_ns\":"
            + START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"b1\","
            + "\"collector_session_id\":\"s1\"}";
    String stop1 =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"b1\","
            + "\"collector_session_id\":\"s1\",\"planned\":true}";
    writeJournal("collector-primary", start1, stop1);

    String start2 =
        "{\"ts_ns\":"
            + start2Ns
            + ",\"event\":\"start\",\"host_boot_id\":\"b2\","
            + "\"collector_session_id\":\"s2\"}";
    String stop2 =
        "{\"ts_ns\":"
            + end2Ns
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"b2\","
            + "\"collector_session_id\":\"s2\",\"planned\":false}";
    writeJournal("collector-backup", start2, stop2);

    // Wide scope to cover both
    AuditScope scope =
        new AuditScope(
            START_MS - 1,
            end2Ns / 1_000_000 + 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(2);
    assertThat(records)
        .extracting(GapRecord::reason)
        .containsExactlyInAnyOrder(GapReason.COLLECTOR_RESTART, GapReason.RESTART_GAP);
  }

  // -------------------------------------------------------------------------
  // Corrupt line is silently skipped
  // -------------------------------------------------------------------------

  @Test
  void corruptLine_silentlySkipped() throws IOException {
    String badLine = "this is not json {{{ broken";
    String startLine =
        "{\"ts_ns\":"
            + START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"b\","
            + "\"collector_session_id\":\"s\"}";
    String stopLine =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"b\","
            + "\"collector_session_id\":\"s\",\"planned\":true}";

    writeJournal("collector-a", badLine, startLine, stopLine);

    AuditScope scope =
        new AuditScope(
            START_MS - 1, END_MS + 1, List.of(), List.of(), List.of(), tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    // Corrupt line is dropped; the valid pair still yields one record
    assertThat(records).hasSize(1);
  }

  // -------------------------------------------------------------------------
  // Missing cryptolake dir → empty list
  // -------------------------------------------------------------------------

  @Test
  void missingCryptolakeDir_returnsEmpty() {
    // tmpDir has no "cryptolake" subdirectory
    AuditScope scope =
        new AuditScope(
            START_MS - 1, END_MS + 1, List.of(), List.of(), List.of(), tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // clean_shutdown without a matching start is dropped
  // -------------------------------------------------------------------------

  @Test
  void shutdownWithoutMatchingStart_dropped() throws IOException {
    String stopLine =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"b\","
            + "\"collector_session_id\":\"orphan\",\"planned\":true}";

    writeJournal("collector-a", stopLine);

    AuditScope scope =
        new AuditScope(
            START_MS - 1, END_MS + 1, List.of(), List.of(), List.of(), tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // name() label
  // -------------------------------------------------------------------------

  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new LedgerGapSource(tmpDir, mapper).name()).isEqualTo("LedgerGapSource");
  }

  // -------------------------------------------------------------------------
  // Fan-out: one journal event + scoped symbols/streams → one record per tuple
  // -------------------------------------------------------------------------

  /**
   * Fan-out: one lifecycle entry + scope with symbol=btcusdt and streams=[depth, trades] → 2
   * GapRecords, one per (symbol, stream) tuple, each with populated exchange/symbol/stream and
   * detail containing "fanout=true".
   */
  @Test
  void fanOutWithMultipleStreams_emitsOneRecordPerTuple() throws IOException {
    String startLine =
        "{\"ts_ns\":"
            + START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-fo\","
            + "\"collector_session_id\":\"sess-fo\"}";
    String stopLine =
        "{\"ts_ns\":"
            + END_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-fo\","
            + "\"collector_session_id\":\"sess-fo\",\"planned\":true,\"maintenance_id\":\"maint-fo\"}";

    writeJournal("binance-collector-primary", startLine, stopLine);

    AuditScope fanOutScope =
        new AuditScope(
            START_MS - 1,
            END_MS + 1,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("depth", "trades"),
            tmpDir.toString());

    LedgerGapSource source = new LedgerGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(fanOutScope);

    assertThat(records).hasSize(2);
    assertThat(records).allMatch(r -> r.exchange().equals("binance"));
    assertThat(records).allMatch(r -> r.symbol().equals("btcusdt"));
    assertThat(records).extracting(GapRecord::stream).containsExactlyInAnyOrder("depth", "trades");
    assertThat(records).allMatch(r -> r.reason() == GapReason.COLLECTOR_RESTART);
    assertThat(records).allMatch(r -> r.detail().contains("fanout=true"));
    assertThat(records).allMatch(r -> r.startMs() == START_MS);
    assertThat(records).allMatch(r -> r.endMs() == END_MS);
  }
}
