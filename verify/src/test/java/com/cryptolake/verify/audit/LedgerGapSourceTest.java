package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

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
    assertThat(r.reason()).isEqualTo("collector_restart");
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
    assertThat(r.reason()).isEqualTo("restart_gap");
    assertThat(r.detail()).contains("maintenance_id=-");
  }

  // -------------------------------------------------------------------------
  // Shutdown outside scope window is not emitted
  // -------------------------------------------------------------------------

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
        .containsExactlyInAnyOrder("collector_restart", "restart_gap");
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
}
