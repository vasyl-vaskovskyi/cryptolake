package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link KafkaOutageGapSource}.
 *
 * <p>Writes kafka_outage.json fixture files into a temp directory that mirrors the {@code
 * ${dataDir}/cryptolake/<collectorId>/kafka_outage.json} layout.
 */
class KafkaOutageGapSourceTest {

  @TempDir Path tmpDir;

  private final ObjectMapper mapper = new ObjectMapper();

  // 2026-05-11T09:00:00Z in millis and nanos
  private static final long OUTAGE_START_MS = 1778490000_000L;
  private static final long OUTAGE_START_NS = OUTAGE_START_MS * 1_000_000L;

  // "now" for the injectable supplier — 2026-05-11T11:00:00Z
  private static final Instant NOW = Instant.parse("2026-05-11T11:00:00Z");

  /** Writes a kafka_outage.json under a named collector subdirectory. */
  private void writeOutageFile(String collectorId, long tsNs) throws IOException {
    Path dir = tmpDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    Files.writeString(
        dir.resolve("kafka_outage.json"),
        "{\"outage_started_at_ns\":" + tsNs + "}",
        StandardCharsets.UTF_8);
  }

  // -------------------------------------------------------------------------
  // Happy path: outage in scope emits one record
  // -------------------------------------------------------------------------

  @Test
  void outageInScope_emitsOneRecord() throws IOException {
    writeOutageFile("binance-collector-primary", OUTAGE_START_NS);

    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS - 1,
            NOW.toEpochMilli() + 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("kafka_outage");
    assertThat(r.exchange()).isEqualTo("");
    assertThat(r.symbol()).isEqualTo("");
    assertThat(r.stream()).isEqualTo("");
    assertThat(r.startMs()).isEqualTo(OUTAGE_START_MS);
    assertThat(r.endMs()).isEqualTo(NOW.toEpochMilli());
    assertThat(r.reason()).isEqualTo("kafka_producer_outage");
    assertThat(r.detail())
        .isEqualTo(
            "collector_id=binance-collector-primary; outage_started_at_ns=" + OUTAGE_START_NS);
  }

  // -------------------------------------------------------------------------
  // Outage started after scope end → not emitted
  // -------------------------------------------------------------------------

  /**
   * Active outage whose "now" extends past scope.endMs — emitted record's endMs must be clamped to
   * scope.endMs so downstream filters never see out-of-window records.
   */
  @Test
  void outageActiveAfterScopeEnd_endMsClampedToScopeEnd() throws IOException {
    writeOutageFile("binance-collector-primary", OUTAGE_START_NS);
    long scopeEndMs = NOW.toEpochMilli() - 60_000L; // scope ends 1 minute before NOW

    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS, scopeEndMs, List.of(), List.of(), List.of(), tmpDir.toString());

    List<GapRecord> records = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW).read(scope);

    assertThat(records).hasSize(1);
    assertThat(records.get(0).endMs()).isEqualTo(scopeEndMs);
  }

  @Test
  void outageAfterScopeEnd_notEmitted() throws IOException {
    long futureNs = NOW.toEpochMilli() * 1_000_000L + 3_600_000_000_000L; // 1 hour after now
    writeOutageFile("binance-collector-primary", futureNs);

    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS,
            NOW.toEpochMilli(),
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // No kafka_outage.json file → empty list
  // -------------------------------------------------------------------------

  @Test
  void missingOutageFile_returnsEmpty() throws IOException {
    // Create directory but no kafka_outage.json
    Files.createDirectories(tmpDir.resolve("cryptolake").resolve("binance-collector-primary"));

    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS,
            NOW.toEpochMilli(),
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Missing cryptolake dir → empty list
  // -------------------------------------------------------------------------

  @Test
  void missingCryptolakeDir_returnsEmpty() {
    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS,
            NOW.toEpochMilli(),
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Unparseable JSON → silently skipped
  // -------------------------------------------------------------------------

  @Test
  void unparseableOutageFile_silentlySkipped() throws IOException {
    Path dir = tmpDir.resolve("cryptolake").resolve("binance-collector-primary");
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("kafka_outage.json"), "NOT JSON !!!", StandardCharsets.UTF_8);

    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS,
            NOW.toEpochMilli(),
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Multiple collectors with active outages both emit records
  // -------------------------------------------------------------------------

  @Test
  void multipleCollectors_bothEmitRecords() throws IOException {
    writeOutageFile("collector-primary", OUTAGE_START_NS);
    writeOutageFile("collector-backup", OUTAGE_START_NS + 1_000_000_000L); // 1 ms later

    AuditScope scope =
        new AuditScope(
            OUTAGE_START_MS - 1,
            NOW.toEpochMilli() + 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(2);
    assertThat(records).allMatch(r -> r.reason().equals("kafka_producer_outage"));
    assertThat(records).allMatch(r -> r.source().equals("kafka_outage"));
  }

  // -------------------------------------------------------------------------
  // name() label
  // -------------------------------------------------------------------------

  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new KafkaOutageGapSource(tmpDir, mapper, () -> NOW).name())
        .isEqualTo("KafkaOutageGapSource");
  }

  // -------------------------------------------------------------------------
  // Fan-out: one outage file + scoped symbols/streams → one record per tuple
  // -------------------------------------------------------------------------

  /**
   * Fan-out: one kafka_outage.json + scope with symbol=btcusdt and streams=[depth, trades] → 2
   * GapRecords, one per (symbol, stream) tuple, each with populated exchange/symbol/stream and
   * detail containing "fanout=true".
   */
  @Test
  void fanOutWithMultipleStreams_emitsOneRecordPerTuple() throws IOException {
    writeOutageFile("binance-collector-primary", OUTAGE_START_NS);

    AuditScope fanOutScope =
        new AuditScope(
            OUTAGE_START_MS - 1,
            NOW.toEpochMilli() + 1,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("depth", "trades"),
            tmpDir.toString());

    KafkaOutageGapSource source = new KafkaOutageGapSource(tmpDir, mapper, () -> NOW);
    List<GapRecord> records = source.read(fanOutScope);

    assertThat(records).hasSize(2);
    assertThat(records).allMatch(r -> r.exchange().equals("binance"));
    assertThat(records).allMatch(r -> r.symbol().equals("btcusdt"));
    assertThat(records).extracting(GapRecord::stream).containsExactlyInAnyOrder("depth", "trades");
    assertThat(records).allMatch(r -> r.reason().equals("kafka_producer_outage"));
    assertThat(records).allMatch(r -> r.detail().contains("fanout=true"));
    assertThat(records).allMatch(r -> r.startMs() == OUTAGE_START_MS);
    assertThat(records).allMatch(r -> r.endMs() == NOW.toEpochMilli());
  }
}
