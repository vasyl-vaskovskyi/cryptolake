package com.cryptolake.writer.rotate;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Sha256;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.buffer.FileTarget;
import com.cryptolake.writer.consumer.LateArrivalSequencer;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link FileRotator}.
 *
 * <p>Ports: Python's {@code test_file_rotator.py} — verifies seal writes a data file, sidecar, and
 * increments the rotated counter; the late-arrival path generates {@code hour-H.late-N.jsonl.zst}
 * when the primary hour is already sealed (Tier 5 I6, M15).
 */
class FileRotatorTest {

  private FileRotator buildRotator(Path baseDir, BufferManager buffers) {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    return new FileRotator(
        new DurableAppender(),
        new ZstdFrameCompressor(3),
        new LateArrivalSequencer(),
        buffers,
        metrics,
        baseDir.toString());
  }

  // Tier 5 I6 — seal writes the hour file + sidecar when nothing exists yet
  @Test
  void seal_freshHour_writesDataFileAndSidecar(@TempDir Path tmp) throws Exception {
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(tmp.toString(), 100, 60, codec);

    // Buffer one envelope so seal actually writes bytes
    long tsNs = 1705329600_000_000_000L; // 2024-01-15 14:00:00 UTC
    DataEnvelope env =
        new DataEnvelope(
            1, "data", "binance", "btcusdt", "trades", tsNs, tsNs, "col_sess", 1L, "{}", "abc");
    buffers.add(env, new BrokerCoordinates("binance.trades", 0, 100L), "primary");

    FileRotator rotator = buildRotator(tmp, buffers);
    FileTarget target = new FileTarget("binance", "btcusdt", "trades", "2024-01-15", 14);
    SealResult result = rotator.seal(target);

    assertThat(result.dataPath()).exists();
    assertThat(result.sidecarPath()).exists();
    assertThat(result.dataPath().getFileName().toString()).isEqualTo("hour-14.jsonl.zst");
    // .sha256 sidecar
    assertThat(result.sidecarPath().getFileName().toString()).endsWith(".sha256");
  }

  // Tier 5 M15 — when hour file already exists, seal writes late-N variant
  @Test
  void seal_existingHour_createsLateSequenceFile(@TempDir Path tmp) throws Exception {
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(tmp.toString(), 100, 60, codec);

    // Pre-create the hour file so the rotator detects a late arrival
    Path streamDir = tmp.resolve("binance/btcusdt/trades/2024-01-15");
    Files.createDirectories(streamDir);
    Path existingHour = streamDir.resolve("hour-14.jsonl.zst");
    Files.writeString(existingHour, "dummy");

    // Buffer a new envelope that would land in hour 14
    long tsNs = 1705329600_000_000_000L;
    DataEnvelope env =
        new DataEnvelope(
            1, "data", "binance", "btcusdt", "trades", tsNs, tsNs, "col_sess", 1L, "{}", "abc");
    buffers.add(env, new BrokerCoordinates("binance.trades", 0, 100L), "primary");

    FileRotator rotator = buildRotator(tmp, buffers);
    FileTarget target = new FileTarget("binance", "btcusdt", "trades", "2024-01-15", 14);
    SealResult result = rotator.seal(target);

    // Late path: hour-14.late-1.jsonl.zst
    assertThat(result.dataPath().getFileName().toString())
        .matches("hour-14\\.late-\\d+\\.jsonl\\.zst");
    assertThat(result.dataPath()).exists();
  }

  // Design §2.4 — seal increments writer_files_rotated (counter exposed as _total in scrape)
  @Test
  void seal_incrementsFilesRotatedCounter(@TempDir Path tmp) throws Exception {
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(tmp.toString(), 100, 60, codec);

    long tsNs = 1705329600_000_000_000L;
    DataEnvelope env =
        new DataEnvelope(
            1, "data", "binance", "btcusdt", "trades", tsNs, tsNs, "col_sess", 1L, "{}", "abc");
    buffers.add(env, new BrokerCoordinates("binance.trades", 0, 100L), "primary");

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    FileRotator rotator =
        new FileRotator(
            new DurableAppender(),
            new ZstdFrameCompressor(3),
            new LateArrivalSequencer(),
            buffers,
            metrics,
            tmp.toString());

    rotator.seal(new FileTarget("binance", "btcusdt", "trades", "2024-01-15", 14));

    assertThat(registry.scrape()).contains("writer_files_rotated_total");
  }

  // Bug B fix — writeMissingSidecars creates .sha256 for archive files that lack one
  @Test
  void writeMissingSidecars_createsSidecarForUnsealedFile(@TempDir Path tmp) throws Exception {
    // Simulate the current-hour file written by periodic flushes (no seal → no sidecar yet).
    Path streamDir = tmp.resolve("binance/btcusdt/trades/2024-01-15");
    Files.createDirectories(streamDir);
    Path archiveFile = streamDir.resolve("hour-14.jsonl.zst");
    Files.writeString(archiveFile, "some compressed data"); // non-empty archive
    Path sidecarFile = streamDir.resolve("hour-14.jsonl.zst.sha256");
    assertThat(sidecarFile).doesNotExist();

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(tmp.toString(), 100, 60, codec);
    FileRotator rotator = buildRotator(tmp, buffers);

    rotator.writeMissingSidecars();

    assertThat(sidecarFile).exists();
    String content = Files.readString(sidecarFile);
    String expectedHex = Sha256.hexFile(archiveFile);
    assertThat(content).startsWith(expectedHex);
    assertThat(content).contains("  hour-14.jsonl.zst");
  }

  // Bug B fix — writeMissingSidecars skips empty archive files (they have no data to hash)
  @Test
  void writeMissingSidecars_skipsEmptyFiles(@TempDir Path tmp) throws Exception {
    Path streamDir = tmp.resolve("binance/btcusdt/trades/2024-01-15");
    Files.createDirectories(streamDir);
    Path emptyArchive = streamDir.resolve("hour-14.jsonl.zst");
    Files.writeString(emptyArchive, ""); // empty — no data written yet
    Path sidecarFile = streamDir.resolve("hour-14.jsonl.zst.sha256");

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(tmp.toString(), 100, 60, codec);
    FileRotator rotator = buildRotator(tmp, buffers);

    rotator.writeMissingSidecars();

    assertThat(sidecarFile).doesNotExist();
  }

  // Bug B fix — writeMissingSidecars skips archives that already have a sidecar
  @Test
  void writeMissingSidecars_skipsAlreadySealedFiles(@TempDir Path tmp) throws Exception {
    Path streamDir = tmp.resolve("binance/btcusdt/trades/2024-01-15");
    Files.createDirectories(streamDir);
    Path archiveFile = streamDir.resolve("hour-14.jsonl.zst");
    Files.writeString(archiveFile, "existing data");
    Path sidecarFile = streamDir.resolve("hour-14.jsonl.zst.sha256");
    Files.writeString(sidecarFile, "existing-checksum  hour-14.jsonl.zst\n");

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(tmp.toString(), 100, 60, codec);
    FileRotator rotator = buildRotator(tmp, buffers);

    rotator.writeMissingSidecars();

    // Content should remain unchanged — should not overwrite existing sidecar
    assertThat(Files.readString(sidecarFile)).isEqualTo("existing-checksum  hour-14.jsonl.zst\n");
  }
}
