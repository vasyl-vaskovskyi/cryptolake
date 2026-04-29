package com.cryptolake.writer.rotate;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Sha256;
import com.cryptolake.writer.buffer.BufferManager;
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
 * Unit tests for startup sidecar repair via {@link FileRotator#writeMissingSidecars()}.
 *
 * <p>Verifies Bug 2 fix: when the writer starts after a crash, any {@code *.jsonl.zst} that exists
 * on disk without its {@code .sha256} sidecar must have the sidecar written — with the correct
 * SHA-256 hash — before the consume loop begins (Tier 1 sidecar invariant; design §3.4).
 */
class StartupSidecarRepairTest {

  private FileRotator buildRotator(Path baseDir) {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager(baseDir.toString(), 100, 60, codec);
    return new FileRotator(
        new DurableAppender(),
        new ZstdFrameCompressor(3),
        new LateArrivalSequencer(),
        buffers,
        metrics,
        baseDir.toString());
  }

  /**
   * Scenario: writer crashed after writing the archive file but before writing the sidecar. On the
   * NEXT writer startup, {@code writeMissingSidecars()} must produce the correct SHA-256 sidecar.
   */
  @Test
  void writeMissingSidecars_archiveExistsNoSidecar_sidecarWrittenWithCorrectHash(@TempDir Path tmp)
      throws Exception {
    // Simulate post-crash state: archive exists, sidecar missing
    Path streamDir = tmp.resolve("binance/btcusdt/depth/2026-04-29");
    Files.createDirectories(streamDir);
    Path archiveFile = streamDir.resolve("hour-16.jsonl.zst");
    byte[] content = "some compressed zstd data representing depth records".getBytes();
    Files.write(archiveFile, content);

    Path sidecarFile = streamDir.resolve("hour-16.jsonl.zst.sha256");
    assertThat(sidecarFile).doesNotExist(); // precondition: no sidecar

    // Simulate startup: construct FileRotator and call writeMissingSidecars()
    FileRotator rotator = buildRotator(tmp);
    rotator.writeMissingSidecars(); // this is the startup repair call

    // Sidecar must now exist
    assertThat(sidecarFile).exists();

    // SHA-256 in sidecar must match the actual file
    String sidecarContent = Files.readString(sidecarFile);
    String expectedHex = Sha256.hexFile(archiveFile);
    assertThat(sidecarContent).startsWith(expectedHex);
    assertThat(sidecarContent).contains("  hour-16.jsonl.zst");
    assertThat(sidecarContent).endsWith("\n");
  }

  /**
   * Scenario: multiple streams with missing sidecars — all must be repaired in one startup scan.
   */
  @Test
  void writeMissingSidecars_multipleStreamsMissingSidecars_allRepaired(@TempDir Path tmp)
      throws Exception {
    String[] streams = {"depth", "depth_snapshot", "open_interest"};
    Path[] archiveFiles = new Path[streams.length];

    for (int i = 0; i < streams.length; i++) {
      Path dir = tmp.resolve("binance/btcusdt/" + streams[i] + "/2026-04-29");
      Files.createDirectories(dir);
      archiveFiles[i] = dir.resolve("hour-16.jsonl.zst");
      Files.writeString(archiveFiles[i], "data for " + streams[i]);
    }

    FileRotator rotator = buildRotator(tmp);
    rotator.writeMissingSidecars();

    for (int i = 0; i < streams.length; i++) {
      Path sidecar = archiveFiles[i].resolveSibling("hour-16.jsonl.zst.sha256");
      assertThat(sidecar).as("sidecar for stream %s", streams[i]).exists();

      String hex = Sha256.hexFile(archiveFiles[i]);
      assertThat(Files.readString(sidecar)).startsWith(hex);
    }
  }

  /** Scenario: sidecar already exists (clean shutdown path). Must not be overwritten. */
  @Test
  void writeMissingSidecars_sidecarAlreadyExists_notOverwritten(@TempDir Path tmp)
      throws Exception {
    Path dir = tmp.resolve("binance/btcusdt/depth_snapshot/2026-04-29");
    Files.createDirectories(dir);
    Path archiveFile = dir.resolve("hour-16.jsonl.zst");
    Files.writeString(archiveFile, "existing archive data");
    Path sidecarFile = dir.resolve("hour-16.jsonl.zst.sha256");
    String existingContent =
        "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
            + "  hour-16.jsonl.zst\n";
    Files.writeString(sidecarFile, existingContent);

    FileRotator rotator = buildRotator(tmp);
    rotator.writeMissingSidecars();

    // Content unchanged — must not overwrite an existing sidecar
    assertThat(Files.readString(sidecarFile)).isEqualTo(existingContent);
  }

  /** Scenario: empty archive file (writer started but wrote nothing). Must not get a sidecar. */
  @Test
  void writeMissingSidecars_emptyArchive_noSidecarWritten(@TempDir Path tmp) throws Exception {
    Path dir = tmp.resolve("binance/btcusdt/open_interest/2026-04-29");
    Files.createDirectories(dir);
    Path archiveFile = dir.resolve("hour-16.jsonl.zst");
    Files.writeString(archiveFile, ""); // zero bytes — nothing written yet

    FileRotator rotator = buildRotator(tmp);
    rotator.writeMissingSidecars();

    assertThat(archiveFile.resolveSibling("hour-16.jsonl.zst.sha256")).doesNotExist();
  }
}
