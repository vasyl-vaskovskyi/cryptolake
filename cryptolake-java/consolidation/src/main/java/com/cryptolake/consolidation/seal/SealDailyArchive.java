package com.cryptolake.consolidation.seal;

import com.cryptolake.common.util.Sha256;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

/**
 * Packs all per-stream daily files for a date into a single {@code .tar.zst} archive.
 *
 * <p>Ports {@code seal_daily_archive} from {@code consolidate.py}. Writes a SHA-256 sidecar for the
 * sealed archive.
 *
 * <p>Uses {@code commons-compress} for tar creation (design §5; Q6).
 *
 * <p>Thread safety: stateless per call.
 */
public final class SealDailyArchive {

  private SealDailyArchive() {}

  /**
   * Seals all stream daily files for {@code (exchange, symbol, date)} into a {@code .tar.zst}.
   *
   * @param baseDir archive base directory
   * @param exchange exchange name
   * @param symbol symbol name
   * @param date date (YYYY-MM-DD)
   * @throws IOException on I/O failure
   */
  public static void seal(Path baseDir, String exchange, String symbol, String date)
      throws IOException {
    Path symbolDir = baseDir.resolve(exchange).resolve(symbol);
    if (!Files.exists(symbolDir)) {
      return;
    }

    Path tarPath = symbolDir.resolve(date + ".tar.zst");
    try (OutputStream fileOut = Files.newOutputStream(tarPath);
        ZstdCompressorOutputStream zstdOut = new ZstdCompressorOutputStream(fileOut, 3);
        TarArchiveOutputStream tar = new TarArchiveOutputStream(zstdOut)) {

      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

      // Discover all stream directories with a daily file for this date
      List<Path> streamDirs = new ArrayList<>();
      try (var stream = Files.list(symbolDir)) {
        stream.filter(Files::isDirectory).forEach(streamDirs::add);
      }
      streamDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

      for (Path streamDir : streamDirs) {
        Path dateDir = streamDir.resolve(date);
        Path dailyFile = dateDir.resolve(date + ".jsonl.zst");
        if (!Files.exists(dailyFile)) {
          continue;
        }
        ConsolidateStreamToTar.pack(tar, streamDir.getFileName().toString(), date, dailyFile);
      }

      tar.finish();
    }

    // Write SHA-256 sidecar for the tar
    Path sidecarPath = symbolDir.resolve(date + ".tar.zst.sha256");
    String hex = Sha256.hexFile(tarPath);
    Files.writeString(
        sidecarPath, hex + "  " + tarPath.getFileName() + "\n", StandardCharsets.UTF_8);
  }
}
