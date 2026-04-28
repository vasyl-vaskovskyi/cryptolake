package com.cryptolake.consolidation.seal;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

/**
 * Packs a single stream's daily file into the tar output stream.
 *
 * <p>Ports {@code consolidate_stream_to_tar} from {@code consolidate.py}. Single-pass: reads the
 * daily file and streams bytes into the tar archive under a deterministic path.
 *
 * <p>Thread safety: stateless; operates on caller-supplied output stream.
 */
public final class ConsolidateStreamToTar {

  private static final int BUFFER_SIZE = 65_536;

  private ConsolidateStreamToTar() {}

  /**
   * Packs {@code dailyFile} into {@code tar} under the path {@code stream/date/filename}.
   *
   * @param tar open {@link TarArchiveOutputStream} to write into
   * @param stream stream name (tar path component)
   * @param date date string (tar path component)
   * @param dailyFile path to the {@code .jsonl.zst} daily file
   * @throws IOException on I/O failure
   */
  public static void pack(TarArchiveOutputStream tar, String stream, String date, Path dailyFile)
      throws IOException {

    long fileSize = Files.size(dailyFile);
    String entryName = stream + "/" + date + "/" + dailyFile.getFileName();

    TarArchiveEntry entry = new TarArchiveEntry(dailyFile.toFile(), entryName);
    entry.setSize(fileSize);
    tar.putArchiveEntry(entry);

    try (var in = new BufferedInputStream(Files.newInputStream(dailyFile), BUFFER_SIZE)) {
      byte[] buf = new byte[BUFFER_SIZE];
      int n;
      while ((n = in.read(buf)) > 0) {
        tar.write(buf, 0, n);
      }
    }

    tar.closeArchiveEntry();
  }
}
