package com.cryptopanner.sealer;

import com.cryptopanner.common.Paths;
import com.cryptopanner.common.Sha256Sidecar;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads minute-segment files for one hour and produces one sealed hour file. Skeleton-only: no
 * backfill, no sequence-ID validation, no event arrays.
 */
public final class HourMerger {

  private static final Pattern MINUTE_NAME =
      Pattern.compile("minute-(\\d{2})-(\\d{2})\\.jsonl\\.zst");
  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOUR =
      DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC);

  private final Path baseSegments;
  private final Path baseSealed;

  public HourMerger(Path baseSegments, Path baseSealed) {
    this.baseSegments = baseSegments;
    this.baseSealed = baseSealed;
  }

  /** Result of one merge, used by the manifest writer. */
  public record Result(
      Path file,
      String sha256Hex,
      long fileSizeBytes,
      long recordCount,
      List<Integer> minutesPresent) {}

  public Result mergeHour(String symbol, String stream, Instant hourStart) throws IOException {
    String date = DATE.format(hourStart);
    String hour = HOUR.format(hourStart);
    Path segDir = baseSegments.resolve(symbol).resolve(stream).resolve(date);
    if (!Files.isDirectory(segDir)) {
      throw new IOException("no segments directory: " + segDir);
    }
    List<Path> minutesInHour = new ArrayList<>();
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(segDir, "minute-*.jsonl.zst")) {
      for (Path p : ds) {
        Matcher m = MINUTE_NAME.matcher(p.getFileName().toString());
        if (m.matches() && m.group(1).equals(hour)) {
          minutesInHour.add(p);
        }
      }
    }
    minutesInHour.sort((a, b) -> a.getFileName().compareTo(b.getFileName()));

    ByteArrayOutputStream merged = new ByteArrayOutputStream();
    List<Integer> present = new ArrayList<>();
    long records = 0;
    for (Path p : minutesInHour) {
      byte[] compressed = Files.readAllBytes(p);
      long size = Zstd.decompressedSize(compressed);
      byte[] raw = new byte[(int) size];
      Zstd.decompress(raw, compressed);
      merged.write(raw);
      for (byte b : raw) {
        if (b == '\n') records++;
      }
      Matcher m = MINUTE_NAME.matcher(p.getFileName().toString());
      m.matches();
      present.add(Integer.parseInt(m.group(2)));
    }

    byte[] mergedBytes = merged.toByteArray();
    byte[] compressedOut = Zstd.compress(mergedBytes, 3);

    Path out = Paths.hourSealed(baseSealed, symbol, stream, hourStart);
    Files.createDirectories(out.getParent());
    Path tmp = out.resolveSibling(out.getFileName() + ".tmp");
    try (FileChannel ch =
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(ByteBuffer.wrap(compressedOut));
      ch.force(true);
    }
    Files.move(tmp, out, StandardCopyOption.ATOMIC_MOVE);

    Path sidecar = out.resolveSibling(out.getFileName() + ".sha256");
    Sha256Sidecar.computeAndWrite(out, sidecar);

    return new Result(out, Sha256Sidecar.readHash(sidecar), Files.size(out), records, present);
  }
}
