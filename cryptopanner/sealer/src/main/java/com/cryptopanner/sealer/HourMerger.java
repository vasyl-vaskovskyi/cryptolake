package com.cryptopanner.sealer;

import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.Paths;
import com.cryptopanner.common.SequenceId;
import com.cryptopanner.common.Sha256Sidecar;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads minute-segment files for one hour, validates sequence-ID continuity for ID-bearing streams,
 * optionally fills gaps via REST backfill, and produces the sealed hour file (master spec §9.b).
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
  private final ObjectMapper mapper;
  private final RestBackfiller backfiller; // nullable; null disables backfill

  public HourMerger(Path baseSegments, Path baseSealed) {
    this(baseSegments, baseSealed, EnvelopeCodec.newMapper(), null);
  }

  public HourMerger(
      Path baseSegments, Path baseSealed, ObjectMapper mapper, RestBackfiller backfiller) {
    this.baseSegments = baseSegments;
    this.baseSealed = baseSealed;
    this.mapper = mapper;
    this.backfiller = backfiller;
  }

  /**
   * {@code sequence} is null for non-ID streams; {@code gapOutcomes} is parallel to {@code
   * sequence.gaps()}; {@code backfillAttempts} is empty when no backfill ran.
   */
  public record Result(
      Path file,
      String sha256Hex,
      long fileSizeBytes,
      long recordCount,
      List<Integer> minutesPresent,
      SequenceAnalyzer.Analysis sequence,
      List<RestBackfiller.Outcome> gapOutcomes,
      List<BackfillAttempt> backfillAttempts) {}

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
    minutesInHour.sort(Comparator.comparing(p -> p.getFileName().toString()));

    ByteArrayOutputStream merged = new ByteArrayOutputStream();
    List<Integer> present = new ArrayList<>();
    for (Path p : minutesInHour) {
      byte[] compressed = Files.readAllBytes(p);
      long size = Zstd.decompressedSize(compressed);
      byte[] raw = new byte[(int) size];
      Zstd.decompress(raw, compressed);
      merged.write(raw);
      Matcher m = MINUTE_NAME.matcher(p.getFileName().toString());
      m.matches();
      present.add(Integer.parseInt(m.group(2)));
    }

    byte[] mergedBytes = merged.toByteArray();
    SequenceAnalyzer.Analysis sequence = SequenceAnalyzer.analyze(mergedBytes, stream, mapper);

    // Backfill flow — only when the stream is gap-fillable AND a backfiller is configured.
    List<RestBackfiller.Outcome> gapOutcomes = new ArrayList<>();
    List<BackfillAttempt> attempts = new ArrayList<>();
    if (sequence != null
        && !sequence.gaps().isEmpty()
        && backfiller != null
        && RestBackfiller.endpointFor(stream) != null) {
      String endpoint = RestBackfiller.endpointFor(stream);
      Map<SequenceAnalyzer.Gap, List<RecordWithId>> backfillsByGap = new HashMap<>();
      Instant fetchedAt = Instant.now();
      for (SequenceAnalyzer.Gap gap : sequence.gaps()) {
        RestBackfiller.AttemptResult r = backfiller.fetchGap(symbol, stream, gap.from(), gap.to());
        attempts.add(
            new BackfillAttempt(
                endpoint,
                gap.from(),
                gap.to(),
                1,
                r.httpStatus(),
                r.recordsInserted(),
                r.outcome(),
                r.error()));
        gapOutcomes.add(r.outcome());
        if (r.outcome() != RestBackfiller.Outcome.FAILED) {
          List<RecordWithId> wrapped = new ArrayList<>();
          for (JsonNode rec : r.records()) {
            long id = extractRestId(rec, stream);
            wrapped.add(new RecordWithId(id, backfiller.wrapRecord(endpoint, fetchedAt, rec)));
          }
          wrapped.sort(Comparator.comparingLong(RecordWithId::id));
          backfillsByGap.put(gap, wrapped);
        }
      }
      mergedBytes = splice(mergedBytes, stream, backfillsByGap);
    } else if (sequence != null) {
      for (int i = 0; i < sequence.gaps().size(); i++) {
        gapOutcomes.add(RestBackfiller.Outcome.NOT_ATTEMPTED);
      }
    }

    long records = countLines(mergedBytes);
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

    return new Result(
        out,
        Sha256Sidecar.readHash(sidecar),
        Files.size(out),
        records,
        present,
        sequence,
        gapOutcomes,
        attempts);
  }

  private record RecordWithId(long id, byte[] wrappedBytes) {}

  /**
   * Walks {@code original} line by line. Whenever the next line's ID jumps past {@code prev + 1}
   * we've crossed one of the analyzed gaps — splice the corresponding backfilled records in ID
   * order before continuing.
   */
  private byte[] splice(
      byte[] original, String stream, Map<SequenceAnalyzer.Gap, List<RecordWithId>> backfills)
      throws IOException {
    String idField = SequenceId.idField(stream);
    ByteArrayOutputStream out = new ByteArrayOutputStream(original.length);
    long prevId = -1;
    int start = 0;
    for (int i = 0; i < original.length; i++) {
      if (original[i] != '\n') continue;
      int len = i - start;
      if (len > 0) {
        String line = new String(original, start, len, StandardCharsets.UTF_8);
        JsonNode frame = CaptureEnvelope.unwrap(mapper, mapper.readTree(line));
        long id = frame.path("data").path(idField).asLong();
        if (prevId >= 0 && id > prevId + 1) {
          SequenceAnalyzer.Gap g = new SequenceAnalyzer.Gap(prevId + 1, id - 1, id - prevId - 1);
          List<RecordWithId> filled = backfills.get(g);
          if (filled != null) {
            for (RecordWithId r : filled) out.write(r.wrappedBytes());
          }
        }
        out.write(original, start, len);
        out.write('\n');
        prevId = id;
      }
      start = i + 1;
    }
    return out.toByteArray();
  }

  private static long extractRestId(JsonNode rec, String stream) {
    // REST endpoints use a different ID field than the WS frame's data.t/data.a.
    String restIdField = "trade".equals(stream) ? "id" : "a";
    JsonNode n = rec.get(restIdField);
    return n == null ? -1 : n.asLong();
  }

  private static long countLines(byte[] bytes) {
    long n = 0;
    for (byte b : bytes) if (b == '\n') n++;
    return n;
  }
}
