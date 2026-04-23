package com.cryptolake.writer.harness;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Sha256;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.rotate.FilePaths;
import com.cryptolake.writer.rotate.Sha256Sidecar;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration harness that produces synthetic archive files for gate 5 (verify-parity check).
 *
 * <p>Produces {@code binance/btcusdt/<stream>/<YYYY-MM-DD>/hour-NN.jsonl.zst} plus matching {@code
 * .sha256} sidecar files in the directory supplied as {@code args[0]}.
 *
 * <p>No Kafka, no Postgres — purely in-memory envelope production + local FS write. Uses the same
 * pipeline as production: {@link ZstdFrameCompressor} + {@link DurableAppender} + {@link
 * Sha256Sidecar}.
 */
public final class SyntheticArchiveHarness {

  private static final String EXCHANGE = "binance";
  private static final String SYMBOL = "btcusdt";
  private static final String SESSION_ID = "harness_2026-04-21T00:00:00Z";
  private static final String TOPIC = "binance.btcusdt.trades";

  // Streams to generate archives for — must match what verify CLI expects
  private static final String[] STREAMS = {"trades", "bookticker", "depth"};

  // Number of synthetic envelopes per archive
  private static final int ENVELOPES_PER_FILE = 50;

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: SyntheticArchiveHarness <output-dir>");
      System.exit(1);
    }
    Path baseDir = Path.of(args[0]);
    // Ensure idempotency: the DurableAppender *appends* to existing files,
    // so re-running the harness against a non-empty output dir would double
    // (or triple) the envelopes and trip Python verify's duplicate-offset check.
    if (Files.exists(baseDir)) {
      try (var walk = Files.walk(baseDir)) {
        walk.sorted(java.util.Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.delete(p);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
      }
    }
    Files.createDirectories(baseDir);

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);
    DurableAppender appender = new DurableAppender();

    // Use today's UTC date
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(now);
    int hour = now.getHour();

    // Base timestamp in nanoseconds (start of this hour UTC)
    long baseNs = now.toInstant().toEpochMilli() * 1_000_000L;

    for (String stream : STREAMS) {
      List<byte[]> lines = new ArrayList<>(ENVELOPES_PER_FILE);
      for (int i = 0; i < ENVELOPES_PER_FILE; i++) {
        long receivedNs = baseNs + (long) i * 1_000_000_000L; // 1s apart
        String rawText = buildRawText(stream, i, receivedNs / 1_000_000L);
        DataEnvelope env =
            new DataEnvelope(
                1,
                "data",
                EXCHANGE,
                SYMBOL,
                stream,
                receivedNs,
                receivedNs / 1_000_000L, // exchange_ts in ms
                SESSION_ID,
                i,
                rawText,
                Sha256.hexDigestUtf8(rawText));
        BrokerCoordinates coords = new BrokerCoordinates(TOPIC + "." + stream, 0, (long) i);
        byte[] jsonBytes = codec.toJsonBytes(EnvelopeCodec.withBrokerCoordinates(env, coords));
        lines.add(codec.appendNewline(jsonBytes));
      }

      // Compress all lines as a single frame
      byte[] compressed = compressor.compressFrame(lines);

      // Build archive path
      Path archivePath =
          FilePaths.buildFilePath(baseDir.toString(), EXCHANGE, SYMBOL, stream, date, hour, null);
      appender.appendAndFsync(archivePath, compressed);

      // Write sidecar
      Path sidecarPath = FilePaths.sidecarPath(archivePath);
      Sha256Sidecar.write(archivePath, sidecarPath);

      System.out.println("SyntheticArchiveHarness: wrote " + archivePath + " + sidecar");
    }
  }

  /**
   * Builds a minimal valid raw-text JSON payload for the given stream and sequence.
   *
   * <p>The raw_text is stored verbatim and its SHA-256 is verified by the Python verify CLI. We
   * just need a valid JSON string — the actual content doesn't matter for verify.
   */
  private static String buildRawText(String stream, int seq, long ts) {
    return switch (stream) {
      case "trades" ->
          "{\"e\":\"trade\",\"E\":"
              + ts
              + ",\"s\":\"BTCUSDT\",\"t\":"
              + (1000000 + seq)
              + ",\"p\":\"65000.00\",\"q\":\"0.001\",\"b\":"
              + seq
              + ",\"a\":"
              + (seq + 1)
              + ",\"T\":"
              + ts
              + ",\"m\":false,\"M\":true}";
      case "bookticker" ->
          "{\"u\":"
              + (seq + 1)
              + ",\"s\":\"BTCUSDT\",\"b\":\"64999.99\",\"B\":\"1.000\",\"a\":\"65000.01\",\"A\":\"1.000\"}";
      case "depth" ->
          "{\"e\":\"depthUpdate\",\"E\":"
              + ts
              + ",\"s\":\"BTCUSDT\",\"U\":"
              + (seq * 2 + 1)
              + ",\"u\":"
              + (seq * 2 + 2)
              + ",\"pu\":"
              + (seq == 0 ? 0 : seq * 2 - 1)
              + ",\"b\":[[\"65000.00\",\"1.000\"]],\"a\":[[\"65001.00\",\"1.000\"]]}";
      default -> "{\"type\":\"" + stream + "\",\"seq\":" + seq + ",\"ts\":" + ts + "}";
    };
  }
}
