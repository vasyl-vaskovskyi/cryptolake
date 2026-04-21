package com.cryptolake.writer.recovery;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.github.luben.zstd.ZstdInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the last data envelope from a sealed {@code .jsonl.zst} file.
 *
 * <p>Ports Python's {@code WriterConsumer._get_last_envelope_from_sealed_file} (design §2.10;
 * Tier 5 I2).
 *
 * <p>Uses streaming decompression ({@link ZstdInputStream}) to avoid loading multi-GB files into
 * memory. Scans all lines and returns the last one that deserializes as a {@link DataEnvelope}
 * (skipping gap envelopes).
 *
 * <p>Thread safety: immutable after construction; called only from startup (T1).
 */
public final class LastEnvelopeReader {

  private static final Logger log = LoggerFactory.getLogger(LastEnvelopeReader.class);

  private final EnvelopeCodec codec;

  public LastEnvelopeReader(EnvelopeCodec codec) {
    this.codec = codec;
  }

  /**
   * Returns the last {@link DataEnvelope} (skipping gap envelopes) from the given sealed file.
   *
   * <p>Uses {@link ZstdInputStream} for streaming multi-frame decompression (Tier 5 I2). Returns
   * {@link Optional#empty()} if the file does not exist, is empty, or contains only gap envelopes.
   *
   * @param zstFile path to a sealed {@code .jsonl.zst} archive
   */
  public Optional<DataEnvelope> lastDataEnvelope(Path zstFile) {
    if (!Files.exists(zstFile)) {
      return Optional.empty();
    }
    DataEnvelope last = null;
    try (var fis = Files.newInputStream(zstFile);
        var zstdIn = new ZstdInputStream(fis);
        var reader =
            new BufferedReader(new InputStreamReader(zstdIn, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.strip();
        if (trimmed.isEmpty()) continue;
        // Only accept lines where "type":"data" (skip gaps)
        if (!trimmed.contains("\"type\":\"data\"")) continue;
        try {
          DataEnvelope env = codec.readData(trimmed.getBytes(StandardCharsets.UTF_8));
          last = env;
        } catch (Exception e) {
          // Malformed line — skip (not a fatal error during recovery)
          log.debug("last_envelope_reader_skip_malformed_line", "error", e.getMessage());
        }
      }
    } catch (IOException e) {
      log.warn("last_envelope_reader_io_error", "path", zstFile.toString(), "error", e.getMessage());
      return Optional.empty();
    }
    return Optional.ofNullable(last);
  }
}
