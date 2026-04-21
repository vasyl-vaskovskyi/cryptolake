package com.cryptolake.common.jsonl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * JSONL append writer — one record per syscall.
 *
 * <p>Ports Python's {@code append_jsonl}. Creates parent directories if missing. Does NOT fsync —
 * this helper is used for the lifecycle ledger; Python doesn't fsync the ledger either. Stateless
 * (Tier 2 §12).
 */
public final class JsonlWriter {

  private JsonlWriter() {}

  /**
   * Appends {@code record} as a compact JSON line (+ newline) to {@code path}.
   *
   * <p>Parent directories are created if they do not exist. Uses a single {@link FileChannel#write}
   * syscall per record to minimise the partial-write window.
   *
   * @throws UncheckedIOException on I/O failure
   */
  public static void appendOne(Path path, JsonNode record, ObjectMapper mapper) {
    try {
      Files.createDirectories(path.getParent());
      byte[] jsonBytes = mapper.writeValueAsBytes(record);
      byte[] line = new byte[jsonBytes.length + 1];
      System.arraycopy(jsonBytes, 0, line, 0, jsonBytes.length);
      line[jsonBytes.length] = 0x0A; // newline (Tier 5 B2)

      try (FileChannel fc =
          FileChannel.open(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
        fc.write(ByteBuffer.wrap(line));
        // No fsync — lifecycle ledger, matches Python (design §2.4)
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
