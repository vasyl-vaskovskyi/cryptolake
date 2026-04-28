package com.cryptolake.verify.archive;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Decompresses a {@code .jsonl.zst} archive file and parses each line into a {@link JsonNode}.
 *
 * <p>Ports {@code decompress_and_parse(file_path)} from {@code verify.py:77-82} and {@code
 * _decompress_and_parse} from {@code consolidate.py} (blank-line-tolerant variant).
 *
 * <p>Tier 5 I2 — uses {@link ZstdInputStream} wrapped by {@link BufferedReader} with UTF-8.
 *
 * <p>Thread safety: stateless; each call opens its own stream.
 */
public final class DecompressAndParse {

  private DecompressAndParse() {}

  /**
   * Decompresses and parses all non-empty lines in {@code path} into a list of {@link JsonNode}s.
   *
   * @param path path to a {@code .jsonl.zst} file
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return ordered list of parsed envelopes (empty lines skipped)
   * @throws IOException on decompression or parse failure
   */
  public static List<JsonNode> parse(Path path, ObjectMapper mapper) throws IOException {
    List<JsonNode> result = new ArrayList<>();
    // Tier 5 I2: ZstdInputStream + BufferedReader(InputStreamReader(zstdIn, UTF_8))
    try (var in = Files.newInputStream(path);
        var zstdIn = new ZstdInputStream(in);
        var reader = new BufferedReader(new InputStreamReader(zstdIn, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue; // skip blank lines (matches Python's `if line:` guard)
        }
        result.add(mapper.readTree(line));
      }
    }
    return result;
  }

  /**
   * Returns an iterator over the raw UTF-8 bytes of each non-empty line in {@code path}.
   *
   * <p>Streaming mode for consolidation (Tier 5 I2); avoids loading the full file into memory.
   *
   * @param path path to a {@code .jsonl.zst} file
   * @return iterator of UTF-8-encoded line bytes
   * @throws IOException on decompression failure
   */
  public static Iterator<byte[]> streamLines(Path path) throws IOException {
    var in = Files.newInputStream(path);
    var zstdIn = new ZstdInputStream(in);
    var reader = new BufferedReader(new InputStreamReader(zstdIn, StandardCharsets.UTF_8));

    return new Iterator<>() {
      private String next = advance();

      private String advance() {
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
              return line;
            }
          }
          reader.close();
          return null;
        } catch (IOException e) {
          try {
            reader.close();
          } catch (IOException ignored) {
            // best-effort shutdown; never block main shutdown path (Tier 5 G1)
          }
          throw new java.io.UncheckedIOException(e);
        }
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public byte[] next() {
        if (next == null) {
          throw new java.util.NoSuchElementException();
        }
        byte[] bytes = next.getBytes(StandardCharsets.UTF_8);
        next = advance();
        return bytes;
      }
    };
  }
}
