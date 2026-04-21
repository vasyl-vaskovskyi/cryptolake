package com.cryptolake.common.jsonl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Crash-resilient JSONL reader.
 *
 * <p>Ports Python's {@code read_jsonl}. Drops malformed/empty lines; returns an empty list if the
 * file is missing (matches Python's {@code if not path.exists(): return []}). Stateless (Tier 2
 * §12).
 */
public final class JsonlReader {

  private JsonlReader() {}

  /**
   * Reads all valid JSONL records from {@code path}, skipping blank and malformed lines.
   *
   * <p>Returns {@link Collections#emptyList()} if the file does not exist.
   *
   * @throws UncheckedIOException on I/O errors other than file-not-found
   */
  public static List<JsonNode> readAll(Path path, ObjectMapper mapper) {
    try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      List<JsonNode> result = new ArrayList<>();
      String line;
      while ((line = br.readLine()) != null) {
        line = line.strip();
        if (line.isEmpty()) continue;
        try {
          result.add(mapper.readTree(line));
        } catch (JsonProcessingException ignored) {
          // bad line — skip silently, matching Python's except clause
        }
      }
      return result;
    } catch (NoSuchFileException e) {
      return Collections.emptyList();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
