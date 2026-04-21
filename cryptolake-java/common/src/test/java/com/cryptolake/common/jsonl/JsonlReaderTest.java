// ports: tests/unit/test_host_lifecycle_agent.py (ledger I/O subset — partial-line tolerance,
//        empty/missing file)
package com.cryptolake.common.jsonl;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JsonlReaderTest {

  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = EnvelopeCodec.newMapper();
  }

  @Test
  void returnsEmptyListForMissingFile(@TempDir Path tmpDir) {
    Path nonExistent = tmpDir.resolve("does_not_exist.jsonl");
    List<JsonNode> result = JsonlReader.readAll(nonExistent, mapper);
    assertThat(result).isEmpty();
  }

  @Test
  void readsValidLines(@TempDir Path tmpDir) throws IOException {
    Path file = tmpDir.resolve("test.jsonl");
    Files.writeString(file, "{\"a\":1}\n{\"b\":2}\n");

    List<JsonNode> result = JsonlReader.readAll(file, mapper);
    assertThat(result).hasSize(2);
    assertThat(result.get(0).get("a").asInt()).isEqualTo(1);
    assertThat(result.get(1).get("b").asInt()).isEqualTo(2);
  }

  @Test
  void skipsBlankLines(@TempDir Path tmpDir) throws IOException {
    Path file = tmpDir.resolve("blanks.jsonl");
    Files.writeString(file, "{\"a\":1}\n\n{\"b\":2}\n\n");

    List<JsonNode> result = JsonlReader.readAll(file, mapper);
    assertThat(result).hasSize(2);
  }

  @Test
  void skipsMalformedLines(@TempDir Path tmpDir) throws IOException {
    // partial-line tolerance — matches Python's crash-resilient behavior
    Path file = tmpDir.resolve("partial.jsonl");
    Files.writeString(file, "{\"ok\":1}\nnot-valid-json\n{\"ok\":2}\n");

    List<JsonNode> result = JsonlReader.readAll(file, mapper);
    assertThat(result).hasSize(2);
    assertThat(result.get(0).get("ok").asInt()).isEqualTo(1);
    assertThat(result.get(1).get("ok").asInt()).isEqualTo(2);
  }

  @Test
  void emptyFileReturnsEmptyList(@TempDir Path tmpDir) throws IOException {
    Path file = tmpDir.resolve("empty.jsonl");
    Files.writeString(file, "");

    List<JsonNode> result = JsonlReader.readAll(file, mapper);
    assertThat(result).isEmpty();
  }
}
