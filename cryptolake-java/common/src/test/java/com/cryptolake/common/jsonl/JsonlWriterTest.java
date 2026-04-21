// ports: tests/unit/test_host_lifecycle_agent.py (ledger I/O subset — parent-dir-create)
package com.cryptolake.common.jsonl;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JsonlWriterTest {

  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = EnvelopeCodec.newMapper();
  }

  @Test
  void appendsRecordToFile(@TempDir Path tmpDir) throws IOException {
    Path file = tmpDir.resolve("test.jsonl");
    ObjectNode record = mapper.createObjectNode();
    record.put("event", "test");
    record.put("value", 42);

    JsonlWriter.appendOne(file, record, mapper);

    List<String> lines = Files.readAllLines(file);
    assertThat(lines).hasSize(1);
    JsonNode parsed = mapper.readTree(lines.get(0));
    assertThat(parsed.get("event").asText()).isEqualTo("test");
    assertThat(parsed.get("value").asInt()).isEqualTo(42);
  }

  @Test
  void createsParentDirectories(@TempDir Path tmpDir) throws IOException {
    // ports: parent-dir-create behavior
    Path file = tmpDir.resolve("nested/deep/dir/test.jsonl");
    ObjectNode record = mapper.createObjectNode();
    record.put("x", 1);

    JsonlWriter.appendOne(file, record, mapper);

    assertThat(file).exists();
    List<String> lines = Files.readAllLines(file);
    assertThat(lines).hasSize(1);
  }

  @Test
  void appendsMultipleRecords(@TempDir Path tmpDir) throws IOException {
    Path file = tmpDir.resolve("multi.jsonl");

    for (int i = 0; i < 3; i++) {
      ObjectNode record = mapper.createObjectNode();
      record.put("i", i);
      JsonlWriter.appendOne(file, record, mapper);
    }

    List<JsonNode> records = JsonlReader.readAll(file, mapper);
    assertThat(records).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(records.get(i).get("i").asInt()).isEqualTo(i);
    }
  }

  @Test
  void endsWithNewline(@TempDir Path tmpDir) throws IOException {
    Path file = tmpDir.resolve("newline.jsonl");
    ObjectNode record = mapper.createObjectNode();
    record.put("k", "v");
    JsonlWriter.appendOne(file, record, mapper);

    byte[] bytes = Files.readAllBytes(file);
    assertThat(bytes[bytes.length - 1]).isEqualTo((byte) 0x0A);
  }
}
