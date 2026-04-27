package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.verify.verify.DuplicateOffsetChecker;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ports: tests/unit/cli/test_verify.py::TestDuplicateOffsets */
class DuplicateOffsetCheckerTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private ObjectNode buildEnv(String topic, int partition, long offset) {
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "data");
    node.put("_topic", topic);
    node.put("_partition", partition);
    node.put("_offset", offset);
    return node;
  }

  @Test
  void noDuplicates() {
    // ports: tests/unit/cli/test_verify.py::TestDuplicateOffsets::test_no_duplicates
    List<JsonNode> envs = List.of(buildEnv("topic.a", 0, 0L), buildEnv("topic.a", 0, 1L));
    assertThat(DuplicateOffsetChecker.check(envs)).isEmpty();
  }

  @Test
  void duplicateDetected() {
    // ports: tests/unit/cli/test_verify.py::TestDuplicateOffsets::test_duplicate
    var e1 = buildEnv("topic.a", 0, 42L);
    var e2 = buildEnv("topic.a", 0, 42L); // duplicate
    List<String> errors = DuplicateOffsetChecker.check(List.of(e1, e2));
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("Duplicate broker record");
    assertThat(errors.get(0)).contains("'topic.a'");
    assertThat(errors.get(0)).contains("42");
  }

  @Test
  void negativeOffsetSkipped() {
    // new — Tier 5 M9 sentinel skip
    var env = buildEnv("topic.a", 0, -1L);
    var env2 = buildEnv("topic.a", 0, -1L); // same negative offset — both skipped
    assertThat(DuplicateOffsetChecker.check(List.of(env, env2))).isEmpty();
  }

  @Test
  void tupleReprMatchesPython() {
    // new — gate 5 byte-identity for line 9 in §6.7 table
    var e1 = buildEnv("binance.depth", 0, 12345L);
    var e2 = buildEnv("binance.depth", 0, 12345L);
    List<String> errors = DuplicateOffsetChecker.check(List.of(e1, e2));
    assertThat(errors).hasSize(1);
    // Python tuple repr: ('binance.depth', 0, 12345)
    assertThat(errors.get(0)).isEqualTo("Duplicate broker record: ('binance.depth', 0, 12345)");
  }
}
