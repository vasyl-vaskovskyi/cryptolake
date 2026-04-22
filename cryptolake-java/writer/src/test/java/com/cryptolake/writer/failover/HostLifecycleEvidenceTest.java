package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HostLifecycleEvidence}.
 *
 * <p>Ports: Python's {@code test_host_lifecycle_evidence.py} — indexing and lookup (design §2.7).
 */
class HostLifecycleEvidenceTest {

  private static ObjectNode dieEvent(String component, boolean cleanExit) {
    ObjectNode n = JsonNodeFactory.instance.objectNode();
    n.put("type", "component_die");
    n.put("component", component);
    n.put("clean_exit", cleanExit);
    return n;
  }

  private static ObjectNode stopEvent(String component) {
    ObjectNode n = JsonNodeFactory.instance.objectNode();
    n.put("type", "component_stop");
    n.put("component", component);
    return n;
  }

  private static ObjectNode intentEvent() {
    ObjectNode n = JsonNodeFactory.instance.objectNode();
    n.put("type", "maintenance_intent");
    return n;
  }

  // ports: design §2.7 — empty events → empty evidence
  @Test
  void empty_isEmptyTrue() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(List.of());
    assertThat(evidence.isEmpty()).isTrue();
    assertThat(evidence.hasMaintenanceIntent()).isFalse();
    assertThat(evidence.hasComponentDie("writer")).isFalse();
  }

  // ports: design §2.7 — component_die event indexed by component
  @Test
  void hasComponentDie_matchingComponent_true() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(
        List.of(dieEvent("writer", false)));

    assertThat(evidence.hasComponentDie("writer")).isTrue();
    assertThat(evidence.hasComponentDie("redpanda")).isFalse();
  }

  // ports: design §2.7 — componentCleanExit returns correct boolean
  @Test
  void componentCleanExit_cleanTrue_returnsPresent() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(
        List.of(dieEvent("writer", true)));

    Optional<Boolean> result = evidence.componentCleanExit("writer");

    assertThat(result).isPresent().hasValue(true);
  }

  // ports: design §2.7 — componentCleanExit absent for unknown component
  @Test
  void componentCleanExit_unknownComponent_returnsEmpty() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(
        List.of(dieEvent("writer", false)));

    Optional<Boolean> result = evidence.componentCleanExit("redpanda");

    assertThat(result).isEmpty();
  }

  // ports: design §2.7 — most recent die event wins (last in list)
  @Test
  void componentCleanExit_multipleEvents_mostRecentWins() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(
        List.of(dieEvent("writer", false), dieEvent("writer", true)));

    Optional<Boolean> result = evidence.componentCleanExit("writer");

    assertThat(result).isPresent().hasValue(true); // most recent is last
  }

  // ports: design §2.7 — component_stop tracked
  @Test
  void hasComponentStop_matchingComponent_true() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(
        List.of(stopEvent("postgres")));

    assertThat(evidence.hasComponentStop("postgres")).isTrue();
    assertThat(evidence.hasComponentStop("writer")).isFalse();
  }

  // ports: design §2.7 — maintenance_intent event sets flag
  @Test
  void hasMaintenanceIntent_intentEvent_true() {
    HostLifecycleEvidence evidence = new HostLifecycleEvidence(
        List.of(intentEvent()));

    assertThat(evidence.hasMaintenanceIntent()).isTrue();
  }

  // ports: design §2.7 — events without type field are skipped
  @Test
  void constructor_eventWithoutTypeField_gracefullyIgnored() {
    ObjectNode noType = JsonNodeFactory.instance.objectNode();
    noType.put("component", "writer");

    HostLifecycleEvidence evidence = new HostLifecycleEvidence(List.of(noType));

    assertThat(evidence.hasComponentDie("writer")).isFalse();
    assertThat(evidence.isEmpty()).isFalse(); // event was added to list
  }
}
