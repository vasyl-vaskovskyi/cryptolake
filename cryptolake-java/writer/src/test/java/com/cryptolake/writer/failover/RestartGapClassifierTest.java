package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.writer.state.MaintenanceIntent;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RestartGapClassifier}.
 *
 * <p>Ports: Python's {@code test_restart_gap_classifier.py} — all classification branches (design
 * §4.8; design §2.7).
 */
class RestartGapClassifierTest {

  private static final long NOW_NS = 2_000_000_000_000L; // some future time
  // nowNs = 2000s since epoch ≈ not actually real but works for expiry test
  private static final com.cryptolake.common.util.ClockSupplier FIXED_CLOCK = () -> NOW_NS;

  // ports: design §4.8 — boot ID change → host reboot (unplanned)
  @Test
  void classify_bootIdChanged_noIntent_hostReboot() {
    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-A", "boot-B",
        "sess-1", "sess-2",
        false, false,
        null, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("host");
    assertThat(c.cause()).isEqualTo("host_reboot");
    assertThat(c.planned()).isFalse();
    assertThat(c.classifier()).isEqualTo("writer_recovery_v1");
    assertThat(c.evidence()).contains("host_boot_id_changed");
  }

  // ports: design §4.8 — boot ID change + valid intent → planned host shutdown
  @Test
  void classify_bootIdChanged_withValidIntent_plannedHostShutdown() {
    // Create a maintenance intent that expires far in the future
    MaintenanceIntent intent = new MaintenanceIntent("m-001", "host", "operator",
        "rolling-restart", "2024-01-15T14:00:00Z", "2099-12-31T23:59:59Z", null);

    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-A", "boot-B",
        "sess-1", "sess-2",
        false, false,
        intent, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("host");
    assertThat(c.cause()).isEqualTo("operator_shutdown");
    assertThat(c.planned()).isTrue();
    assertThat(c.maintenanceId()).isEqualTo("m-001");
  }

  // ports: design §4.8 — same boot, collector clean exit → collector operator shutdown
  @Test
  void classify_sameBootId_collectorCleanExit_noIntent_collectorShutdown() {
    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-same", "boot-same",
        "sess-1", "sess-2",
        true, false,
        null, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("collector");
    assertThat(c.cause()).isEqualTo("operator_shutdown");
    assertThat(c.planned()).isFalse(); // no intent
    assertThat(c.evidence()).contains("collector_clean_exit");
  }

  // ports: design §4.8 — same boot, no clean exit, session changed → unclean collector exit
  @Test
  void classify_sameBootId_sessionChanged_noCleanExit_uncleanExit() {
    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-same", "boot-same",
        "sess-1", "sess-2",
        false, false,
        null, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("collector");
    assertThat(c.cause()).isEqualTo("unclean_exit");
    assertThat(c.planned()).isFalse();
  }

  // ports: design §4.8 — writer clean exit, valid intent → planned writer shutdown
  @Test
  void classify_writerCleanExit_validIntent_plannedWriterShutdown() {
    MaintenanceIntent intent = new MaintenanceIntent("m-002", "host", "operator",
        "writer-restart", "2024-01-15T14:00:00Z", "2099-12-31T23:59:59Z", null);

    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-same", "boot-same",
        "sess-1", "sess-1",
        false, true,
        intent, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("writer");
    assertThat(c.cause()).isEqualTo("operator_shutdown");
    assertThat(c.planned()).isTrue();
  }

  // ports: design §4.8 — no signals → unknown
  @Test
  void classify_noSignals_unknown() {
    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-same", "boot-same",
        "sess-1", "sess-1",
        false, false,
        null, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("unknown");
    assertThat(c.cause()).isEqualTo("unknown");
    assertThat(c.planned()).isFalse();
    assertThat(c.evidence()).contains("no_clear_signal");
  }

  // ports: design §4.8 phase 2 — component die event promotes classification
  @Test
  void classify_componentDieUnclean_promotesComponent() {
    // Same boot, no session info, but a redpanda die event (unclean)
    com.fasterxml.jackson.databind.node.ObjectNode dieEvent =
        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
    dieEvent.put("type", "component_die");
    dieEvent.put("component", "redpanda");
    dieEvent.put("clean_exit", false);

    HostLifecycleEvidence evidence = new HostLifecycleEvidence(java.util.List.of(dieEvent));

    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-same", "boot-same",
        "sess-1", "sess-1",
        false, false,
        null, evidence,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("redpanda");
    assertThat(c.cause()).isEqualTo("unclean_exit");
    assertThat(c.evidence()).anyMatch(e -> e.contains("component_die:redpanda"));
  }

  // ports: design §4.8 phase 2 — clean die does NOT promote
  @Test
  void classify_componentDieClean_doesNotPromote() {
    com.fasterxml.jackson.databind.node.ObjectNode dieEvent =
        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
    dieEvent.put("type", "component_die");
    dieEvent.put("component", "writer");
    dieEvent.put("clean_exit", true);

    HostLifecycleEvidence evidence = new HostLifecycleEvidence(java.util.List.of(dieEvent));

    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-same", "boot-same",
        "sess-1", "sess-1",
        true, false, // collector clean exit
        null, evidence,
        FIXED_CLOCK);

    // Clean die should not override the collector clean exit classification
    assertThat(c.component()).isNotEqualTo("writer").isEqualTo("collector");
  }

  // ports: design §4.8 — expired intent treated as no intent
  @Test
  void classify_expiredIntent_treatedAsNoIntent() {
    // Intent expired in the past (epoch 1 second is definitely before NOW_NS)
    MaintenanceIntent expiredIntent = new MaintenanceIntent("m-003", "host", "operator",
        "old-maint", "1970-01-01T00:00:00Z", "1970-01-01T00:00:01Z", null);

    RestartGapClassifier.Classification c = RestartGapClassifier.classify(
        "boot-A", "boot-B",
        "sess-1", "sess-2",
        false, false,
        expiredIntent, null,
        FIXED_CLOCK);

    assertThat(c.component()).isEqualTo("host");
    assertThat(c.cause()).isEqualTo("host_reboot");
    assertThat(c.planned()).isFalse(); // expired intent = not planned
    assertThat(c.evidence()).contains("maintenance_intent_expired");
  }
}
