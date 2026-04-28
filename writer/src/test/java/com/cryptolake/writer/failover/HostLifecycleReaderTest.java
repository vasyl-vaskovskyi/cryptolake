package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link HostLifecycleReader}.
 *
 * <p>Ports: Python's {@code test_host_lifecycle_reader.py} — window filtering, timestamp parsing
 * (Tier 5 F2; design §4.7).
 */
class HostLifecycleReaderTest {

  @TempDir Path tmp;
  private final ObjectMapper mapper = new ObjectMapper();

  // ports: Tier 5 F2 — missing ledger file returns empty evidence
  @Test
  void load_missingFile_returnsEmpty() {
    Path missing = tmp.resolve("nonexistent.jsonl");

    HostLifecycleEvidence evidence = HostLifecycleReader.load(missing, null, null, mapper);

    assertThat(evidence.isEmpty()).isTrue();
  }

  // ports: design §4.7 — events within window are returned
  @Test
  void load_eventsInWindow_returned() throws IOException {
    Path ledger = tmp.resolve("events.jsonl");
    // Event at 2024-01-15T14:00:00Z
    Files.writeString(
        ledger,
        "{\"type\":\"component_die\",\"component\":\"writer\",\"clean_exit\":false,\"timestamp\":\"2024-01-15T14:00:00Z\"}\n");

    HostLifecycleEvidence evidence =
        HostLifecycleReader.load(ledger, "2024-01-15T13:00:00Z", "2024-01-15T15:00:00Z", mapper);

    assertThat(evidence.hasComponentDie("writer")).isTrue();
  }

  // ports: design §4.7 — events before window are excluded
  @Test
  void load_eventsBeforeWindow_excluded() throws IOException {
    Path ledger = tmp.resolve("events.jsonl");
    Files.writeString(
        ledger,
        "{\"type\":\"component_die\",\"component\":\"writer\",\"timestamp\":\"2024-01-15T10:00:00Z\"}\n");

    HostLifecycleEvidence evidence =
        HostLifecycleReader.load(ledger, "2024-01-15T13:00:00Z", "2024-01-15T15:00:00Z", mapper);

    assertThat(evidence.hasComponentDie("writer")).isFalse();
  }

  // ports: design §4.7 — events after window are excluded
  @Test
  void load_eventsAfterWindow_excluded() throws IOException {
    Path ledger = tmp.resolve("events.jsonl");
    Files.writeString(
        ledger,
        "{\"type\":\"component_die\",\"component\":\"writer\",\"timestamp\":\"2024-01-15T16:00:00Z\"}\n");

    HostLifecycleEvidence evidence =
        HostLifecycleReader.load(ledger, "2024-01-15T13:00:00Z", "2024-01-15T15:00:00Z", mapper);

    assertThat(evidence.hasComponentDie("writer")).isFalse();
  }

  // ports: Tier 5 F2 — null window bounds means no filtering
  @Test
  void load_nullWindowBounds_noFiltering() throws IOException {
    Path ledger = tmp.resolve("events.jsonl");
    Files.writeString(
        ledger, "{\"type\":\"maintenance_intent\",\"timestamp\":\"2024-01-15T14:00:00Z\"}\n");

    HostLifecycleEvidence evidence = HostLifecycleReader.load(ledger, null, null, mapper);

    assertThat(evidence.hasMaintenanceIntent()).isTrue();
  }

  // ports: Tier 5 F2 — Z and +00:00 timestamps both parse
  @Test
  void parseIsoOrNull_zSuffix_parsesCorrectly() {
    Instant result = HostLifecycleReader.parseIsoOrNull("2024-01-15T14:00:00Z");
    assertThat(result).isNotNull();
  }

  // ports: Tier 5 F2 — +00:00 timezone suffix
  @Test
  void parseIsoOrNull_plusZeroZero_parsesCorrectly() {
    Instant result = HostLifecycleReader.parseIsoOrNull("2024-01-15T14:00:00+00:00");
    assertThat(result).isNotNull();
    assertThat(result).isEqualTo(HostLifecycleReader.parseIsoOrNull("2024-01-15T14:00:00Z"));
  }

  // ports: Tier 5 F2 — naive timestamp interpreted as UTC
  @Test
  void parseIsoOrNull_naiveDatetime_parsedAsUtc() {
    Instant result = HostLifecycleReader.parseIsoOrNull("2024-01-15T14:00:00");
    assertThat(result).isNotNull();
    assertThat(result).isEqualTo(HostLifecycleReader.parseIsoOrNull("2024-01-15T14:00:00Z"));
  }

  // ports: Tier 5 F2 — null or blank returns null
  @Test
  void parseIsoOrNull_nullOrBlank_returnsNull() {
    assertThat(HostLifecycleReader.parseIsoOrNull(null)).isNull();
    assertThat(HostLifecycleReader.parseIsoOrNull("")).isNull();
    assertThat(HostLifecycleReader.parseIsoOrNull("  ")).isNull();
  }

  // ports: Tier 5 F2 — malformed timestamp returns null
  @Test
  void parseIsoOrNull_malformed_returnsNull() {
    assertThat(HostLifecycleReader.parseIsoOrNull("not-a-date")).isNull();
  }

  // ports: design §4.7 — events without timestamp field pass through
  @Test
  void load_eventWithoutTimestamp_passesThrough() throws IOException {
    Path ledger = tmp.resolve("events.jsonl");
    Files.writeString(ledger, "{\"type\":\"maintenance_intent\"}\n"); // no timestamp field

    HostLifecycleEvidence evidence =
        HostLifecycleReader.load(ledger, "2024-01-15T13:00:00Z", "2024-01-15T15:00:00Z", mapper);

    assertThat(evidence.hasMaintenanceIntent()).isTrue();
  }
}
