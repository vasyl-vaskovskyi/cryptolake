package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** §11.c — parsing the OpenMetrics {@code /metrics} text exposition into a queryable snapshot. */
class MetricsSnapshotTest {

  private static final String METRICS =
      """
      # HELP cryptopanner_sealed_files_pending_upload Files sealed but not yet uploaded
      # TYPE cryptopanner_sealed_files_pending_upload gauge
      cryptopanner_sealed_files_pending_upload 3
      # TYPE cryptopanner_current_connection_age_seconds gauge
      cryptopanner_current_connection_age_seconds 76800
      # TYPE cryptopanner_uploads_total counter
      cryptopanner_uploads_total 4567
      # TYPE cryptopanner_heartbeat_age_seconds gauge
      cryptopanner_heartbeat_age_seconds{slot="a"} 2.1
      cryptopanner_heartbeat_age_seconds{slot="b"} 1.4
      """;

  @Test
  void parsesUnlabeledGauges() {
    MetricsSnapshot m = MetricsSnapshot.parse(METRICS);
    assertEquals(3.0, m.gauge("cryptopanner_sealed_files_pending_upload").orElseThrow());
    assertEquals(76800.0, m.currentConnectionAgeSeconds().orElseThrow());
    assertEquals(3.0, m.sealedFilesPendingUpload().orElseThrow());
  }

  @Test
  void sumsLabeledSeriesUnderBareName() {
    MetricsSnapshot m = MetricsSnapshot.parse(METRICS);
    // two slot series 2.1 + 1.4
    assertEquals(3.5, m.gauge("cryptopanner_heartbeat_age_seconds").orElseThrow(), 1e-9);
  }

  @Test
  void skipsCommentAndBlankLines() {
    MetricsSnapshot m = MetricsSnapshot.parse(METRICS);
    assertEquals(4567.0, m.gauge("cryptopanner_uploads_total").orElseThrow());
  }

  @Test
  void absentMetricIsEmpty() {
    MetricsSnapshot m = MetricsSnapshot.parse(METRICS);
    assertTrue(m.gauge("nope_not_here").isEmpty());
  }

  @Test
  void emptyTextYieldsNoMetrics() {
    MetricsSnapshot m = MetricsSnapshot.parse("");
    assertFalse(m.sealedFilesPendingUpload().isPresent());
  }
}
