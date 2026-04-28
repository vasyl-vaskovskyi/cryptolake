package com.cryptolake.collector.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SnapshotScheduler} interval parsing.
 *
 * <p>Ports {@code test_snapshot.py::parse_interval_seconds} tests.
 */
class SnapshotSchedulerTest {

  @Test
  // ports: tests/unit/collector/test_snapshot.py::test_interval_parses_minutes
  void intervalParsesMinutesAndSeconds() {
    assertThat(SnapshotScheduler.parseIntervalSeconds("5m")).isEqualTo(300);
    assertThat(SnapshotScheduler.parseIntervalSeconds("1m")).isEqualTo(60);
    assertThat(SnapshotScheduler.parseIntervalSeconds("30s")).isEqualTo(30);
    assertThat(SnapshotScheduler.parseIntervalSeconds("10s")).isEqualTo(10);
  }

  @Test
  void invalidIntervalThrows() {
    assertThatThrownBy(() -> SnapshotScheduler.parseIntervalSeconds("5h"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
